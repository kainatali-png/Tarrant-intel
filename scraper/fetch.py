"""
Tarrant County, TX — Motivated Seller Lead Scraper
Clerk Portal : https://tarrant.tx.publicsearch.us  (Neumo REST API)
Parcel Data  : https://www.tad.org  (PropertyData + PropertyLocation pipe-delimited TXT)
"""

import asyncio
import csv
import io
import json
import os
import re
import sys
import time
import traceback
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── optional dbfread (kept for compatibility; TAD uses TXT not DBF) ───────────
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "7"))

# Tarrant County Clerk – Neumo public records portal
CLERK_BASE     = "https://tarrant.tx.publicsearch.us"
CLERK_API      = f"{CLERK_BASE}/api/search/instruments"   # REST endpoint
CLERK_DOC_URL  = f"{CLERK_BASE}/doc"                      # {CLERK_DOC_URL}/{{id}}

# Tarrant Appraisal District bulk data (pipe-delimited, free, no login)
TAD_BASE       = "https://www.tad.org"
TAD_PROP_DATA  = f"{TAD_BASE}/data/PropertyData.txt"           # owner + mailing
TAD_PROP_LOC   = f"{TAD_BASE}/data/PropertyLocation.txt"       # site address

OUTPUT_PATHS   = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 3   # seconds
PAGE_SIZE      = 200 # Neumo API page size

# ── doc-type → (cat, cat_label) ──────────────────────────────────────────────
DOC_TYPES = {
    "LP":       ("LP",     "Lis Pendens"),
    "NOFC":     ("NOFC",   "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED","Tax Deed"),
    "JUD":      ("JUD",    "Judgment"),
    "CCJ":      ("JUD",    "Certified Judgment"),
    "DRJUD":    ("JUD",    "Domestic Judgment"),
    "LNCORPTX": ("LNTAX",  "Corp Tax Lien"),
    "LNIRS":    ("LNTAX",  "IRS Lien"),
    "LNFED":    ("LNTAX",  "Federal Tax Lien"),
    "LN":       ("LN",     "Lien"),
    "LNMECH":   ("LN",     "Mechanic Lien"),
    "LNHOA":    ("LN",     "HOA Lien"),
    "MEDLN":    ("LN",     "Medicaid Lien"),
    "PRO":      ("PRO",    "Probate"),
    "NOC":      ("NOC",    "Notice of Commencement"),
    "RELLP":    ("RELLP",  "Release Lis Pendens"),
}

# Map our codes → Neumo instrument-type codes used by tarrant.tx.publicsearch.us
# Discovered by inspecting network traffic on the portal.
# The portal uses full instrument names; we map to the closest match.
NEUMO_TYPE_MAP = {
    "LP":       ["LIS PENDENS", "LP"],
    "NOFC":     ["NOTICE OF TRUSTEE SALE", "APPOINTMENT OF SUBSTITUTE TRUSTEE",
                 "NOTICE OF FORECLOSURE", "NOFC", "NTS"],
    "TAXDEED":  ["TAX DEED", "TAXDEED", "CONSTABLE TAX DEED"],
    "JUD":      ["ABSTRACT OF JUDGMENT", "JUDGMENT", "JUD",
                 "CERTIFIED JUDGMENT", "DOMESTIC JUDGMENT", "CCJ"],
    "LNCORPTX": ["STATE TAX LIEN", "CORP TAX LIEN", "LNCORPTX"],
    "LNIRS":    ["FEDERAL TAX LIEN", "IRS LIEN", "LNIRS", "LNFED"],
    "LNFED":    ["FEDERAL TAX LIEN", "LNFED"],
    "LN":       ["MECHANIC LIEN", "HOA LIEN", "LIEN", "LN",
                 "RELEASE OF LIEN", "MEDICAID LIEN"],
    "LNMECH":   ["MECHANIC LIEN", "MECHANIC'S LIEN", "LNMECH"],
    "LNHOA":    ["HOA LIEN", "LNHOA", "HOMEOWNERS ASSOCIATION"],
    "MEDLN":    ["MEDICAID LIEN", "MEDLN"],
    "PRO":      ["PROBATE", "PRO", "AFFIDAVIT OF HEIRSHIP", "MUNIMENT OF TITLE"],
    "NOC":      ["NOTICE OF COMMENCEMENT", "NOC"],
    "RELLP":    ["RELEASE LIS PENDENS", "RELLP", "RELEASE OF LIS PENDENS"],
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def retry(fn, *args, attempts=RETRY_ATTEMPTS, delay=RETRY_DELAY, **kwargs):
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if i == attempts - 1:
                raise
            print(f"  [retry {i+1}/{attempts}] {exc}")
            time.sleep(delay)


def safe_float(val) -> Optional[float]:
    if not val:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", str(val)))
    except Exception:
        return None


def parse_date(s: str) -> Optional[str]:
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # ISO with time: 2024-03-15T00:00:00
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s.strip())
    if m:
        return m.group(1)
    return s.strip() or None


def date_range():
    end   = datetime.utcnow()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return start, end


def name_variants(full_name: str):
    """Return lookup keys: ORIGINAL, FIRST LAST, LAST FIRST, LAST, FIRST"""
    n = full_name.upper().strip()
    variants = [n]
    if "," in n:
        parts = [p.strip() for p in n.split(",", 1)]
        variants.append(f"{parts[1]} {parts[0]}")
        variants.append(n.replace(",", "").strip())
    else:
        tokens = n.split()
        if len(tokens) >= 2:
            variants.append(f"{tokens[-1]} {' '.join(tokens[:-1])}")
            variants.append(f"{tokens[-1]}, {' '.join(tokens[:-1])}")
    return list(dict.fromkeys(v for v in variants if v))

# ─────────────────────────────────────────────────────────────────────────────
# TARRANT APPRAISAL DISTRICT — Bulk TXT Data
# ─────────────────────────────────────────────────────────────────────────────

def _download_tad_file(url: str, label: str) -> Optional[str]:
    """Download a TAD pipe-delimited TXT file, return content string."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)",
        "Referer": "https://www.tad.org/resources/data-downloads",
    }
    candidate_urls = [url]

    # TAD sometimes serves from a dated subdirectory – try variants
    year = datetime.utcnow().year
    base = "https://www.tad.org/data"
    candidate_urls += [
        f"{base}/{label}.txt",
        f"{base}/{year}/{label}.txt",
        f"{base}/current/{label}.txt",
        # Legacy paths seen in older TAD pages
        f"https://www.tad.org/content/data-download/{label}.txt",
        f"https://www.tad.org/content/data-download/{label}.zip",
    ]

    for try_url in candidate_urls:
        for attempt in range(RETRY_ATTEMPTS):
            try:
                print(f"[tad] trying {try_url}")
                r = requests.get(try_url, headers=headers, timeout=120, stream=True)
                if r.status_code == 200 and len(r.content) > 5000:
                    # Handle zip
                    if try_url.endswith(".zip") or r.headers.get("content-type","").startswith("application/zip"):
                        zf = zipfile.ZipFile(io.BytesIO(r.content))
                        txt_names = [n for n in zf.namelist()
                                     if n.lower().endswith(".txt") and label.lower() in n.lower()]
                        if not txt_names:
                            txt_names = [n for n in zf.namelist() if n.lower().endswith(".txt")]
                        if txt_names:
                            content = zf.read(txt_names[0]).decode("latin-1", errors="replace")
                            print(f"[tad] downloaded {label} from zip ({len(content):,} chars)")
                            return content
                    else:
                        content = r.content.decode("latin-1", errors="replace")
                        print(f"[tad] downloaded {label} ({len(content):,} chars)")
                        return content
                break
            except Exception as exc:
                if attempt == RETRY_ATTEMPTS - 1:
                    print(f"[tad] {try_url} failed: {exc}")
                else:
                    time.sleep(RETRY_DELAY)

    return None


def _parse_tad_pipe(content: str, col_names: list[str]) -> list[dict]:
    """Parse TAD pipe-delimited file; first line is header."""
    rows = []
    if not content:
        return rows
    lines = content.splitlines()
    if not lines:
        return rows

    # Detect header line
    header_line = lines[0]
    if "|" in header_line:
        headers = [h.strip().upper() for h in header_line.split("|")]
        data_lines = lines[1:]
    else:
        # Fixed-width fallback – just skip, we rely on header
        headers = col_names
        data_lines = lines

    for line in data_lines:
        if not line.strip():
            continue
        cells = [c.strip() for c in line.split("|")]
        if len(cells) < 3:
            continue
        row = {}
        for i, h in enumerate(headers):
            row[h] = cells[i] if i < len(cells) else ""
        rows.append(row)
    return rows


def build_parcel_lookup() -> dict:
    """
    Download TAD PropertyData (owner + mailing) and PropertyLocation (site addr).
    Returns owner_name → parcel_dict lookup.
    """
    owner_map: dict = {}

    # ── PropertyLocation (site address) ──────────────────────────────────────
    loc_content = _download_tad_file(TAD_PROP_LOC, "PropertyLocation")
    loc_map: dict = {}  # account_num → {prop_address, prop_city, prop_zip}
    if loc_content:
        rows = _parse_tad_pipe(loc_content, [])
        for row in rows:
            acct = row.get("ACCOUNT_NUM") or row.get("ACCT") or row.get("PROP_ID") or ""
            if not acct:
                # Try first column as account
                vals = list(row.values())
                acct = vals[0] if vals else ""
            addr  = (row.get("SITUS_NUM","") + " " + row.get("SITUS_STREET","")).strip()
            if not addr:
                addr = row.get("SITUS_ADDR","") or row.get("SITE_ADDR","") or ""
            city  = row.get("SITUS_CITY","") or row.get("SITE_CITY","") or "Fort Worth"
            state = row.get("SITUS_STATE","") or "TX"
            zip_  = row.get("SITUS_ZIP","")  or row.get("SITE_ZIP","")  or ""
            if acct:
                loc_map[acct.strip()] = {
                    "prop_address": addr.strip(),
                    "prop_city":    city.strip() or "Fort Worth",
                    "prop_state":   state.strip() or "TX",
                    "prop_zip":     zip_.strip(),
                }
        print(f"[tad] location rows: {len(loc_map):,}")

    # ── PropertyData (owner + mailing) ───────────────────────────────────────
    prop_content = _download_tad_file(TAD_PROP_DATA, "PropertyData")
    if not prop_content:
        print("[tad] no property data – parcel enrichment disabled")
        return owner_map

    rows = _parse_tad_pipe(prop_content, [])
    for row in rows:
        try:
            owner = (row.get("OWNER_NAME","") or row.get("OWNER","") or
                     row.get("OWN1","")       or row.get("OWNERNAME","")).strip()
            if not owner:
                continue

            acct  = (row.get("ACCOUNT_NUM","") or row.get("ACCT","") or
                     row.get("PROP_ID","")).strip()

            # Mailing address
            mail_addr  = (row.get("MAIL_ADDR","") or row.get("ADDR_1","")  or
                          row.get("MAILADR1","")   or row.get("MAIL_STREET","") or "").strip()
            mail_city  = (row.get("MAIL_CITY","")  or row.get("CITY","")   or
                          row.get("MAILCITY","")).strip()
            mail_state = (row.get("MAIL_STATE","") or row.get("STATE","")  or "TX").strip()
            mail_zip   = (row.get("MAIL_ZIP","")   or row.get("ZIP","")    or
                          row.get("MAILZIP","")).strip()

            # Site address – prefer from loc_map, fall back to this file
            site = loc_map.get(acct, {})
            prop_address = site.get("prop_address","") or (
                row.get("SITE_ADDR","") or row.get("SITUS_ADDR","") or "").strip()
            prop_city    = site.get("prop_city","")    or "Fort Worth"
            prop_state   = site.get("prop_state","")   or "TX"
            prop_zip     = site.get("prop_zip","")     or ""

            parcel = {
                "prop_address": prop_address,
                "prop_city":    prop_city,
                "prop_state":   prop_state,
                "prop_zip":     prop_zip,
                "mail_address": mail_addr,
                "mail_city":    mail_city,
                "mail_state":   mail_state or "TX",
                "mail_zip":     mail_zip,
            }
            for variant in name_variants(owner):
                if variant not in owner_map:
                    owner_map[variant] = parcel
        except Exception:
            pass

    print(f"[tad] owner lookup: {len(owner_map):,} entries")
    return owner_map


def enrich_from_parcel(record: dict, owner_map: dict) -> dict:
    if not owner_map:
        return record
    for variant in name_variants(record.get("owner", "")):
        if variant in owner_map:
            p = owner_map[variant]
            record.update({k: v for k, v in p.items() if not record.get(k)})
            break
    return record

# ─────────────────────────────────────────────────────────────────────────────
# TARRANT COUNTY CLERK  — Neumo REST API
# ─────────────────────────────────────────────────────────────────────────────
# The portal at tarrant.tx.publicsearch.us runs on the Neumo platform.
# It exposes a JSON REST API used by the front-end JavaScript.
# Endpoint: GET /api/search/instruments
# Key params:
#   searchTerm  – instrument type name (e.g. "LIS PENDENS")
#   dateFrom    – MM/DD/YYYY
#   dateTo      – MM/DD/YYYY
#   recordedDateFrom / recordedDateTo
#   offset      – pagination
#   limit       – page size (max ~200)
#   department  – "RP" (Real Property)

NEUMO_HEADERS = {
    "User-Agent":  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept":      "application/json, text/plain, */*",
    "Referer":     CLERK_BASE + "/",
    "Origin":      CLERK_BASE,
}


def _neumo_search(session: requests.Session, search_term: str,
                  date_from: str, date_to: str,
                  offset: int = 0) -> dict:
    """Call the Neumo /api/search/instruments endpoint."""
    params = {
        "searchTerm":        search_term,
        "dateFrom":          date_from,
        "dateTo":            date_to,
        "recordedDateFrom":  date_from,
        "recordedDateTo":    date_to,
        "department":        "RP",
        "offset":            offset,
        "limit":             PAGE_SIZE,
        "sort":              "recordedDate",
        "order":             "desc",
    }
    url = CLERK_API
    r = session.get(url, params=params, timeout=30)
    if r.status_code == 200:
        return r.json()
    # Try alternate endpoint paths
    for alt in [f"{CLERK_BASE}/api/instruments", f"{CLERK_BASE}/api/search"]:
        try:
            r2 = session.get(alt, params=params, timeout=30)
            if r2.status_code == 200:
                return r2.json()
        except Exception:
            pass
    r.raise_for_status()
    return {}


def _parse_neumo_hit(hit: dict, code: str, cat: str, cat_label: str) -> dict:
    """Convert a Neumo API result object into our standard record dict."""
    # Neumo field names (observed from portal network traffic)
    doc_id   = str(hit.get("id","") or hit.get("documentId","") or "")
    doc_num  = (hit.get("instrumentNumber","") or hit.get("docNumber","") or
                hit.get("bookPage","") or doc_id)
    filed    = (hit.get("recordedDate","") or hit.get("filedDate","") or
                hit.get("instrumentDate","") or "")
    grantor  = ""
    grantee  = ""

    # Parties array: [{partyType: "GRANTOR", name: "..."}, ...]
    parties = hit.get("parties", []) or []
    for p in parties:
        pt = (p.get("partyType","") or "").upper()
        nm = (p.get("name","") or p.get("partyName","") or "").strip()
        if "GRANTOR" in pt and not grantor:
            grantor = nm
        elif "GRANTEE" in pt and not grantee:
            grantee = nm
    # Fallback: flat fields
    if not grantor:
        grantor = (hit.get("grantor","") or hit.get("grantor1","") or "").strip()
    if not grantee:
        grantee = (hit.get("grantee","") or hit.get("grantee1","") or "").strip()

    amount   = safe_float(hit.get("amount","") or hit.get("consideration","") or "")
    legal    = (hit.get("legalDescription","") or hit.get("legal","") or "").strip()
    # Property address may be embedded
    prop_addr = (hit.get("siteAddress","") or hit.get("propertyAddress","") or "").strip()

    clerk_url = ""
    if doc_id:
        clerk_url = f"{CLERK_BASE}/doc/{doc_id}"

    return {
        "doc_num":     str(doc_num).strip(),
        "doc_type":    code,
        "filed":       parse_date(str(filed)) or "",
        "cat":         cat,
        "cat_label":   cat_label,
        "owner":       grantor,
        "grantee":     grantee,
        "amount":      amount,
        "legal":       legal[:300] if legal else "",
        "prop_address": prop_addr,
        "prop_city":   "Fort Worth",
        "prop_state":  "TX",
        "prop_zip":    "",
        "mail_address":"",
        "mail_city":   "",
        "mail_state":  "",
        "mail_zip":    "",
        "clerk_url":   clerk_url,
        "flags":       [],
        "score":       0,
    }


def scrape_clerk_api(start_dt: datetime, end_dt: datetime) -> list[dict]:
    """
    Use Neumo REST API to pull all target instrument types for the date window.
    Falls back to Playwright if the API is unavailable.
    """
    records  = []
    date_from = start_dt.strftime("%m/%d/%Y")
    date_to   = end_dt.strftime("%m/%d/%Y")

    session = requests.Session()
    session.headers.update(NEUMO_HEADERS)

    # First: get a session cookie by visiting the portal homepage
    try:
        session.get(CLERK_BASE, timeout=15)
    except Exception:
        pass

    seen_ids: set = set()

    for code, (cat, cat_label) in DOC_TYPES.items():
        search_terms = NEUMO_TYPE_MAP.get(code, [code])
        code_records = []

        for term in search_terms:
            offset = 0
            while True:
                try:
                    data = retry(_neumo_search, session, term,
                                 date_from, date_to, offset)
                except Exception as exc:
                    print(f"[api] {code}/{term} offset={offset} error: {exc}")
                    break

                hits = []
                # Neumo returns either {hits:[...], total:N}
                # or {instruments:[...], total:N} or just a list
                if isinstance(data, list):
                    hits = data
                elif isinstance(data, dict):
                    hits = (data.get("hits") or data.get("instruments") or
                            data.get("results") or data.get("data") or [])

                if not hits:
                    break

                for h in hits:
                    doc_id = str(h.get("id","") or h.get("documentId","") or
                                 h.get("instrumentNumber","") or "")
                    if doc_id and doc_id in seen_ids:
                        continue
                    if doc_id:
                        seen_ids.add(doc_id)
                    rec = _parse_neumo_hit(h, code, cat, cat_label)
                    code_records.append(rec)

                total = (data.get("total",0) if isinstance(data,dict) else len(hits))
                offset += len(hits)
                if offset >= total or len(hits) < PAGE_SIZE:
                    break
                time.sleep(0.3)

        records.extend(code_records)
        print(f"[api] {code}: {len(code_records)} records")
        time.sleep(0.2)

    return records

# ─────────────────────────────────────────────────────────────────────────────
# PLAYWRIGHT FALLBACK  — scrapes the portal UI when API fails
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_clerk_playwright(start_dt: datetime, end_dt: datetime) -> list[dict]:
    records    = []
    date_from  = start_dt.strftime("%m/%d/%Y")
    date_to    = end_dt.strftime("%m/%d/%Y")
    seen_ids: set = set()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent=NEUMO_HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 900},
        )

        # Intercept JSON responses from the Neumo API
        api_responses: list[dict] = []

        async def on_response(resp):
            if "api" in resp.url and resp.status == 200:
                try:
                    body = await resp.json()
                    api_responses.append({"url": resp.url, "body": body})
                except Exception:
                    pass

        page = await ctx.new_page()
        page.on("response", on_response)

        print(f"[pw] loading {CLERK_BASE}")
        try:
            await page.goto(CLERK_BASE, wait_until="networkidle", timeout=60000)
        except PWTimeout:
            await page.goto(CLERK_BASE, timeout=60000)
        await page.wait_for_timeout(3000)

        # Navigate to advanced search
        try:
            adv = await page.query_selector("a:has-text('Advanced Search'), [href*='advanced']")
            if adv:
                await adv.click()
                await page.wait_for_timeout(2000)
        except Exception:
            try:
                await page.goto(f"{CLERK_BASE}/search/advanced",
                                wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(2000)
            except Exception:
                pass

        for code, (cat, cat_label) in DOC_TYPES.items():
            terms = NEUMO_TYPE_MAP.get(code, [code])
            for term in terms[:1]:  # Playwright: try first term only (slow)
                try:
                    api_responses.clear()

                    # Fill date range
                    for date_sel, val in [
                        ("input[placeholder*='From'], input[name*='from'], input[id*='from']", date_from),
                        ("input[placeholder*='To'],   input[name*='to'],   input[id*='to']",   date_to),
                    ]:
                        for sel in date_sel.split(","):
                            try:
                                el = await page.query_selector(sel.strip())
                                if el:
                                    await el.triple_click()
                                    await el.fill(val)
                                    break
                            except Exception:
                                pass

                    # Fill search term
                    for sel in ["input[placeholder*='Search'], input[name*='search']",
                                "input[type='text']"]:
                        for s in sel.split(","):
                            try:
                                el = await page.query_selector(s.strip())
                                if el:
                                    await el.triple_click()
                                    await el.fill(term)
                                    break
                            except Exception:
                                pass

                    # Submit
                    for btn in ["button:has-text('Search')", "input[type='submit']",
                                "[class*='search-btn']"]:
                        try:
                            b = await page.query_selector(btn)
                            if b:
                                await b.click()
                                break
                        except Exception:
                            pass

                    await page.wait_for_timeout(4000)

                    # Parse intercepted API responses
                    for resp_data in api_responses:
                        body = resp_data["body"]
                        hits = []
                        if isinstance(body, list):
                            hits = body
                        elif isinstance(body, dict):
                            hits = (body.get("hits") or body.get("instruments") or
                                    body.get("results") or [])
                        for h in hits:
                            doc_id = str(h.get("id","") or h.get("instrumentNumber","") or "")
                            if doc_id in seen_ids:
                                continue
                            if doc_id:
                                seen_ids.add(doc_id)
                            records.append(_parse_neumo_hit(h, code, cat, cat_label))

                    # Also try parsing the HTML table directly
                    html = await page.content()
                    soup = BeautifulSoup(html, "lxml")
                    for table in soup.find_all("table"):
                        rows = table.find_all("tr")
                        if len(rows) < 2:
                            continue
                        hdrs = [th.get_text(strip=True).lower()
                                for th in rows[0].find_all(["th","td"])]
                        for tr in rows[1:]:
                            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                            if not cells:
                                continue
                            row = dict(zip(hdrs, cells))
                            link = tr.find("a", href=True)
                            href = link["href"] if link else ""
                            clerk_url = (href if href.startswith("http")
                                        else CLERK_BASE + "/" + href.lstrip("/")) if href else ""
                            doc_num = _pick(row, ["instrument","doc","number"]) or cells[0]
                            owner   = _pick(row, ["grantor","owner","party"])
                            if not doc_num:
                                continue
                            if doc_num in seen_ids:
                                continue
                            seen_ids.add(doc_num)
                            records.append({
                                "doc_num": doc_num, "doc_type": code,
                                "filed": parse_date(_pick(row,["date","filed"])) or "",
                                "cat": cat, "cat_label": cat_label,
                                "owner": owner or "", "grantee": _pick(row,["grantee"]) or "",
                                "amount": safe_float(_pick(row,["amount"])),
                                "legal": _pick(row,["legal"]) or "",
                                "prop_address":"","prop_city":"Fort Worth",
                                "prop_state":"TX","prop_zip":"",
                                "mail_address":"","mail_city":"","mail_state":"","mail_zip":"",
                                "clerk_url": clerk_url, "flags":[], "score":0,
                            })

                    print(f"[pw] {code}/{term}: {len(records)} total so far")

                except Exception as exc:
                    print(f"[pw] {code}/{term} error: {exc}")
                    traceback.print_exc()

        await browser.close()

    return records


def _pick(row: dict, keys: list) -> str:
    for k in keys:
        for rk, rv in row.items():
            if k in rk and rv:
                return str(rv).strip()
    return ""

# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────

def score_record(rec: dict, week_ago: datetime) -> dict:
    flags  = []
    score  = 30   # base

    cat    = rec.get("cat", "")
    code   = rec.get("doc_type", "")
    amount = rec.get("amount") or 0
    filed  = rec.get("filed", "")
    owner  = rec.get("owner", "").upper()

    if cat == "LP":
        flags.append("Lis pendens");     score += 10
    if cat == "NOFC":
        flags.append("Pre-foreclosure"); score += 10
    if cat == "JUD":
        flags.append("Judgment lien");   score += 10
    if cat == "LNTAX":
        flags.append("Tax lien");        score += 10
    if code == "LNMECH":
        flags.append("Mechanic lien");   score += 10
    if code == "LNHOA":
        score += 5
    if cat == "PRO":
        flags.append("Probate / estate"); score += 10
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|\bLLP\b", owner):
        flags.append("LLC / corp owner"); score += 10

    if amount and amount > 100_000:
        score += 15;  flags.append("High-value debt")
    elif amount and amount > 50_000:
        score += 10

    if filed:
        try:
            if datetime.strptime(filed, "%Y-%m-%d") >= week_ago:
                score += 5; flags.append("New this week")
        except Exception:
            pass

    if rec.get("prop_address"):
        score += 5; flags.append("Has address")

    rec["flags"] = list(dict.fromkeys(flags))
    rec["score"] = min(score, 100)
    return rec


def apply_lp_fc_combo(records: list[dict]) -> list[dict]:
    """Grant +20 to owners who appear in both LP and NOFC."""
    owner_cats: dict = {}
    for r in records:
        o = r.get("owner", "").upper()
        if o:
            owner_cats.setdefault(o, set()).add(r.get("cat"))
    for r in records:
        o = r.get("owner", "").upper()
        if o and {"LP", "NOFC"}.issubset(owner_cats.get(o, set())):
            r["score"] = min(r["score"] + 20, 100)
            for fl in ("Lis pendens", "Pre-foreclosure"):
                if fl not in r["flags"]:
                    r["flags"].append(fl)
    return records

# ─────────────────────────────────────────────────────────────────────────────
# GHL CSV EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def export_ghl_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed",
        "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in records:
            owner = r.get("owner", "")
            if "," in owner:
                parts = [p.strip() for p in owner.split(",", 1)]
                last, first = parts[0], parts[1]
            else:
                tokens = owner.rsplit(" ", 1)
                last  = tokens[0].strip()
                first = tokens[1].strip() if len(tokens) > 1 else ""
            w.writerow({
                "First Name":            first,
                "Last Name":             last,
                "Mailing Address":       r.get("mail_address", ""),
                "Mailing City":          r.get("mail_city", ""),
                "Mailing State":         r.get("mail_state", ""),
                "Mailing Zip":           r.get("mail_zip", ""),
                "Property Address":      r.get("prop_address", ""),
                "Property City":         r.get("prop_city", ""),
                "Property State":        r.get("prop_state", ""),
                "Property Zip":          r.get("prop_zip", ""),
                "Lead Type":             r.get("cat_label", ""),
                "Document Type":         r.get("doc_type", ""),
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount/Debt Owed":      r.get("amount", ""),
                "Seller Score":          r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":               "Tarrant County Clerk",
                "Public Records URL":    r.get("clerk_url", ""),
            })
    print(f"[export] GHL CSV → {path}")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    start_dt, end_dt = date_range()
    week_ago = datetime.utcnow() - timedelta(days=7)
    print(f"[run] date range {start_dt.date()} → {end_dt.date()}")

    # ── 1. Scrape clerk via REST API ──────────────────────────────────────────
    records: list[dict] = []
    try:
        records = scrape_clerk_api(start_dt, end_dt)
        print(f"[api] total records: {len(records)}")
    except Exception as exc:
        print(f"[api] failed: {exc}")
        traceback.print_exc()

    # ── 2. Playwright fallback if API returned nothing ────────────────────────
    if not records:
        print("[run] API returned 0 – falling back to Playwright")
        try:
            records = await scrape_clerk_playwright(start_dt, end_dt)
            print(f"[pw] total records: {len(records)}")
        except Exception as exc:
            print(f"[pw] failed: {exc}")
            traceback.print_exc()

    # ── 3. Deduplicate by doc_num ─────────────────────────────────────────────
    seen: set = set()
    unique: list[dict] = []
    for r in records:
        key = r.get("doc_num","") or id(r)
        if key not in seen:
            seen.add(key)
            unique.append(r)
    records = unique
    print(f"[run] unique records after dedup: {len(records)}")

    # ── 4. Parcel enrichment (TAD) ────────────────────────────────────────────
    owner_map: dict = {}
    try:
        owner_map = build_parcel_lookup()
    except Exception as exc:
        print(f"[tad] lookup failed: {exc}")
        traceback.print_exc()

    for r in records:
        try:
            enrich_from_parcel(r, owner_map)
        except Exception:
            pass

    # ── 5. Score ──────────────────────────────────────────────────────────────
    for r in records:
        try:
            score_record(r, week_ago)
        except Exception:
            pass
    records = apply_lp_fc_combo(records)

    # ── 6. Sort by score desc ─────────────────────────────────────────────────
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    # ── 7. Build payload ──────────────────────────────────────────────────────
    with_address = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at":  datetime.utcnow().isoformat() + "Z",
        "source":      "Tarrant County Clerk",
        "date_range":  {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end":   end_dt.strftime("%Y-%m-%d"),
        },
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }

    # ── 8. Save JSON ──────────────────────────────────────────────────────────
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))
        print(f"[save] {path} ({len(records)} records)")

    # ── 9. GHL CSV ────────────────────────────────────────────────────────────
    export_ghl_csv(records, Path("data/leads_ghl.csv"))

    print(f"\n✅ Done — {len(records)} records, {with_address} with address")


if __name__ == "__main__":
    asyncio.run(main())
