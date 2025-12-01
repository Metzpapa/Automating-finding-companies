
#!/usr/bin/env python3
"""
The "Sniper" Phone Number Scraper.
Ruthlessly filters out noise (legal footers, directories, generic lines).
Only saves numbers with a clear identity and strategy.
"""

import asyncio
import csv
import re
import aiohttp
import phonenumbers
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Optional

# ========================================
# CONFIGURATION
# ========================================

INPUT_CSV = Path("companies.csv")
OUTPUT_CSV = Path("sniper_phone_leads.csv")

# Only scan high-probability pages
PAGES_TO_SCAN = [
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/our-team",
    "/meet-the-team",
    "/staff",
    "/owners",
    "/property-management",
    "/emergency",
]

# Blacklist keywords - if these appear near the number, DROP IT.
# This kills the Vacasa/Legal footers.
BLACKLIST_CONTEXT = [
    "llc", "license", "broker", "registered", "copyright", "reserved", 
    "fax", "facsimile", "toll free", "reservations", "bookings", "front desk"
]

# Whitelist - We ONLY keep numbers that match these categories
CATEGORIES = {
    "OPS_MAINTENANCE": {
        "keywords": ["emergency", "after hours", "urgent", "maintenance", "on-call", "housekeeping", "cleaning", "inspector", "field"],
        "strategy": "Vendor/Maintenance Script"
    },
    "DIRECT_MOBILE": {
        "keywords": ["cell", "mobile", "direct", "text", "sms", "personal"],
        "strategy": "Text First / Direct Call"
    },
    "GROWTH_OWNER": {
        "keywords": ["owner services", "property management", "list with us", "homeowner", "revenue", "partnership"],
        "strategy": "Asset Protection Script"
    },
    "NAMED_CONTACT": {
        "keywords": [], # Logic handled separately (regex for names)
        "strategy": "Name Drop Call"
    }
}

MAX_CONCURRENT_SITES = 30

# ========================================
# LOGIC
# ========================================

@dataclass
class SniperResult:
    company_name: str
    website: str
    phone_number: str
    contact_name: Optional[str]
    role_or_context: str
    category: str
    suggested_strategy: str
    confidence_score: int # 0-100

async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as response:
            if response.status == 200:
                return await response.text(errors="ignore")
    except Exception:
        pass
    return ""

def clean_text(text: str) -> str:
    return " ".join(text.split())

def extract_name_before_number(text_before: str) -> Optional[str]:
    """
    Looks for a pattern like "Call [Name] at" or just "[Name] :"
    Matches 2-3 capitalized words.
    """
    # Regex for 2-3 Capitalized words, possibly separated by a hyphen or comma
    # e.g. "Greg Ku", "Mary-Anne Smith", "John Doe"
    # We look at the last 30 chars before the number
    segment = text_before[-40:]
    
    # Strict regex: Look for Name pattern at the END of the segment (ignoring punctuation like : or -)
    match = re.search(r'([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,2})', segment)
    if match:
        name = match.group(1)
        # Filter out common false positives that look like names
        if name.lower() in ["Contact Us", "Call Us", "Toll Free", "Phone Number", "Emergency Call", "After Hours"]:
            return None
        return name
    return None

def analyze_number(number_obj, text_content: str, match_start: int, match_end: int) -> Optional[SniperResult]:
    """
    Analyzes a found number to see if it survives the filters.
    """
    formatted_number = phonenumbers.format_number(number_obj, phonenumbers.PhoneNumberFormat.NATIONAL)
    
    # 1. Get Context (50 chars before/after)
    start_index = max(0, match_start - 60)
    end_index = min(len(text_content), match_end + 60)
    
    text_before = text_content[start_index:match_start]
    text_after = text_content[match_end:end_index]
    full_context = (text_before + " " + text_after).lower()
    
    # 2. BLACKLIST CHECK (The "Vacasa Filter")
    if any(bad in full_context for bad in BLACKLIST_CONTEXT):
        return None

    # 3. CATEGORY CHECK
    category = None
    strategy = None
    confidence = 0
    
    # Check Ops/Maint
    if any(k in full_context for k in CATEGORIES["OPS_MAINTENANCE"]["keywords"]):
        category = "OPS_MAINTENANCE"
        strategy = CATEGORIES["OPS_MAINTENANCE"]["strategy"]
        confidence = 90
        
    # Check Direct/Mobile
    elif any(k in full_context for k in CATEGORIES["DIRECT_MOBILE"]["keywords"]):
        category = "DIRECT_MOBILE"
        strategy = CATEGORIES["DIRECT_MOBILE"]["strategy"]
        confidence = 95
        
    # Check Owner/Growth
    elif any(k in full_context for k in CATEGORIES["GROWTH_OWNER"]["keywords"]):
        category = "GROWTH_OWNER"
        strategy = CATEGORIES["GROWTH_OWNER"]["strategy"]
        confidence = 85

    # 4. NAME CHECK (The "Greg Ku" Factor)
    extracted_name = extract_name_before_number(text_before)
    
    if extracted_name:
        # If we found a name, it's a high-value target even if context is generic
        if not category:
            category = "NAMED_CONTACT"
            strategy = CATEGORIES["NAMED_CONTACT"]["strategy"]
            confidence = 80
        else:
            # Boost confidence if we have Name + Category
            confidence = 100
            
    # 5. FINAL DECISION
    # If it didn't match a category AND didn't have a name, we drop it.
    if not category:
        return None
        
    return SniperResult(
        company_name="", # Filled later
        website="",      # Filled later
        phone_number=formatted_number,
        contact_name=extracted_name,
        role_or_context=clean_text(text_before[-30:] + " [NUM] " + text_after[:30]),
        category=category,
        suggested_strategy=strategy,
        confidence_score=confidence
    )

async def process_company(session: aiohttp.ClientSession, company: Dict) -> List[SniperResult]:
    base_url = company.get("website", "").strip()
    company_name = company.get("company_name", "Unknown")
    
    if not base_url: return []
    if not base_url.startswith("http"): base_url = "https://" + base_url

    urls_to_scan = {base_url}
    parsed_base = urlparse(base_url)
    base_domain = f"{parsed_base.scheme}://{parsed_base.netloc}"
    
    for path in PAGES_TO_SCAN:
        urls_to_scan.add(urljoin(base_domain, path))

    found_leads = []
    seen_numbers = set()
    
    for url in urls_to_scan:
        html = await fetch_html(session, url)
        if not html: continue
        
        soup = BeautifulSoup(html, "html.parser")
        for script in soup(["script", "style", "noscript", "footer"]): # Exclude footer tag if possible
            script.decompose()
        
        text_content = soup.get_text(separator=" | ")
        
        # DENSITY CHECK: If page has > 15 numbers, it's a directory. Skip it.
        # (Unless we find a specific keyword match, but for safety we skip)
        matches = list(phonenumbers.PhoneNumberMatcher(text_content, "US"))
        if len(matches) > 15:
            continue 

        for match in matches:
            result = analyze_number(match.number, text_content, match.start, match.end)
            
            if result and result.phone_number not in seen_numbers:
                result.company_name = company_name
                result.website = base_url
                found_leads.append(result)
                seen_numbers.add(result.phone_number)
    
    return found_leads

def append_to_csv(results: List[SniperResult]):
    if not results: return
    
    fieldnames = ["confidence_score", "suggested_strategy", "category", "contact_name", "phone_number", "company_name", "role_or_context", "website"]
    file_exists = OUTPUT_CSV.exists()
    
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for res in results:
            writer.writerow(asdict(res))

async def main():
    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()

    companies = []
    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        companies = list(reader)

    print(f"Loaded {len(companies)} companies. Starting SNIPER scan...")
    print("Filtering for: Direct Mobile, Maintenance/Ops, Owner Services, and Named Contacts.")
    print("Dropping: Legal footers, Directories, General lines.")
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SITES)
    total_leads = 0
    
    async def worker(company):
        async with semaphore:
            results = await process_company(session, company)
            if results:
                append_to_csv(results)
            return len(results)

    async with aiohttp.ClientSession() as session:
        tasks = [worker(company) for company in companies]
        
        completed = 0
        for future in asyncio.as_completed(tasks):
            count = await future
            total_leads += count
            completed += 1
            if completed % 20 == 0:
                print(f"Scanned {completed}/{len(companies)} sites... Found {total_leads} HIGH QUALITY leads.")

    print("="*60)
    print(f"DONE. Scanned {len(companies)} sites.")
    print(f"Total SNIPER leads saved to {OUTPUT_CSV}: {total_leads}")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())