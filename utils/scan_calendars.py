import asyncio
import csv
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

INPUT_CSV = Path("companies.csv")
OUTPUT_CSV = Path("calendar_candidates.csv")

# Pages we’ll quickly probe on each site
CANDIDATE_PATHS = [
    "",  # homepage
    "/contact",
    "/contact-us",
    "/contactus",
    "/owners",
    "/owner",
    "/owner-services",
    "/property-management",
    "/property-management/",
    "/management",
    "/schedule",
    "/book",
    "/book-now",
    "/demo",
    "/consult",
    "/consultation",
]

# External schedulers (very strong signal)
SCHEDULER_HOST_PATTERNS = [
    "calendly.com",
    "hubspot.com/meetings",
    "acuityscheduling.com",
    "acuityscheduling.com",
    "youcanbook.me",
    "oncehub.com",
    "tidycal.com",
    "vcita.com",
    "squareup.com/appointments",
    "zoho.com/bookings",
    "microsoft.com/booking",
    "outlook.office365.com/book",
    "meetings.hubspot.com",
]

# URL substrings that often mean “this page is a scheduler”
SCHEDULER_PATH_KEYWORDS = [
    "schedule",
    "book",
    "booking",
    "appointment",
    "appointments",
    "consult",
    "consultation",
    "demo",
    "strategy-call",
]

# Text patterns that strongly suggest a booking CTA
TEXT_PATTERNS = [
    r"schedule (a )?(call|meeting|demo)",
    r"book (a )?(call|meeting|demo)",
    r"schedule your (call|demo|consultation)",
    r"book your (call|demo|consultation)",
    r"schedule an? (appointment|intro)",
    r"owner (intro|consultation|call)",
]


@dataclass
class ScanResult:
    company_name: str
    website: str
    calendar_confidence: int  # 0–100
    evidence_url: Optional[str]
    evidence_type: Optional[str]   # "external_scheduler" | "internal_link" | "text"
    evidence_detail: Optional[str] # e.g. matched pattern or href snippet


async def fetch(session: aiohttp.ClientSession, url: str) -> Tuple[str, Optional[str]]:
    """Fetch a URL and return (url, html or None)."""
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status >= 400:
                return url, None
            text = await resp.text(errors="ignore")
            return url, text
    except Exception:
        return url, None


def normalize_base_url(raw: str) -> str:
    """Ensure we have a proper base URL with scheme."""
    raw = raw.strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if not parsed.scheme:
        # assume https if no scheme
        return "https://" + raw
    return raw


def analyze_page(html: str, page_url: str) -> Tuple[int, Optional[str], Optional[str], Optional[str]]:
    """
    Analyze a single HTML page for calendar signals.
    Returns (score, evidence_url, evidence_type, evidence_detail).
    """
    soup = BeautifulSoup(html, "html.parser")

    best_score = 0
    best_evidence_url = None
    best_type = None
    best_detail = None

    def update(score: int, evidence_url: str, typ: str, detail: str):
        nonlocal best_score, best_evidence_url, best_type, best_detail
        if score > best_score:
            best_score = score
            best_evidence_url = evidence_url
            best_type = typ
            best_detail = detail

    # Check all links, iframes, and scripts
    for tag in soup.find_all(["a", "iframe", "script"]):
        href = tag.get("href") or tag.get("src")
        if not href:
            continue
        full_url = urljoin(page_url, href)
        href_lower = full_url.lower()

        # External scheduler domains
        for pattern in SCHEDULER_HOST_PATTERNS:
            if pattern in href_lower:
                update(
                    100,
                    full_url,
                    "external_scheduler",
                    f"matched scheduler host pattern: {pattern}",
                )

        # Internal path patterns
        parsed = urlparse(full_url)
        path = (parsed.path or "").lower()
        for kw in SCHEDULER_PATH_KEYWORDS:
            if kw in path:
                update(
                    max(best_score, 60),
                    full_url,
                    "internal_link",
                    f"matched path keyword: {kw}",
                )

        # Also look at anchor text itself
        anchor_text = (tag.get_text() or "").strip().lower()
        if "schedule" in anchor_text or "book" in anchor_text:
            update(
                max(best_score, 50),
                full_url,
                "text",
                f"anchor text: {anchor_text[:80]}",
            )

    # Check visible text patterns (for strong CTAs)
    full_text = soup.get_text(separator=" ", strip=True)
    full_text_lower = full_text.lower()
    for pattern in TEXT_PATTERNS:
        if re.search(pattern, full_text_lower):
            update(
                max(best_score, 70),
                page_url,
                "text",
                f"matched text pattern: {pattern}",
            )

    return best_score, best_evidence_url, best_type, best_detail


async def scan_site(session: aiohttp.ClientSession, company: dict) -> ScanResult:
    base_url = normalize_base_url(company.get("website", ""))
    company_name = company.get("company_name", "")

    if not base_url:
        return ScanResult(company_name, "", 0, None, None, "no_website")

    best_score = 0
    best_evidence_url = None
    best_type = None
    best_detail = None

    # Probe a handful of candidate paths sequentially (fast enough in practice)
    for path in CANDIDATE_PATHS:
        url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
        url, html = await fetch(session, url)
        if not html:
            continue

        score, evidence_url, typ, detail = analyze_page(html, url)
        if score > best_score:
            best_score = score
            best_evidence_url = evidence_url
            best_type = typ
            best_detail = detail

        # Early exit if we already have a super strong signal
        if best_score >= 95:
            break

    return ScanResult(
        company_name=company_name,
        website=base_url,
        calendar_confidence=best_score,
        evidence_url=best_evidence_url,
        evidence_type=best_type,
        evidence_detail=best_detail,
    )


async def main():
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

    companies: List[dict] = []
    with INPUT_CSV.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            companies.append(row)

    print(f"Loaded {len(companies)} companies from {INPUT_CSV}")

    # You can tweak this threshold
    CONFIDENCE_THRESHOLD = 60

    # Concurrency control
    semaphore = asyncio.Semaphore(20)

    async def wrapped_scan(company):
        async with semaphore:
            return await scan_site(session, company)

    results: List[ScanResult] = []

    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(wrapped_scan(c)) for c in companies]
        for idx, task in enumerate(asyncio.as_completed(tasks), start=1):
            result = await task
            results.append(result)
            print(
                f"[{idx}/{len(companies)}] {result.company_name} -> "
                f"score={result.calendar_confidence} "
                f"evidence={result.evidence_type or '-'} "
                f"url={result.evidence_url or '-'}"
            )

    # Write only candidates above threshold
    fieldnames = [
        "company_name",
        "website",
        "calendar_confidence",
        "evidence_url",
        "evidence_type",
        "evidence_detail",
    ]
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            if r.calendar_confidence >= CONFIDENCE_THRESHOLD:
                writer.writerow(asdict(r))

    print(f"Done. Wrote candidates with confidence >= {CONFIDENCE_THRESHOLD} to {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
