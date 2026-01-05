import csv
import curses
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

INPUT_CSV = Path("all_leads.csv")
OUTPUT_DIR = Path("research_reports")

AGENT_NAME = "deep-research-pro-preview-12-2025"
CREATE_URL = "https://generativelanguage.googleapis.com/v1beta/interactions"
GET_URL = "https://generativelanguage.googleapis.com/v1beta/interactions/{id}"

DEFAULT_ROLE_GROUP = "decision_maker"
POLL_SECONDS = 10

ROLE_GROUPS = {
    "decision_maker": [
        "founder",
        "co-founder",
        "cofounder",
        "ceo",
        "chief",
        "president",
        "owner",
        "principal",
        "managing director",
        "partner",
    ],
    "operations": [
        "operations",
        "ops",
        "property manager",
        "portfolio manager",
        "regional manager",
        "general manager",
        "director of operations",
    ],
    "maintenance": [
        "maintenance",
        "facilities",
        "housekeeping",
        "turnover",
        "cleaning",
        "field operations",
    ],
    "guest_experience": [
        "guest experience",
        "customer experience",
        "hospitality",
    ],
    "revenue_marketing": [
        "revenue",
        "pricing",
        "marketing",
        "growth",
    ],
    "custom": [],
    "any": [],
}

SIZE_BANDS = {
    "small": (1, 50),
    "medium": (51, 200),
    "large": (201, None),
    "any": (None, None),
}

RESEARCH_FIELDS = [
    "research_status",
    "research_report",
    "research_interaction_id",
    "researched_at",
    "research_input_tokens",
    "research_output_tokens",
    "research_reasoning_tokens",
    "research_total_tokens",
    "research_search_queries",
    "research_token_cost_usd",
    "research_search_cost_usd",
    "research_total_cost_usd",
]

# Gemini 3 Pro Preview list rates (USD per 1M tokens)
INPUT_COST_PER_M_TOKEN = 2.00
OUTPUT_COST_PER_M_TOKEN = 12.00
# Search grounding cost (USD per query), if billed/available
SEARCH_COST_PER_QUERY = 0.014


def load_leads() -> List[Dict[str, str]]:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")
    with INPUT_CSV.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_leads(leads: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with INPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in leads:
            writer.writerow(row)


def parse_num_properties(value: str) -> Tuple[Optional[int], Optional[int]]:
    if not value:
        return None, None
    text = value.lower()
    nums = [int(n) for n in re.findall(r"\d+", text)]
    if not nums:
        return None, None
    if len(nums) >= 2:
        return nums[0], nums[1]
    num = nums[0]
    if "over" in text or "+" in text:
        return num, None
    return num, num


def range_overlaps(
    lead_range: Tuple[Optional[int], Optional[int]],
    band_range: Tuple[Optional[int], Optional[int]],
) -> bool:
    lead_min, lead_max = lead_range
    band_min, band_max = band_range
    if band_min is None and band_max is None:
        return True
    if lead_min is None and lead_max is None:
        return False
    lead_min = lead_min if lead_min is not None else 0
    lead_max = lead_max if lead_max is not None else 10**9
    band_min = band_min if band_min is not None else 0
    band_max = band_max if band_max is not None else 10**9
    return not (lead_max < band_min or lead_min > band_max)


def choose_role_group() -> str:
    print("Role groups:")
    for idx, name in enumerate(ROLE_GROUPS.keys(), start=1):
        default_marker = " (default)" if name == DEFAULT_ROLE_GROUP else ""
        print(f"  {idx}. {name}{default_marker}")
    choice = input(f"Choose role group [{DEFAULT_ROLE_GROUP}]: ").strip()
    if not choice:
        return DEFAULT_ROLE_GROUP
    if choice.isdigit():
        idx = int(choice) - 1
        keys = list(ROLE_GROUPS.keys())
        if 0 <= idx < len(keys):
            return keys[idx]
    if choice in ROLE_GROUPS:
        return choice
    print("Unrecognized role group, using default.")
    return DEFAULT_ROLE_GROUP


def choose_size_band() -> str:
    print("Company size bands (by number of properties):")
    for idx, name in enumerate(SIZE_BANDS.keys(), start=1):
        print(f"  {idx}. {name}")
    choice = input("Choose size band [any]: ").strip()
    if not choice:
        return "any"
    if choice.isdigit():
        idx = int(choice) - 1
        keys = list(SIZE_BANDS.keys())
        if 0 <= idx < len(keys):
            return keys[idx]
    if choice in SIZE_BANDS:
        return choice
    print("Unrecognized size band, using any.")
    return "any"






def matches_role(title: str, group: str, custom_keywords: Optional[List[str]]) -> bool:
    if group == "any":
        return True
    title_lower = (title or "").lower()
    keywords = custom_keywords if group == "custom" else ROLE_GROUPS.get(group, [])
    if not keywords:
        return True
    return any(k in title_lower for k in keywords)


def filter_leads(
    leads: List[Dict[str, str]],
    group: str,
    size_band: str,
    allow_unverified: bool,
    custom_keywords: Optional[List[str]] = None,
    allow_researched: bool = False,
) -> List[Dict[str, str]]:
    band_range = SIZE_BANDS[size_band]
    results = []
    for lead in leads:
        status = (lead.get("research_status") or "").lower()
        if not allow_researched and status == "completed":
            continue
        status = (lead.get("verification_status") or "").lower()
        if not allow_unverified and status not in {"valid", "ok"}:
            continue
        if not matches_role(lead.get("title", ""), group, custom_keywords):
            continue
        lead_range = parse_num_properties(lead.get("num_properties", ""))
        if not range_overlaps(lead_range, band_range):
            continue
        results.append(lead)
    return results


def score_lead(lead: Dict[str, str]) -> int:
    score = 0
    priority = lead.get("contact_priority") or ""
    if priority.isdigit():
        score += max(0, 10 - int(priority))

    title = (lead.get("title") or "").lower()
    if any(k in title for k in ["founder", "co-founder", "cofounder", "ceo", "president", "owner", "principal", "partner", "chief operating officer", "coo"]):
        score += 6
    elif any(k in title for k in ["operations", "ops", "property manager", "general manager", "regional manager", "director of operations"]):
        score += 4
    elif any(k in title for k in ["maintenance", "facilities", "housekeeping", "cleaning", "turnover"]):
        score += 2

    lead_range = parse_num_properties(lead.get("num_properties", ""))
    if range_overlaps(lead_range, (20, 150)):
        score += 6
    elif range_overlaps(lead_range, (151, 300)):
        score += 3
    elif range_overlaps(lead_range, (1, 19)):
        score += 2
    elif range_overlaps(lead_range, (301, 500)):
        score += 1
    elif range_overlaps(lead_range, (501, None)):
        score -= 2

    description = (lead.get("description") or "").lower()
    pms_keywords = [
        "breezeway",
        "guesty",
        "hostfully",
        "streamline",
        "ownerrez",
        "track",
        "escapia",
        "appfolio",
        "buildium",
        "guesty",
    ]
    if any(k in description for k in pms_keywords):
        score += 3

    location = (lead.get("location") or "").lower()
    if location and not any(k in description for k in ["national", "nationwide", "across the us", "across the united states"]):
        score += 2

    return score


def rank_leads(leads: List[Dict[str, str]]) -> List[Dict[str, str]]:
    ranked = []
    for lead in leads:
        lead_copy = dict(lead)
        lead_copy["_score"] = score_lead(lead)
        ranked.append(lead_copy)
    ranked.sort(key=lambda x: x.get("_score", 0), reverse=True)
    return ranked


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def choose_lead_curses(leads: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if not leads:
        return None

    def _draw(stdscr: "curses._CursesWindow") -> Optional[Dict[str, str]]:
        curses.curs_set(0)
        stdscr.nodelay(False)
        stdscr.keypad(True)
        index = 0
        offset = 0

        while True:
            stdscr.clear()
            height, width = stdscr.getmaxyx()
            header = "Pick a lead (up/down, page up/down, enter, q to quit)"
            stdscr.addnstr(0, 0, header, width - 1)

            detail_top = 2
            list_top = 6

            current = leads[index]
            name = f"{current.get('first_name', '').strip()} {current.get('last_name', '').strip()}".strip()
            title = current.get("title", "")
            company = current.get("company_name", "")
            score = current.get("_score", 0)
            description = current.get("description", "") or ""

            stdscr.addnstr(detail_top, 0, f"Selected: {name} | {title}", width - 1)
            stdscr.addnstr(detail_top + 1, 0, f"Company: {company} | Score: {score}", width - 1)
            stdscr.addnstr(detail_top + 2, 0, _truncate(description, width - 1), width - 1)

            visible = max(1, height - list_top - 1)
            if index < offset:
                offset = index
            elif index >= offset + visible:
                offset = index - visible + 1

            for i in range(visible):
                lead_idx = offset + i
                if lead_idx >= len(leads):
                    break
                lead = leads[lead_idx]
                name = f"{lead.get('first_name', '').strip()} {lead.get('last_name', '').strip()}".strip()
                title = lead.get("title", "")
                company = lead.get("company_name", "")
                score = lead.get("_score", 0)
                line = f"{lead_idx + 1}. {name} | {title} | {company} | score={score}"
                if lead_idx == index:
                    stdscr.attron(curses.A_REVERSE)
                    stdscr.addnstr(list_top + i, 0, _truncate(line, width - 1), width - 1)
                    stdscr.attroff(curses.A_REVERSE)
                else:
                    stdscr.addnstr(list_top + i, 0, _truncate(line, width - 1), width - 1)

            stdscr.refresh()
            key = stdscr.getch()
            if key in (curses.KEY_UP, ord("k")):
                index = max(0, index - 1)
            elif key in (curses.KEY_DOWN, ord("j")):
                index = min(len(leads) - 1, index + 1)
            elif key in (curses.KEY_NPAGE,):
                index = min(len(leads) - 1, index + visible)
            elif key in (curses.KEY_PPAGE,):
                index = max(0, index - visible)
            elif key in (curses.KEY_ENTER, 10, 13):
                return leads[index]
            elif key in (ord("q"), 27):
                return None

    try:
        return curses.wrapper(_draw)
    except Exception:
        return None


def build_prompt(lead: Dict[str, str]) -> str:
    name = f"{lead.get('first_name', '').strip()} {lead.get('last_name', '').strip()}".strip()
    title = lead.get("title", "")
    company = lead.get("company_name", "")
    location = lead.get("location", "")
    website = lead.get("website", "")
    description = lead.get("description", "")
    num_props = lead.get("num_properties", "")

    return f"""
You are a research assistant preparing a background report for outbound outreach.

Company context:
- Product: AI software that detects damages in property photos.
- Target teams: property management teams that use Breezeway or other PMS/tools that store inspection or turnover photos (e.g., Guesty, Hostfully, Streamline, OwnerRez, Track, Escapia).

Lead to research:
- Name: {name}
- Title: {title}
- Company: {company}
- Location: {location}
- Website: {website}
- Company size (properties): {num_props}
- Company description: {description}

Research goal:
Find anything and everything useful or interesting about this person, their role, and their company that could help craft a compelling outreach message. Be thorough and wide-ranging.
""".strip()


def get_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        raise RuntimeError("Missing GEMINI_API_KEY or GOOGLE_API_KEY in environment.")
    return key


def extract_text(output: Dict[str, object]) -> str:
    if "text" in output and isinstance(output["text"], str):
        return output["text"]
    content = output.get("content")
    if isinstance(content, dict):
        parts = content.get("parts", [])
        if isinstance(parts, list):
            return "".join(p.get("text", "") for p in parts if isinstance(p, dict))
    if isinstance(content, list):
        texts = []
        for item in content:
            if isinstance(item, dict):
                parts = item.get("parts", [])
                if isinstance(parts, list):
                    texts.append("".join(p.get("text", "") for p in parts if isinstance(p, dict)))
        return "\n".join(texts)
    return ""


def parse_usage(result: Dict[str, object]) -> Dict[str, Optional[float]]:
    usage = result.get("usage") if isinstance(result, dict) else None
    if not isinstance(usage, dict):
        return {
            "input_tokens": None,
            "output_tokens": None,
            "reasoning_tokens": None,
            "total_tokens": None,
            "search_queries": None,
        }
    return {
        "input_tokens": usage.get("total_input_tokens"),
        "output_tokens": usage.get("total_output_tokens"),
        "reasoning_tokens": usage.get("total_reasoning_tokens"),
        "total_tokens": usage.get("total_tokens"),
        "search_queries": usage.get("search_queries"),
    }


def compute_costs(usage: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    reasoning_tokens = usage.get("reasoning_tokens")
    search_queries = usage.get("search_queries")

    token_cost = None
    if input_tokens is not None and output_tokens is not None:
        reasoning_tokens = reasoning_tokens or 0
        input_cost = input_tokens * (INPUT_COST_PER_M_TOKEN / 1_000_000)
        output_cost = (output_tokens + reasoning_tokens) * (OUTPUT_COST_PER_M_TOKEN / 1_000_000)
        token_cost = input_cost + output_cost

    search_cost = None
    if search_queries is not None:
        search_cost = search_queries * SEARCH_COST_PER_QUERY

    total_cost = None
    if token_cost is not None or search_cost is not None:
        total_cost = (token_cost or 0) + (search_cost or 0)

    return {
        "token_cost_usd": token_cost,
        "search_cost_usd": search_cost,
        "total_cost_usd": total_cost,
    }


def start_interaction(prompt: str) -> str:
    key = get_api_key()
    payload = {
        "input": prompt,
        "agent": AGENT_NAME,
        "background": True,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        CREATE_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": key,
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Create interaction failed: {e.read().decode('utf-8', 'ignore')}") from e
    result = json.loads(body)
    interaction_id = result.get("id")
    if not interaction_id:
        raise RuntimeError(f"Unexpected create response: {result}")
    return interaction_id


def poll_interaction(interaction_id: str) -> Tuple[str, Dict[str, Optional[float]]]:
    key = get_api_key()
    url = GET_URL.format(id=interaction_id)
    while True:
        req = urllib.request.Request(url, headers={"x-goog-api-key": key})
        try:
            with urllib.request.urlopen(req) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"Poll failed: {e.read().decode('utf-8', 'ignore')}") from e
        result = json.loads(body)
        status = result.get("status")
        if status == "completed":
            outputs = result.get("outputs", [])
            report = extract_text(outputs[-1]) if outputs else ""
            usage = parse_usage(result)
            return report, usage
        if status in {"failed", "cancelled"}:
            raise RuntimeError(f"Interaction ended: {status} {result.get('error')}")
        if status == "requires_action":
            raise RuntimeError("Interaction requires action; tool flow not supported in this script.")
        time.sleep(POLL_SECONDS)


def save_report(lead: Dict[str, str], report: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", f"{lead.get('first_name','')}_{lead.get('last_name','')}_{lead.get('company_name','')}")
    path = OUTPUT_DIR / f"{safe_name}.txt"
    path.write_text(report, encoding="utf-8")
    return path


def update_lead_row(
    lead: Dict[str, str],
    report: str,
    interaction_id: str,
    status: str,
    usage: Dict[str, Optional[float]],
    costs: Dict[str, Optional[float]],
) -> None:
    lead["research_status"] = status
    lead["research_report"] = report
    lead["research_interaction_id"] = interaction_id
    lead["researched_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    lead["research_input_tokens"] = usage.get("input_tokens")
    lead["research_output_tokens"] = usage.get("output_tokens")
    lead["research_reasoning_tokens"] = usage.get("reasoning_tokens")
    lead["research_total_tokens"] = usage.get("total_tokens")
    lead["research_search_queries"] = usage.get("search_queries")
    lead["research_token_cost_usd"] = costs.get("token_cost_usd")
    lead["research_search_cost_usd"] = costs.get("search_cost_usd")
    lead["research_total_cost_usd"] = costs.get("total_cost_usd")


def main() -> None:
    leads = load_leads()
    matches = filter_leads(
        leads,
        "any",
        "any",
        False,
        None,
        False,
    )
    ranked = rank_leads(matches)
    lead = choose_lead_curses(ranked)
    if not lead:
        return

    prompt = build_prompt(lead)
    print("\nStarting deep research...")
    interaction_id = start_interaction(prompt)
    print(f"Interaction ID: {interaction_id}")
    report, usage = poll_interaction(interaction_id)
    costs = compute_costs(usage)
    update_lead_row(lead, report, interaction_id, "completed", usage, costs)

    existing_fields = list(leads[0].keys()) if leads else []
    fieldnames = list(existing_fields)
    for field in RESEARCH_FIELDS:
        if field not in fieldnames:
            fieldnames.append(field)
    save_leads(leads, fieldnames)
    print("\nReport saved into all_leads.csv")


if __name__ == "__main__":
    main()
