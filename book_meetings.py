import asyncio
import csv
import json
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from browser_use import Agent, Browser, ChatGoogle

load_dotenv()

INPUT_CSV = Path("companies.csv")
OUTPUT_CSV = Path("meeting_results.csv")

# How many companies to process this run. Set to None to process all.
MAX_COMPANIES = 10

# If your real email is different, change this in one place.
BOOKING_EMAIL = "sam@rapideyeinspection.com"


def build_task(company: Dict[str, str]) -> str:
    """
    Build the natural-language task for the Browser Use agent.
    The more specific we are about WHEN to book and WHEN NOT to book,
    the more predictable the behavior.
    """
    return f"""
You are an AI assistant that books intro meetings with vacation-rental
property management companies on behalf of a startup called RapidEye Inspection.

Goal:
- For this specific company, open their website and try to book the NEXT AVAILABLE
  intro call / demo / owner consultation / discovery call with their team.
- ONLY use a proper scheduling / calendar interface (e.g. Calendly, HubSpot Meetings,
  OnceHub, Acuity, Chili Piper, Setmore, YouCanBook.me, or a custom date+time picker).
- DO NOT submit plain "contact us" or "request info" forms that just send a message.

Company context:
- Name: {company.get("company_name")}
- Location: {company.get("location")}
- Website: {company.get("website")}
- Description: {company.get("description", "").strip()}

Required meeting details to use:
- Visitor email: {BOOKING_EMAIL}
- Visitor first name: "Sam"
- Visitor last name: "Rapideye"
- Company: "RapidEye Inspection"
- If phone is required, use: "+1 555 000 0000".
- If a short text "reason for meeting" is required, write something like:
  "I'd like to show you RapidEye Inspection, an AI-powered video inspection tool
   that helps your cleaners automatically flag cleaning issues and damages."

VERY IMPORTANT RULES ABOUT WHAT COUNTS AS A VALID BOOKING FLOW:

1. VALID booking flow:
   - The page clearly shows a date picker and list of time slots.
   - After you pick a slot, it asks for contact details (name/email/etc.).
   - There is a final "Schedule", "Confirm", "Book", or similar button.
   -> In this case, you MUST actually complete the booking.

2. INVALID booking flow (do NOT submit):
   - A contact form with fields like name/email/message, but NO date/time selector.
   - A simple "send us a message" or "request info" form.
   - A generic CRM form that just generates a lead.
   - An email link (mailto:) without a scheduling widget.
   - Pages that say "Call us to schedule" with only a phone number.

3. Login or gated flows:
   - If you must log into an owner portal or guest portal to schedule, STOP.
   - Do NOT attempt to create accounts or log in.
   - Instead, report that scheduling requires login.

4. Email or SMS verifications:
   - If the flow requires a verification code sent to email or phone, STOP when asked.
   - Do NOT try to bypass this; just report that verification was required.

5. Multiple meeting types:
   - If there are multiple meeting types, pick the one that best matches:
     "Owner intro call", "Schedule a demo", "Talk to sales", or equivalent.
   - If unsure, pick the shortest remote call (15–30 min).

Navigation instructions:

- Start by opening exactly this URL: {company.get("website")}
- Look for text like:
  "Schedule a call", "Schedule a demo", "Book a meeting", 
  "Owner consultation", "Request a demo", "Talk to our team", "Get started".
- Also check menu items like:
  "Contact", "Owners", "Schedule a call", "Get in touch", "Consultation", "Book now".
- If you find a Calendly, HubSpot, or similar embedded widget, use it.

What to do when you FINISH:

- If you successfully booked a meeting:
  - Capture the confirmed date/time and timezone shown in the UI.
  - Capture the confirmation or thank-you page URL.
  - If a join link (like a Zoom/Meet link) is shown, include it.

- If you cannot find any real scheduling interface and only see plain forms:
  - Do NOT submit anything.
  - Explain briefly what you found and why it wasn't a calendar.

- If scheduling requires login or verification:
  - Stop and report that as the status.

FINAL OUTPUT FORMAT (VERY IMPORTANT):

At the very end, output ONLY a single line of MINIFIED JSON.
No markdown, no backticks, no commentary.
The JSON object MUST have exactly these keys:

- "status": one of:
    "booked",
    "no_calendar",
    "requires_login",
    "needs_manual_review",
    "error"
- "company_name": string
- "website": string
- "booking_url": string or null
- "start_time_iso": string or null  (e.g. "2025-11-25T16:30:00")
- "timezone": string or null        (e.g. "America/Los_Angeles" or "Pacific/Honolulu")
- "join_link": string or null       (e.g. Zoom/Meet URL, if visible)
- "notes": string

Examples:
- If you book a meeting successfully, "status" = "booked".
- If you only see a generic contact form and no calendar, "status" = "no_calendar".
- If you hit a login wall, "status" = "requires_login".
- If something unexpected happens but you think a human should take a look,
  "status" = "needs_manual_review".
- If you completely fail to understand the site, "status" = "error".

AGAIN: Return ONLY ONE LINE OF JSON as described, and nothing else.
    """.strip()


async def book_for_company(company: Dict[str, str]) -> Dict[str, Any]:
    """
    Run a Browser Use agent for a single company and return a normalized result dict.
    """
    browser = Browser(
        # If you later use Browser Use Cloud, uncomment:
        # use_cloud=True,
    )

    llm = ChatGoogle(
        model="gemini-3-pro-preview",  # adjust if Google changes the name
        temperature=0.1,
    )

    task = build_task(company)

    agent = Agent(
        task=task,
        llm=llm,
        browser=browser,
        use_vision="auto",
        max_actions_per_step=4,
        max_failures=3,
    )

    try:
        history = await agent.run()
        raw_result = history.final_result()  # last extracted content from the run
    except Exception as e:  # safeguard so one failure doesn't stop the run
        return {
            "status": "error",
            "company_name": company.get("company_name"),
            "website": company.get("website"),
            "booking_url": None,
            "start_time_iso": None,
            "timezone": None,
            "join_link": None,
            "notes": f"Agent run failed: {e}",
        }

    try:
        data = json.loads(raw_result)
        base = {
            "status": "error",
            "company_name": company.get("company_name"),
            "website": company.get("website"),
            "booking_url": None,
            "start_time_iso": None,
            "timezone": None,
            "join_link": None,
            "notes": "",
        }
        base.update(data if isinstance(data, dict) else {})
        return base
    except Exception as e:
        return {
            "status": "error",
            "company_name": company.get("company_name"),
            "website": company.get("website"),
            "booking_url": None,
            "start_time_iso": None,
            "timezone": None,
            "join_link": None,
            "notes": f"Failed to parse JSON output: {e}. Raw: {raw_result!r}",
        }


async def main():
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

    companies = []
    with INPUT_CSV.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            companies.append(row)

    if MAX_COMPANIES is not None:
        companies = companies[:MAX_COMPANIES]

    print(f"Loaded {len(companies)} companies from {INPUT_CSV}")

    output_fields = [
        "company_name",
        "website",
        "status",
        "booking_url",
        "start_time_iso",
        "timezone",
        "join_link",
        "notes",
    ]

    file_exists = OUTPUT_CSV.exists()
    out_f = OUTPUT_CSV.open("a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_f, fieldnames=output_fields)
    if not file_exists:
        writer.writeheader()

    for idx, company in enumerate(companies, start=1):
        print(f"[{idx}/{len(companies)}] Processing {company.get('company_name')}...")
        result = await book_for_company(company)
        writer.writerow(
            {
                "company_name": result.get("company_name"),
                "website": result.get("website"),
                "status": result.get("status"),
                "booking_url": result.get("booking_url"),
                "start_time_iso": result.get("start_time_iso"),
                "timezone": result.get("timezone"),
                "join_link": result.get("join_link"),
                "notes": result.get("notes"),
            }
        )
        out_f.flush()
        print(f"  -> status={result.get('status')} website={result.get('website')}")

    out_f.close()
    print(f"Done. Wrote results to {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
