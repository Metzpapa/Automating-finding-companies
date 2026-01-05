#!/usr/bin/env python3
import csv
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dotenv import load_dotenv

INPUT_PATH = "all_leads.csv"
SLEEP_SECONDS = 0.0
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2.0
RATE_LIMIT_COOLDOWN_SECONDS = 90.0

NEW_COLUMNS = [
    "verification_status",
    "verification_raw",
    "verified_at",
    "zb_status",
    "zb_sub_status",
    "zb_free_email",
    "zb_mx_found",
    "zb_mx_record",
    "zb_did_you_mean",
    "zb_smtp_provider",
    "zb_score",
    "zb_domain_age_days",
    "zb_last_known_activity",
    "zb_processed_at",
]


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def map_provider_status(value):
    text = (value or "").strip().lower()
    if text in {"ok", "valid"}:
        return "valid"
    if text in {
        "email_disabled",
        "dead_server",
        "invalid_mx",
        "disposable",
        "spamtrap",
        "invalid_syntax",
        "invalid",
        "abuse",
        "do_not_mail",
    }:
        return "invalid"
    if text in {"ok_for_all", "smtp_protocol", "antispam_system", "unknown", "catch-all"}:
        return "unknown"
    if any(token in text for token in ["valid", "deliverable", "good"]):
        return "valid"
    if any(token in text for token in ["invalid", "undeliverable", "bad", "bounced", "bounce"]):
        return "invalid"
    if any(token in text for token in ["accept_all", "catch-all", "catchall", "unknown", "risky"]):
        return "unknown"
    return "unknown"


def normalize_status(raw_result):
    if raw_result is None:
        return "unknown"

    if isinstance(raw_result, dict):
        status = raw_result.get("status") or raw_result.get("result")
        if status:
            return map_provider_status(str(status))
        return "unknown"

    return map_provider_status(str(raw_result))


def ensure_fieldnames(fieldnames):
    updated = list(fieldnames)
    for col in NEW_COLUMNS:
        if col not in updated:
            updated.append(col)
    return updated


def should_verify(row):
    status = (row.get("verification_status") or "").strip().lower()
    raw = (row.get("verification_raw") or "").strip().lower()
    if "invalid api key" in raw or "ran out of credits" in raw:
        return True
    if not status or status == "unverified":
        return True
    if status == "error":
        return True
    return False


def load_api_key():
    load_dotenv()
    api_key = os.getenv("ZEROBOUNCE_API_KEY")
    if not api_key:
        raise RuntimeError("ZEROBOUNCE_API_KEY is not set in the environment")
    return api_key


def get_base_url():
    return os.getenv("ZEROBOUNCE_BASE_URL", "https://api.zerobounce.net")


def fetch_credits(api_key):
    base_url = get_base_url()
    url = f"{base_url.rstrip('/')}/v2/getcredits"
    params = urlencode({"api_key": api_key})
    req = Request(f"{url}?{params}", headers={"Accept": "application/json"})
    with urlopen(req, timeout=30) as resp:
        payload = resp.read().decode("utf-8").strip()
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload


def verify_email(api_key, email):
    base_url = get_base_url()
    url = f"{base_url.rstrip('/')}/v2/validate"
    params = urlencode({"api_key": api_key, "email": email})
    req = Request(f"{url}?{params}", headers={"Accept": "application/json"})
    with urlopen(req, timeout=30) as resp:
        payload = resp.read().decode("utf-8").strip()
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload


def verify_email_with_retry(api_key, email):
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return verify_email(api_key, email)
        except HTTPError as exc:
            status = getattr(exc, "code", None)
            body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            if status == 429 and attempt < RETRY_ATTEMPTS:
                print(f"Rate limited (429). Cooling down for {RATE_LIMIT_COOLDOWN_SECONDS}s...")
                time.sleep(RATE_LIMIT_COOLDOWN_SECONDS)
                continue
            if status in {500, 502, 503, 504} and attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                continue
            raise RuntimeError(f"HTTP {status}: {body}") from exc
        except URLError as exc:
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                continue
            raise RuntimeError(f"Network error: {exc}") from exc


def ensure_row_fields(row, fieldnames):
    for col in fieldnames:
        if col not in row:
            row[col] = ""


def write_all_rows(rows, fieldnames, path):
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    shutil.move(temp_path, path)


def process_file(path):
    api_key = load_api_key()
    credit_check_counter = 0

    credits_info = fetch_credits(api_key)
    credits_value = None
    if isinstance(credits_info, dict):
        print(f"Credits raw response: {json.dumps(credits_info, sort_keys=True)}")
        for key in ("Credits", "credits", "credits_left", "creditsRemaining", "credits_remaining", "remaining"):
            if key in credits_info:
                credits_value = credits_info[key]
                break
    else:
        print(f"Credits raw response: {credits_info}")
        credits_value = credits_info

    print(f"Credits check: {credits_value}")
    try:
        if credits_value is not None and float(credits_value) <= 0:
            print("No credits available. Exiting without verifying.")
            return
    except (TypeError, ValueError):
        pass

    with open(path, newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise RuntimeError("Input CSV has no header row")
        fieldnames = ensure_fieldnames(reader.fieldnames)
        rows = list(reader)

    for row in rows:
        ensure_row_fields(row, fieldnames)

    processed = 0
    verified = 0
    skipped = 0
    errors = 0

    print(f"Resuming verification for {len(rows)} rows")

    for row in rows:
        processed += 1
        credit_check_counter += 1

        if credit_check_counter >= 50:
            credit_check_counter = 0
            credits_info = fetch_credits(api_key)
            credits_value = None
            if isinstance(credits_info, dict):
                for key in (
                    "Credits",
                    "credits",
                    "credits_left",
                    "creditsRemaining",
                    "credits_remaining",
                    "remaining",
                ):
                    if key in credits_info:
                        credits_value = credits_info[key]
                        break
            else:
                credits_value = credits_info
            print(f"Credits check: {credits_value}")

        if not should_verify(row):
            skipped += 1
            continue

        email = (row.get("email") or "").strip()
        if not email:
            row["verification_status"] = "no_email"
            row["verification_raw"] = ""
            row["verified_at"] = utc_now_iso()
            write_all_rows(rows, fieldnames, path)
            continue

        try:
            raw_result = verify_email_with_retry(api_key, email)
            if isinstance(raw_result, dict) and raw_result.get("error"):
                error_msg = str(raw_result.get("error"))
                row["verification_status"] = "error"
                row["verification_raw"] = json.dumps(raw_result, sort_keys=True)
                row["verified_at"] = utc_now_iso()
                errors += 1
                print(f"Row {processed}: error {email}: {error_msg}")
                write_all_rows(rows, fieldnames, path)
                if "invalid api key" in error_msg.lower() or "ran out of credits" in error_msg.lower():
                    print("Stopping due to API key/credits error. Fix and re-run to resume.")
                    break
                continue

            row["verification_status"] = normalize_status(raw_result)
            row["verification_raw"] = (
                json.dumps(raw_result, sort_keys=True)
                if isinstance(raw_result, dict)
                else str(raw_result)
            )
            if isinstance(raw_result, dict):
                row["zb_status"] = raw_result.get("status", "")
                row["zb_sub_status"] = raw_result.get("sub_status", "")
                row["zb_free_email"] = raw_result.get("free_email", "")
                row["zb_mx_found"] = raw_result.get("mx_found", "")
                row["zb_mx_record"] = raw_result.get("mx_record", "")
                row["zb_did_you_mean"] = raw_result.get("did_you_mean", "")
                row["zb_smtp_provider"] = raw_result.get("smtp_provider", "")
                row["zb_score"] = raw_result.get("score", "")
                row["zb_domain_age_days"] = raw_result.get("domain_age_days", "")
                row["zb_last_known_activity"] = raw_result.get("last_known_activity", "")
                row["zb_processed_at"] = raw_result.get("processed_at", "")
            row["verified_at"] = utc_now_iso()
            verified += 1
            print(f"Row {processed}: verified {email}: {row['verification_status']}")
        except Exception as exc:
            row["verification_status"] = "error"
            row["verification_raw"] = f"error:{exc}"
            row["verified_at"] = utc_now_iso()
            errors += 1
            print(f"Row {processed}: error {email}: {exc}")

        write_all_rows(rows, fieldnames, path)

        if SLEEP_SECONDS:
            time.sleep(SLEEP_SECONDS)

    print("Verification run complete")
    print(f"Rows processed: {processed}")
    print(f"Rows verified: {verified}")
    print(f"Rows skipped: {skipped}")
    print(f"Rows errors: {errors}")


def main():
    process_file(INPUT_PATH)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}")
        sys.exit(1)
