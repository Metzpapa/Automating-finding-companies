#!/usr/bin/env python3
"""
Campaign 2: Contact Finder with Property Addresses
Enriches companies.csv with contact info + specific property data for outreach.
"""

import csv
import json
import os
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Thread-safe lock for CSV writing
csv_lock = threading.Lock()

# ========================================
# CONFIGURATION SECTION - EDIT THIS
# ========================================

# Input CSV file containing companies
INPUT_CSV = "companies.csv"

# Output CSV filename
OUTPUT_CSV = "leads_campaign2.csv"

# Tracking file (SAME as previous script to avoid re-doing work)
TRACKING_FILE = "processed_companies.json"

# Number of companies to process in this run (set to None to process all)
LIMIT_COMPANIES = 180

# Prompt template for Campaign 2
CONTACT_PROMPT_TEMPLATE = """I am building a company that allows vacation rental property managers the ability to have their cleaners simply scan after they're clean, and it detects if they completed their clean correctly and also if there's any damages to the property. This is super helpful for property managers and we're looking for people to contact.

I need to find the best contact people at {company_name} (website: {website}, location: {location}).

Company description: {description}

All the people that you find should be directly relevant to what we're selling (Operations, Owners, Maintenance, Quality Assurance).

For EACH contact person you identify, provide:
1. Their first name
2. Their last name
3. Their professional email address
4. Their job title
5. The approximate number of properties the company manages
6. Contact priority (1 = highest, 2 = second, etc.). Use judgement: Operations/Maintenance Directors are often better than CEOs for large companies. Owners are best for small companies.
7. A casual/shortened company name (e.g., "Canyon Services").
8. A CONVERSATIONAL PROPERTY REFERENCE (For Email Body): Find the ONE specific unit they manage that has the HIGHEST NUMBER OF REVIEWS.
   - **CRITICAL**: Format this so it flows naturally in a sentence like: *"I was reading reviews for [YOUR OUTPUT] and noticed..."*
   - **DO NOT** provide a full postal address with Zip Code/City/State.
   - **DO** provide a natural reference.
     - *Example:* "your property at 123 Main St" or "your Konea 136 unit at Honua Kai"
9. A SHORT PROPERTY NAME (For Email Subject Line):
   - A very short, punchy reference to the same property.
   - Remove "your property at". Just give the identifier.
   - *Example:* "123 Main St" or "Unit 402" or "Konea 136"
10. PROPERTY URL: The direct link to that specific property listing (Airbnb, Vrbo, or direct booking site).

Please format your response as a JSON array where each contact is an object with these exact keys: "first_name", "last_name", "email", "title", "num_properties", "contact_priority", "casual_company_name", "sample_property_address", "property_name_short", "property_url".

Example format:
[
  {{"first_name": "John", "last_name": "Smith", "email": "john@company.com", "title": "Ops Manager", "num_properties": "over 100", "contact_priority": 1, "casual_company_name": "Canyon Services", "sample_property_address": "Unit 4B at the Cliffside Condos", "property_name_short": "Unit 4B", "property_url": "https://airbnb.com/rooms/12345"}}
]

If you find an email pattern and cannot find specific emails, assume that pattern applies. Use generic emails only as a last resort."""

# Number of parallel workers
MAX_WORKERS = 5

# ========================================
# END CONFIGURATION SECTION
# ========================================


def create_contact_prompt(company_data):
    """Create a prompt for finding contact information for a specific company."""
    return CONTACT_PROMPT_TEMPLATE.format(
        company_name=company_data.get('company_name', ''),
        website=company_data.get('website', ''),
        location=company_data.get('location', ''),
        description=company_data.get('description', '')
    )


def query_gpt5_for_contact(prompt, company_name):
    """Query GPT-5 with web search."""
    print(f"  🔍 Searching for contact at: {company_name}")
    client = OpenAI()
    try:
        resp = client.responses.create(
            model="gpt-5", 
            input=prompt,
            tools=[{"type": "web_search"}],
        )
        print(f"  ✓ Received response for {company_name}")
        return resp.output_text
    except Exception as e:
        print(f"  ✗ Error querying GPT-5 for {company_name}: {str(e)}")
        return None


def parse_contact_response(response_text, company_name):
    """Parse the GPT-5 response and extract contact information."""
    contacts = []
    if not response_text:
        return contacts

    try:
        start_idx = response_text.find('[')
        end_idx = response_text.rfind(']') + 1

        if start_idx != -1 and end_idx > start_idx:
            json_str = response_text[start_idx:end_idx]
            parsed_data = json.loads(json_str)

            if isinstance(parsed_data, list):
                for contact in parsed_data:
                    contact_data = {
                        'first_name': contact.get('first_name', ''),
                        'last_name': contact.get('last_name', ''),
                        'email': contact.get('email', ''),
                        'title': contact.get('title', ''),
                        'num_properties': str(contact.get('num_properties', '')),
                        'contact_priority': str(contact.get('contact_priority', '3')), # Default to 3 if missing
                        'casual_company_name': contact.get('casual_company_name', ''),
                        'sample_property_address': contact.get('sample_property_address', ''),
                        'property_name_short': contact.get('property_name_short', ''),
                        'property_url': contact.get('property_url', '')
                    }
                    contacts.append(contact_data)

                print(f"  ✓ Parsed {len(contacts)} contact(s) for {company_name}")
                return contacts

    except json.JSONDecodeError as e:
        print(f"  ⚠️  Could not parse JSON for {company_name}: {str(e)}")

    return contacts


def load_processed_companies(tracking_file):
    """Load the set of already processed companies."""
    try:
        with open(tracking_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get('processed', []))
    except FileNotFoundError:
        return set()
    except Exception as e:
        print(f"⚠️  Could not load tracking file: {str(e)}")
        return set()


def save_processed_company(company_name, tracking_file):
    """Add a company to the processed tracking file."""
    with csv_lock:
        try:
            processed = load_processed_companies(tracking_file)
            processed.add(company_name)
            with open(tracking_file, 'w', encoding='utf-8') as f:
                json.dump({'processed': list(processed)}, f, indent=2)
        except Exception as e:
            print(f"⚠️  Could not save to tracking file: {str(e)}")


def read_companies_csv(filename):
    """Read the input companies CSV file."""
    companies = []
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                companies.append(row)
        return companies
    except Exception as e:
        print(f"✗ Error reading {filename}: {str(e)}")
        return []


def append_leads_to_csv(leads, filename):
    """Append lead data to the output CSV file."""
    fieldnames = [
        'contact_priority', 'first_name', 'last_name', 'email', 'title', 
        'num_properties', 'casual_company_name', 
        'sample_property_address', 'property_name_short', 'property_url',
        'company_name', 'location', 'website', 'company_phone', 'company_email', 'description'
    ]

    with csv_lock:
        # Check if file exists to write header
        file_exists = os.path.isfile(filename)
        
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            if not file_exists:
                writer.writeheader()
            for lead in leads:
                writer.writerow(lead)


def sort_csv_by_priority(filename):
    """Reads the final CSV, sorts by priority (1, 2, 3), and overwrites it."""
    print(f"\n🔄 Sorting {filename} by priority...")
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
            fieldnames = reader.fieldnames

        if not data:
            return

        # Sort logic: Convert priority to int for sorting, handle non-numeric gracefully
        def get_priority(row):
            try:
                return int(row.get('contact_priority', 99))
            except ValueError:
                return 99

        data.sort(key=get_priority)

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"✅ File sorted successfully.")
        
    except Exception as e:
        print(f"⚠️  Could not sort file: {str(e)}")


def process_company(company_data, idx, total):
    """Worker function to process a single company."""
    company_name = company_data.get('company_name', 'Unknown')
    print(f"\n[{idx}/{total}] 🔄 STARTED: {company_name}")

    prompt = create_contact_prompt(company_data)
    response_text = query_gpt5_for_contact(prompt, company_name)
    contacts = parse_contact_response(response_text, company_name)

    leads = []
    for contact in contacts:
        lead = {
            **contact, # Unpack contact fields
            'company_name': company_data.get('company_name', ''),
            'location': company_data.get('location', ''),
            'website': company_data.get('website', ''),
            'company_phone': company_data.get('phone', ''),
            'company_email': company_data.get('email', ''),
            'description': company_data.get('description', '')
        }
        leads.append(lead)

    if leads:
        append_leads_to_csv(leads, OUTPUT_CSV)
        save_processed_company(company_name, TRACKING_FILE)
        print(f"[{idx}/{total}] ✅ COMPLETED: {company_name} - Found {len(leads)} lead(s)")
    else:
        print(f"[{idx}/{total}] ⚠️  COMPLETED: {company_name} - No contacts found")

    return len(leads)


def main():
    start_time = datetime.now()

    print("="*70)
    print(f"Campaign 2: Contact Finder + Addresses ({MAX_WORKERS} workers)")
    print("="*70)
    print(f"Input: {INPUT_CSV}")
    print(f"Output: {OUTPUT_CSV}")
    print(f"Tracking: {TRACKING_FILE}")

    all_companies = read_companies_csv(INPUT_CSV)
    processed_companies = load_processed_companies(TRACKING_FILE)
    
    # Filter out processed companies
    companies_to_process = [
        c for c in all_companies
        if c.get('company_name', '') not in processed_companies
    ]

    # Sort alphabetically by location
    companies_to_process.sort(key=lambda x: x.get('location', ''))

    if not companies_to_process:
        print("\n✅ All companies processed! Nothing to do.")
        return

    if LIMIT_COMPANIES and LIMIT_COMPANIES > 0:
        companies_to_process = companies_to_process[:LIMIT_COMPANIES]
        print(f"✓ Limiting to first {len(companies_to_process)} companies")

    print(f"\nProcessing {len(companies_to_process)} companies...\n")

    total_leads_found = 0
    completed_companies = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_company = {
            executor.submit(process_company, company, idx, len(companies_to_process)): company
            for idx, company in enumerate(companies_to_process, 1)
        }

        for future in as_completed(future_to_company):
            try:
                num_leads = future.result()
                total_leads_found += num_leads
                completed_companies += 1
            except Exception as e:
                print(f"\n❌ ERROR: {str(e)}")

    # Final Sort
    sort_csv_by_priority(OUTPUT_CSV)

    end_time = datetime.now()
    print("\n" + "="*70)
    print(f"✅ CAMPAIGN 2 COMPLETE!")
    print(f"Found {total_leads_found} leads")
    print(f"Results saved and sorted in: {OUTPUT_CSV}")
    print(f"Duration: {end_time - start_time}")
    print("="*70)


if __name__ == "__main__":
    main()