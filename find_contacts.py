#!/usr/bin/env python3
"""
Contact Finder for Property Management Companies
Enriches companies.csv with best contact information for outreach.
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

# Input CSV file containing companies (should have: location, company_name, website, phone, email, description)
INPUT_CSV = "companies.csv"

# Output folder for all lead files
OUTPUT_FOLDER = "leads_output"

# Output CSV filename with enriched contact data (one row per contact/lead)
OUTPUT_CSV = f"{OUTPUT_FOLDER}/leads_all.csv"

# Tracking file to store which companies have been successfully processed
TRACKING_FILE = "processed_companies.json"

# Number of companies to process in this run (set to None to process all)
# Use this to test on a small batch first, then increase or set to None
LIMIT_COMPANIES = 180

# Prompt template for finding the best contact people
# Available variables: {company_name}, {website}, {location}, {description}
CONTACT_PROMPT_TEMPLATE = """I am building a company that allows vacation rental property managers the ability to have their cleaners simply scan after they're clean, and it detects if they completed their clean correctly and also if there's any damages to the property. This is super helpful for property managers and we're looking for people to contact.

I need to find the best contact people at {company_name} (website: {website}, location: {location}).

Company description: {description}

All the people that you find should be directly relevant to what we're selling. Either they're an owner and can make a decision based on the fact that they know how the company handles cleaners and damage detection, or they're in operations, or they do something else that concerns inspections/operations etc.


For EACH contact person you identify, provide:
1. Their first name
2. Their last name
3. Their professional email address
4. Their job title
5. The approximate number of properties the company manages (or minimum number if exact isn't available - e.g., "around 50" or "over 50")
6. Contact priority (1 = highest priority to contact first, 2 = second priority, 3 = third priority, etc.) based on How likely they'll want to talk to us about product and turnovers, and cleaning and damage detection and such. For example, a giant property management company may have a founder and CEO, and we wouldn't want to talk to him about it. We'd want to talk to the person in operations per se. But at the same time, if it's a small business, then the founder might be a high priority. So, just use your judgement to figure out who is the most relevant for our product. 
7. A casual/shortened company name that would sound natural in an email (e.g., "Canyon Services" instead of "Canyon Services (Alta & Snowbird Luxury Vacation Rentals)")

Please format your response as a JSON array where each contact is an object with these exact keys: "first_name", "last_name", "email", "title", "num_properties", "contact_priority", "casual_company_name".

Example format:
[
  {{"first_name": "John", "last_name": "Smith", "email": "john@company.com", "title": "CEO", "num_properties": "over 100", "contact_priority": 1, "casual_company_name": "Canyon Services"}},
  {{"first_name": "Jane", "last_name": "Doe", "email": "jane@company.com", "title": "Director of Operations", "num_properties": "over 100", "contact_priority": 2, "casual_company_name": "Canyon Services"}}
]

If you find an email pattern and  cannot find the other contacts emails, assume that pattern is for the rest of contacts you find.  if you cannot find any pattern use a standard first-name email pattern  first@company_domain whenever a person is identified but their email is not public. Only fall back to generic emails if the person’s name cannot be determined. """

# Number of parallel workers (companies processed simultaneously)
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
    """
    Query GPT-5 with web search to find the best contact person.
    Returns the structured response.
    """
    print(f"  🔍 Searching for contact at: {company_name}")

    client = OpenAI()

    try:
        resp = client.responses.create(
            model="gpt-5",  # Use "gpt-5-pro" if you have access
            input=prompt,
            tools=[{"type": "web_search"}],  # Enable built-in web search
        )

        print(f"  ✓ Received response for {company_name}")

        return resp.output_text

    except Exception as e:
        print(f"  ✗ Error querying GPT-5 for {company_name}: {str(e)}")
        return None


def parse_contact_response(response_text, company_name):
    """
    Parse the GPT-5 response and extract contact information.
    Returns a list of contact dicts (one per contact person found).
    """
    contacts = []

    if not response_text:
        return contacts

    # Try to find JSON array in the response
    try:
        # Look for JSON array in the response
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
                        'contact_priority': str(contact.get('contact_priority', '')),
                        'casual_company_name': contact.get('casual_company_name', '')
                    }
                    contacts.append(contact_data)

                print(f"  ✓ Parsed {len(contacts)} contact(s) for {company_name}")
                for idx, contact in enumerate(contacts, 1):
                    print(f"    {idx}. {contact['first_name']} {contact['last_name']} ({contact['title']})")
                return contacts

    except json.JSONDecodeError as e:
        print(f"  ⚠️  Could not parse JSON for {company_name}: {str(e)}")

    return contacts


def load_processed_companies(tracking_file):
    """Load the set of already processed companies from tracking file."""
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
    """Add a company to the processed tracking file (thread-safe)."""
    with csv_lock:  # Reuse CSV lock for file safety
        try:
            # Load existing data
            processed = load_processed_companies(tracking_file)
            processed.add(company_name)

            # Save updated data
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
        print(f"✓ Loaded {len(companies)} companies from {filename}")
        return companies
    except FileNotFoundError:
        print(f"✗ Error: Could not find {filename}")
        return []
    except Exception as e:
        print(f"✗ Error reading {filename}: {str(e)}")
        return []


def append_leads_to_csv(leads, filename):
    """Append lead data to the output CSV file (thread-safe). One row per contact."""
    # Lead-first CSV structure: contact fields first, then company fields
    fieldnames = [
        'contact_priority', 'first_name', 'last_name', 'email', 'title', 'num_properties',
        'casual_company_name', 'company_name', 'location', 'website', 'company_phone', 'company_email', 'description'
    ]

    # Use lock to ensure only one thread writes to CSV at a time
    with csv_lock:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            for lead in leads:
                writer.writerow(lead)


def append_leads_to_priority_files(leads, output_folder):
    """Append leads to priority-specific CSV files (e.g., leads_priority_1.csv, leads_priority_2.csv)."""
    fieldnames = [
        'contact_priority', 'first_name', 'last_name', 'email', 'title', 'num_properties',
        'casual_company_name', 'company_name', 'location', 'website', 'company_phone', 'company_email', 'description'
    ]

    # Group leads by priority
    priority_groups = {}
    for lead in leads:
        priority = lead.get('contact_priority', 'unknown')
        if priority not in priority_groups:
            priority_groups[priority] = []
        priority_groups[priority].append(lead)

    # Write each priority group to its own file
    with csv_lock:
        for priority, priority_leads in priority_groups.items():
            priority_file = f"{output_folder}/leads_priority_{priority}.csv"

            # Check if file exists to determine if we need to write header
            file_exists = False
            try:
                with open(priority_file, 'r') as f:
                    file_exists = True
            except FileNotFoundError:
                pass

            # Append to priority file
            with open(priority_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                if not file_exists:
                    writer.writeheader()
                for lead in priority_leads:
                    writer.writerow(lead)


def process_company(company_data, idx, total):
    """Process a single company to find contact information (worker function)."""
    company_name = company_data.get('company_name', 'Unknown')

    print(f"\n[{idx}/{total}] 🔄 STARTED: {company_name}")

    # Create the prompt for this company
    prompt = create_contact_prompt(company_data)

    # Query GPT-5 with web search
    response_text = query_gpt5_for_contact(prompt, company_name)

    # Parse the response to get a list of contacts
    contacts = parse_contact_response(response_text, company_name)

    # Create lead rows - one row per contact, with company data repeated
    leads = []
    for contact in contacts:
        lead = {
            # Contact fields first
            'contact_priority': contact['contact_priority'],
            'first_name': contact['first_name'],
            'last_name': contact['last_name'],
            'email': contact['email'],
            'title': contact['title'],
            'num_properties': contact['num_properties'],
            'casual_company_name': contact['casual_company_name'],
            # Company fields
            'company_name': company_data.get('company_name', ''),
            'location': company_data.get('location', ''),
            'website': company_data.get('website', ''),
            'company_phone': company_data.get('phone', ''),
            'company_email': company_data.get('email', ''),
            'description': company_data.get('description', '')
        }
        leads.append(lead)

    # Write leads to CSV immediately
    if leads:
        append_leads_to_csv(leads, OUTPUT_CSV)
        # Also write to priority-specific files
        append_leads_to_priority_files(leads, OUTPUT_FOLDER)
        # Mark company as successfully processed
        save_processed_company(company_name, TRACKING_FILE)
        print(f"[{idx}/{total}] ✅ COMPLETED: {company_name} - Found {len(leads)} lead(s)")
    else:
        print(f"[{idx}/{total}] ⚠️  COMPLETED: {company_name} - No contacts found")

    return {
        'company_name': company_name,
        'num_leads': len(leads),
        'success': len(leads) > 0
    }


def main():
    """Main execution function with parallel processing."""
    start_time = datetime.now()

    print("="*70)
    print(f"Contact Finder - PARALLEL MODE ({MAX_WORKERS} workers)")
    print("="*70)
    print(f"Input file: {INPUT_CSV}")
    print(f"Output file: {OUTPUT_CSV}")
    print(f"Tracking file: {TRACKING_FILE}")
    print(f"Parallel workers: {MAX_WORKERS}")
    if LIMIT_COMPANIES:
        print(f"Limit: Processing first {LIMIT_COMPANIES} unprocessed companies")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    # Read companies from input CSV
    all_companies = read_companies_csv(INPUT_CSV)

    if not all_companies:
        print("\n✗ No companies to process. Exiting.")
        return

    # Load already processed companies
    processed_companies = load_processed_companies(TRACKING_FILE)
    print(f"✓ Already processed: {len(processed_companies)} companies")

    # Filter out already processed companies
    companies_to_process = [
        company for company in all_companies
        if company.get('company_name', '') not in processed_companies
    ]

    # Sort by location alphabetically (A-Z)
    companies_to_process.sort(key=lambda x: x.get('location', ''))
    print(f"✓ Sorted companies alphabetically by location")

    print(f"✓ Remaining to process: {len(companies_to_process)} companies")

    if not companies_to_process:
        print("\n✅ All companies have already been processed! Nothing to do.")
        return

    # Apply limit if specified
    if LIMIT_COMPANIES and LIMIT_COMPANIES > 0:
        companies_to_process = companies_to_process[:LIMIT_COMPANIES]
        print(f"✓ Limiting to first {len(companies_to_process)} companies")

    print(f"\nProcessing {len(companies_to_process)} companies...\n")

    # Create output folder if it doesn't exist
    try:
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        print(f"✓ Output folder ready: {OUTPUT_FOLDER}")
    except Exception as e:
        print(f"⚠️  Error creating output folder: {str(e)}\n")

    # Create or append to output CSV file with header
    try:
        # Check if file exists to decide whether to write header
        file_exists = False
        try:
            with open(OUTPUT_CSV, 'r') as f:
                file_exists = True
        except FileNotFoundError:
            pass

        if not file_exists:
            fieldnames = [
                'contact_priority', 'first_name', 'last_name', 'email', 'title', 'num_properties',
                'casual_company_name', 'company_name', 'location', 'website', 'company_phone', 'company_email', 'description'
            ]
            with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
            print(f"✓ Created main leads file: {OUTPUT_CSV}")
            print(f"✓ Priority-specific files will be created as leads are found\n")
        else:
            print(f"✓ Appending to existing files in {OUTPUT_FOLDER}\n")
    except Exception as e:
        print(f"⚠️  Error with output CSV: {str(e)}\n")

    total_leads_found = 0
    completed_companies = 0

    # Process companies in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all companies to the thread pool
        future_to_company = {
            executor.submit(process_company, company, idx, len(companies_to_process)): company
            for idx, company in enumerate(companies_to_process, 1)
        }

        # Process completed tasks as they finish
        for future in as_completed(future_to_company):
            company = future_to_company[future]
            try:
                result = future.result()
                total_leads_found += result['num_leads']
                completed_companies += 1

                # Print running summary
                print(f"\n📊 Progress: {completed_companies}/{len(companies_to_process)} companies processed | {total_leads_found} total leads found")

            except Exception as e:
                print(f"\n❌ ERROR processing {company.get('company_name', 'Unknown')}: {str(e)}")
                completed_companies += 1

    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "="*70)
    print(f"✅ COMPLETE! Found {total_leads_found} leads from {len(companies_to_process)} companies")
    print(f"Started:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"\nOutput files in {OUTPUT_FOLDER}/:")
    print(f"  - leads_all.csv (all leads)")
    print(f"  - leads_priority_1.csv (highest priority contacts)")
    print(f"  - leads_priority_2.csv (second priority contacts)")
    print(f"  - leads_priority_3.csv (third priority contacts)")
    print(f"  - etc. (additional priority files as needed)")
    print(f"\nTracking saved to: {TRACKING_FILE}")
    print(f"Total processed so far: {len(processed_companies) + completed_companies}/{len(all_companies)} companies")
    print("="*70)


if __name__ == "__main__":
    main()
