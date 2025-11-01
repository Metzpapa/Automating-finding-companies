#!/usr/bin/env python3
"""
Property Management Company Finder
Uses GPT-5 with web search to find vacation rental management companies across multiple locations.
"""

import csv
import json
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

# Your prompt template - [LOCATION] will be replaced with each location from the list
PROMPT_TEMPLATE = """Find all the vacation rental management companies in [LOCATION] that manage short-term rentals. For each company found, provide:
1. Company name
2. Website URL
3. Phone number
4. Email for outreach
5. Brief description of their size and relevant details about what they do

Please format your response as a JSON array where each company is an object with these exact keys: "company_name", "website", "phone", "email", "description"."""

# List of locations to search - add more as needed
LOCATIONS = [
    # Hawaii-Aleutian Time Zone
    "Wailea, HI",
    "Kihei, HI",
    "Lahaina, HI",
    "Ka'anapali, HI",
    "Poipu, HI",
    "Princeville, HI",
    "Hanalei, HI",
    "Waikiki, HI",
    "Haleiwa, HI",
    "Kailua-Kona, HI",
    "Kohala Coast, HI",
    # Pacific Time Zone
    "San Diego, CA",
    "La Jolla, CA",
    "Newport Beach, CA",
    "Laguna Beach, CA",
    "Malibu, CA",
    "Santa Monica, CA",
    "Santa Barbara, CA",
    "Pismo Beach, CA",
    "Monterey, CA",
    "Carmel-by-the-Sea, CA",
    "Santa Cruz, CA",
    "South Lake Tahoe, CA",
    "Tahoe City, CA",
    "Mammoth Lakes, CA",
    "Big Bear Lake, CA",
    "Groveland, CA",
    "Mariposa, CA",
    "Three Rivers, CA",
    "Palm Springs, CA",
    "Palm Desert, CA",
    "La Quinta, CA",
    "Cannon Beach, OR",
    "Seaside, OR",
    "Lincoln City, OR",
    "Ocean Shores, WA",
    "Long Beach, WA",
    "Las Vegas, NV",
    "Lake Las Vegas, NV",
    # Mountain Time Zone
    "Aspen, CO",
    "Snowmass, CO",
    "Vail, CO",
    "Beaver Creek, CO",
    "Breckenridge, CO",
    "Telluride, CO",
    "Steamboat Springs, CO",
    "Crested Butte, CO",
    "Park City, UT",
    "Deer Valley, UT",
    "Alta, UT",
    "Snowbird, UT",
    "Springdale, UT",
    "St. George, UT",
    "Jackson, WY",
    "Cody, WY",
    "Big Sky, MT",
    "Whitefish, MT",
    "West Yellowstone, MT",
    "Gardiner, MT",
    "Sun Valley, ID",
    "Scottsdale, AZ",
    "Phoenix, AZ",
    "Sedona, AZ",
    "Flagstaff, AZ",
    "Williams, AZ",
    # Central Time Zone
    "Destin, FL",
    "Miramar Beach, FL",
    "Seaside, FL",
    "Santa Rosa Beach, FL",
    "Panama City Beach, FL",
    "Pensacola Beach, FL",
    "Gulf Shores, AL",
    "Orange Beach, AL",
    "South Padre Island, TX",
    "Galveston, TX",
    "Port Aransas, TX",
    "Lake Travis, TX",
    "Lake of the Ozarks, MO",
    "Branson, MO",
    "Hot Springs, AR",
    "Lake Geneva, WI",
    "Wisconsin Dells, WI",
    "Brainerd, MN",
    # Eastern Time Zone
    "Clearwater Beach, FL",
    "St. Pete Beach, FL",
    "Siesta Key, FL",
    "Anna Maria Island, FL",
    "Fort Myers Beach, FL",
    "Sanibel, FL",
    "Captiva, FL",
    "Naples, FL",
    "Marco Island, FL",
    "Amelia Island, FL",
    "St. Augustine, FL",
    "New Smyrna Beach, FL",
    "Cocoa Beach, FL",
    "Jupiter, FL",
    "Palm Beach, FL",
    "Fort Lauderdale, FL",
    "Miami Beach, FL",
    "Key Largo, FL",
    "Islamorada, FL",
    "Marathon, FL",
    "Key West, FL",
    "Corolla, NC",
    "Duck, NC",
    "Kitty Hawk, NC",
    "Kill Devil Hills, NC",
    "Nags Head, NC",
    "Hatteras, NC",
    "Ocracoke, NC",
    "Bald Head Island, NC",
    "Oak Island, NC",
    "Ocean Isle Beach, NC",
    "Boone, NC",
    "Blowing Rock, NC",
    "Asheville, NC",
    "Banner Elk, NC",
    "Myrtle Beach, SC",
    "North Myrtle Beach, SC",
    "Hilton Head Island, SC",
    "Isle of Palms, SC",
    "Folly Beach, SC",
    "Kiawah Island, SC",
    "Cape May, NJ",
    "The Wildwoods, NJ",
    "Ocean City, NJ",
    "Long Beach Island, NJ",
    "Rehoboth Beach, DE",
    "Bethany Beach, DE",
    "Dewey Beach, DE",
    "Ocean City, MD",
    "Provincetown, MA",
    "Chatham, MA",
    "Hyannis, MA",
    "Martha's Vineyard, MA",
    "Nantucket, MA",
    "Kennebunkport, ME",
    "Ogunquit, ME",
    "Bar Harbor, ME",
    "Old Orchard Beach, ME",
    "Stowe, VT",
    "Killington, VT",
    "Stratton, VT",
    "North Conway, NH",
    "Lake George, NY",
    "Lake Placid, NY",
    "The Finger Lakes, NY",
    "Gatlinburg, TN",
    "Pigeon Forge, TN",
    "Sevierville, TN",
    "Snowshoe, WV",
    "Traverse City, MI",
    "Saugatuck, MI",
    "South Haven, MI",
]

# Output CSV filename
OUTPUT_CSV = "property_management_companies.csv"

# Number of parallel workers (locations processed simultaneously)
MAX_WORKERS = 5

# ========================================
# END CONFIGURATION SECTION
# ========================================


def create_prompt_for_location(location):
    """Replace [LOCATION] placeholder with the actual location."""
    return PROMPT_TEMPLATE.replace("[LOCATION]", location)


def query_gpt5_with_web_search(prompt, location):
    """
    Send a prompt to GPT-5 using the Responses API with web_search enabled.
    Returns the structured response.
    """
    print(f"\n{'='*60}")
    print(f"Searching for companies in: {location}")
    print(f"{'='*60}")

    client = OpenAI()

    try:
        resp = client.responses.create(
            model="gpt-5",  # Use "gpt-5-pro" if you have access
            input=prompt,
            tools=[{"type": "web_search"}],  # Enable built-in web search
        )

        print(f"✓ Received response for {location}")

        # Log web search sources if available
        for item in resp.output:
            if hasattr(item, "type") and item.type == "web_search":
                if hasattr(item, "results"):
                    print(f"  Web sources used: {len(item.results)} sources")

        return resp.output_text

    except Exception as e:
        print(f"✗ Error querying GPT-5 for {location}: {str(e)}")
        return None


def parse_gpt5_response(response_text, location):
    """
    Parse the GPT-5 response and extract company information.
    Expects JSON array format, but handles plain text fallback.
    """
    companies = []

    if not response_text:
        return companies

    # Try to find JSON in the response
    try:
        # Look for JSON array in the response
        start_idx = response_text.find('[')
        end_idx = response_text.rfind(']') + 1

        if start_idx != -1 and end_idx > start_idx:
            json_str = response_text[start_idx:end_idx]
            parsed_data = json.loads(json_str)

            if isinstance(parsed_data, list):
                for company in parsed_data:
                    companies.append({
                        'location': location,
                        'company_name': company.get('company_name', ''),
                        'website': company.get('website', ''),
                        'phone': company.get('phone', ''),
                        'email': company.get('email', ''),
                        'description': company.get('description', ''),
                        'comments': company.get('comments', company.get('description', ''))
                    })

                print(f"  Parsed {len(companies)} companies from JSON")
                return companies

    except json.JSONDecodeError as e:
        print(f"  Could not parse JSON, will save raw response: {str(e)}")

    # Fallback: if JSON parsing fails, save the entire response as one entry
    if not companies:
        companies.append({
            'location': location,
            'company_name': 'See description/comments',
            'website': '',
            'phone': '',
            'email': '',
            'description': response_text[:500] + '...' if len(response_text) > 500 else response_text,
            'comments': 'Full response - manual parsing needed'
        })
        print(f"  Saved raw response (JSON parsing failed)")

    return companies


def write_to_csv(all_companies, filename, mode='w'):
    """Write all company data to a CSV file."""
    if not all_companies:
        return

    fieldnames = ['location', 'company_name', 'website', 'phone', 'email', 'description', 'comments']

    with open(filename, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Only write header if creating new file
        if mode == 'w':
            writer.writeheader()

        for company in all_companies:
            writer.writerow(company)


def append_to_csv(companies, filename):
    """Append new companies to existing CSV file (thread-safe)."""
    if not companies:
        return

    fieldnames = ['location', 'company_name', 'website', 'phone', 'email', 'description', 'comments']

    # Use lock to ensure only one thread writes to CSV at a time
    with csv_lock:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            for company in companies:
                writer.writerow(company)


def process_location(location, idx, total):
    """Process a single location (worker function for parallel execution)."""
    print(f"\n[{idx}/{total}] 🔄 STARTED: {location}")

    # Create the prompt for this location
    prompt = create_prompt_for_location(location)

    # Query GPT-5 with web search
    response_text = query_gpt5_with_web_search(prompt, location)

    # Parse the response
    companies = parse_gpt5_response(response_text, location)

    # Write companies to CSV immediately after each location
    if companies:
        append_to_csv(companies, OUTPUT_CSV)
        print(f"[{idx}/{total}] ✅ COMPLETED: {location} - Found {len(companies)} companies")
    else:
        print(f"[{idx}/{total}] ⚠️  COMPLETED: {location} - No companies found")

    return len(companies) if companies else 0


def main():
    """Main execution function with parallel processing."""
    start_time = datetime.now()

    print("="*70)
    print(f"Property Management Company Finder - PARALLEL MODE ({MAX_WORKERS} workers)")
    print("="*70)
    print(f"Locations to search: {len(LOCATIONS)}")
    print(f"Output file: {OUTPUT_CSV}")
    print(f"Parallel workers: {MAX_WORKERS}")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    # Create CSV file with header
    fieldnames = ['location', 'company_name', 'website', 'phone', 'email', 'description', 'comments']
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    print(f"✓ Created CSV file: {OUTPUT_CSV}\n")

    total_companies = 0
    completed_locations = 0

    # Process locations in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all locations to the thread pool
        future_to_location = {
            executor.submit(process_location, location, idx, len(LOCATIONS)): location
            for idx, location in enumerate(LOCATIONS, 1)
        }

        # Process completed tasks as they finish
        for future in as_completed(future_to_location):
            location = future_to_location[future]
            try:
                num_companies = future.result()
                total_companies += num_companies
                completed_locations += 1

                # Print running summary
                print(f"\n📊 Progress: {completed_locations}/{len(LOCATIONS)} locations completed | {total_companies} total companies found")

            except Exception as e:
                print(f"\n❌ ERROR processing {location}: {str(e)}")
                completed_locations += 1

    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "="*70)
    print(f"✅ COMPLETE! Found {total_companies} total companies across {len(LOCATIONS)} locations")
    print(f"Started:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration}")
    print(f"Results saved to: {OUTPUT_CSV}")
    print("="*70)


if __name__ == "__main__":
    main()
