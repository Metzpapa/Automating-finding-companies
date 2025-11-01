# Contact Finder Script

This script enriches your `companies.csv` file with contact information for outreach.

## What it does

The script takes your existing `companies.csv` (with company names, websites, locations, and descriptions) and uses GPT-5 with web search to find:

- **Contact Email**: The best person's email for outreach
- **First Name**: Contact's first name
- **Last Name**: Contact's last name
- **Title**: Their job title (e.g., CEO, Owner, Director of Operations)
- **Number of Properties**: How many properties they manage (or "over X" if exact number unavailable)

The original company data (location, company_name, website, phone, email, description) is preserved in the output.

## Setup

1. Make sure you have a `.env` file with your OpenAI API key:
   ```
   OPENAI_API_KEY=your_api_key_here
   ```

2. Ensure you have Python dependencies installed (already done if you ran the original script)

## Usage

### Basic Usage

1. Place your companies CSV file as `companies.csv` in this directory

2. Run the script:
   ```bash
   python find_contacts.py
   ```

3. Results will be saved to `companies_with_contacts.csv`

### Customizing the Script

Open `find_contacts.py` and edit the **CONFIGURATION SECTION** at the top:

#### Change Input/Output Files
```python
INPUT_CSV = "companies.csv"        # Your input file
OUTPUT_CSV = "companies_with_contacts.csv"  # Output file
```

#### Customize the Prompt
The `CONTACT_PROMPT_TEMPLATE` is where you can modify what GPT-5 searches for. You can:
- Change the tone of the outreach context
- Request additional fields
- Modify the search criteria for contacts

Example of the prompt template:
```python
CONTACT_PROMPT_TEMPLATE = """I need to find the best contact person at {company_name}...
Please find:
1. The best contact person for this outreach...
2. Their first name
...
"""
```

#### Adjust Parallelization
```python
MAX_WORKERS = 5  # Number of companies to process simultaneously
```

## Output Format

The output CSV (`companies_with_contacts.csv`) includes:

**Original Fields:**
- location
- company_name
- website
- phone
- email
- description

**New Contact Fields:**
- contact_email
- contact_first_name
- contact_last_name
- contact_title
- num_properties

## Example Output

| company_name | location | contact_email | contact_first_name | contact_last_name | contact_title | num_properties |
|--------------|----------|---------------|-------------------|-------------------|---------------|----------------|
| Maui Resort Rentals | Lahaina, HI | john@example.com | John | Smith | CEO | over 100 |

## Tips

1. **Test First**: Run on a small subset of companies first to verify the prompt works as expected
2. **Monitor Costs**: GPT-5 with web search can be expensive - monitor your API usage
3. **Customize Prompts**: The better your prompt, the better the contact data you'll get
4. **Parallel Processing**: Adjust MAX_WORKERS based on your API rate limits

## Using the Data for Email Outreach

The enriched CSV can be used with email merge tools. You'll have personalized fields like:
- "Hi {contact_first_name},"
- "I saw you manage {num_properties} properties..."
- "As the {contact_title} at {company_name}..."

This makes your outreach feel personal and researched rather than generic spam.
