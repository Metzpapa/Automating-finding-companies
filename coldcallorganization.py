import pandas as pd
import re

# 1. Load the data
input_path = 'Campaign_1_Leads/leads_all.csv'
output_path = 'Campaign_1_Leads/sorted_cold_call_list.csv'

try:
    df = pd.read_csv(input_path)
    print("CSV loaded successfully.")
except FileNotFoundError:
    print(f"Error: Could not find file at {input_path}")
    exit()

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def clean_property_count(val):
    """Extracts the first number found in a string (e.g., 'around 20' -> 20)."""
    if pd.isna(val):
        return 0
    # Find all numbers in the string
    numbers = re.findall(r'\d+', str(val))
    if numbers:
        return int(numbers[0])
    return 0

def score_job_title(title):
    """
    Assigns a priority score based on keywords in the job title.
    Score 1 (High): Operations, Housekeeping, Maintenance (Direct Users)
    Score 2 (Med):  Founder, Owner, CEO, GM (Decision Makers)
    Score 3 (Low):  Reservations, Office, Admin (Gatekeepers)
    """
    if pd.isna(title):
        return 3 # Low priority if no title
    
    t = str(title).lower()
    
    # Tier 1: The "Pain" Tier (Best for damage detection pitch)
    ops_keywords = ['operation', 'housekeep', 'maintenance', 'inspector', 'quality', 'clean', 'field']
    if any(keyword in t for keyword in ops_keywords):
        return 1
    
    # Tier 2: The "Boss" Tier
    boss_keywords = ['founder', 'owner', 'president', 'ceo', 'general manager', 'gm', 'director']
    if any(keyword in t for keyword in boss_keywords):
        return 2
        
    # Tier 3: Everyone else
    return 3

def determine_company_bucket(count):
    """Labels the company size for context."""
    if count >= 100:
        return "Large/National"
    elif count >= 30:
        return "Mid-Sized (Sweet Spot)"
    else:
        return "Small/Boutique"

# ---------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------

# 1. Clean the property count column
df['clean_prop_count'] = df['num_properties'].apply(clean_property_count)

# 2. Score the job titles
df['role_priority'] = df['title'].apply(score_job_title)

# 3. Create a descriptive bucket column
df['company_size_bucket'] = df['clean_prop_count'].apply(determine_company_bucket)

# 4. SORTING LOGIC
# Sort by Property Count (Desc) -> Then keep Company Names together -> Then by Role Priority (Ascending, so 1 is first)
df_sorted = df.sort_values(
    by=['clean_prop_count', 'casual_company_name', 'role_priority'], 
    ascending=[False, True, True]
)

# 5. Clean up columns for the export
# We arrange columns so the most important info for the caller is on the left
cols_to_keep = [
    'company_size_bucket',
    'clean_prop_count',
    'casual_company_name',
    'first_name',
    'last_name',
    'title',
    'company_phone',
    'email',
    'location',
    'website',
    'description'
]

final_df = df_sorted[cols_to_keep]

# 6. Save to CSV
final_df.to_csv(output_path, index=False)

print(f"Done! Sorted list saved to: {output_path}")
print("Rows sorted first by Company Size, then identifying Operations leads first.")