import pandas as pd
import re

# 1. Load the data
input_path = 'Campaign_1_Leads/leads_all.csv'
output_path = 'Campaign_1_Leads/sorted_cold_call_list_w_zones.csv'

try:
    df = pd.read_csv(input_path)
    print("CSV loaded successfully.")
except FileNotFoundError:
    print(f"Error: Could not find file at {input_path}")
    exit()

# ---------------------------------------------------------
# TIME ZONE DATA (Relative to EST/Pittsburgh)
# ---------------------------------------------------------
# Mapping states to their standard time zones
state_to_tz = {
    # Pacific (-3)
    'WA': 'PST', 'OR': 'PST', 'CA': 'PST', 'NV': 'PST',
    # Mountain (-2)
    'MT': 'MST', 'ID': 'MST', 'WY': 'MST', 'UT': 'MST', 'CO': 'MST', 'AZ': 'MST', 'NM': 'MST',
    # Central (-1)
    'ND': 'CST', 'SD': 'CST', 'NE': 'CST', 'KS': 'CST', 'OK': 'CST', 'TX': 'CST', 
    'MN': 'CST', 'IA': 'CST', 'MO': 'CST', 'AR': 'CST', 'LA': 'CST', 'WI': 'CST', 
    'IL': 'CST', 'TN': 'CST', 'MS': 'CST', 'AL': 'CST',
    # Eastern (0)
    'MI': 'EST', 'IN': 'EST', 'KY': 'EST', 'OH': 'EST', 'WV': 'EST', 'VA': 'EST', 
    'PA': 'EST', 'NY': 'EST', 'VT': 'EST', 'NH': 'EST', 'ME': 'EST', 'MA': 'EST', 
    'RI': 'EST', 'CT': 'EST', 'NJ': 'EST', 'DE': 'EST', 'MD': 'EST', 'DC': 'EST', 
    'NC': 'EST', 'SC': 'EST', 'GA': 'EST', 'FL': 'EST',
    # Others
    'AK': 'AKST', 'HI': 'HST'
}

tz_offsets = {
    'EST': 0,
    'CST': -1,
    'MST': -2,
    'PST': -3,
    'AKST': -4,
    'HST': -5
}

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def get_timezone_info(location_str):
    """
    Parses 'City, ST' to find the state, determines TZ, 
    and calculates offset from EST (Pittsburgh).
    """
    if pd.isna(location_str):
        return "Unknown"
    
    # regex to find 2 letter state code (e.g. UT, FL)
    match = re.search(r'\b([A-Z]{2})\b', str(location_str))
    
    if match:
        state = match.group(1)
        tz = state_to_tz.get(state, 'Unknown')
        
        if tz in tz_offsets:
            offset = tz_offsets[tz]
            if offset == 0:
                return f"{tz} (Same Time)"
            else:
                return f"{tz} ({offset} hrs)"
        return tz
    return "Unknown"

def clean_property_count(val):
    if pd.isna(val): return 0
    numbers = re.findall(r'\d+', str(val))
    return int(numbers[0]) if numbers else 0

def score_job_title(title):
    if pd.isna(title): return 3
    t = str(title).lower()
    
    # Tier 1: Operations/Maintenance (The Pain)
    ops_keywords = ['operation', 'housekeep', 'maintenance', 'inspector', 'quality', 'clean', 'field']
    if any(keyword in t for keyword in ops_keywords): return 1
    
    # Tier 2: Decision Makers
    boss_keywords = ['founder', 'owner', 'president', 'ceo', 'general manager', 'gm', 'director']
    if any(keyword in t for keyword in boss_keywords): return 2
        
    return 3 # Tier 3: Gatekeepers

def determine_company_bucket(count):
    if count >= 100: return "Large/National"
    elif count >= 30: return "Mid-Sized (Sweet Spot)"
    else: return "Small/Boutique"

# ---------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------

# 1. Clean & Score
df['clean_prop_count'] = df['num_properties'].apply(clean_property_count)
df['role_priority'] = df['title'].apply(score_job_title)
df['company_size_bucket'] = df['clean_prop_count'].apply(determine_company_bucket)

# 2. Add Time Zone Info
df['Time_Zone_Diff'] = df['location'].apply(get_timezone_info)

# 3. Add Status Column (Pre-filled)
# Note: CSVs cannot store actual dropdown menus, but this sets up the column for you.
df['Call_Status'] = "Not Contacted"

# 4. SORTING
df_sorted = df.sort_values(
    by=['clean_prop_count', 'casual_company_name', 'role_priority'], 
    ascending=[False, True, True]
)

# 5. Organize Columns
cols_to_keep = [
    'Call_Status',          # Put this first so you can edit it easily
    'Time_Zone_Diff',       # Put this second so you see the time immediately
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

# 6. Save
final_df.to_csv(output_path, index=False)

print(f"Done! Saved to: {output_path}")
print("Added 'Time_Zone_Diff' relative to Pittsburgh.")
print("Added 'Call_Status' column.")