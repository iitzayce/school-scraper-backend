"""
STEP 2: FILTER SCHOOLS (Remove Churches, Camps, Out-of-State Results)
======================================================================
Filter Step 1 output to remove churches, camps, ministries, and any
results that are not clearly Texas-based schools.

Input: CSV from Step 1 with all discovered candidates
Output: CSV containing only Texas schools/academies
"""

import pandas as pd
import argparse
from datetime import datetime
import re

def filter_schools(input_csv: str, output_csv: str):
    """
    Filter out churches, camps, and non-school organizations
        
        Args:
        input_csv: CSV from Step 1
        output_csv: Filtered CSV output
    """
    print("\n" + "="*70)
    print("STEP 2: FILTERING SCHOOLS")
    print("="*70)
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Input: {input_csv}")
    print("="*70 + "\n")
    
    # Read Step 1 results
    df = pd.read_csv(input_csv)
    print(f"Initial schools: {len(df)}")
        
    if 'state' not in df.columns:
        df['state'] = ''
    if 'address' not in df.columns:
        df['address'] = ''

    def is_texas_row(row) -> bool:
        detected_state = str(row.get('detected_state', '')).strip().lower()
        state_field = str(row.get('state', '')).strip().lower()
        address_field = str(row.get('address', '')).upper()

        if detected_state in ('tx', 'texas'):
            return True
        if state_field in ('tx', 'texas'):
            return True
        if ' TEXAS' in address_field or ', TX' in address_field:
            return True
        match = re.search(r',\s*([A-Z]{2})\s+\d{5}', address_field)
        if match and match.group(1) == 'TX':
            return True
        return False

    texas_mask = df.apply(is_texas_row, axis=1)
    non_texas = len(df) - texas_mask.sum()
    if non_texas > 0:
        print(f"Removed {non_texas} non-TX rows based on detected state/address.")
        sample = df[~texas_mask].head(10)
        for _, row in sample.iterrows():
            print(f"  - {row.get('name', 'Unknown')} | {row.get('address', '')}")
    else:
        print("No out-of-state rows detected.")

    df = df[texas_mask].copy()
                
    # Exclusion keywords (churches, camps, etc.)
    exclude_keywords = [
        'church', 'camp', 'ministry', 'fellowship', 'worship center',
        'bible institute', 'seminary', 'mission', 'outreach center',
        'worship', 'pastor', 'minister', 'chapel'
    ]
    
    # School keywords (must have at least one if it also has exclusion keywords)
    school_keywords = [
        'school', 'academy', 'preschool', 'elementary', 'high school',
        'middle school', 'primary school', 'secondary school', 'college',
        'university', 'prep', 'preparatory', 'education'
    ]
    
    # Filter logic
    def is_valid_school(name: str) -> bool:
        """Check if this is a valid school (not just a church/camp)"""
        if pd.isna(name):
            return False
        
        name_lower = str(name).lower()
        
        # Check if it contains exclusion keywords
        has_exclusion = any(keyword in name_lower for keyword in exclude_keywords)
        
        # If it has exclusion keywords, it must also have school keywords
        if has_exclusion:
            has_school_keyword = any(keyword in name_lower for keyword in school_keywords)
            if not has_school_keyword:
                return False  # It's a church/camp, not a school
        
        return True
    
    # Apply filter
    df['is_valid'] = df['name'].apply(is_valid_school)
    df_filtered = df[df['is_valid'] == True].copy()
    df_filtered = df_filtered.drop(columns=['is_valid'])
    
    # Statistics
    excluded = len(df) - len(df_filtered)
    print(f"Filtered out: {excluded} churches/camps/non-schools")
    print(f"Remaining schools: {len(df_filtered)}")
    
    # Show what was excluded
    if excluded > 0:
        df_excluded = df[df['is_valid'] == False]
        print(f"\nExcluded organizations (sample):")
        for idx, row in df_excluded.head(10).iterrows():
            print(f"  - {row['name']} ({row['county']} County)")
        if len(df_excluded) > 10:
            print(f"  ... and {len(df_excluded) - 10} more")
    
    # Save filtered results
    df_filtered.to_csv(output_csv, index=False)
    print(f"\n✅ Filtered schools saved to: {output_csv}")
    print(f"   Schools with websites: {len(df_filtered[df_filtered['website'].notna() & (df_filtered['website'] != '')])}")
        
        print("\n" + "="*70)
    print(f"COMPLETE: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Step 2: Filter Texas schools from Step 1 output")
    parser.add_argument("--input", required=True, help="Input CSV from Step 1")
    parser.add_argument("--output", required=True, help="Output filtered CSV")
    
    args = parser.parse_args()
    filter_schools(args.input, args.output)
    
