"""
STEP 2: FILTER SCHOOLS - STREAMING VERSION
===========================================
Filter single school object - remove churches, camps, out-of-state results.

Input: School object from Step 1
Output: Filtered School object or None (if filtered out)
"""

import re
from typing import Optional
from shared.models import School


# Exclusion keywords (churches, camps, etc.)
EXCLUDE_KEYWORDS = [
    'church', 'camp', 'ministry', 'fellowship', 'worship center',
    'bible institute', 'seminary', 'mission', 'outreach center',
    'worship', 'pastor', 'minister', 'chapel'
]

# School keywords (must have at least one if it also has exclusion keywords)
SCHOOL_KEYWORDS = [
    'school', 'academy', 'preschool', 'elementary', 'high school',
    'middle school', 'primary school', 'secondary school', 'college',
    'university', 'prep', 'preparatory', 'education'
]

# State name to abbreviation mapping
STATE_ABBREVIATIONS = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
    'new_hampshire': 'NH', 'new_jersey': 'NJ', 'new_mexico': 'NM', 'new_york': 'NY',
    'north_carolina': 'NC', 'north_dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
    'oregon': 'OR', 'pennsylvania': 'PA', 'rhode_island': 'RI', 'south_carolina': 'SC',
    'south_dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west_virginia': 'WV',
    'wisconsin': 'WI', 'wyoming': 'WY'
}


def get_state_info(state_name: str) -> tuple:
    """
    Get state abbreviation and normalized name from state name.
    
    Args:
        state_name: State name (e.g., 'texas', 'Texas', 'new_york', 'New York')
        
    Returns:
        tuple: (state_abbrev, normalized_state_name)
    """
    # Normalize: lowercase, replace spaces with underscores
    normalized = state_name.lower().replace(' ', '_')
    abbrev = STATE_ABBREVIATIONS.get(normalized, '')
    
    # Get full state name (capitalize words)
    full_name = normalized.replace('_', ' ').title()
    
    return abbrev, normalized, full_name


def is_state_school(school: School, target_state: str) -> bool:
    """
    Check if school is in the target state.
    
    Args:
        school: School object
        target_state: Target state name (e.g., 'texas', 'california')
        
    Returns:
        True if school is in target state, False otherwise
    """
    state_abbrev, normalized_state, full_state_name = get_state_info(target_state)
    
    detected_state = (school.detected_state or '').strip().lower()
    state_field = (school.state or '').strip().lower()
    address_field = (school.address or '').upper()
    
    # Check detected state
    if detected_state:
        if detected_state == state_abbrev.lower() or detected_state == normalized_state or detected_state == full_state_name.lower():
            return True
    
    # Check state field
    if state_field:
        if state_field == state_abbrev.lower() or state_field == normalized_state or state_field == full_state_name.lower():
            return True
    
    # Check address for state abbreviation
    if state_abbrev and f', {state_abbrev} ' in address_field:
        return True
    if state_abbrev and address_field.endswith(f', {state_abbrev}'):
        return True
    
    # Check address for full state name
    if f' {full_state_name.upper()}' in address_field:
        return True
    
    # Last fallback: look for state abbreviation pattern in address
    if state_abbrev:
        match = re.search(r',\s*([A-Z]{2})\s+\d{5}', address_field)
        if match and match.group(1) == state_abbrev:
            return True
    
    return False


def is_valid_school(school: School) -> bool:
    """Check if this is a valid school (not just a church/camp)"""
    if not school.name:
        return False
    
    name_lower = school.name.lower()
    
    # Check if it contains exclusion keywords
    has_exclusion = any(keyword in name_lower for keyword in EXCLUDE_KEYWORDS)
    
    # If it has exclusion keywords, it must also have school keywords
    if has_exclusion:
        has_school_keyword = any(keyword in name_lower for keyword in SCHOOL_KEYWORDS)
        if not has_school_keyword:
            return False  # It's a church/camp, not a school
    
    return True


def filter_school(school: School, target_state: str = 'texas') -> Optional[School]:
    """
    Filter a single school object.
    Returns the School if it passes filters, None if it should be excluded.
    
    Args:
        school: School object from Step 1
        target_state: Target state to filter for (default: 'texas')
        
    Returns:
        School object if valid, None if filtered out
    """
    # Check state-only
    if not is_state_school(school, target_state):
        return None  # Not in target state
    
    # Check if valid school (not church/camp)
    if not is_valid_school(school):
        return None  # Not a valid school
    
    # Passed all filters
    return school


def filter_schools_generator(schools: list, target_state: str = 'texas') -> list:
    """
    Filter a list of schools (for batch processing compatibility).
    Returns list of filtered schools.
    
    Args:
        schools: List of School objects
        target_state: Target state to filter for (default: 'texas')
        
    Returns:
        List of filtered School objects
    """
    filtered = []
    for school in schools:
        result = filter_school(school, target_state)
        if result:
            filtered.append(result)
    return filtered


