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


def is_texas_school(school: School) -> bool:
    """Check if school is in Texas"""
    detected_state = (school.detected_state or '').strip().lower()
    state_field = (school.state or '').strip().lower()
    address_field = (school.address or '').upper()
    
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


def filter_school(school: School) -> Optional[School]:
    """
    Filter a single school object.
    Returns the School if it passes filters, None if it should be excluded.
    
    Args:
        school: School object from Step 1
        
    Returns:
        School object if valid, None if filtered out
    """
    # Check Texas-only
    if not is_texas_school(school):
        return None  # Not in Texas
    
    # Check if valid school (not church/camp)
    if not is_valid_school(school):
        return None  # Not a valid school
    
    # Passed all filters
    return school


def filter_schools_generator(schools: list) -> list:
    """
    Filter a list of schools (for batch processing compatibility).
    Returns list of filtered schools.
    
    Args:
        schools: List of School objects
        
    Returns:
        List of filtered School objects
    """
    filtered = []
    for school in schools:
        result = filter_school(school)
        if result:
            filtered.append(result)
    return filtered


