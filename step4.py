"""
STEP 4: PARSE HTML WITH LLM (WITH INTELLIGENT ROLE FILTERING)
================================================================
Take ALL raw HTML from Step 3 and use GPT to extract and filter contacts.

INTELLIGENT FILTERING via LLM:
- LLM extracts all contacts from the page
- LLM intelligently matches titles to target roles (handles misspellings, variations)
- LLM excludes non-target roles (coaches, teachers, etc.)
- Returns only decision-makers: Principals, Directors, IT, Facilities, Security, Operations, Finance

Validation:
- Name exists and is not empty
- Title exists and is not empty
- Email exists and is not empty
- Role filtering handled by LLM prompt (intelligent matching)

Input: CSV from Step 3 with ALL raw HTML content
Output: CSV with filtered contacts matching target roles (name, title, email, phone)
"""

from openai import OpenAI
import pandas as pd
import json
import time
from typing import List, Dict, Optional
import re
from bs4 import BeautifulSoup
from difflib import SequenceMatcher


class LLMParser:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        """
        Initialize LLM parser
        
        Args:
            api_key: OpenAI API key
            model: Model to use (default: gpt-3.5-turbo)
        """
        self.api_key = api_key
        self.model = model
        self.client = OpenAI(api_key=api_key)
        
        # Target titles to filter for (with fuzzy matching)
        # Focus on decision-makers relevant to security grants (per Nonprofit Security Advisors):
        # - Budget/operations decision-makers (Principal, Superintendent, Director of Operations/Finance)
        # - Facilities/security oversight (Facilities Director, Security Director)
        # - IT infrastructure (IT Director, CTO)
        self.target_titles = [
            # Principal variants (top decision makers)
            'principal', 'superintendent', 'superintendant',  # Note: both spellings
            'assistant principal', 'head of school', 'acting principal',
            'vice principal', 'vice-principal', 'director of schools',
            'secondary principal', 'interim principal', 'school administrator',
            'deputy principal', 'headmaster', 'executive principal',
            'primary principal', 'head of primary', 'lower school principal',
            'head of lower school', 'head of upper school',
            # Operations/Finance (budget decision makers)
            'chief academic officer', 'school director', 'director of operations',
            'director of finance', 'business manager', 'operations director',
            'finance director', 'chief financial officer', 'cfo',
            'director of finance and operations', 'chief operating officer', 'coo',
            # IT/Technology (security infrastructure)
            'information technology director', 'it director', 'technology director',
            'cto', 'chief technology officer', 'director of technology',
            # Facilities (physical security)
            'facilities director', 'facilities manager', 'director of facilities',
            # Security (security oversight)
            'security director', 'security manager', 'director of security', 
            'chief security officer'
        ]
        
        # Titles to EXCLUDE (not relevant to security grants)
        # These roles don't make budget/security decisions for facilities
        self.exclude_titles = [
            # Athletics (not relevant to security grants)
            'athletic director', 'athletics director', 'athletics',
            'coach', 'athletic coach', 'sports director',
            # Admissions (not a decision-maker for security grants)
            'director of admissions', 'admissions director', 'admissions coordinator',
            # Academic roles (curriculum-focused, not operations)
            'academic dean', 'lower school dean', 'upper school dean', 
            'dean of students', 'dean of academics',
            # Activities (not operations)
            'activities director', 'student activities director',
            'student life director',
            # Arts/Curriculum (academic-focused)
            'music director', 'arts director', 'fine arts director', 'art director',
            'curriculum director', 'academic director', 'director of curriculum',
            # Library (academic/operational support, not security)
            'library director', 'librarian',
            # Other non-decision-maker roles
            'teacher', 'instructor', 'guidance counselor', 'counselor'
        ]
    
    def extract_text_from_html(self, html: str, max_length: int = None) -> str:
        """
        Extract clean text from HTML, removing scripts/styles
        NO TRUNCATION - GPT-4o-mini supports 128k tokens (~500k chars)
        Extract full content to ensure nothing is missed
        """
        if not html:
            return ""
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'meta', 'link', 'noscript', 'header', 'footer', 'nav']):
            element.decompose()
        
        # Get all text
        all_text = soup.get_text(separator='\n', strip=True)
        
        # Clean up excessive whitespace
        all_text = re.sub(r'\n\s*\n', '\n\n', all_text)
        all_text = re.sub(r' +', ' ', all_text)
        
        # NO TRUNCATION - return full text
        # GPT-4o-mini supports 128k tokens, which is ~500k characters
        # Most pages are well under this limit
        return all_text
    
    def create_parsing_prompt(self, html_text: str, school_name: str) -> str:
        """
        Create prompt for LLM to extract and intelligently filter contact information
        
        The LLM will:
        1. Extract ALL contacts from the page
        2. Intelligently match titles to target roles (handling variations, misspellings)
        3. Exclude non-target roles
        4. Return only filtered contacts
        """
        
        # Build target roles list for prompt
        target_roles_text = """
TARGET ROLES (Include these - decision-makers for security grants):
- Principals: Principal, Head of School, Superintendent, Superintendant (both spellings), 
  Assistant Principal, Vice Principal, Acting Principal, Interim Principal, 
  Lower/Middle/High School Principal, Headmaster, Executive Principal
- Directors: Director of Operations, Director of Finance, Director of IT/Technology, 
  Director of Facilities, Director of Security, Director of Business, 
  School Director, Director of Schools
- Managers: Facilities Manager, Security Manager, Operations Manager, Business Manager
- Executives: CFO (Chief Financial Officer), COO (Chief Operating Officer), 
  CTO (Chief Technology Officer), Chief Academic Officer
- IT/Technology: IT Director, Technology Director, Director of Technology, 
  IT Manager, IT Assistant (include assistants in IT roles)
- Facilities: Facilities Director, Facilities Manager, Director of Facilities, 
  Maintenance Supervisor (if oversees facilities)
- Security: Security Director, Security Manager, Director of Security, 
  Chief Security Officer
- Operations/Finance: Operations Director, Finance Director, Business Manager, 
  Director of Finance and Operations

Use INTELLIGENT MATCHING:
- Handle misspellings: "Superintendant" = "Superintendent"
- Handle variations: "Head of School" = "Principal", "IT Director" = "Technology Director"
- Handle synonyms: "Facilities Manager" = "Building Manager" (if context suggests facilities)
- Include assistants in IT/Facilities roles: "IT Assistant" (include), "Facilities Assistant" (include)
- Match partial titles: "Director" with "IT" or "Facilities" or "Security" in context
"""
        
        exclude_roles_text = """
EXCLUSION ROLES (Exclude these - not decision-makers for security grants):
- Athletics: Athletic Director, Athletics Director, Coach, Head Coach, Sports Director, 
  Varsity Coach, Assistant Coach, Athletic Coordinator
- Academic: Academic Dean, Dean of Students, Dean of Academics, Curriculum Director, 
  Academic Director, Director of Curriculum
- Admissions: Director of Admissions, Admissions Director, Admissions Coordinator
  (These are NOT decision-makers for security grants)
- Teachers: Teacher, Instructor (unless also has admin title like "Principal")
- Support Staff: Counselor, Guidance Counselor, Librarian, Library Director, 
  Registrar, Receptionist, Aide, Assistant (unless IT/Facilities)
- Arts/Activities: Music Director, Arts Director, Fine Arts Director, 
  Activities Director, Student Activities Director, Student Life Director
- Other: Morning Care, Extended Care (unless Director level)

EXCLUSION RULES:
- If title is ONLY "Teacher", "Coach", "Counselor" → EXCLUDE
- If title contains "Athletic" or "Athletics" → EXCLUDE
- If title contains "Admissions" → EXCLUDE (e.g., "Director of Admissions", "Admissions Director")
- If title is "Assistant" without IT/Facilities context → EXCLUDE
- If title is generic like "Admissions", "Connect", "Administration" (not a person) → EXCLUDE
"""
        
        prompt = f"""You are an expert at extracting and filtering contact information from school websites.

Extract contact information from the following text from {school_name}'s website.

CRITICAL RULE: ZERO GUESSING - ONLY EXTRACT WHAT YOU CAN ACTUALLY SEE

ABSOLUTE PROHIBITIONS (DO NOT VIOLATE THESE):
- DO NOT guess, infer, assume, or create ANY data
- DO NOT match names to titles unless they appear TOGETHER on the page
- DO NOT match names to emails unless they appear TOGETHER on the page
- DO NOT create emails based on name patterns (e.g., "John Smith" → "jsmith@school.edu")
- DO NOT assign titles to names if they are not clearly associated on the page
- DO NOT use placeholder names: "John Doe", "Jane Smith", "Test User", etc.
- DO NOT extract generic page text as names: "About", "Admissions", "Contact Us"
- DO NOT infer relationships - if name and title are on different parts of the page, DO NOT match them
- DO NOT fill in missing data - if email is missing, DO NOT include the contact
- DO NOT use "best guess" or "likely" - ONLY use what is EXPLICITLY visible

STEP 1: EXTRACTION (STRICT RULES)
1. Extract contacts where you can see name and title together (email is OPTIONAL - see below)
2. If an email is visible, it MUST appear on the same line or immediately near the person's name
3. DO NOT create, infer, guess, or assume email addresses - ONLY extract emails that are EXPLICITLY displayed
4. If no email is visible near a person's name, still extract the contact but leave email as empty string ""
5. DO NOT use placeholder names like "John Doe", "John Smith", "Jane Doe", "Bob Jones", "Test User", or any generic names
6. Match each person's name with their ACTUAL title/role that appears IMMEDIATELY NEXT TO or ON THE SAME LINE as their name
7. Name and title MUST appear together - if they are separated, DO NOT match them
8. DO NOT extract generic page text as names. Common mistakes to avoid:
   - Navigation links: "About", "Admissions", "Contact Us", "Home", "Staff Directory"
   - Page headings: "About Us", "School Information", "General Information"
   - Section labels: "Faculty", "Administration", "Our Team"
   - Generic departments: "Admissions", "Athletics", "Administration" (unless it's clearly a person's name)
   - These are NOT person names - they are page elements
9. Person names are typically 2-4 words (first name, middle name/initial, last name)
   - Real examples: "Mary Johnson", "Robert T. Williams", "Dr. Sarah Martinez"
   - NOT: "John Doe", "Jane Smith", "About", "Admissions", "Contact Us"
10. Extract contacts if you can see name and title together - email is OPTIONAL (can be empty string "")

STEP 2: INTELLIGENT ROLE FILTERING
{target_roles_text}

{exclude_roles_text}

FILTERING INSTRUCTIONS:
- Extract contacts from the page where name and title are visible together (email is OPTIONAL)
- For each contact, determine if their title matches TARGET ROLES using intelligent matching
- Handle misspellings, variations, and synonyms intelligently (e.g., "Superintendant" = "Superintendent")
- If title matches EXCLUSION ROLES, exclude that contact
- If title is ambiguous, use context clues (e.g., "Assistant" in IT department = include)
- Return contacts that:
  1. Have name and title visible together on the page (email is OPTIONAL - can be empty string "")
  2. Match TARGET ROLES (using intelligent matching)
  3. Do NOT match EXCLUSION ROLES

STEP 3: OUTPUT FORMAT
Each contact MUST have:
- name: Actual person name visible on the page (NOT placeholder names or generic text)
- title: Their EXACT role from the page (as it appears, must be visible near the name)
- email: Email address if VISIBLY SHOWN on the page (NOT inferred or created, must be visible near the name), otherwise empty string ""
- phone: Phone number if visible, otherwise empty string ""

CRITICAL: Name and title MUST be visible together - email is OPTIONAL (can be empty string "").
CRITICAL: DO NOT guess which email belongs to which person - only match if they appear together.
CRITICAL: DO NOT guess which title belongs to which person - only match if they appear together.
CRITICAL: Extract contacts matching TARGET ROLES even if they don't have visible emails - leave email as "".
Return ONLY valid JSON array format - no markdown, no explanation.

MATCHING RULES (critical for accuracy - STRICT ENFORCEMENT):
- Name and title MUST appear on the SAME LINE or immediately adjacent lines
- If an email is visible, it MUST appear on the SAME LINE or immediately adjacent lines to the name
- If name and title are visible together, extract the contact (email is OPTIONAL - can be empty string "")
- DO NOT create emails based on name patterns (e.g., don't create "jdoe@" from "John Doe")
- DO NOT assign a title to a name if they are not clearly associated on the same line/area
- DO NOT mix up names and titles from different people on the page
- DO NOT assume relationships - only use what is explicitly visible
- If you see a name and title together but no email visible, STILL extract the contact with email as ""
- If you see an email but no name visible near it, DO NOT include that contact
- If you see a name but no title visible near it, DO NOT include that contact

EXAMPLES OF INTELLIGENT MATCHING:
- "Head of School" -> MATCHES (Principal variant)
- "Superintendant" -> MATCHES (Superintendent, misspelling handled)
- "IT Assistant" -> MATCHES (IT role, assistant is OK)
- "Director of Information Technology" -> MATCHES (IT Director variant)
- "Facilities Manager" -> MATCHES (Target role)
- "Athletic Director" -> EXCLUDE (Exclusion role)
- "Varsity Head Coach" -> EXCLUDE (Coach, exclusion role)
- "High School Math Teacher" -> EXCLUDE (Teacher, not admin)
- "IT Director" -> MATCHES (Target role)
- "Principal" -> MATCHES (Target role)

Example output format (use REAL names and emails from the page, not these examples):
[
  {{"name": "Actual Name From Page", "title": "Principal", "email": "real.email@school.edu", "phone": "555-123-4567"}},
  {{"name": "Another Real Name", "title": "IT Director", "email": "another.real@school.edu", "phone": ""}}
]

Website text:
{html_text}

FINAL CHECKLIST - Before including any contact, verify:
1. Name is visible on the page (not generic text, not placeholder)
2. Title is visible on the page (near the name, on same line or adjacent)
3. Email is OPTIONAL - if visible, it must be near the name (on same line or adjacent), otherwise use empty string ""
4. Name and title appear together in the same area (email is OPTIONAL)
5. Title matches TARGET ROLES (using intelligent matching)
6. Title does NOT match EXCLUSION ROLES
7. You did NOT guess, infer, or create any of the data

Return ONLY the JSON array with contacts that meet the above criteria.
Extract contacts matching TARGET ROLES even if they don't have visible emails - use empty string "" for email."""
        
        return prompt
    
    def parse_with_llm(self, html_text: str, school_name: str, max_retries: int = 3) -> List[Dict]:
        """
        Send HTML text to LLM and parse response
        
        Returns:
            List of contact dictionaries
        """
        for attempt in range(max_retries):
            try:
                prompt = self.create_parsing_prompt(html_text, school_name)
                
                # Use new OpenAI client API
                # GPT-4o-mini supports 128k tokens for output - use generous limits
                # Estimate tokens: ~4 characters per token
                estimated_input_tokens = len(html_text) // 4
                estimated_contacts = html_text.count('@')  # Rough estimate
                
                # Set max_tokens very high to ensure nothing is truncated
                # Allow up to 32k tokens for response (well within 128k limit)
                if estimated_contacts > 50 or estimated_input_tokens > 10000:
                    max_tokens = 32000  # Very large pages - maximum response size
                elif estimated_contacts > 20 or estimated_input_tokens > 5000:
                    max_tokens = 16000  # Large pages
                else:
                    max_tokens = 8000  # Normal pages - still generous
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a data extraction assistant. Return only valid JSON. Match names to their correct titles."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0,  # Zero temperature for maximum accuracy, no creativity
                    max_tokens=max_tokens
                )
                
                # Extract response text
                response_text = response.choices[0].message.content.strip()
                
                # Clean up response (remove markdown code blocks if present)
                response_text = re.sub(r'^```json\s*', '', response_text)
                response_text = re.sub(r'^```\s*', '', response_text)
                response_text = re.sub(r'\s*```$', '', response_text)
                
                # Parse JSON
                contacts = json.loads(response_text)
                
                # Validate structure
                if not isinstance(contacts, list):
                    print(f"      WARNING: Response not a list: {type(contacts)}")
                    return []
                
                # Validate contacts (only email/name validation, NO title filtering)
                valid_contacts = []
                invalid_count = 0
                for contact in contacts:
                    if self._is_valid_contact(contact):
                        valid_contacts.append(contact)
                    else:
                        invalid_count += 1
                
                if invalid_count > 0:
                    print(f"      (Removed {invalid_count} contacts - missing name, title, or email)")
                
                return valid_contacts
                
            except json.JSONDecodeError as e:
                if attempt < max_retries - 1:
                    print(f"      WARNING: JSON parse error (attempt {attempt + 1}/{max_retries}), retrying...")
                    time.sleep(2)
                    continue
                print(f"      ERROR: JSON parse error: {e}")
                print(f"      Response length: {len(response_text)} chars")
                print(f"      Response preview: {response_text[:300]}")
                
                # Try to recover partial JSON (improved recovery)
                try:
                    # Strategy 1: Find last complete array closing bracket
                    last_complete = response_text.rfind(']')
                    if last_complete > 0:
                        partial_json = response_text[:last_complete + 1]
                        # Try to find and close any incomplete objects
                        open_braces = partial_json.count('{')
                        close_braces = partial_json.count('}')
                        if open_braces > close_braces:
                            # Add missing closing braces
                            partial_json += '}' * (open_braces - close_braces)
                        try:
                            contacts = json.loads(partial_json)
                            if isinstance(contacts, list) and len(contacts) > 0:
                                print(f"      WARNING: Recovered {len(contacts)} contacts from partial JSON")
                                # Validate (only email/name, NO title filtering)
                                valid_contacts = [c for c in contacts if self._is_valid_contact(c)]
                                if valid_contacts:
                                    return valid_contacts
                        except:
                            pass
                    
                    # Strategy 2: Extract individual complete JSON objects
                    # Find all complete { ... } objects
                    object_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                    matches = re.findall(object_pattern, response_text)
                    if matches:
                        recovered_contacts = []
                        for match in matches:
                            try:
                                obj = json.loads(match)
                                if isinstance(obj, dict) and 'name' in obj and 'email' in obj:
                                    recovered_contacts.append(obj)
                            except:
                                continue
                        if recovered_contacts:
                            print(f"      WARNING: Recovered {len(recovered_contacts)} contacts from individual objects")
                            # Validate (only email/name, NO title filtering)
                            valid_contacts = [c for c in recovered_contacts if self._is_valid_contact(c)]
                            if valid_contacts:
                                return valid_contacts
                except Exception as recovery_error:
                    pass  # Recovery failed, return empty
                
                return []
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"      WARNING: LLM error (attempt {attempt + 1}/{max_retries}): {e}, retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                print(f"      ERROR: LLM error: {e}")
                return []
        
        return []  # All retries failed
    
    def _similarity(self, a: str, b: str) -> float:
        """
        Calculate similarity ratio between two strings (0.0 to 1.0)
        Uses SequenceMatcher for fuzzy matching
        """
        return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()
    
    def _matches_target_title(self, title: str, threshold: float = 0.7) -> bool:
        """
        Check if title matches any target title using fuzzy matching
        EXCLUDES titles not relevant to security grants (athletic, academic dean, etc.)
        
        Uses multiple strategies:
        1. Exclusion check (filter out non-relevant titles first)
        2. Exact substring match (fastest)
        3. Fuzzy similarity matching (70% similarity)
        4. Keyword-based matching for variations
        
        Args:
            title: Contact's title to check
            threshold: Similarity threshold (0.0 to 1.0), default 0.7 (70% similar)
        
        Returns:
            True if title matches any target title (fuzzy) AND is not excluded
        """
        if not title:
            return False
        
        title_lower = title.lower().strip()
        
        # EXCLUSION CHECK: Filter out non-relevant titles first
        # These are NOT decision-makers for security grants
        for exclude in self.exclude_titles:
            exclude_lower = exclude.lower()
            # Check exact match or if excluded title is in the title
            if exclude_lower == title_lower or exclude_lower in title_lower or title_lower in exclude_lower:
                return False
            # Fuzzy match for exclusions (catch variations)
            similarity = self._similarity(title, exclude)
            if similarity >= 0.8:  # High threshold for exclusions (avoid false positives)
                return False
        
        # Strategy 1: Exact substring match (fastest)
        # Check if any target title is contained in the title or vice versa
        # Handle comma-separated titles (e.g., "Head of School, Principal")
        title_parts = [part.strip() for part in title_lower.split(',')]
        for target in self.target_titles:
            target_lower = target.lower()
            # Check if target matches any part of the title (handles comma-separated)
            for part in title_parts:
                if target_lower in part or part in target_lower:
                    return True
            # Also check full title match
            if target_lower in title_lower or title_lower in target_lower:
                return True
        
        # Strategy 2: Fuzzy similarity matching for variations
        # Catches slight differences like "Superintendent" vs "Superintendant"
        for target in self.target_titles:
            similarity = self._similarity(title, target)
            if similarity >= threshold:
                return True
        
        # Strategy 3: Keyword-based matching for complex titles
        # (e.g., "Director of Information Technology" should match "IT Director")
        # Extract key words from title
        title_words = set(re.findall(r'\b\w+\b', title_lower))
        
        # Check if title contains multiple relevant keywords
        principal_keywords = ['principal', 'superintendent', 'superintendant', 'headmaster', 
                             'head', 'administrator', 'director', 'school']
        it_keywords = ['it', 'technology', 'tech', 'cto', 'information', 'computer', 'systems']
        facilities_keywords = ['facilities', 'facility', 'maintenance', 'building', 'operations']
        security_keywords = ['security', 'safety', 'protection']
        operations_keywords = ['operations', 'finance', 'business', 'financial']
        
        # Principal/admin matching (BUT exclude "dean" unless it's operational)
        if any(kw in title_lower for kw in principal_keywords[:4]):  # principal, superintendent, headmaster, head
            # Skip if it's an academic dean
            if 'academic dean' in title_lower or 'dean of students' in title_lower:
                # Only include if it's clearly operational (dean of students might be relevant)
                if 'dean of students' not in title_lower:
                    return False
            if any(kw in title_lower for kw in ['principal', 'superintendent', 'headmaster', 'head of school', 
                                                 'director', 'administrator', 'school']):
                return True
        
        # IT matching
        if any(kw in title_lower for kw in it_keywords[:3]):  # it, technology, tech
            if 'director' in title_lower or 'manager' in title_lower or 'officer' in title_lower:
                return True
        
        # Facilities matching
        if any(kw in title_lower for kw in facilities_keywords):
            if 'director' in title_lower or 'manager' in title_lower:
                return True
        
        # Security matching
        if any(kw in title_lower for kw in security_keywords):
            if 'director' in title_lower or 'manager' in title_lower or 'officer' in title_lower:
                return True
        
        # Operations/Finance matching (budget decision makers)
        if any(kw in title_lower for kw in operations_keywords):
            if 'director' in title_lower or 'manager' in title_lower or 'officer' in title_lower:
                return True
        
        return False
    
    def _is_valid_contact(self, contact: Dict) -> bool:
        """
        Validate contact - check that name and title exist (email is optional)
        NO email format validation - let GPT prompt handle what's valid
        NO title filtering - extract all contacts the LLM finds
        """
        required_fields = ['name', 'title']

        # Check required fields exist
        if not all(field in contact for field in required_fields):
            return False

        # Check name and title are non-empty (email is optional)
        if not contact.get('name') or not contact.get('title'):
            return False

        # Email is optional - can be empty string "" for contacts without visible emails
        # NO EMAIL FORMAT VALIDATION - GPT prompt handles what's valid
        # NO TITLE FILTERING - accept all contacts with name and title
        return True
    
    def parse_pages(self, input_csv: str, output_csv: str, output_no_emails_csv: str = None):
        """
        Parse all pages from Step 3 using LLM
        
        Args:
            input_csv: CSV from Step 3 with HTML content
            output_csv: Output CSV with parsed contacts that HAVE emails
            output_no_emails_csv: Output CSV with parsed contacts WITHOUT emails (for enrichment)
        """
        print("\n" + "="*70)
        print("STEP 4: PARSING HTML WITH LLM")
        print("="*70)
        print(f"Model: {self.model}")
        
        # Read crawled pages
        df = pd.read_csv(input_csv)
        
        # Step 3 outputs: school_name, url, html_content, fetch_method, email_count, has_emails
        # Filter to pages with content (process ALL pages with HTML)
        df_success = df[df['html_content'].notna() & (df['html_content'] != '')].copy()
        
        print(f"Processing {len(df_success)} pages")
        print("="*70 + "\n")
        
        all_contacts = []
        pages_processed = 0
        pages_with_contacts = 0
        total_contacts = 0
        
        for idx, row in df_success.iterrows():
            school_name = row['school_name']
            url = row['url']
            html_content = row['html_content'] if 'html_content' in row else ''
            
            print(f"\n[{idx + 1}/{len(df_success)}] {school_name}")
            print(f"  URL: {url[:60]}...")
            
            # Extract text from HTML
            html_text = self.extract_text_from_html(html_content)
            
            if not html_text:
                print(f"    WARNING: No text extracted")
                continue
            
            print(f"    Extracted {len(html_text)} chars of text")
            print(f"    Sending to {self.model}...")
            
            # Parse with LLM
            contacts = self.parse_with_llm(html_text, school_name)
            
            pages_processed += 1
            
            if contacts:
                pages_with_contacts += 1
                total_contacts += len(contacts)
                print(f"    Found {len(contacts)} contacts")
                
                # Add school and URL to each contact
                for contact in contacts:
                    contact['school_name'] = school_name
                    contact['source_url'] = url
                    all_contacts.append(contact)
            else:
                print(f"    - No valid contacts found")
            
            # Save progress every 10 pages (temporary - will split at end)
            if pages_processed % 10 == 0:
                # Temporarily save all contacts together
                temp_contacts_with = [c for c in all_contacts if c.get('email', '').strip()]
                temp_contacts_without = [c for c in all_contacts if not c.get('email', '').strip()]
                if temp_contacts_with:
                    self._save_results(temp_contacts_with, output_csv)
                if output_no_emails_csv and temp_contacts_without:
                    self._save_results(temp_contacts_without, output_no_emails_csv)
                print(f"\n  Progress saved: {total_contacts} contacts from {pages_processed} pages")
            
            # Minimal rate limiting - GPT-4o-mini has high rate limits
            # Only small delay to avoid overwhelming the API
            time.sleep(0.5)
        
        # Split contacts into two groups: with emails and without emails
        contacts_with_emails = []
        contacts_without_emails = []
        
        for contact in all_contacts:
            email = contact.get('email', '').strip()
            if email and email != '':
                contacts_with_emails.append(contact)
            else:
                contacts_without_emails.append(contact)
        
        # Save contacts with emails
        if contacts_with_emails:
            self._save_results(contacts_with_emails, output_csv)
        
        # Save contacts without emails (for enrichment)
        if output_no_emails_csv and contacts_without_emails:
            self._save_results(contacts_without_emails, output_no_emails_csv)
        
        # Print summary
        self._print_summary(all_contacts, output_csv, pages_processed, pages_with_contacts, 
                          contacts_with_emails, contacts_without_emails, output_no_emails_csv)
    
    def _save_results(self, contacts: List[Dict], filename: str):
        """Save parsed contacts to CSV"""
        if not contacts:
            return
        
        df = pd.DataFrame(contacts)
        
        # Reorder columns
        column_order = ['school_name', 'name', 'title', 'email', 'phone', 'source_url']
        df = df[column_order]
        
        df.to_csv(filename, index=False)
    
    def _print_summary(self, contacts: List[Dict], output_file: str, 
                       pages_processed: int, pages_with_contacts: int,
                       contacts_with_emails: List[Dict], contacts_without_emails: List[Dict],
                       output_no_emails_csv: str = None):
        """Print parsing summary"""
        if not contacts:
            print("\nERROR: No contacts extracted")
            return
        
        df = pd.DataFrame(contacts)
        
        print("\n" + "="*70)
        print("PARSING COMPLETE")
        print("="*70)
        print(f"Pages processed: {pages_processed}")
        print(f"Pages with contacts: {pages_with_contacts} ({pages_with_contacts/pages_processed*100:.1f}%)")
        print(f"Total contacts extracted: {len(df)}")
        print(f"  - Contacts WITH emails: {len(contacts_with_emails)}")
        print(f"  - Contacts WITHOUT emails: {len(contacts_without_emails)}")
        print(f"Unique schools: {df['school_name'].nunique()}")
        print(f"\nAverage contacts per page: {len(df)/pages_processed:.2f}")
        print(f"Contacts with phone numbers: {df['phone'].ne('').sum()} ({df['phone'].ne('').sum()/len(df)*100:.1f}%)")
        print(f"\nOutput file (with emails): {output_file}")
        if output_no_emails_csv:
            print(f"Output file (without emails, for enrichment): {output_no_emails_csv}")
        print("="*70)
        
        # Show top schools
        print("\nTop 10 schools by contacts found:")
        top_schools = df.groupby('school_name').size().sort_values(ascending=False).head(10)
        for school, count in top_schools.items():
            print(f"  {school[:40]:40} | {count} contacts")
        
        # Show title distribution
        print("\nTitle distribution:")
        title_counts = df['title'].value_counts().head(10)
        for title, count in title_counts.items():
            print(f"  {title[:40]:40} | {count}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse HTML with LLM to extract contacts')
    parser.add_argument('--input', required=True, help='Input CSV from Step 3')
    parser.add_argument('--output', default='step4_contacts_with_emails.csv', help='Output CSV filename for contacts WITH emails')
    parser.add_argument('--output-no-emails', default='step4_contacts_no_emails.csv', help='Output CSV filename for contacts WITHOUT emails (for enrichment)')
    parser.add_argument('--api-key', required=True, help='OpenAI API key')
    parser.add_argument('--model', default='gpt-4o-mini', help='Model to use (default: gpt-4o-mini)')
    args = parser.parse_args()
    
    parser = LLMParser(api_key=args.api_key, model=args.model)
    parser.parse_pages(args.input, args.output, args.output_no_emails)
