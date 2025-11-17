"""
STEP 4: PARSE HTML WITH LLM
============================
Take ALL raw HTML from Step 3 and use GPT to extract contacts,
then filter for target titles using fuzzy matching.

Extracts ALL contacts, then filters for:
- Principal variants (Principal, Head of School, Superintendent, etc.)
- IT roles (IT Director, CTO, Technology Director, etc.)
- Facilities roles (Facilities Director, Facilities Manager, etc.)
- Security roles (Security Director, Security Manager, etc.)

Uses fuzzy matching to catch slight variations (e.g., "Superintendent" vs "Superintendant")

Input: CSV from Step 3 with ALL raw HTML content
Output: CSV with filtered contacts matching target titles (name, title, email, phone)
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
    def __init__(self, api_key: str, model: str = "gpt-3.5-turbo"):
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
    
    def extract_text_from_html(self, html: str, max_length: int = 8000) -> str:
        """
        Extract clean text from HTML, removing scripts/styles
        Limit to max_length characters to avoid token limits
        """
        if not html:
            return ""
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'meta', 'link', 'noscript', 'header', 'footer', 'nav']):
            element.decompose()
        
        # Get text
        text = soup.get_text(separator='\n', strip=True)
        
        # Clean up excessive whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        
        # Truncate if too long
        if len(text) > max_length:
            text = text[:max_length] + "..."
        
        return text
    
    def create_parsing_prompt(self, html_text: str, school_name: str) -> str:
        """
        Create prompt for LLM to extract contact information
        
        IMPROVED PROMPT to fix title mismatch and generic text issues:
        - Emphasize matching names with their CORRECT titles
        - Extract actual person names, not generic page text
        - Be more explicit about structure matching
        """
        prompt = f"""You are an expert at extracting contact information from school websites.

Extract ALL contacts (people with names, titles, and emails) from the following text from {school_name}'s website.

CRITICAL INSTRUCTIONS - READ CAREFULLY:
1. Extract EVERY contact you find - do NOT filter by role (include all: teachers, staff, admin, coaches, etc.)
2. Match each person's name with their ACTUAL title/role that appears IMMEDIATELY NEXT TO or NEAR their name
3. DO NOT assign generic titles like "principal" to people who are not principals - only use the EXACT title that appears with that person's name
4. DO NOT extract generic page text as names. Common mistakes to avoid:
   - Navigation links: "About", "Admissions", "Contact Us", "Home", "Staff Directory"
   - Page headings: "About Us", "School Information", "General Information"
   - Section labels: "Faculty", "Administration", "Our Team"
   - These are NOT person names - they are page elements
5. Only extract actual person names - look for patterns like:
   - "John Smith, Principal" (name followed by comma and title)
   - "Principal: Jane Doe" (title followed by colon and name)
   - "Jane Doe - Director of Finance" (name followed by dash and title)
   - Email addresses paired with a name (e.g., "jsmith@school.edu" near "John Smith")
6. Person names are typically 2-4 words (first name, middle name/initial, last name)
   - Examples: "John Smith", "Mary Jane Doe", "Dr. Robert Jones"
   - NOT: "About", "Admissions", "Contact Us", "Staff Information"
7. Include ALL roles: teachers, coaches, counselors, administrators, staff, principals, directors, etc.
8. Each contact MUST have: name (actual person name), title (their EXACT role from the page), email
9. If phone is not found, use empty string ""
10. Ensure email addresses are valid format
11. Return ONLY valid JSON array format - no markdown, no explanation

MATCHING RULES (critical for accuracy):
- Look for names and titles that appear TOGETHER on the same line or nearby
- If you see "John Smith, Principal" → name: "John Smith", title: "Principal"
- If you see "Principal: Jane Doe" → name: "Jane Doe", title: "Principal"
- If you see "Bob Jones - Basketball Coach" → name: "Bob Jones", title: "Basketball Coach"
- DO NOT assign a title to a name if they are not clearly associated
- DO NOT mix up names and titles from different people on the page

COMMON MISTAKES TO AVOID:
- Extracting page navigation as names: "About", "Contact", "Staff Directory" ❌
- Extracting section headings as names: "Our Team", "Faculty", "Administration" ❌
- Assigning wrong titles: If page shows "Director of Finance: John Smith", don't assign "Principal" ❌
- Extracting generic phrases: "about sl and admissions", "general information", etc. ❌

Example output format:
[
  {{"name": "John Smith", "title": "Principal", "email": "jsmith@school.edu", "phone": "555-123-4567"}},
  {{"name": "Jane Doe", "title": "Teacher", "email": "jdoe@school.edu", "phone": ""}},
  {{"name": "Bob Jones", "title": "Basketball Coach", "email": "bjones@school.edu", "phone": "555-987-6543"}}
]

Website text:
{html_text}

Return ONLY the JSON array:"""
        
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
                # Estimate tokens needed: ~50 tokens per contact, allow up to 4000 tokens for large pages
                # Detect if this is a large page (many emails) and increase max_tokens
                estimated_contacts = html_text.count('@')  # Rough estimate
                max_tokens = 4000 if estimated_contacts > 20 else 2000
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a data extraction assistant. Return only valid JSON. Match names to their correct titles."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
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
                    print(f"      ⚠️  Response not a list: {type(contacts)}")
                    return []
                
                # Validate and filter contacts by target titles (fuzzy matching)
                valid_contacts = []
                filtered_count = 0
                for contact in contacts:
                    if self._is_valid_contact(contact):
                        valid_contacts.append(contact)
                    else:
                        filtered_count += 1
                
                if filtered_count > 0:
                    print(f"      (Filtered {filtered_count} contacts - didn't match target titles)")
                
                return valid_contacts
                
            except json.JSONDecodeError as e:
                if attempt < max_retries - 1:
                    print(f"      ⚠️  JSON parse error (attempt {attempt + 1}/{max_retries}), retrying...")
                    time.sleep(2)
                    continue
                print(f"      ❌ JSON parse error: {e}")
                print(f"      Response length: {len(response_text)} chars")
                print(f"      Response preview: {response_text[:300]}")
                
                # Try to recover partial JSON (find complete objects before the error)
                try:
                    # Find the last complete JSON object before the error
                    # Look for closing braces and brackets
                    last_complete = response_text.rfind(']')
                    if last_complete > 0:
                        # Try to parse up to the last complete bracket
                        partial_json = response_text[:last_complete + 1]
                        contacts = json.loads(partial_json)
                        if isinstance(contacts, list) and len(contacts) > 0:
                            print(f"      ⚠️  Recovered {len(contacts)} contacts from partial JSON")
                            # Validate and filter
                            valid_contacts = [c for c in contacts if self._is_valid_contact(c)]
                            return valid_contacts
                except:
                    pass  # Recovery failed, return empty
                
                return []
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"      ⚠️  LLM error (attempt {attempt + 1}/{max_retries}): {e}, retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                print(f"      ❌ LLM error: {e}")
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
        for target in self.target_titles:
            target_lower = target.lower()
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
        Validate contact and filter by target titles using fuzzy matching
        """
        required_fields = ['name', 'title', 'email']
        
        # Check required fields exist
        if not all(field in contact for field in required_fields):
            return False
        
        # Check name and title are non-empty
        if not contact['name'] or not contact['title']:
            return False
        
        # Check email format (basic validation)
        email = contact.get('email', '')
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
        if not re.match(email_pattern, email):
            return False
        
        # Filter by target titles using fuzzy matching
        if not self._matches_target_title(contact['title']):
            return False
        
        return True
    
    def parse_pages(self, input_csv: str, output_csv: str):
        """
        Parse all pages from Step 3 using LLM
        
        Args:
            input_csv: CSV from Step 3 with HTML content
            output_csv: Output CSV with parsed contacts
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
                print(f"    ⚠️  No text extracted")
                continue
            
            print(f"    Extracted {len(html_text)} chars of text")
            print(f"    Sending to {self.model}...")
            
            # Parse with LLM
            contacts = self.parse_with_llm(html_text, school_name)
            
            pages_processed += 1
            
            if contacts:
                pages_with_contacts += 1
                total_contacts += len(contacts)
                print(f"    ✓ Found {len(contacts)} contacts")
                
                # Add school and URL to each contact
                for contact in contacts:
                    contact['school_name'] = school_name
                    contact['source_url'] = url
                    all_contacts.append(contact)
            else:
                print(f"    - No valid contacts found")
            
            # Save progress every 10 pages
            if pages_processed % 10 == 0:
                self._save_results(all_contacts, output_csv)
                print(f"\n  💾 Progress saved: {total_contacts} contacts from {pages_processed} pages")
            
            # Rate limiting to avoid API throttling
            time.sleep(1)
        
        # Final save
        self._save_results(all_contacts, output_csv)
        
        # Print summary
        self._print_summary(all_contacts, output_csv, pages_processed, pages_with_contacts)
    
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
                       pages_processed: int, pages_with_contacts: int):
        """Print parsing summary"""
        if not contacts:
            print("\n❌ No contacts extracted")
            return
        
        df = pd.DataFrame(contacts)
        
        print("\n" + "="*70)
        print("PARSING COMPLETE")
        print("="*70)
        print(f"Pages processed: {pages_processed}")
        print(f"Pages with contacts: {pages_with_contacts} ({pages_with_contacts/pages_processed*100:.1f}%)")
        print(f"Total contacts extracted: {len(df)}")
        print(f"Unique schools: {df['school_name'].nunique()}")
        print(f"\nAverage contacts per page: {len(df)/pages_processed:.2f}")
        print(f"Contacts with phone numbers: {df['phone'].ne('').sum()} ({df['phone'].ne('').sum()/len(df)*100:.1f}%)")
        print(f"\nOutput file: {output_file}")
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
    parser.add_argument('--output', default='parsed_contacts.csv', help='Output CSV filename')
    parser.add_argument('--api-key', required=True, help='OpenAI API key')
    parser.add_argument('--model', default='gpt-3.5-turbo', help='Model to use (default: gpt-3.5-turbo)')
    args = parser.parse_args()
    
    parser = LLMParser(api_key=args.api_key, model=args.model)
    parser.parse_pages(args.input, args.output)
