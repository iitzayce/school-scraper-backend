"""
STEP 4: PARSE HTML WITH LLM (ALL FILTERING BY LLM)
================================================================
Take ALL raw HTML from Step 3 and use GPT to extract contacts.

IMPORTANT: NO FILTERING IN PYTHON CODE - ALL FILTERING DONE BY LLM
- HTML reduction and chunking only (pre-processing)
- LLM does all title filtering, exclusion logic, etc.
- Extract contacts with AND without emails

Input: CSV from Step 3 with ALL raw HTML content
Output: CSV with contacts (name, title, email, phone) - LLM filtered
"""

from openai import OpenAI
import pandas as pd
import csv
import io
import time
from typing import List, Dict, Optional
import re
from bs4 import BeautifulSoup


# New CSV-based prompt (from ChatGPT conversation)
ADMIN_CONTACT_EXTRACTION_PROMPT = """You are an information-extraction assistant helping a web scraper for Nonprofit Security Advisors.

INPUT
- You receive:
  - The school or organization name (metadata, not a person).
  - The page URL (metadata, not a person).
  - The FULL HTML of a single web page OR a contiguous CHUNK of that HTML from a single page.
- The HTML may contain tables, lists, "staff" or "faculty & leadership" pages, sidebars, footers, etc.
- Treat each call independently: only use the HTML text you are given in this call.

GOAL
From ONLY the information actually present in the provided HTML (or HTML chunk), extract SCHOOL ADMINISTRATIVE CONTACTS and return them as clean CSV-style data with the following columns, in this exact order:

First Name,Last Name,Title,Email,Phone

CRITICAL: EXCLUSION TAKES ABSOLUTE PRIORITY
Before extracting ANY contact, you MUST check if their title contains ANY exclusion keyword. If it does, DO NOT extract that contact, regardless of what other words appear in the title. Exclusion rules override inclusion rules completely.

REQUIREMENTS & RULES

1. GENERAL BEHAVIOR
- Read and analyze ALL relevant parts of the provided HTML (tables, cards, lists, headings, mailto: links, tel: links, etc.).
- Do NOT invent or guess any data that is not clearly present in this HTML chunk.
- Keep each person's name, title, email, and phone correctly matched.
- Return only real people (no departments, offices, generic inboxes, or anonymous contacts).
- If a field is missing, leave it EMPTY but keep the comma (so each row always has 5 columns).
- If a contact appears to be cut off or incomplete because the chunk ends in the middle of their data, IGNORE that partial contact and do not output them.

2. WHO TO INCLUDE (ADMINISTRATIVE CONTACTS ONLY)
Include a contact if their role is clearly ADMINISTRATIVE in a K–12 or higher-ed context.

POSITIVELY include titles that clearly indicate school leadership or administration, including but not limited to (case-insensitive, including variations and minor typos):
- principal, acting principal, interim principal, primary principal, secondary principal
- assistant principal, associate principal, vice principal, vice-principal, deputy principal
- superintendent, superintendant
- head of school, headmaster, headmistress
- school administrator, school administration, academic administrator
- director of schools, school director
- chief academic officer, academic dean, dean of students (only if obviously an admin role)
- executive principal, executive director (of the school or district)
- lower school principal, head of lower school, lower school dean
- middle school principal, head of middle school, middle school dean
- upper school principal, head of upper school, upper school dean
- primary school principal, head of primary, elementary principal, high school principal
- president, school president, college president, university president
- chancellor, provost, chief operating officer, chief executive officer (when clearly a school/org leader)

ALSO include the following administrative / operational titles (and close variants):
- Information Technology Director, Director of Technology, IT Director, Technology Manager
- CTO, Chief Technology Officer
- Facilities Director, Director of Facilities, Facilities Manager, Facility Manager, Plant Operations (when clearly facilities/operations)
- Security Director, Director of Security, Security Manager, Campus Security, Safety & Security (when clearly security/safety leadership)
- Other "Director of …" roles that clearly relate to school-wide administration or operations
  (e.g., Director of Communications, Director of Family Outreach, Director of Operations, Director of Administration).

If a title clearly indicates they help run or administrate the school, district, campus, or major operations, treat them as administrative and include them.

3. WHO TO EXCLUDE (DO NOT RETURN) - COMPREHENSIVE BLOCKLIST
Exclude ALL of the following, even if they have an email/phone. If a title contains ANY of these keywords or phrases (case-insensitive), EXCLUDE that contact immediately:

A. TEACHERS & INSTRUCTIONAL STAFF (ALL TYPES):
- teacher, teachers, teaching, faculty, professor, professors, instructor, instructors, lecturer, lecturers
- classroom teacher, homeroom teacher, art teacher, music teacher, science teacher, math teacher, english teacher
- reading teacher, writing teacher, language teacher, foreign language teacher, spanish teacher, french teacher
- history teacher, social studies teacher, physical education teacher, PE teacher, P.E. teacher
- special education teacher, special ed teacher, resource teacher, intervention teacher
- substitute teacher, student teacher, teaching assistant, teacher's aide, paraeducator, paraprofessional
- department chair (if they are primarily a teacher), curriculum coordinator (if primarily instructional)

B. COUNSELORS & STUDENT SUPPORT STAFF (ALL TYPES):
- counselor, counselors, counselling, counseling, guidance counselor, school counselor
- college counselor, college counseling, academic counselor, academic counseling
- social worker, school social worker, psychologist, school psychologist, therapist, school therapist
- mental health counselor, behavioral counselor, student support, student services
- chaplain, campus ministry, pastor, minister, religious coordinator, spiritual director
- student advocate, case manager (if student-facing), intervention specialist (if counseling-related)

C. ATHLETICS & SPORTS (ALL TYPES):
- athletic director, athletics director, director of athletics, director of athletics and activities
- coach, coaches, head coach, assistant coach, associate coach, volunteer coach
- athletic coordinator, sports coordinator, athletics coordinator
- PE director, P.E. director, physical education director, director of physical education
- director of PE and wellness, wellness director (if PE-related)
- sports performance, athletic trainer, trainer, strength and conditioning coach
- intramural director, activities director (if primarily athletics)
- any title containing: athletic, athletics, sports, sport, coach, coaching, PE, P.E., physical education

D. ADMISSIONS & ENROLLMENT (ALL TYPES):
- admissions director, director of admissions, admissions coordinator, admissions counselor
- admissions officer, admissions manager, admissions assistant, admissions specialist
- enrollment director, director of enrollment, enrollment coordinator, enrollment manager
- enrollment officer, enrollment counselor, enrollment specialist, enrollment assistant
- registrar, registrars, registrar's office, registration coordinator
- recruiting, recruiter, student recruiter, admissions recruiter
- outreach for admissions, admissions outreach, enrollment outreach
- graduate admissions, undergraduate admissions, admissions and marketing
- any title containing: admissions, admission, enrollment, enroll, registrar, recruiting, recruiter

E. MARKETING & COMMUNICATIONS (ALL TYPES):
- marketing director, director of marketing, marketing manager, marketing coordinator
- marketing specialist, marketing assistant, communications director, director of communications
- communications manager, communications coordinator, communications specialist
- public relations director, PR director, director of public relations
- media relations, media coordinator, social media manager, social media coordinator
- brand manager, marketing and communications, marcom director, marketing and enrollment
- development and marketing, advancement and marketing, marketing and outreach
- any title containing: marketing, market, communications, communication, PR, public relations, media relations, social media

F. OFFICE ADMINISTRATORS & SUPPORT STAFF (ALL TYPES):
- office administrator, office manager, office assistant, office coordinator
- administrative assistant, admin assistant, executive assistant, personal assistant
- secretary, secretaries, receptionist, front desk, office staff, office support
- office coordinator, administrative coordinator, operations assistant
- billing coordinator, student billing, accounts receivable, accounts payable (if office/admin role)
- data entry, administrative support, clerical, office clerk
- any title containing: office admin, office administrator, office manager, office assistant, secretary, receptionist, administrative assistant

G. OTHER EXCLUSIONS:
- Food & dining: food service, cafeteria, nutrition, dining services, food manager, lunch coordinator, kitchen manager
- Board & governance: board member, board of directors, board of trustees, trustee, governor, regent, advisory board
- Health services: nurse, health office, school nurse, health services, health coordinator (unless also main administrator)
- Advancement & fundraising: advancement, development, fundraising, alumni relations, donor relations, development office
- Preschool: preschool director, director of preschool, early childhood director (not targeting preschool admins)
- Residential: director of residential services, residential services director, dorm director, housing director
- Transition services: director of transition services, transition services director, transition coordinator
- Family outreach: director of family outreach, family outreach director, parent coordinator (if primarily outreach)
- Any roles that clearly indicate regular faculty, coaching, counseling, chaplaincy, student-facing support, or non-administrative operations rather than core school administration.

IMPLEMENTATION TIP - STRICT EXCLUSION RULES:
If a title contains ANY of these exclusion keywords or phrases (case-insensitive), EXCLUDE that contact immediately, even if other words might sound administrative.

HOW TO CHECK: For each contact's title, convert it to lowercase and check if ANY of the blocklist keywords appear as a substring. Partial matches count. For example:
- "Athletic Director" contains "athletic" → EXCLUDE
- "Office Manager" contains "office manager" → EXCLUDE
- "PE/Athletic Director" contains both "PE" and "athletic" → EXCLUDE
- "Marketing & Communications" contains both "marketing" and "communications" → EXCLUDE
- "Administrative Assistant" contains "administrative assistant" → EXCLUDE

BLOCKLIST KEYWORDS (case-insensitive substring matching - if title contains ANY of these, EXCLUDE):
teacher, teachers, teaching, faculty, professor, instructors, lecturer, classroom, homeroom,
counselor, counselors, counselling, counseling, guidance, "social worker", psychologist, therapist, chaplain, pastor, minister,
athletic, athletics, sports, sport, coach, coaches, coaching, PE, "P.E.", "physical education",
admissions, admission, enrollment, enroll, registrar, recruiting, recruiter,
marketing, market, communications, communication, "public relations", PR, "social media", "media relations",
"office admin", "office administrator", "office manager", "office assistant", secretary, secretaries, receptionist, "administrative assistant", "executive assistant",
cafeteria, nutrition, dining, "food service", lunch,
board, trustee, governor, regent,
nurse, "health services", "health office",
advancement, development, fundraising, alumni, "donor relations",
preschool, "early childhood",
residential, dorm, housing,
transition, outreach (if combined with admissions/family/marketing).

CRITICAL ENFORCEMENT RULES:
1. Exclusion takes ABSOLUTE PRIORITY over inclusion
2. If a title contains ANY blocklist keyword (even as part of another word), EXCLUDE it
3. Do NOT extract contacts with excluded titles, even if they have emails or seem important
4. Do NOT try to include excluded titles even if they also contain words like "director" or "administrator"
5. When in doubt about a title, check it against the blocklist first - if it matches, exclude it

MANDATORY PRE-OUTPUT VALIDATION:
Before outputting ANY contact to the CSV, you MUST perform this check:
1. Read the contact's title (case-insensitive)
2. Check if the title contains ANY word from the blocklist keywords above
3. If YES → DO NOT OUTPUT THIS CONTACT. Skip it entirely.
4. If NO → Proceed to check if it matches inclusion criteria

EXAMPLES OF TITLES TO EXCLUDE (DO NOT EXTRACT):
- "PE/Athletic Director" → Contains "athletic" → EXCLUDE
- "Office Manager" → Contains "office manager" → EXCLUDE
- "Administrative Assistant" → Contains "administrative assistant" → EXCLUDE
- "Office Administrator" → Contains "office administrator" → EXCLUDE
- "Marketing & Communications Coordinator" → Contains "marketing" AND "communications" → EXCLUDE
- "Director of Athletics" → Contains "athletic" → EXCLUDE
- "Office Assistant" → Contains "office assistant" → EXCLUDE
- "Middle School Art Teacher" → Contains "teacher" → EXCLUDE
- "Athletic Trainer" → Contains "athletic" → EXCLUDE
- "ES Office Administrator" → Contains "office administrator" → EXCLUDE
- "MS/HS Office Administrator" → Contains "office administrator" → EXCLUDE

These titles MUST NOT appear in your output, even if they have emails or seem important.

4. NAME, EMAIL, AND PHONE RULES
- Split names into First Name and Last Name. Middle names or initials go with the first name field.
  Example: "Ann M. Horne" → First Name: Ann M. , Last Name: Horne
- Use proper capitalization for names and titles when possible.
- Emails:
  - Use the personal work email associated with that person (often in mailto: links or near their name).
  - Do NOT return generic emails like info@, office@, contact@, unless that is clearly the only email directly labeled as that person's email.
- Phones:
  - If multiple phone numbers appear, choose the direct line next to the person's name, or the main office number if that is clearly associated with them.
  - Preserve phone number formatting as shown on the page, including extensions (e.g., 555-123-4567 ext. 241).
  - If no phone is clearly linked to that person, leave Phone empty.

5. DATA QUALITY & DEDUPLICATION
- Do NOT mix up data between people. Each row must belong to one specific person.
- If the same person appears multiple times with identical details inside this chunk, only keep ONE row.
- If the same person appears with slightly different information, keep the most complete version (more fields filled in).

5B. FINAL EXCLUSION CHECK (MANDATORY BEFORE OUTPUT):
Before writing ANY contact to the CSV output, you MUST:
1. Read the contact's Title field
2. Convert it to lowercase
3. Check if it contains ANY word from the blocklist (teacher, athletic, office manager, marketing, communications, administrative assistant, counselor, admissions, enrollment, etc.)
4. If the title contains ANY blocklist keyword → DO NOT OUTPUT THIS CONTACT
5. Only proceed to output if the title passes the exclusion check

Remember: "PE/Athletic Director" → EXCLUDE (contains "athletic")
Remember: "Office Manager" → EXCLUDE (contains "office manager")
Remember: "Marketing Coordinator" → EXCLUDE (contains "marketing")
Remember: "Administrative Assistant" → EXCLUDE (contains "administrative assistant")
Remember: Any title with "teacher", "coach", "counselor", "admissions", "enrollment", "marketing", "communications", "office admin", "office manager", "office assistant", "athletic", "athletics" → EXCLUDE

6. OUTPUT FORMAT (CSV)
- Output ONLY CSV text. No explanations, no notes, no markdown.
- The first line MUST be this exact header:

First Name,Last Name,Title,Email,Phone

- Each subsequent line is one contact, with exactly 5 comma-separated fields.
- If a field itself contains a comma, wrap that field in double quotes and escape any internal double quotes by doubling them (standard CSV rules).

FINAL VALIDATION BEFORE OUTPUT:
For EACH contact you are about to output:
1. Check the title against the blocklist keywords
2. If title contains ANY blocklist keyword → DO NOT OUTPUT THIS ROW
3. Only output contacts that pass the exclusion check AND match inclusion criteria

Example of correct formatting (only valid administrative contacts):

First Name,Last Name,Title,Email,Phone
Jacqueline,Wright,Principal,jwright@holytrinitychs.org,254-771-0787
Michael,Johnson,Assistant Principal,mjohnson@school.org,555-123-4567
Sarah,Williams,IT Director,swilliams@school.org,555-987-6543

DO NOT output contacts with titles like:
- "Office Manager" (blocked)
- "PE/Athletic Director" (blocked)
- "Marketing Coordinator" (blocked)
- "Administrative Assistant" (blocked)
- Any title containing exclusion keywords (blocked)

- If no valid administrative contacts are found in this HTML chunk, output only the header row and nothing else.

7. IMPORTANT CONSTRAINTS
- Use only the information in the provided HTML (or HTML chunk).
- Do not reference these instructions or your own reasoning in the output.
- Do not output JSON, markdown, or any additional text — only the CSV as specified above."""


class LLMParser:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        """
        Initialize LLM parser
        
        Args:
            api_key: OpenAI API key
            model: Model to use (default: gpt-4o-mini)
        """
        self.api_key = api_key
        self.model = model
        self.client = OpenAI(api_key=api_key)
    
    def reduce_html(self, html: str) -> str:
        """
        Reduce HTML to only "people sections" - keeps blocks likely to contain staff/contact info
        
        Args:
            html: Raw HTML content
            
        Returns:
            Reduced HTML string containing only people sections
        """
        if not html:
            return ""
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove unwanted elements completely
        for element in soup(['script', 'style', 'noscript', 'svg', 'iframe', 'link']):
            element.decompose()
        
        # Remove comments
        from bs4 import Comment
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Staff keywords to look for
        staff_keywords = [
            'staff', 'faculty', 'directory', 'leadership', 'administration', 
            'administrative', 'principal', 'superintendent', 'head of school', 
            'dean', 'director'
        ]
        
        # Conditionally remove header/footer/nav/aside ONLY if they don't contain staff keywords
        for tag_name in ['header', 'footer', 'nav', 'aside']:
            for element in soup.find_all(tag_name):
                element_text = element.get_text().lower()
                if not any(keyword in element_text for keyword in staff_keywords):
                    element.decompose()
        
        # Keep only blocks that are likely to contain people/staff information
        people_sections = []
        
        # Find all block-level elements
        for element in soup.find_all(['section', 'div', 'table', 'ul', 'article']):
            element_text = element.get_text().lower()
            
            # Check if this block contains staff keywords
            has_staff_keywords = any(keyword in element_text for keyword in staff_keywords)
            
            # Check if this block has email + name pattern
            has_email = '@' in element_text
            has_name_pattern = bool(re.search(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', element.get_text()))
            
            # Keep if it has staff keywords OR (email AND name pattern)
            if has_staff_keywords or (has_email and has_name_pattern):
                people_sections.append(str(element))
        
        # If no people sections found, fallback to full HTML (but cleaned)
        # This ensures we don't miss contacts in non-standard layouts
        if not people_sections:
            # Fallback: return cleaned full HTML (remove scripts/styles but keep content)
            # This will be chunked properly to stay within token limits
            cleaned_html = soup.prettify()
            return cleaned_html
        
        # Concatenate sections in original order
        reduced_html = '\n'.join(people_sections)
        
        return reduced_html
    
    def chunk_html(self, html: str, max_chunk_size: int = 40000) -> List[str]:
        """
        Split HTML into chunks of approximately max_chunk_size characters
        Intelligently splits on block boundaries to avoid cutting contact cards in half
        Ensures contacts (name + email/title) stay together within chunks
        
        Args:
            html: HTML string to chunk
            max_chunk_size: Maximum characters per chunk (default: 40,000)
            
        Returns:
            List of HTML chunks
        """
        if not html or len(html) <= max_chunk_size:
            return [html]
        
        chunks = []
        
        # Priority order for split points (most likely to preserve contact cards)
        # </li> is often a contact card boundary
        # </tr> is often a table row with contact info
        # </div> with class/id containing staff/contact keywords
        block_delimiters = ['</li>', '</tr>', '</div>', '</section>', '</article>', '</td>']
        
        # Find all split points with context
        split_points = []
        for delimiter in block_delimiters:
            pattern = re.compile(re.escape(delimiter), re.IGNORECASE)
            for match in pattern.finditer(html):
                # Check context around delimiter for contact indicators
                context_start = max(0, match.start() - 200)
                context_end = min(len(html), match.end() + 200)
                context = html[context_start:context_end].lower()
                
                # Prefer splitting after contact-like structures
                # Look for patterns like name + email or title
                has_contact_pattern = (
                    '@' in context or  # Email present
                    re.search(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', context) or  # Name pattern
                    any(kw in context for kw in ['principal', 'director', 'administrator', 'email', 'phone'])
                )
                
                split_points.append((match.end(), delimiter, has_contact_pattern))
        
        # Sort by position
        split_points.sort(key=lambda x: x[0])
        
        # Build chunks with smart boundary detection
        current_chunk = ""
        last_split = 0
        target_size = max_chunk_size - 5000  # Leave 5k buffer to avoid splitting mid-contact
        
        for split_pos, delimiter, is_contact_boundary in split_points:
            segment = html[last_split:split_pos]
            
            # If adding segment would exceed target, check if we should split
            if len(current_chunk) + len(segment) > target_size and current_chunk:
                # If this is a contact boundary, it's safe to split here
                if is_contact_boundary:
                    chunks.append(current_chunk)
                    current_chunk = segment
                    last_split = split_pos
                # Otherwise, try to find next contact boundary
                elif len(current_chunk) < max_chunk_size * 0.8:  # Still room to grow
                    current_chunk += segment
                else:
                    # Force split to avoid exceeding max
                    chunks.append(current_chunk)
                    current_chunk = segment
                    last_split = split_pos
            else:
                current_chunk += segment
                last_split = split_pos
        
        # Add remaining content
        if current_chunk:
            remaining = html[last_split:]
            if len(current_chunk) + len(remaining) <= max_chunk_size:
                current_chunk += remaining
                chunks.append(current_chunk)
            else:
                # Split remaining intelligently
                if current_chunk:
                    chunks.append(current_chunk)
                # Chunk remaining content
                if len(remaining) > max_chunk_size:
                    # Recursively chunk remaining
                    remaining_chunks = self.chunk_html(remaining, max_chunk_size)
                    chunks.extend(remaining_chunks)
                else:
                    chunks.append(remaining)
        
        # If no good split points found, split at safe boundaries
        if not chunks:
            # Try to split at paragraph or line breaks first
            safe_splits = re.finditer(r'</p>|</br>|</h[1-6]>', html, re.IGNORECASE)
            split_positions = [m.end() for m in safe_splits]
            
            if split_positions:
                last_pos = 0
                for pos in split_positions:
                    if pos - last_pos > max_chunk_size:
                        chunks.append(html[last_pos:pos])
                        last_pos = pos
                if last_pos < len(html):
                    chunks.append(html[last_pos:])
            else:
                # Last resort: split at max_chunk_size but try to avoid mid-word
                for i in range(0, len(html), max_chunk_size):
                    chunk = html[i:i + max_chunk_size]
                    # Try to end at a space or tag boundary
                    if i + max_chunk_size < len(html):
                        # Look for safe break point near end
                        for j in range(len(chunk) - 1, max(0, len(chunk) - 100), -1):
                            if chunk[j] in ['>', '\n', ' ']:
                                chunk = html[i:i+j+1]
                                i = i + j + 1
                                break
                    chunks.append(chunk)
        
        # Ensure no chunk exceeds max (safety check)
        final_chunks = []
        for chunk in chunks:
            if len(chunk) > max_chunk_size:
                # Split oversized chunk
                for i in range(0, len(chunk), max_chunk_size):
                    final_chunks.append(chunk[i:i + max_chunk_size])
            else:
                final_chunks.append(chunk)
        
        return final_chunks if final_chunks else [html]
    
    def parse_csv_response(self, csv_text: str) -> List[Dict]:
        """
        Parse CSV response from LLM into list of contact dictionaries
        
        Args:
            csv_text: CSV text from LLM
            
        Returns:
            List of contact dictionaries
        """
        contacts = []
        
        # Clean up response (remove markdown code blocks if present)
        csv_text = re.sub(r'^```csv\s*', '', csv_text, flags=re.MULTILINE)
        csv_text = re.sub(r'^```\s*', '', csv_text, flags=re.MULTILINE)
        csv_text = re.sub(r'\s*```$', '', csv_text, flags=re.MULTILINE)
        csv_text = csv_text.strip()
        
        # Parse CSV
        try:
            reader = csv.DictReader(io.StringIO(csv_text))
            for row in reader:
                # Convert CSV row to contact dict
                contact = {
                    'name': f"{row.get('First Name', '').strip()} {row.get('Last Name', '').strip()}".strip(),
                    'title': row.get('Title', '').strip(),
                    'email': row.get('Email', '').strip(),
                    'phone': row.get('Phone', '').strip()
                }
                
                # Only add if has name and title (email is optional)
                if contact['name'] and contact['title']:
                    contacts.append(contact)
        except Exception as e:
            print(f"      WARNING: CSV parse error: {e}")
            print(f"      CSV preview: {csv_text[:500]}")
        
        return contacts
    
    def deduplicate_contacts(self, contacts: List[Dict]) -> List[Dict]:
        """
        Deduplicate contacts by email (or name+domain if no email)
        Keep the most complete version
        
        Args:
            contacts: List of contact dictionaries
            
        Returns:
            Deduplicated list
        """
        seen = {}
        
        for contact in contacts:
            email = contact.get('email', '').strip().lower()
            name = contact.get('name', '').strip().lower()
            
            # Primary key: email if present
            if email:
                key = email
            else:
                # Fallback: name + domain (if email domain is present in other fields)
                domain = ''
                if 'source_url' in contact:
                    # Extract domain from URL
                    url = contact['source_url']
                    match = re.search(r'https?://([^/]+)', url)
                    if match:
                        domain = match.group(1)
                key = f"{name}|{domain}"
            
            # If we've seen this key before, keep the more complete version
            if key in seen:
                existing = seen[key]
                # Count non-empty fields
                existing_fields = sum(1 for v in existing.values() if v and str(v).strip())
                new_fields = sum(1 for v in contact.values() if v and str(v).strip())
                
                # Keep the one with more fields, or longer title if equal
                if new_fields > existing_fields:
                    seen[key] = contact
                elif new_fields == existing_fields:
                    if len(contact.get('title', '')) > len(existing.get('title', '')):
                        seen[key] = contact
            else:
                seen[key] = contact
        
        return list(seen.values())
    
    def parse_with_llm(self, html_chunk: str, school_name: str, url: str, max_retries: int = 3) -> List[Dict]:
        """
        Send HTML chunk to LLM and parse CSV response
        
        Args:
            html_chunk: HTML chunk to parse
            school_name: School name (metadata)
            url: Page URL (metadata)
            max_retries: Maximum retry attempts
            
        Returns:
            List of contact dictionaries
        """
        for attempt in range(max_retries):
            try:
                # Build user message with metadata and HTML chunk only
                # The full prompt is in the system message (sent once per session, not per chunk)
                user_message = f"""SCHOOL NAME: {school_name}
PAGE URL: {url}

HTML CONTENT:
{html_chunk}"""
                
                # Estimate tokens for max_tokens calculation
                estimated_input_tokens = len(html_chunk) // 4
                
                # Safety check: if chunk is still too large, it should have been split earlier
                # But as a final safeguard, we'll note it (shouldn't happen with improved chunking)
                if len(html_chunk) > 100000:
                    print(f"      WARNING: HTML chunk still too large ({len(html_chunk):,} chars) - this should have been split earlier!")
                    # Don't truncate - this indicates a bug in chunking logic
                    # Process it anyway but log the issue
                
                # Set max_tokens based on input size
                if estimated_input_tokens > 20000:
                    max_tokens = 32000
                elif estimated_input_tokens > 10000:
                    max_tokens = 16000
                else:
                    max_tokens = 8000
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": ADMIN_CONTACT_EXTRACTION_PROMPT},
                        {"role": "user", "content": user_message}
                    ],
                    temperature=0.0,
                    max_tokens=max_tokens
                )
                
                # Extract response text
                response_text = response.choices[0].message.content.strip()
                
                # Parse CSV response
                contacts = self.parse_csv_response(response_text)
                
                return contacts
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"      WARNING: LLM error (attempt {attempt + 1}/{max_retries}): {e}, retrying...")
                    time.sleep(2 ** attempt)
                    continue
                print(f"      ERROR: LLM error: {e}")
                return []
        
        return []
    
    def parse_pages(self, input_csv: str, output_csv: str, output_no_emails_csv: str = None):
        """
        Parse all pages from Step 3 using LLM with HTML reduction and chunking
        
        Args:
            input_csv: CSV from Step 3 with HTML content
            output_csv: Output CSV with parsed contacts that HAVE emails
            output_no_emails_csv: Output CSV with parsed contacts WITHOUT emails (for enrichment)
        """
        print("\n" + "="*70)
        print("STEP 4: PARSING HTML WITH LLM (NO FILTERING - LLM ONLY)")
        print("="*70)
        print(f"Model: {self.model}")
        print("Processing ALL pages (including those without emails)")
        print("="*70 + "\n")
        
        # Read crawled pages
        df = pd.read_csv(input_csv)
        
        # Process ALL pages with HTML content (NO has_emails filter)
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
            
            # Step 1: Reduce HTML to people sections only
            reduced_html = self.reduce_html(html_content)
            
            if not reduced_html:
                print(f"    No people sections found, skipping")
                continue
            
            print(f"    Reduced HTML: {len(html_content)} → {len(reduced_html)} chars")
            
            # Step 2: Chunk HTML if needed (ensures all content is processed, no truncation)
            # If reduced HTML is very large, chunk it properly instead of truncating
            chunks = self.chunk_html(reduced_html, max_chunk_size=40000)
            
            # Safety check: if any chunk is still too large, split it further
            final_chunks = []
            for chunk in chunks:
                if len(chunk) > 100000:  # Hard limit per chunk
                    print(f"      WARNING: Chunk too large ({len(chunk):,} chars), splitting further...")
                    # Split oversized chunk into smaller pieces
                    sub_chunks = self.chunk_html(chunk, max_chunk_size=40000)
                    final_chunks.extend(sub_chunks)
                else:
                    final_chunks.append(chunk)
            chunks = final_chunks
            print(f"    Split into {len(chunks)} chunk(s)")
            
            # Step 3: Process each chunk
            page_contacts = []
            for chunk_idx, chunk in enumerate(chunks):
                if len(chunks) > 1:
                    print(f"    Processing chunk {chunk_idx + 1}/{len(chunks)}...")
                
                chunk_contacts = self.parse_with_llm(chunk, school_name, url)
                page_contacts.extend(chunk_contacts)
                
                # Rate limiting between chunks
                if chunk_idx < len(chunks) - 1:
                    time.sleep(0.5)
            
            # Step 4: Deduplicate contacts from all chunks for this page
            if page_contacts:
                # Add school and URL metadata
                for contact in page_contacts:
                    contact['school_name'] = school_name
                    contact['source_url'] = url
                
                # Deduplicate
                page_contacts = self.deduplicate_contacts(page_contacts)
                
                pages_with_contacts += 1
                total_contacts += len(page_contacts)
                print(f"    Found {len(page_contacts)} contacts (after deduplication)")
                
                all_contacts.extend(page_contacts)
            else:
                print(f"    - No valid contacts found")
            
            pages_processed += 1
            
            # Rate limiting between pages
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
        else:
            # Create empty CSV with headers
            pd.DataFrame(columns=['school_name', 'name', 'title', 'email', 'phone', 'source_url']).to_csv(output_csv, index=False)
        
        # Save contacts without emails (for enrichment)
        if output_no_emails_csv:
            if contacts_without_emails:
                self._save_results(contacts_without_emails, output_no_emails_csv)
            else:
                # Create empty CSV with headers
                pd.DataFrame(columns=['school_name', 'name', 'title', 'email', 'phone', 'source_url']).to_csv(output_no_emails_csv, index=False)
        
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
        # Only include columns that exist
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]
        
        df.to_csv(filename, index=False)
    
    def _print_summary(self, contacts: List[Dict], output_file: str, 
                       pages_processed: int, pages_with_contacts: int,
                       contacts_with_emails: List[Dict], contacts_without_emails: List[Dict],
                       output_no_emails_csv: str = None):
        """Print parsing summary"""
        print("\n" + "="*70)
        print("PARSING COMPLETE")
        print("="*70)
        print(f"Pages processed: {pages_processed}")
        print(f"Pages with contacts: {pages_with_contacts} ({pages_with_contacts/max(1, pages_processed)*100:.1f}%)")
        print(f"Total contacts extracted: {len(contacts)}")
        print(f"  - Contacts WITH emails: {len(contacts_with_emails)}")
        print(f"  - Contacts WITHOUT emails: {len(contacts_without_emails)}")
        
        if contacts:
            df = pd.DataFrame(contacts)
            print(f"Unique schools: {df['school_name'].nunique()}")
            print(f"\nAverage contacts per page: {len(df)/max(1, pages_processed):.2f}")
            print(f"Contacts with phone numbers: {df['phone'].ne('').sum()} ({df['phone'].ne('').sum()/len(df)*100:.1f}%)")
            
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
        else:
            print("Unique schools: 0")
            print("\nAverage contacts per page: 0.00")
            print("Contacts with phone numbers: 0 (0.0%)")
        
        print(f"\nOutput file (with emails): {output_file}")
        if output_no_emails_csv:
            print(f"Output file (without emails, for enrichment): {output_no_emails_csv}")
        print("="*70)


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
