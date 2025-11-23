"""
STEP 5: EXTRACT ALL CONTACTS FROM HTML (NO FILTERING)
================================================================
Take ALL raw HTML from Step 4 and use GPT to extract ALL contacts.

IMPORTANT: NO FILTERING IN THIS STEP - Extract everyone found
- HTML reduction and chunking only (pre-processing)
- Extract ALL contacts with names and titles (no role filtering)
- Extract contacts with AND without emails
- Title filtering happens in Step 6

Input: CSV from Step 4 with ALL raw HTML content
Output: CSV with ALL contacts (name, title, email, phone) - NO filtering
"""

from openai import OpenAI
import pandas as pd
import csv
import io
import time
from typing import List, Dict, Optional
import re
from bs4 import BeautifulSoup
import unicodedata


# Step 5: Extract ALL contacts (NO filtering - that happens in Step 6)
CONTACT_EXTRACTION_PROMPT = """
You extract ALL PEOPLE from raw HTML. Do not hallucinate or invent data.

INPUT:
- Full HTML from a single web page or a contiguous chunk of HTML.
- The page may contain people in cards, tables, lists, or text blocks.
- Ignore organization names, departments, and generic contact emails.

OUTPUT:
Return ONLY CSV text (no backticks, no explanations) in this exact format:

First Name,Last Name,Title,Email,Phone

GENERAL RULES:
- Extract only real people, never departments, committees, buildings, teams, or offices.
- A valid contact MUST have at least a first and last name.
- Keep each person's name, title, email, and phone correctly matched.
- If a field is missing, leave it blank but keep the comma.
- If no valid contacts are found, return ONLY the header.

HEADER (always the first line):
First Name,Last Name,Title,Email,Phone

STEP-BY-STEP:
1. Identify all blocks that look like people entries (name + title, often near an email).
2. For each candidate person:
   - Extract full name (first + last). Remove prefixes like Dr., Mr., Mrs., Ms., Rev., Fr., Sr.
   - Extract the associated title / role (extract whatever title is shown, even if it's "Teacher", "Coach", "Board Member", etc.)
   - Extract the nearest email (usually mailto:). Skip generic inboxes (info@, office@, contact@, admissions@, marketing@, communications@).
   - Extract phone only if clearly linked to that person; otherwise leave blank.

NAME RULES:
- Valid examples:
  - "Dr. Terry Rodgers" → First Name: Terry, Last Name: Rodgers
  - "Mary Jane Watson" → First Name: Mary Jane, Last Name: Watson
  - "Kent A. Means" → First Name: Kent A., Last Name: Means
- If you cannot find a clear first + last name, skip that contact entirely.

TITLE EXTRACTION:
- Extract whatever title/role is shown for the person
- Include ALL titles: teachers, coaches, board members, administrators, etc.
- Do NOT filter by title in this step - extract everyone
- If someone has multiple titles, extract the primary one (or first one listed)

OUTPUT FORMAT (STRICT):
- First line: exactly
  First Name,Last Name,Title,Email,Phone
- Each subsequent line: one contact, 5 comma-separated fields.
- Use quotes only if a field contains a comma.
- No markdown, no commentary, no extra text.

IMPORTANT: Extract ALL people with names and titles. Do NOT filter by role - that filtering happens in a later step.
"""


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
        Reduce HTML to only sections likely to contain administrative leadership.
        Avoids duplicating nested blocks. Favors completeness over over-filtering.
        
        Args:
            html: Raw HTML content
            
        Returns:
            Reduced HTML string containing only people sections
        """
        if not html:
            return ""
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove obvious token-bloat elements
        for tag in soup(['script', 'style', 'noscript', 'svg', 'iframe', 'canvas', 'link']):
            tag.decompose()
        
        # Remove comments
        from bs4 import Comment
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()
        
        # Keywords that indicate admin / leadership content
        admin_keywords = [
            'leadership', 'administration', 'executive', 'superintendent', 'head of school',
            'principal', 'division head', 'upper school', 'middle school', 'lower school',
            'operations', 'finance', 'cfo', 'controller', 'business office',
            'technology', 'it', 'security', 'safety', 'emergency', 'facilities',
            'hr', 'human resources'
        ]
    
        # Remove header/footer/nav/aside ONLY if clearly unrelated to admin
        for tag_name in ['header', 'footer', 'nav', 'aside']:
            for element in soup.find_all(tag_name):
                text = element.get_text(separator=' ', strip=True).lower()
                if not any(k in text for k in admin_keywords):
                    element.decompose()
        
        # Look at content blocks - include divs as many sites use them for contact cards
        # Prioritize higher-level blocks but don't exclude divs entirely
        blocks = soup.find_all(['section', 'article', 'table', 'ul', 'ol', 'div'])
        
        people_sections = []
        
        # Slightly more forgiving name pattern: at least two capitalized words
        name_regex = re.compile(r'\b([A-Z][a-z]+(?: [A-Z][a-z]+)+)\b')
        
        for block in blocks:
            text = block.get_text(" ", strip=True)
            low = text.lower()
            
            has_email = '@' in low
            has_name = bool(name_regex.search(text))
            has_admin_keyword = any(k in low for k in admin_keywords)
            
            # Keep if it looks like it contains people and/or admin language
            if has_email or has_name or has_admin_keyword:
                people_sections.append(str(block))
        
        # Fallback: if nothing matched, return cleaned full HTML so we don't miss anything
        if not people_sections:
            return soup.prettify()
        
        # Concatenate candidate sections; this stays near original size, not 5x
        return "\n".join(people_sections)
    
    def chunk_html(self, html: str, max_chunk_size: int = 20000) -> List[str]:
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
    
    def clean_email(self, email: str) -> str:
        """
        Clean email address by removing special characters and validating format.
        
        Args:
            email: Raw email string (may contain special characters or invalid text)
        
        Returns:
            Cleaned email string, or empty string if invalid
        """
        if not email or pd.isna(email):
            return ''
        
        email = str(email).strip()
        
        # Handle UTF-8 encoding artifacts like "â€‹" which appears when zero-width space is mis-encoded
        # Try to decode and re-encode to fix encoding issues
        try:
            # If the string contains "â€‹" (common mis-encoding of zero-width space)
            if 'â€‹' in email:
                # Try to fix by removing the problematic sequence
                email = email.replace('â€‹', '')
            # Also try removing if it's the actual zero-width space character
            email = email.lstrip('\u200B\u200C\u200D\uFEFF')  # Zero-width space, zero-width non-joiner, etc.
        except:
            pass
        
        # Remove UTF-8 BOM and other encoding artifacts
        if email.startswith('\ufeff'):
            email = email[1:]
        
        # Remove any other common encoding artifacts at the start
        # Remove leading non-printable characters
        while email and not email[0].isprintable() and email[0] != '@':
            email = email[1:]
        
        # Remove any leading non-ASCII characters that aren't valid email characters
        # Keep only printable ASCII characters and valid email characters
        cleaned = ''
        found_at = False
        for char in email:
            # Always keep @ symbol
            if char == '@':
                found_at = True
                cleaned += char
            # Allow ASCII printable characters and common email characters
            elif ord(char) < 128 and (char.isprintable() or char in '._-+'):
                cleaned += char
            # Skip other non-ASCII characters (but only before @)
            # After @, we might have internationalized domain names, but for now we'll be strict
            elif found_at and ord(char) < 128:
                # After @, allow some non-ASCII if it's part of a valid domain
                # But for safety, we'll be conservative
                pass
        
        email = cleaned.strip()
        
        # Basic email format validation - must contain @ and have valid structure
        # This filters out things like "Bobcat Heavy Civil" or "ISAIAH'S PLACE | ASL & EQUINE ASSISTED LEARNING"
        if not email or '@' not in email:
            return ''
        
        # Split by @ to check domain
        parts = email.split('@')
        if len(parts) != 2:
            return ''
        
        local, domain = parts[0], parts[1]
        
        # Local part must not be empty and should be reasonable
        if not local or len(local) < 1:
            return ''
        
        # Domain must contain at least one dot and have valid TLD
        if '.' not in domain or len(domain.split('.')[-1]) < 2:
            return ''
        
        # Additional check: if it looks like random text (too many spaces, special chars, etc.)
        # This catches cases where non-email text got into the field
        if ' ' in email or len(email.split()) > 1:
            return ''  # Emails shouldn't have spaces
        
        # Final regex validation for proper email format
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            return ''
        
        return email.lower()
    
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
                # Helper function to safely get and strip CSV values (handles None)
                def safe_get(field_name, default=''):
                    value = row.get(field_name, default)
                    return value.strip() if value is not None else default
                
                # Convert CSV row to contact dict (new format: first_name, last_name separate)
                first_name = safe_get('First Name', '')
                last_name = safe_get('Last Name', '')
                
                # Clean email to remove special characters and validate format
                raw_email = safe_get('Email', '')
                cleaned_email = self.clean_email(raw_email)
                
                contact = {
                    'first_name': first_name,
                    'last_name': last_name,
                    'title': safe_get('Title', ''),
                    'email': cleaned_email,  # Use cleaned email
                    'phone': safe_get('Phone', '')
                }
                
                # Only add if has first name, last name, and title (email is optional)
                if contact['first_name'] and contact['last_name'] and contact['title']:
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
            # Use cleaned email for deduplication (already cleaned in parse_csv_response, but clean again to be safe)
            raw_email = contact.get('email', '')
            email = self.clean_email(raw_email) if raw_email else ''
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
                        {"role": "system", "content": CONTACT_EXTRACTION_PROMPT},
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
                error_str = str(e)
                
                # Check if it's a rate limit error (429)
                is_rate_limit = '429' in error_str or 'rate_limit' in error_str.lower() or 'rate limit' in error_str.lower()
                
                if is_rate_limit:
                    # Try to extract wait time from error message
                    wait_seconds = 1.0  # Default wait
                    import re
                    # Look for "Please try again in Xms" or "Please try again in Xs"
                    wait_match = re.search(r'Please try again in (\d+)(ms|s)', error_str, re.IGNORECASE)
                    if wait_match:
                        wait_value = int(wait_match.group(1))
                        wait_unit = wait_match.group(2).lower()
                        if wait_unit == 'ms':
                            wait_seconds = (wait_value / 1000.0) + 0.5  # Add 0.5s buffer
                        else:
                            wait_seconds = wait_value + 0.5  # Add 0.5s buffer
                    
                    # For rate limits, wait longer and retry
                    if attempt < max_retries - 1:
                        print(f"      ⚠️  Rate limit hit (attempt {attempt + 1}/{max_retries}), waiting {wait_seconds:.1f}s...")
                        time.sleep(wait_seconds)
                        continue
                    else:
                        print(f"      ❌ Rate limit exceeded after {max_retries} attempts. Skipping this chunk.")
                        return []
                else:
                    # For other errors, use exponential backoff
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        print(f"      WARNING: LLM error (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    print(f"      ERROR: LLM error: {e}")
                    return []
        
        return []
    
    def parse_pages(self, input_csv: str, output_csv: str, output_no_emails_csv: str = None):
        """
        Parse all pages from Step 4 using LLM with HTML reduction and chunking
        
        Args:
            input_csv: CSV from Step 4 with HTML content
            output_csv: Output CSV with parsed contacts that HAVE emails
            output_no_emails_csv: Output CSV with parsed contacts WITHOUT emails (for enrichment)
        """
        print("\n" + "="*70)
        print("STEP 5: EXTRACTING ALL CONTACTS FROM HTML (NO FILTERING)")
        print("="*70)
        print(f"Model: {self.model}")
        print("Extracting ALL contacts - title filtering happens in Step 6")
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
    parser.add_argument('--input', required=True, help='Input CSV from Step 4')
    parser.add_argument('--output', default='step5_contacts_with_emails.csv', help='Output CSV filename for contacts WITH emails')
    parser.add_argument('--output-no-emails', default='step5_contacts_no_emails.csv', help='Output CSV filename for contacts WITHOUT emails (for enrichment)')
    parser.add_argument('--api-key', required=True, help='OpenAI API key')
    parser.add_argument('--model', default='gpt-4o-mini', help='Model to use (default: gpt-4o-mini)')
    args = parser.parse_args()
    
    parser = LLMParser(api_key=args.api_key, model=args.model)
    parser.parse_pages(args.input, args.output, args.output_no_emails)
