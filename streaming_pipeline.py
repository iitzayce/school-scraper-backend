"""
STREAMING PIPELINE ORCHESTRATOR
================================
Process one lead through all steps, then write to final CSV only.
No intermediate CSV files - all in memory streaming.

Usage:
    python streaming_pipeline.py --api-key KEY --openai-key OPENAI_KEY
"""

import os
import sys
import csv
import argparse
import time
from typing import List, Iterator, Optional, Dict
from datetime import datetime
import traceback

# Import shared models
from shared.models import School, Page, PageContent, Contact

# Import streaming steps
from step1_streaming import SchoolSearcher, TEXAS_COUNTIES, TEXAS_CITIES
from step2_streaming import filter_school

# Import step classes (will need to refactor these to support streaming)
# For now, we'll import the existing classes and wrap them
import step3
import step4
import step5
import step6  # Title filtering
import step7  # Final compilation


class StreamingPipeline:
    """Process leads through entire pipeline one at a time"""
    
    def __init__(
        self,
        google_api_key: str,
        openai_api_key: str,
        global_max_api_calls: int = 25,
        max_pages_per_school: int = 3
    ):
        self.google_api_key = google_api_key
        self.openai_api_key = openai_api_key
        self.global_max_api_calls = global_max_api_calls
        self.max_pages_per_school = max_pages_per_school
        
        # Initialize step processors
        self.school_searcher = SchoolSearcher(google_api_key, global_max_api_calls)
        self.page_discoverer = step3.PageDiscoverer(timeout=120, max_retries=5)
        self.content_collector = step4.ContentCollector(timeout=120, max_retries=5, use_selenium=True)
        self.llm_parser = step5.LLMParser(openai_api_key, model="gpt-4o-mini")
        self.title_filter = step6.TitleFilter(openai_api_key, model="gpt-4o-mini")
        self.final_compiler = step7.FinalCompiler()
        
        # Results accumulator
        self.all_contacts = []
        self.stats = {
            'schools_discovered': 0,
            'schools_filtered_out': 0,
            'schools_processed': 0,
            'pages_discovered': 0,
            'pages_collected': 0,
            'contacts_extracted': 0,
            'contacts_with_emails': 0,
            'contacts_without_emails': 0
        }
    
    def process_single_lead(self, school: School) -> List[Contact]:
        """
        Process one school through all steps.
        Returns list of Contact objects extracted from this school.
        """
        print(f"\n{'='*70}")
        print(f"PROCESSING: {school.name}")
        print(f"{'='*70}")
        print(f"Website: {school.website or 'N/A'}")
        print(f"County: {school.county}")
        
        # Step 2: Filter school
        filtered_school = filter_school(school)
        if not filtered_school:
            print("  ❌ Filtered out (not a valid Texas school)")
            self.stats['schools_filtered_out'] += 1
            return []
        
        if not filtered_school.website:
            print("  ⚠️  No website - skipping")
            return []
        
        # Step 3: Discover pages
        print(f"\n  Step 3: Discovering pages...")
        try:
            # Discover pages for this school (using existing step3 logic)
            # TODO: Refactor step3 to accept single school and return list of Page objects
            pages = self._discover_pages_for_school(filtered_school)
            self.stats['pages_discovered'] += len(pages)
            
            if not pages:
                print("  ⚠️  No pages discovered - skipping")
                return []
            
            print(f"  ✓ Found {len(pages)} pages")
        except Exception as e:
            print(f"  ❌ Error discovering pages: {e}")
            traceback.print_exc()
            return []
        
        # Step 4: Collect content
        print(f"\n  Step 4: Collecting content...")
        page_contents = []
        for page in pages[:self.max_pages_per_school]:  # Limit pages per school
            try:
                content = self._collect_content_for_page(page)
                if content:
                    page_contents.append(content)
                    self.stats['pages_collected'] += 1
            except Exception as e:
                print(f"    ⚠️  Error collecting {page.url}: {e}")
                continue
        
        if not page_contents:
            print("  ⚠️  No content collected - skipping")
            return []
        
        print(f"  ✓ Collected {len(page_contents)} pages")
        
        # Step 5: Parse content with LLM
        print(f"\n  Step 5: Parsing content with LLM...")
        all_contacts = []
        for page_content in page_contents:
            try:
                contacts = self._parse_content_with_llm(page_content, filtered_school)
                all_contacts.extend(contacts)
            except Exception as e:
                print(f"    ⚠️  Error parsing {page_content.url}: {e}")
                continue
        
        if all_contacts:
            print(f"  ✓ Extracted {len(all_contacts)} contacts")
        else:
            print(f"  ⚠️  No contacts extracted")
        
        print(f"{'='*70}\n")
        
        self.stats['schools_processed'] += 1
        return all_contacts
    
    def _discover_pages_for_school(self, school: School) -> List[Page]:
        """
        Discover pages for a single school using step3's discover_pages method.
        """
        if not school.website:
            return []
        
        pages = []
        try:
            # Use step3's discover_pages method
            discovered_pages = self.page_discoverer.discover_pages(
                school_name=school.name,
                base_url=school.website,
                max_depth=3,
                max_pages_per_school=self.max_pages_per_school,
                top_pages_limit=self.max_pages_per_school
            )
            
            # Convert dicts to Page objects
            for page_dict in discovered_pages:
                pages.append(Page(
                    url=page_dict['url'],
                    school_name=school.name,
                    school_place_id=school.place_id,
                    school_website=school.website,
                    priority_score=page_dict.get('priority_score', 0),
                    page_title=page_dict.get('title'),
                    discovered_via=page_dict.get('url')  # Could enhance this
                ))
        except Exception as e:
            print(f"    Error in page discovery: {e}")
            import traceback
            traceback.print_exc()
        
        return pages
    
    def _collect_content_for_page(self, page: Page) -> Optional[PageContent]:
        """
        Collect content for a single page using step4's collect_page_content method.
        """
        try:
            # Use step4's collect_page_content method (requires school_name and url)
            result = self.content_collector.collect_page_content(page.school_name, page.url)
            
            if not result:
                return None
            
            # Extract HTML content and metadata from result dict
            html_content = result.get('html_content', '')
            fetch_method = result.get('fetch_method', 'unknown')
            email_count = result.get('email_count', 0)
            
            return PageContent(
                url=page.url,
                school_name=page.school_name,
                html_content=html_content or '',
                email_count=email_count,
                has_emails=email_count > 0,
                collection_method=fetch_method
            )
        except Exception as e:
            print(f"    Error collecting content: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _parse_content_with_llm(self, page_content: PageContent, school: School) -> List[Contact]:
        """
        Parse content with LLM using step5's reduction and chunking logic.
        """
        if not page_content.html_content:
            return []
        
        try:
            # Step 1: Reduce HTML to people sections only
            reduced_html = self.llm_parser.reduce_html(page_content.html_content)
            
            if not reduced_html:
                return []
            
            # Step 2: Chunk HTML if needed (step5 handles this internally)
            # Reduced chunk size to lower token usage per request and avoid rate limits
            chunks = self.llm_parser.chunk_html(reduced_html, max_chunk_size=20000)
            
            # Step 3: Process each chunk with rate limiting
            all_contacts = []
            for chunk_idx, chunk in enumerate(chunks):
                # Parse chunk with LLM
                contacts_dicts = self.llm_parser.parse_with_llm(
                    chunk,
                    school.name,
                    page_content.url,
                    max_retries=5  # Increased retries for rate limit handling
                )
                
                # Convert dicts to Contact objects
                for c_dict in contacts_dicts:
                    # Step5 now returns first_name and last_name separately
                    first_name = c_dict.get('first_name', '').strip()
                    last_name = c_dict.get('last_name', '').strip()
                    
                    contact = Contact(
                        first_name=first_name,
                        last_name=last_name,
                        title=c_dict.get('title', ''),
                        email=c_dict.get('email') or None,
                        phone=c_dict.get('phone') or None,
                        school_name=school.name,
                        source_url=page_content.url
                    )
                    all_contacts.append(contact)
                
                # Add delay between chunks to avoid rate limits (except for last chunk)
                if chunk_idx < len(chunks) - 1:
                    time.sleep(1.0)  # 1 second delay between chunks to respect rate limits
            
            # Step 6: Filter contacts by title (keep only administrative roles)
            if all_contacts:
                print(f"    Filtering {len(all_contacts)} contacts by title...")
                filtered_contacts = []
                for contact in all_contacts:
                    # Convert Contact to dict for step6.filter_contact
                    contact_dict = {
                        'first_name': contact.first_name,
                        'last_name': contact.last_name,
                        'title': contact.title,
                        'email': contact.email or '',
                        'phone': contact.phone or ''
                    }
                    
                    # Filter by title
                    should_keep = self.title_filter.filter_contact(contact_dict, max_retries=5)
                    if should_keep:
                        filtered_contacts.append(contact)
                
                print(f"    Kept {len(filtered_contacts)}/{len(all_contacts)} administrative contacts")
                
                # Deduplicate contacts from this page
                if filtered_contacts:
                    # Simple deduplication by email or name
                    seen = set()
                    unique_contacts = []
                    for contact in filtered_contacts:
                        key = (contact.email or '').lower() or contact.full_name().lower()
                        if key and key not in seen:
                            seen.add(key)
                            unique_contacts.append(contact)
                    return unique_contacts
            
            return []
        except Exception as e:
            print(f"    Error parsing with LLM: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def run(
        self,
        counties: List[str] = None,
        cities: List[str] = None,
        batch_size: int = 0,
        output_csv: str = "final_contacts.csv",
        output_no_emails_csv: str = "final_contacts_no_emails.csv"
    ):
        """
        Run the streaming pipeline.
        Processes schools one at a time through all steps.
        """
        print("\n" + "="*70)
        print("STREAMING PIPELINE - ONE LEAD THROUGH ENTIRE SYSTEM")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Output: {output_csv} (with emails) and {output_no_emails_csv} (no emails)")
        print("="*70 + "\n")
        
        # Step 1: Discover schools (generator - yields one at a time)
        print("STEP 1: Discovering schools (legacy Places API)...")
        schools_discovered = 0
        
        # Use city-based search if cities explicitly provided, otherwise use counties
        if cities is not None and len(cities) > 0:
            school_generator = self.school_searcher.discover_schools_cities(
                cities=cities,
                state='Texas',
                num_cities=5  # 5 random cities, 1 API call each
            )
        else:
            # Use counties - limit search terms per county to manage API calls
            # With 100 API calls: ~2 search terms per county allows ~50 counties
            # (accounting for Place Details calls for each school found)
            from step1_streaming import TEXAS_COUNTIES
            counties_to_use = counties or TEXAS_COUNTIES
            max_search_terms_per_county = 2  # Limit to 2 search terms per county to manage API calls efficiently
            school_generator = self.school_searcher.discover_schools(
                counties=counties_to_use,
                state='Texas',
                batch_size=batch_size,
                max_search_terms=max_search_terms_per_county
            )
        
        for school in school_generator:
            schools_discovered += 1
            self.stats['schools_discovered'] = schools_discovered
            
            # Process this school through all steps
            contacts = self.process_single_lead(school)
            
            # Accumulate contacts
            self.all_contacts.extend(contacts)
            
            # Print progress
            print(f"\nProgress: {schools_discovered} schools discovered | "
                  f"{self.stats['schools_processed']} processed | "
                  f"{len(self.all_contacts)} contacts extracted")
        
        # Step 6: Write final CSV files
        print("\n" + "="*70)
        print("STEP 6: WRITING FINAL OUTPUT")
        print("="*70)
        
        # Split contacts by email presence
        contacts_with_emails = [c for c in self.all_contacts if c.has_email()]
        contacts_without_emails = [c for c in self.all_contacts if not c.has_email()]
        
        # Write CSV files
        if contacts_with_emails:
            self._write_final_csv(contacts_with_emails, output_csv)
        else:
            print(f"No contacts with emails to write to {output_csv}")
        
        if output_no_emails_csv:
            if contacts_without_emails:
                self._write_final_csv(contacts_without_emails, output_no_emails_csv)
            else:
                print(f"No contacts without emails to write to {output_no_emails_csv}")
        
        # Update stats
        self.stats['contacts_extracted'] = len(self.all_contacts)
        self.stats['contacts_with_emails'] = len(contacts_with_emails)
        self.stats['contacts_without_emails'] = len(contacts_without_emails)
        
        # Print final summary
        self._print_summary()
    
    def _write_final_csv(self, contacts: List[Contact], filename: str):
        """Write contacts to final CSV file"""
        if not contacts:
            print(f"No contacts to write to {filename}")
            return
        
        fieldnames = ['first_name', 'last_name', 'title', 'email', 'phone', 
                     'school_name', 'source_url', 'confidence_score']
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for contact in contacts:
                writer.writerow(contact.to_dict())
        
        print(f"✓ Wrote {len(contacts)} contacts to {filename}")
    
    
    def _print_summary(self):
        """Print final pipeline summary"""
        print("\n" + "="*70)
        print("PIPELINE COMPLETE")
        print("="*70)
        print(f"Schools discovered: {self.stats['schools_discovered']}")
        print(f"Schools filtered out: {self.stats['schools_filtered_out']}")
        print(f"Schools processed: {self.stats['schools_processed']}")
        print(f"Pages discovered: {self.stats['pages_discovered']}")
        print(f"Pages collected: {self.stats['pages_collected']}")
        if self.stats['contacts_extracted'] > 0:
            print(f"Contacts extracted: {self.stats['contacts_extracted']}")
            print(f"  - With emails: {self.stats['contacts_with_emails']}")
            print(f"  - Without emails: {self.stats['contacts_without_emails']}")
        else:
            print("Contacts extracted: 0")
        print(f"End: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Streaming Pipeline - One Lead Through Entire System')
    parser.add_argument('--google-api-key', required=True, help='Google Places API key (legacy)')
    parser.add_argument('--openai-api-key', required=True, help='OpenAI API key')
    parser.add_argument('--global-max-api-calls', type=int, default=25, help='Global API call cap (default: 25)')
    parser.add_argument('--batch-size', type=int, default=0, help='Number of counties to search (0 = all)')
    parser.add_argument('--max-pages-per-school', type=int, default=3, help='Max pages per school (default: 3)')
    parser.add_argument('--output', default='final_contacts.csv', help='Output CSV (with emails)')
    parser.add_argument('--output-no-emails', default='final_contacts_no_emails.csv', help='Output CSV (no emails)')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = StreamingPipeline(
        google_api_key=args.google_api_key,
        openai_api_key=args.openai_api_key,
        global_max_api_calls=args.global_max_api_calls,
        max_pages_per_school=args.max_pages_per_school
    )
    
    # Run pipeline
    try:
        pipeline.run(
            cities=None,  # Use counties instead of cities for scaled test
            counties=None,  # Will use all Texas counties (randomized)
            batch_size=args.batch_size,
            output_csv=args.output,
            output_no_emails_csv=args.output_no_emails
        )
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        print(f"Partial results: {len(pipeline.all_contacts)} contacts extracted")
        if pipeline.all_contacts:
            pipeline._write_final_csv(
                [c for c in pipeline.all_contacts if c.has_email()],
                f"partial_{args.output}"
            )
            pipeline._write_final_csv(
                [c for c in pipeline.all_contacts if not c.has_email()],
                f"partial_{args.output_no_emails}"
            )
    except Exception as e:
        print(f"\n\nPipeline failed: {e}")
        traceback.print_exc()
        sys.exit(1)

