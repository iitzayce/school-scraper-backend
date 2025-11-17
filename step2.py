"""
STEP 2: DISCOVER SITE PAGES
============================
Crawl each school's website to discover ALL pages.
No filtering - just map out the entire site structure.

Input: CSV from Step 1 with base URLs
Output: CSV with all discovered pages per school
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import time
from typing import List, Dict, Set
import re
import pandas as pd


class PageDiscoverer:
    def __init__(self, timeout: int = 30, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        # Keywords to prioritize pages likely to have contact info
        self.priority_keywords = [
            'staff', 'faculty', 'administration', 'admin', 'contact', 'about',
            'leadership', 'team', 'directory', 'personnel', 'our-team',
            'meet-our', 'who-we-are', 'board', 'principal', 'superintendent'
        ]
        
    def safe_get(self, url: str) -> requests.Response:
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                print(f"      Timeout on attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
            except requests.exceptions.RequestException as e:
                print(f"      Request error: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
        return None

    def extract_links(self, base_url: str, html: str) -> Set[str]:
        """Extract all internal links from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        
        # Get domain for filtering internal links only
        base_domain = urlparse(base_url).netloc
        
        # Keywords that indicate important hash fragments to preserve
        important_fragment_keywords = ['team', 'staff', 'faculty', 'leadership', 'directory', 
                                       'contact', 'about', 'administrat', 'office']
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Convert relative URLs to absolute
            full_url = urljoin(base_url, href)
            
            # Parse URL
            parsed = urlparse(full_url)
            
            # Only include internal links from same domain
            if parsed.netloc == base_domain:
                # Check if fragment contains important keywords
                fragment = parsed.fragment.lower() if parsed.fragment else ''
                has_important_fragment = any(keyword in fragment for keyword in important_fragment_keywords)
                
                # Preserve fragment if it contains important keywords, otherwise remove it
                if has_important_fragment and parsed.fragment:
                    # Keep the fragment for important sections
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}#{parsed.fragment}"
                else:
                    # Remove fragments and query params for cleaner URLs (standard behavior)
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                
                # Skip common non-content URLs
                skip_patterns = [
                    r'\.pdf$', r'\.jpg$', r'\.png$', r'\.gif$', r'\.jpeg$',
                    r'\.doc$', r'\.docx$', r'\.zip$', r'\.mp4$', r'\.mp3$',
                    r'/wp-admin/', r'/wp-login', r'/wp-content/uploads/',
                    r'javascript:', r'mailto:', r'tel:'
                ]
                
                # Skip patterns check (removed '#' from patterns since we're preserving some)
                if not any(re.search(pattern, clean_url, re.IGNORECASE) for pattern in skip_patterns):
                    links.add(clean_url)
        
        return links

    def score_page_priority(self, url: str) -> int:
        """Score URL based on likelihood of containing contact info"""
        url_lower = url.lower()
        score = 0
        
        for keyword in self.priority_keywords:
            if keyword in url_lower:
                score += 10
        
        # Boost for specific high-value pages
        if 'contact' in url_lower or 'staff' in url_lower:
            score += 20
        
        # EXTRA BOOST for hash fragments indicating team/staff pages
        # e.g., /about/#about-team should score very high
        if '#' in url_lower:
            hash_part = url_lower.split('#')[1] if '#' in url_lower else ''
            hash_keywords = ['team', 'staff', 'faculty', 'leadership', 'directory', 'contact']
            if any(keyword in hash_part for keyword in hash_keywords):
                score += 30  # Big boost for team/staff hash fragments
        
        return score

    def discover_pages(self, school_name: str, base_url: str, max_depth: int = 2, max_pages_per_school: int = 30, top_pages_limit: int = 5) -> List[Dict]:
        """
        Discover all pages on a school website
        
        Args:
            school_name: Name of the school
            base_url: Homepage URL
            max_depth: Maximum crawl depth (default 2)
            max_pages_per_school: Maximum pages to discover per school (default 30)
            
        Returns:
            List of page dictionaries with URL, title, and priority score
        """
        print(f"\n  Discovering pages for: {school_name}")
        print(f"  Base URL: {base_url}")
        
        if not base_url or base_url == '':
            print("    ⚠️  No website URL provided")
            return []
        
        visited = set()
        # Use priority queue: (negative_priority, depth, url) - negative for max-heap behavior
        import heapq
        to_visit = []
        heapq.heappush(to_visit, (0, 0, base_url))  # Start with homepage (priority 0)
        discovered_pages = []
        high_priority_found = 0  # Count pages with score >= 30
        
        while to_visit and len(discovered_pages) < max_pages_per_school:
            # Pop highest priority page
            neg_priority, depth, current_url = heapq.heappop(to_visit)
            priority_estimate = -neg_priority
            
            # Skip if already visited or max depth reached
            if current_url in visited or depth > max_depth:
                continue
            
            # Early stopping: if we found enough high-priority pages, stop crawling low-priority ones
            if len(discovered_pages) >= max_pages_per_school:
                break
            
            # Skip low-priority pages if we already have enough high-priority ones
            if high_priority_found >= 10 and priority_estimate < 20:
                continue
            
            visited.add(current_url)
            
            try:
                response = self.safe_get(current_url)
                if not response:
                    continue
                
                # Extract page title
                soup = BeautifulSoup(response.text, 'html.parser')
                title = soup.find('title')
                title_text = title.get_text().strip() if title else ''
                
                # Calculate priority score
                priority = self.score_page_priority(current_url)
                
                # Track high-priority pages
                if priority >= 30:
                    high_priority_found += 1
                
                # Store page info
                page_info = {
                    'school_name': school_name,
                    'url': current_url,
                    'title': title_text,
                    'priority_score': priority,
                    'depth': depth
                }
                discovered_pages.append(page_info)
                
                # Extract links for next level crawl (if not at max depth and not at page limit)
                if depth < max_depth and len(discovered_pages) < max_pages_per_school:
                    new_links = self.extract_links(base_url, response.text)
                    
                    # Prioritize links: score them before adding to queue
                    scored_links = []
                    for link in new_links:
                        if link not in visited:
                            link_priority = self.score_page_priority(link)
                            scored_links.append((link, link_priority))
                    
                    # Sort by priority and add top links first (limit to top 20 per page to avoid explosion)
                    scored_links.sort(key=lambda x: x[1], reverse=True)
                    for link, link_priority in scored_links[:20]:
                        if link not in visited and len(discovered_pages) < max_pages_per_school:
                            heapq.heappush(to_visit, (-link_priority, depth + 1, link))
                
                time.sleep(0.5)  # Polite crawling delay
                
            except Exception as e:
                print(f"    Error crawling {current_url}: {e}")
                continue
        
        # Sort by priority score (highest first)
        discovered_pages.sort(key=lambda x: x['priority_score'], reverse=True)
        
        # Final filter: Only keep top N pages per school (by priority)
        discovered_pages = discovered_pages[:top_pages_limit]
        
        print(f"    ✓ Discovered {len(discovered_pages)} pages (kept top {top_pages_limit} by priority)")
        if discovered_pages:
            print(f"    Top priority pages:")
            for page in discovered_pages:
                print(f"      - {page['title'][:50]} (score: {page['priority_score']})")
        
        return discovered_pages

    def process_schools_csv(self, input_csv: str, output_csv: str, max_depth: int = 2, max_pages_per_school: int = 30, top_pages_limit: int = 5):
        """
        Process schools from Step 1 CSV and discover all their pages
        
        Args:
            input_csv: CSV file from Step 1 with school data
            output_csv: Output CSV with discovered pages
            max_depth: Maximum crawl depth
            max_pages_per_school: Maximum pages per school
        """
        print("\n" + "="*70)
        print("STEP 2: PAGE DISCOVERY")
        print("="*70)
        
        # Read input CSV
        df = pd.read_csv(input_csv)
        
        # Filter to schools with websites (process ALL schools)
        df_with_urls = df[df['website'].notna() & (df['website'] != '')]
        
        print(f"Processing {len(df_with_urls)} schools with websites")
        print("="*70 + "\n")
        
        all_pages = []
        
        for idx, row in df_with_urls.iterrows():
            school_name = row['name']
            base_url = row['website']
            
            print(f"\n[{idx + 1}/{len(df_with_urls)}] {school_name}")
            
            try:
                pages = self.discover_pages(school_name, base_url, max_depth=max_depth, max_pages_per_school=max_pages_per_school, top_pages_limit=top_pages_limit)
                all_pages.extend(pages)
                
            except Exception as e:
                print(f"    ❌ Error: {e}")
                continue
            
            # Save progress every 10 schools
            if (idx + 1) % 10 == 0:
                self._save_progress(all_pages, output_csv)
                print(f"\n  💾 Progress saved: {len(all_pages)} pages discovered")
        
        # Final save
        self._save_progress(all_pages, output_csv)
        
        print("\n" + "="*70)
        print("PAGE DISCOVERY COMPLETE")
        print("="*70)
        print(f"Total pages discovered: {len(all_pages)}")
        print(f"Output file: {output_csv}")
        print("="*70)

    def _save_progress(self, pages: List[Dict], filename: str):
        """Save discovered pages to CSV"""
        if not pages:
            return
        
        df = pd.DataFrame(pages)
        df.to_csv(filename, index=False)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Discover pages on school websites')
    parser.add_argument('--input', required=True, help='Input CSV from Step 1')
    parser.add_argument('--output', default='step2_pages.csv', help='Output CSV filename')
    parser.add_argument('--max-depth', type=int, default=2, help='Maximum crawl depth (default: 2)')
    parser.add_argument('--max-pages-per-school', type=int, default=30, help='Maximum pages to discover per school (default: 30)')
    parser.add_argument('--top-pages-limit', type=int, default=5, help='Final filter: keep only top N pages per school by priority (default: 5)')
    
    args = parser.parse_args()
    
    discoverer = PageDiscoverer()
    discoverer.process_schools_csv(args.input, args.output, max_depth=args.max_depth, max_pages_per_school=args.max_pages_per_school, top_pages_limit=args.top_pages_limit)
