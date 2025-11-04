"""
School Scraper Module
Adapted from the original texas_scraper_mac.py to work as an API service
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
from bs4 import BeautifulSoup
import requests
import re
from urllib.parse import urljoin, urlparse
import time

# ============================================================================
# SELENIUM SETUP
# ============================================================================

def setup_selenium():
    """Initialize headless Chrome browser"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver


# ============================================================================
# VALIDATION & SCORING
# ============================================================================

def is_valid_email(email):
    """Check if email format is valid"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email)) if email else False


def is_valid_admin_email(email, title):
    """Check if email is not a generic non-admin email"""
    if not email:
        return False
    bad = ['lunch@', 'kitchen@', 'food@', 'athletics@', 'ffa@', 'sports@']
    return not any(p in email.lower() for p in bad)


def is_admin_contact(name, title, email):
    """Determine if a contact is likely an administrator"""
    if not title:
        return any(p in email.lower() for p in [
            'principal@', 'headmaster@', 'admin@', 'administrator@',
            'director@', 'superintendent@', 'president@', 'dean@',
            'office@', 'secretary@', 'manager@'
        ])
    
    title_lower = re.sub(r'^(mr\.|mrs\.|ms\.|dr\.|miss|father|fr\.|rev\.)\s+', '', title.lower())
    
    include = [
        'principal', 'head of school', 'headmaster', 'superintendent', 'president',
        'administrator', 'admin', 'director', 'dean', 'ceo', 'chief', 'executive',
        'vice principal', 'assistant principal', 'office manager', 'secretary'
    ]
    
    exclude = [
        r'\d+(st|nd|rd|th)\s+grade', 'kindergarten teacher', 'coach', 'custodian'
    ]
    
    if any(re.search(p, title_lower) for p in exclude):
        return False
    
    return any(k in title_lower for k in include)


# ============================================================================
# HTTP REQUESTS WITH RETRY
# ============================================================================

def safe_get(url, headers, timeout=20, max_retries=2):
    """Make HTTP request with retry logic and timeout handling"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
            return response
        except (requests.exceptions.Timeout, 
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout) as e:
            if attempt == max_retries - 1:
                print(f"    ⏱️ Timeout - Skipping {url[:50]}...")
                return None
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            print(f"    ❌ Error - Skipping {url[:50]}: {str(e)[:80]}")
            return None
        except Exception as e:
            print(f"    ❌ Unexpected error - Skipping {url[:50]}: {str(e)[:80]}")
            return None
    return None


# ============================================================================
# CONTACT EXTRACTION
# ============================================================================

def extract_contacts_bs4(soup, url):
    """Extract contacts using BeautifulSoup"""
    contacts = []
    
    if not soup:
        return contacts
    
    # Find all mailto links
    for link in soup.find_all('a', href=re.compile(r'^mailto:')):
        try:
            email = link['href'].replace('mailto:', '').split('?')[0].strip()
            if not is_valid_email(email):
                continue
            
            name = title = ""
            
            # Try to find name and title from nearby text
            parent = link.find_parent(['div', 'p', 'li', 'td'])
            if parent:
                text = parent.get_text(separator='\n', strip=True)
                lines = [l.strip() for l in text.split('\n') if l.strip()]
                
                for i, line in enumerate(lines):
                    if email in line:
                        continue
                    if re.match(r'^[A-Z]', line) and len(line) > 2:
                        name = line
                        if i + 1 < len(lines):
                            title = lines[i + 1]
                        break
            
            contacts.append({
                'name': name[:100] if name else "",
                'title': title[:150] if title else "",
                'email': email
            })
            
        except Exception:
            continue
    
    return contacts


def extract_contacts_selenium(driver, url):
    """Extract contacts using Selenium"""
    contacts = []
    
    try:
        driver.get(url)
        time.sleep(3)
    except (TimeoutException, WebDriverException):
        return contacts
    
    try:
        links = driver.find_elements(By.CSS_SELECTOR, 'a[href^="mailto:"]')
    except:
        return contacts
    
    for link in links:
        try:
            email = link.get_attribute('href').replace('mailto:', '').split('?')[0].strip()
            if not is_valid_email(email):
                continue
            
            name = title = ""
            
            # Try to extract name and title from parent element
            try:
                parent = link.find_element(By.XPATH, '..')
                lines = [l.strip() for l in parent.text.split('\n') if l.strip()]
                if len(lines) >= 1:
                    name = lines[0]
                if len(lines) >= 2:
                    title = lines[1]
            except:
                pass
            
            contacts.append({
                'name': name[:100] if name else "",
                'title': title[:150] if title else "",
                'email': email
            })
            
        except (StaleElementReferenceException, Exception):
            continue
    
    return contacts


# ============================================================================
# MAIN SCRAPING FUNCTION
# ============================================================================

def scrape_school(school_name, website, driver):
    """Scrape a single school website for admin contacts"""
    try:
        if not website or website.strip() == '':
            return []
        
        if not website.startswith('http'):
            website = 'https://' + website
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        
        # Get main page
        response = safe_get(website, headers)
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract contacts using both methods
        bs4_contacts = extract_contacts_bs4(soup, website)
        sel_contacts = extract_contacts_selenium(driver, website)
        
        all_contacts = bs4_contacts + sel_contacts
        
        # Deduplicate by email
        unique_contacts = {}
        for contact in all_contacts:
            email = contact['email']
            if email and email not in unique_contacts:
                if is_admin_contact(contact['name'], contact['title'], email) and \
                   is_valid_admin_email(email, contact['title']):
                    unique_contacts[email] = contact
        
        return list(unique_contacts.values())
        
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f"    ❌ Error scraping {school_name}: {str(e)[:100]}")
        return []


# ============================================================================
# JOB RUNNER
# ============================================================================

def run_scraper_job(job_id: str, jobs: dict):
    """
    Run the scraper for a specific job.
    This function is called as a background task.
    """
    job = jobs[job_id]
    schools = job["schools"]
    results = []
    
    # Setup Selenium
    driver = setup_selenium()
    
    try:
        for i, school in enumerate(schools):
            try:
                school_name = school.get('name', '')
                website = school.get('website', '')
                
                print(f"Scraping {i+1}/{len(schools)}: {school_name}")
                
                # Scrape the school
                contacts = scrape_school(school_name, website, driver)
                
                # Add school info to each contact
                for contact in contacts:
                    results.append({
                        'school_name': school_name,
                        'website': website,
                        'contact_name': contact['name'],
                        'title': contact['title'],
                        'email': contact['email']
                    })
                
                # Update progress
                jobs[job_id]["completed"] = i + 1
                jobs[job_id]["results"] = results
                
            except Exception as e:
                print(f"Error on school {school.get('name', 'unknown')}: {e}")
                jobs[job_id]["completed"] = i + 1
                continue
        
        # Mark job as complete
        jobs[job_id]["status"] = "completed"
        print(f"Job {job_id} completed! Found {len(results)} contacts")
        
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)
        print(f"Job {job_id} failed: {e}")
        
    finally:
        try:
            driver.quit()
        except:
            pass
