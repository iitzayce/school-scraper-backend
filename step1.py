"""
STEP 1: SCHOOL SEARCH WITH GOOGLE PLACES API
==============================================
Search for schools and return ALL results - no filtering.

Input: Search parameters (counties, states, search terms)
Output: CSV with school name, address, phone, website URL, metadata
"""

import requests
import csv
import time
from datetime import datetime
from typing import List, Dict, Tuple
import argparse
import random
import re

class SchoolSearcher:
    def __init__(self, api_key: str, global_max_api_calls: int = None):
        self.api_key = api_key
        self.global_max_api_calls = global_max_api_calls
        self.base_url = "https://places.googleapis.com/v1/places:searchText"
        self.all_schools = []
        self.seen_place_ids = set()
        self.stats = {
            'counties_searched': 0,
            'total_api_calls': 0,
            'total_schools_found': 0,
            'schools_with_websites': 0,
            'non_texas_skipped': 0
        }

    def _hit_global_limit(self) -> bool:
        return (
            self.global_max_api_calls is not None and
            self.stats['total_api_calls'] >= self.global_max_api_calls
        )

    def _extract_state_and_county(self, place: Dict) -> Tuple[str, str]:
        """
        Parse addressComponents to pull out the state (level 1) and county (level 2)
        """
        state_value = ''
        county_value = ''
        for component in place.get('addressComponents', []):
            types = component.get('types', [])
            text_value = component.get('shortText') or component.get('longText') or component.get('text') or ''
            text_value = text_value.strip()
            if 'administrative_area_level_1' in types and not state_value:
                state_value = text_value
            if 'administrative_area_level_2' in types and not county_value:
                county_value = text_value
        return state_value, county_value

    def _is_texas_result(self, detected_state: str, formatted_address: str) -> bool:
        """
        Determine if the result belongs to Texas using multiple signals
        """
        if detected_state:
            normalized_state = detected_state.strip().lower()
            if normalized_state in ('tx', 'texas'):
                return True

        address_upper = (formatted_address or '').upper()
        if ', TX ' in address_upper or address_upper.endswith(', TX') or ' TEXAS' in address_upper:
            return True

        # Last fallback: look for state abbreviation pattern
        match = re.search(r',\s*([A-Z]{2})\s+\d{5}', formatted_address or '')
        if match and match.group(1) == 'TX':
            return True

        return False

    def search_county(
        self,
        county: str,
        state: str = 'Texas',
        max_search_terms: int = None,
        max_api_calls: int = None,
        per_county_api_limit: int = None
    ) -> tuple:
        """Search for Christian schools in a specific county"""
        schools = []
        county_api_calls = 0
        
        # Define search terms for Christian schools
        search_terms = [
            f"Christian schools in {county} County, {state}",
            f"Christian academy in {county} County, {state}",
            f"Catholic schools in {county} County, {state}",
            f"Catholic elementary school in {county} County, {state}",
            f"Baptist schools in {county} County, {state}",
            f"Methodist schools in {county} County, {state}",
            f"Lutheran schools in {county} County, {state}",
            f"Presbyterian schools in {county} County, {state}",
            f"Episcopal schools in {county} County, {state}",
            f"Pentecostal schools in {county} County, {state}",
            f"Assembly of God schools in {county} County, {state}",
            f"Church of God schools in {county} County, {state}",
            f"nondenominational Christian schools in {county} County, {state}",
            f"evangelical schools in {county} County, {state}",
            f"private religious schools in {county} County, {state}",
            f"parochial schools in {county} County, {state}"
        ]

        if max_search_terms is not None:
            search_terms = search_terms[:max(0, max_search_terms)]

        county_new_schools = 0

        for query in search_terms:
            if (
                (max_api_calls is not None and self.stats['total_api_calls'] >= max_api_calls) or
                self._hit_global_limit()
            ):
                print("    API call limit reached for this run. Stopping further queries.")
                break
            if per_county_api_limit is not None and county_api_calls >= per_county_api_limit:
                print("    Per-county API call cap reached. Moving to next county.")
                break

            try:
                self.stats['total_api_calls'] += 1
                county_api_calls += 1
                
                headers = {
                    'Content-Type': 'application/json',
                    'X-Goog-Api-Key': self.api_key,
                    'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.types,places.businessStatus,places.addressComponents'
                }
                
                data = {'textQuery': query, 'pageSize': 20}
                response = requests.post(self.base_url, headers=headers, json=data, timeout=60)  # Increased timeout

                if response.status_code == 200:
                    result = response.json()
                    if 'places' in result:
                        for place in result['places']:
                            place_id = place.get('id', '')
                            types = place.get('types', [])
                            address = place.get('formattedAddress', '')

                            detected_state, detected_county = self._extract_state_and_county(place)

                            if not self._is_texas_result(detected_state, address):
                                self.stats['non_texas_skipped'] += 1
                                continue

                            # === SIMPLE DEDUPLICATION ONLY ===
                            # No filtering - return all results from Google Places
                            if place_id in self.seen_place_ids:
                                continue

                            self.seen_place_ids.add(place_id)
                            county_new_schools += 1

                            # Build school record (no filtering - Step 1.5 handles filtering)
                            school = {
                                'place_id': place_id,
                                'name': place.get('displayName', {}).get('text', ''),
                                'address': place.get('formattedAddress', ''),
                                'website': place.get('websiteUri', ''),
                                'phone': place.get('nationalPhoneNumber', ''),
                                'rating': place.get('rating', ''),
                                'user_ratings': place.get('userRatingCount', ''),
                                'types': ', '.join(types),
                                'business_status': place.get('businessStatus', ''),
                                'county': (detected_county or county).replace('County', '').strip(),
                                'state': 'Texas',
                                'detected_state': detected_state or '',
                                'found_via': query.split(' in ')[0]
                            }
                            schools.append(school)
                            
                            if school['website']:
                                self.stats['schools_with_websites'] += 1

                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                print(f"    Error on query: {e}")
                time.sleep(2)

            if (
                (max_api_calls is not None and self.stats['total_api_calls'] >= max_api_calls) or
                self._hit_global_limit() or
                (per_county_api_limit is not None and county_api_calls >= per_county_api_limit)
            ):
                break

        return schools, county_new_schools

    def search_random_county_sample(
        self,
        counties: List[str],
        state: str,
        output_file: str,
        sample_size: int = 10,
        max_search_terms: int = None,
        max_api_calls: int = None
    ):
        """Pick a random county, search it, and return a random sample of schools"""
        print("\n" + "="*70)
        print(f"RANDOM COUNTY SAMPLE MODE - {state.upper()}")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Pick a random county
        selected_county = random.choice(counties)
        print(f"Selected random county: {selected_county} County")
        print(f"Target sample size: {sample_size} schools")
        print("="*70 + "\n")
        
        start_time = time.time()
        
        # Search the selected county
        print(f"Searching {selected_county} County...")
        county_schools, new_count = self.search_county(
            selected_county,
            state,
            max_search_terms=max_search_terms,
            max_api_calls=max_api_calls
        )
        
        self.all_schools.extend(county_schools)
        self.stats['counties_searched'] = 1
        self.stats['total_schools_found'] = len(self.all_schools)
        
        print(f"\nFound {len(self.all_schools)} total schools in {selected_county} County")
        
        # Randomly sample the requested number
        if len(self.all_schools) > sample_size:
            print(f"Randomly sampling {sample_size} schools from {len(self.all_schools)} found...")
            self.all_schools = random.sample(self.all_schools, sample_size)
            self.stats['total_schools_found'] = len(self.all_schools)
        elif len(self.all_schools) < sample_size:
            print(f"Warning: Only found {len(self.all_schools)} schools (requested {sample_size})")
        
        self._save_to_csv(output_file)
        self._print_summary(time.time() - start_time, output_file)

    def search_batch_counties(
        self,
        counties: List[str],
        state: str,
        output_file: str,
        batch_size: int = 0
    ):
        """Search N (or all) counties with global API guardrails"""
        print("\n" + "="*70)
        print(f"BATCH MODE - {state.upper()}")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("Counties: random order, no per-county limit")
        print(f"Global API call cap: {self.global_max_api_calls or 'None'}")
        if batch_size is None or batch_size <= 0 or batch_size >= len(counties):
            print(f"Batch size: ALL {len(counties)} counties (randomized)")
        else:
            print(f"Batch size: {batch_size} counties (randomized)")
        print("="*70 + "\n")
        
        start_time = time.time()
        
        # Shuffle counties for randomness, then take batch_size
        shuffled_counties = counties.copy()
        random.shuffle(shuffled_counties)
        if batch_size is None or batch_size <= 0 or batch_size >= len(counties):
            batch_counties = shuffled_counties
        else:
            batch_counties = shuffled_counties[:batch_size]
        
        print(f"Processing batch of {len(batch_counties)} counties...\n")
        
        for i, county in enumerate(batch_counties, 1):
            if self._hit_global_limit():
                print("Global API call cap reached. Ending batch early.")
                break
            print(f"[{i}/{len(batch_counties)}] Searching {county} County...")
            county_schools, new_count = self.search_county(
                county,
                state,
                max_search_terms=None,  # No limit - search all terms
                max_api_calls=None  # No limit - make all API calls needed
            )
            
            self.all_schools.extend(county_schools)
            self.stats['counties_searched'] = i
            self.stats['total_schools_found'] = len(self.all_schools)
            
            print(f"    Found {new_count} new schools | Total: {self.stats['total_schools_found']}")
            if self._hit_global_limit():
                print("Global API call cap reached. Ending batch early.")
                break
        
        self._save_to_csv(output_file)
        self._print_summary(time.time() - start_time, output_file)

    def search_multiple_random_counties(
        self,
        counties: List[str],
        state: str,
        output_file: str,
        target_schools: int = 100,
        max_search_terms: int = None,
        max_api_calls: int = None
    ):
        """Search multiple random counties until we reach target number of schools"""
        print("\n" + "="*70)
        print(f"MULTIPLE RANDOM COUNTIES MODE - {state.upper()}")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Target: {target_schools} schools")
        print("="*70 + "\n")
        
        start_time = time.time()
        counties_searched = []
        max_counties_to_search = len(counties)
        
        # Shuffle counties for randomness
        shuffled_counties = counties.copy()
        random.shuffle(shuffled_counties)
        
        for county in shuffled_counties[:max_counties_to_search]:
            if len(self.all_schools) >= target_schools:
                break
                
            if (
                (max_api_calls is not None and self.stats['total_api_calls'] >= max_api_calls) or
                self._hit_global_limit()
            ):
                print(f"\nAPI call limit reached. Stopping county searches.")
                break
            
            print(f"Searching {county} County...")
            county_schools, new_count = self.search_county(
                county,
                state,
                max_search_terms=max_search_terms,
                max_api_calls=max_api_calls
            )
            
            self.all_schools.extend(county_schools)
            counties_searched.append(county)
            self.stats['counties_searched'] = len(counties_searched)
            self.stats['total_schools_found'] = len(self.all_schools)
            
            print(f"    Found {new_count} new schools | Total: {self.stats['total_schools_found']}")
            
            if len(self.all_schools) >= target_schools:
                print(f"\nReached target of {target_schools} schools!")
                break
        
        # Randomly sample to exactly target_schools if we have more
        if len(self.all_schools) > target_schools:
            print(f"\nRandomly sampling {target_schools} schools from {len(self.all_schools)} found...")
            self.all_schools = random.sample(self.all_schools, target_schools)
            self.stats['total_schools_found'] = len(self.all_schools)
        elif len(self.all_schools) < target_schools:
            print(f"\nWarning: Only found {len(self.all_schools)} schools (requested {target_schools})")
        
        self._save_to_csv(output_file)
        self._print_summary(time.time() - start_time, output_file)

    def search_split_counties_equal_calls(
        self,
        counties: List[str],
        state: str,
        output_file: str,
        counties_to_pick: int = 5,
        calls_per_county: int = 5
    ):
        """Pick N random counties and evenly split API calls among them"""
        print("\n" + "="*70)
        print(f"EQUAL-SPLIT COUNTY MODE - {state.upper()}")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Target counties: {counties_to_pick} | Calls per county: {calls_per_county}")
        print(f"Global API call cap: {self.global_max_api_calls or 'None'}")
        print("="*70 + "\n")

        if not counties:
            print("No counties provided.")
            return

        available_counties = counties.copy()
        random.shuffle(available_counties)
        selected_counties = available_counties[:min(counties_to_pick, len(available_counties))]

        if self.global_max_api_calls is not None:
            max_possible_calls = len(selected_counties) * calls_per_county
            if self.global_max_api_calls < max_possible_calls:
                adjusted_calls = max(1, self.global_max_api_calls // len(selected_counties))
                print(f"Warning: Global cap ({self.global_max_api_calls}) < desired ({max_possible_calls}).")
                print(f"Adjusting per-county calls to {adjusted_calls}.")
                calls_per_county = adjusted_calls

        start_time = time.time()
        for idx, county in enumerate(selected_counties, 1):
            if calls_per_county <= 0:
                print("Per-county call budget exhausted. Ending early.")
                break
            if self._hit_global_limit():
                print("Global API call cap reached. Ending selection early.")
                break

            remaining_global = None
            if self.global_max_api_calls is not None:
                remaining_global = self.global_max_api_calls - self.stats['total_api_calls']
                if remaining_global <= 0:
                    print("Global API call cap reached. Ending selection early.")
                    break

            effective_calls = calls_per_county
            if remaining_global is not None:
                effective_calls = min(calls_per_county, remaining_global)

            print(f"[{idx}/{len(selected_counties)}] Searching {county} County "
                  f"(up to {effective_calls} API calls)...")
            county_schools, new_count = self.search_county(
                county,
                state,
                max_search_terms=None,
                max_api_calls=None,
                per_county_api_limit=effective_calls
            )
            self.all_schools.extend(county_schools)
            self.stats['counties_searched'] += 1
            self.stats['total_schools_found'] = len(self.all_schools)
            print(f"    Found {new_count} new schools | Total: {self.stats['total_schools_found']}")

        self._save_to_csv(output_file)
        self._print_summary(time.time() - start_time, output_file)

    def search_all_counties(
        self,
        counties: List[str],
        state: str,
        output_file: str,
        max_counties: int = None,
        max_search_terms: int = None,
        max_api_calls: int = None,
        max_schools: int = None
    ):
        """Search all counties and save results"""
        print("\n" + "="*70)
        print(f"{state.upper()} CHRISTIAN SCHOOL DISCOVERY")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if max_counties is not None:
            counties = counties[:max(0, max_counties)]
        print(f"Counties: {len(counties)}")

        if max_search_terms is not None:
            print(f"Search terms per county limited to: {max_search_terms}")
        if max_api_calls is not None:
            print(f"Total API call limit: {max_api_calls}")
        if max_schools is not None:
            print(f"Maximum schools to collect: {max_schools}")

        print("="*70 + "\n")

        start_time = time.time()
        
        for i, county in enumerate(counties, 1):
            if max_api_calls is not None and self.stats['total_api_calls'] >= max_api_calls:
                print("\nAPI call limit reached. Stopping further county searches.")
                break

            print(f"[{i}/{len(counties)}] Searching {county} County...")
            county_schools, new_count = self.search_county(
                county,
                state,
                max_search_terms=max_search_terms,
                max_api_calls=max_api_calls
            )
            self.all_schools.extend(county_schools)
            self.stats['counties_searched'] += 1
            self.stats['total_schools_found'] = len(self.seen_place_ids)

            print(f"    Found {new_count} new schools | Total: {self.stats['total_schools_found']}")

            # Check if we've reached max_schools limit
            if max_schools is not None and len(self.all_schools) >= max_schools:
                print(f"\nReached maximum schools limit ({max_schools}). Stopping search.")
                # Trim to exactly max_schools
                self.all_schools = self.all_schools[:max_schools]
                self.stats['total_schools_found'] = len(self.all_schools)
                break

            # Progress update every 20 counties
            if i % 20 == 0:
                elapsed = time.time() - start_time
                print(f"    Progress: {i}/{len(counties)} ({i/len(counties)*100:.1f}%) | Elapsed: {elapsed/60:.1f} min")

        self._save_to_csv(output_file)
        self._print_summary(time.time() - start_time, output_file)

    def _save_to_csv(self, filename: str):
        """Save results to CSV"""
        fieldnames = [
            'place_id',
            'name',
            'address',
            'website',
            'phone',
            'rating',
            'user_ratings',
            'types',
            'business_status',
            'county',
            'state',
            'detected_state',
            'found_via'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.all_schools)

    def _print_summary(self, elapsed: float, output_file: str):
        """Print final summary"""
        print("\n" + "="*70)
        print("DISCOVERY COMPLETE!")
        print("="*70)
        print(f"Time: {elapsed/60:.1f} min")
        print(f"Schools Found: {self.stats['total_schools_found']}")
        print(f"With Website: {self.stats['schools_with_websites']} "
              f"({self.stats['schools_with_websites']/max(1, self.stats['total_schools_found'])*100:.1f}%)")
        print(f"API Calls: {self.stats['total_api_calls']}")
        if self.stats.get('non_texas_skipped'):
            print(f"Skipped non-TX results: {self.stats['non_texas_skipped']}")
        print(f"File: {output_file}")
        print("="*70)


# Texas counties list (all 254 counties, alphabetical)
TEXAS_COUNTIES = [
    "Anderson", "Andrews", "Angelina", "Aransas", "Archer", "Armstrong", "Atascosa", "Austin",
    "Bailey", "Bandera", "Bastrop", "Baylor", "Bee", "Bell", "Bexar", "Blanco", "Borden", "Bosque",
    "Bowie", "Brazoria", "Brazos", "Brewster", "Briscoe", "Brooks", "Brown", "Burleson", "Burnet",
    "Caldwell", "Calhoun", "Callahan", "Cameron", "Camp", "Carson", "Cass", "Castro", "Chambers",
    "Cherokee", "Childress", "Clay", "Cochran", "Coke", "Coleman", "Collin", "Collingsworth",
    "Colorado", "Comal", "Comanche", "Concho", "Cooke", "Coryell", "Cottle", "Crane", "Crockett",
    "Crosby", "Culberson", "Dallam", "Dallas", "Dawson", "Deaf Smith", "Delta", "Denton", "DeWitt",
    "Dickens", "Dimmit", "Donley", "Duval", "Eastland", "Ector", "Edwards", "Ellis", "El Paso",
    "Erath", "Falls", "Fannin", "Fayette", "Fisher", "Floyd", "Foard", "Fort Bend", "Franklin",
    "Freestone", "Frio", "Gaines", "Galveston", "Garza", "Gillespie", "Glasscock", "Goliad",
    "Gonzales", "Gray", "Grayson", "Gregg", "Grimes", "Guadalupe", "Hale", "Hall", "Hamilton",
    "Hansford", "Hardeman", "Hardin", "Harris", "Harrison", "Hartley", "Haskell", "Hays", "Hemphill",
    "Henderson", "Hidalgo", "Hill", "Hockley", "Hood", "Hopkins", "Houston", "Howard", "Hudspeth",
    "Hunt", "Hutchinson", "Irion", "Jack", "Jackson", "Jasper", "Jeff Davis", "Jefferson", "Jim Hogg",
    "Jim Wells", "Johnson", "Jones", "Karnes", "Kaufman", "Kendall", "Kenedy", "Kent", "Kerr",
    "Kimble", "King", "Kinney", "Kleberg", "Knox", "La Salle", "Lamar", "Lamb", "Lampasas", "Lavaca",
    "Lee", "Leon", "Liberty", "Limestone", "Lipscomb", "Live Oak", "Llano", "Loving", "Lubbock",
    "Lynn", "Madison", "Marion", "Martin", "Mason", "Matagorda", "Maverick", "McCulloch", "McLennan",
    "McMullen", "Medina", "Menard", "Midland", "Milam", "Mills", "Mitchell", "Montague", "Montgomery",
    "Moore", "Morris", "Motley", "Nacogdoches", "Navarro", "Newton", "Nolan", "Nueces", "Ochiltree",
    "Oldham", "Orange", "Palo Pinto", "Panola", "Parker", "Parmer", "Pecos", "Polk", "Potter",
    "Presidio", "Rains", "Randall", "Reagan", "Real", "Red River", "Reeves", "Refugio", "Roberts",
    "Robertson", "Rockwall", "Runnels", "Rusk", "Sabine", "San Augustine", "San Jacinto",
    "San Patricio", "San Saba", "Schleicher", "Scurry", "Shackelford", "Shelby", "Sherman", "Smith",
    "Somervell", "Starr", "Stephens", "Sterling", "Stonewall", "Sutton", "Swisher", "Tarrant",
    "Taylor", "Terrell", "Terry", "Throckmorton", "Titus", "Tom Green", "Travis", "Trinity", "Tyler",
    "Upshur", "Upton", "Uvalde", "Val Verde", "Van Zandt", "Victoria", "Walker", "Waller", "Ward",
    "Washington", "Webb", "Wharton", "Wheeler", "Wichita", "Wilbarger", "Willacy", "Williamson",
    "Wilson", "Winkler", "Wise", "Wood", "Yoakum", "Young", "Zapata", "Zavala"
]

# Iowa counties list (abbreviated for testing - add more as needed)
IOWA_COUNTIES = [
    'Adair', 'Adams', 'Allamakee', 'Appanoose', 'Audubon', 'Benton', 'Black Hawk', 'Boone',
    'Bremer', 'Buchanan', 'Buena Vista', 'Butler', 'Calhoun', 'Carroll', 'Cass', 'Cedar',
    'Cerro Gordo', 'Cherokee', 'Chickasaw', 'Clarke', 'Clay', 'Clayton', 'Clinton', 'Crawford',
    'Dallas', 'Davis', 'Decatur', 'Delaware', 'Des Moines', 'Dickinson', 'Dubuque', 'Emmet',
    # Add more counties as needed for full runs (99 total)
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search for Christian schools using Google Places API')
    parser.add_argument('--api-key', required=True, help='Google Places API key')
    parser.add_argument('--state', default='Texas', help='State to search (default: Texas)')
    parser.add_argument('--output', default='schools_found.csv', help='Output CSV filename')
    parser.add_argument('--max-counties', type=int, help='Maximum number of counties to search')
    parser.add_argument('--max-search-terms', type=int, help='Maximum search queries per county')
    parser.add_argument('--max-api-calls', type=int, help='Maximum total Google Places API calls for this run')
    parser.add_argument('--max-schools', type=int, help='Maximum number of schools to collect (stops early when reached)')
    parser.add_argument('--random-county-sample', type=int, metavar='N', help='Pick a random county and return N random schools (testing mode)')
    parser.add_argument('--multiple-random-counties', type=int, metavar='N', help='Search multiple random counties until N schools are found')
    parser.add_argument('--batch-counties', type=int, metavar='N', default=0, help='BATCH MODE: Search N counties (0 = all counties)')
    parser.add_argument('--global-max-api-calls', type=int, default=25, help='Absolute cap on total Google Places API calls (default: 25)')
    parser.add_argument('--split-five-counties', action='store_true',
                        help='Pick 5 random counties and split API calls evenly across them (default 5 calls each)')
    
    args = parser.parse_args()
    
    # Select county list based on state
    counties = TEXAS_COUNTIES if args.state.lower() == 'texas' else IOWA_COUNTIES
    
    searcher = SchoolSearcher(api_key=args.api_key, global_max_api_calls=args.global_max_api_calls)
    
    try:
        if args.split_five_counties:
            searcher.search_split_counties_equal_calls(
                counties,
                args.state,
                args.output,
                counties_to_pick=5,
                calls_per_county=5
            )
        # If batch-counties is specified, use batch mode (NO LIMITERS)
        elif args.batch_counties is not None:
            searcher.search_batch_counties(
                counties,
                args.state,
                args.output,
                batch_size=args.batch_counties
            )
        # If multiple-random-counties is specified, use that mode
        elif args.multiple_random_counties is not None:
            searcher.search_multiple_random_counties(
                counties,
                args.state,
                args.output,
                target_schools=args.multiple_random_counties,
                max_search_terms=args.max_search_terms,
                max_api_calls=args.max_api_calls
            )
        # If random-county-sample is specified, use that mode
        elif args.random_county_sample is not None:
            searcher.search_random_county_sample(
                counties,
                args.state,
                args.output,
                sample_size=args.random_county_sample,
                max_search_terms=args.max_search_terms,
                max_api_calls=args.max_api_calls
            )
        else:
            # Otherwise use the normal search mode
            print(f"\nStarting Christian School Discovery for {args.state}...")
            time.sleep(2)
        searcher.search_all_counties(
            counties,
            args.state,
            args.output,
            max_counties=args.max_counties,
            max_search_terms=args.max_search_terms,
                max_api_calls=args.max_api_calls,
                max_schools=args.max_schools
        )
    except KeyboardInterrupt:
        searcher._save_to_csv(args.output)
        print(f"\nInterrupted. Saved partial results to: {args.output}")
    except Exception as e:
        searcher._save_to_csv(args.output)
        print(f"\nError: {e} | Saved partial results to: {args.output}")
