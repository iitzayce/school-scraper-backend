"""
STEP 5: COMPILE FINAL CSV
==========================
Validate and clean the contacts extracted by the LLM in Step 4.
Remove duplicates, validate emails, calculate confidence scores.

Input: CSV from Step 4 with LLM-extracted contacts
Output: Final cleaned CSV with validated admin contacts
"""

import pandas as pd
import re
from typing import List, Dict, Set
from datetime import datetime
from pathlib import Path
import shutil


class FinalCompiler:
    def __init__(self):
        # Email validation pattern
        self.email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        # Phone formatting pattern
        self.phone_pattern = r'(\d{3})[-.]?(\d{3})[-.]?(\d{4})'
        
        # Generic/invalid emails to filter
        self.invalid_emails = [
            'info@', 'contact@', 'admin@', 'office@', 'webmaster@',
            'noreply@', 'no-reply@', 'hello@', 'support@'
        ]
        
        # Generic text patterns that should NOT be names (per meeting notes)
        self.generic_name_patterns = [
            'about', 'admissions', 'contact us', 'home', 'welcome',
            'staff directory', 'faculty', 'administration', 'our team',
            'meet our', 'who we are', 'school information', 'general information'
        ]
        
        # Common placeholder/fake names to filter out
        self.placeholder_names = [
            'john doe', 'jane doe', 'john smith', 'jane smith',
            'bob jones', 'test user', 'example name', 'sample user',
            'john test', 'jane test', 'placeholder', 'demo user',
            'john example', 'jane example', 'test name', 'sample name'
        ]
        
        # NO FILTERING - LLM handles all filtering
        # Removed exclude_keywords - not used anymore
    
    def is_valid_email(self, email: str) -> bool:
        """Validate email format and filter generic addresses"""
        if not email or pd.isna(email):
            return False
        
        email = str(email).strip().lower()
        
        # Check format
        if not re.match(self.email_pattern, email):
            return False
        
        # Check for invalid generic emails
        if any(invalid in email for invalid in self.invalid_emails):
            return False
        
        return True
    
    # Removed is_admin_role() - NO FILTERING in Python, LLM handles all filtering
    
    def format_phone(self, phone: str) -> str:
        """Format phone number to standard format"""
        if not phone or pd.isna(phone):
            return ''
        
        phone = str(phone).strip()
        
        # Extract digits
        digits = re.sub(r'\D', '', phone)
        
        # Format if 10 digits
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        else:
            return phone  # Return as-is if can't format
    
    def is_valid_name(self, name: str) -> bool:
        """
        Check if name is valid (not generic page text or placeholder names)
        
        Returns:
            True if name appears to be a real person name
        """
        if not name or pd.isna(name):
            return False
        
        name_lower = str(name).strip().lower()
        
        # Check for placeholder/fake names (exact match or contains)
        for placeholder in self.placeholder_names:
            if placeholder == name_lower or placeholder in name_lower:
                return False
        
        # Check for generic text patterns
        for pattern in self.generic_name_patterns:
            if pattern in name_lower:
                return False
        
        # Names should typically be 2-4 words
        parts = name.split()
        if len(parts) < 1 or len(parts) > 5:
            return False
        
        # Check if it looks like a person name (has letters, not just numbers/symbols)
        if not re.search(r'[a-zA-Z]', name):
            return False
        
        return True
    
    def clean_name(self, name: str) -> tuple:
        """
        Split name into first and last name
        
        Returns:
            (first_name, last_name) tuple
        """
        if not name or pd.isna(name):
            return ('', '')
        
        name = str(name).strip()
        
        # Remove titles
        name = re.sub(r'^(mr\.|mrs\.|ms\.|dr\.|miss|father|fr\.|rev\.)\s+', '', name, flags=re.IGNORECASE)
        
        parts = name.split()
        
        if len(parts) == 0:
            return ('', '')
        elif len(parts) == 1:
            return (parts[0], '')
        elif len(parts) == 2:
            return (parts[0], parts[1])
        else:
            # If more than 2 parts, assume first is first name, rest is last name
            return (parts[0], ' '.join(parts[1:]))
    
    def calculate_confidence_score(self, row: pd.Series) -> int:
        """
        Calculate confidence score for a contact (0-100)
        Based on completeness and quality of data
        NO TITLE FILTERING - just data completeness
        """
        score = 0
        
        # Email presence (optional) - 20 points if present and valid
        email = row.get('email', '')
        if email and not pd.isna(email) and str(email).strip():
            if self.is_valid_email(email):
                score += 20
        
        # Name completeness - 30 points
        if row.get('first_name') and row.get('last_name'):
            score += 30
        elif row.get('first_name') or row.get('last_name'):
            score += 15
        
        # Title presence - 20 points (no quality judgment, just presence)
        if row.get('title') and not pd.isna(row.get('title')):
            score += 20
        
        # Phone presence - 20 points
        if row.get('phone') and not pd.isna(row.get('phone')) and str(row.get('phone')).strip():
            score += 20
        
        return min(score, 100)
    
    def deduplicate_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate contacts
        - If email exists: dedupe by email
        - If no email: dedupe by name + school_name
        Keep the one with highest confidence score
        """
        # Sort by confidence score (descending)
        df = df.sort_values('confidence_score', ascending=False)
        
        # Separate contacts with and without emails
        df_with_email = df[df['email'].notna() & (df['email'] != '')].copy()
        df_no_email = df[df['email'].isna() | (df['email'] == '')].copy()
        
        # Dedupe contacts with emails by email
        if not df_with_email.empty:
            df_with_email = df_with_email.drop_duplicates(subset=['email'], keep='first')
        
        # Dedupe contacts without emails by name + school_name
        if not df_no_email.empty:
            df_no_email = df_no_email.drop_duplicates(subset=['name', 'school_name'], keep='first')
        
        # Combine back
        if not df_with_email.empty and not df_no_email.empty:
            return pd.concat([df_with_email, df_no_email]).reset_index(drop=True)
        elif not df_with_email.empty:
            return df_with_email.reset_index(drop=True)
        elif not df_no_email.empty:
            return df_no_email.reset_index(drop=True)
        else:
            return df.reset_index(drop=True)
    
    def compile_final_csv(self, input_csv: str, output_csv: str):
        """
        Create final cleaned and validated CSV
        
        Args:
            input_csv: CSV from Step 4 with parsed contacts
            output_csv: Final output CSV filename
        """
        print("\n" + "="*70)
        print("STEP 5: COMPILING FINAL CSV")
        print("="*70)
        
        # Read parsed contacts
        df = pd.read_csv(input_csv)
        
        print(f"Initial contacts: {len(df)}")
        
        # Clean and validate
        print("\nCleaning and validating...")
        
        # Validate emails (only format validation, don't filter empty emails)
        # Empty emails are valid - we want to keep contacts without emails
        # Only validate format if email is present
        df['email_valid'] = df['email'].apply(lambda x: True if (pd.isna(x) or str(x).strip() == '') else self.is_valid_email(x))
        df = df[df['email_valid'] == True]
        print(f"  After email validation: {len(df)} (empty emails kept)")
        
        # Validate names (filter out generic text/placeholders only)
        df['name_valid'] = df['name'].apply(self.is_valid_name)
        df = df[df['name_valid'] == True]
        print(f"  After name validation (removed generic text): {len(df)}")
        
        # NO ROLE FILTERING - LLM handles all title filtering
        print(f"  Skipping role validation (LLM handles all filtering)")
        
        # Check if dataframe is empty
        if len(df) == 0:
            print("  WARNING: No contacts remaining after validation")
            # Create empty CSV with proper columns
            empty_df = pd.DataFrame(columns=['School Name', 'First Name', 'Last Name', 'Title', 'Email', 'Phone', 'Confidence Score', 'Source URL'])
            empty_df.to_csv(output_csv, index=False)
            self._copy_to_downloads(output_csv)
            return
        
        # Split names
        df[['first_name', 'last_name']] = df['name'].apply(
            lambda x: pd.Series(self.clean_name(x))
        )
        
        # Format phones
        df['phone'] = df['phone'].apply(self.format_phone)
        
        # Calculate confidence scores
        df['confidence_score'] = df.apply(self.calculate_confidence_score, axis=1)
        
        # Deduplicate
        print(f"\nBefore deduplication: {len(df)}")
        df = self.deduplicate_contacts(df)
        print(f"After deduplication: {len(df)}")
        
        # Create final structure
        final_df = pd.DataFrame({
            'School Name': df['school_name'],
            'First Name': df['first_name'],
            'Last Name': df['last_name'],
            'Title': df['title'],
            'Email': df['email'],
            'Phone': df['phone'],
            'Source URL': df['source_url'],
            'Confidence Score': df['confidence_score']
        })
        
        # Sort by confidence score (descending)
        final_df = final_df.sort_values('Confidence Score', ascending=False)
        
        # Add metadata columns
        final_df['Date Collected'] = datetime.now().strftime('%Y-%m-%d')
        final_df['Verified'] = ''  # For manual verification tracking
        final_df['Notes'] = ''
        
        # Save final CSV
        final_df.to_csv(output_csv, index=False)
        
        # Copy to user's Downloads folder for easy access
        self._copy_to_downloads(output_csv)
        
        # Print summary
        self._print_summary(final_df, output_csv)
        
        # Create quality report
        self._create_quality_report(final_df, output_csv.replace('.csv', '_quality_report.txt'))
    
    def _copy_to_downloads(self, file_path: str):
        """Copy the final CSV to the user's Downloads folder"""
        try:
            downloads_dir = Path.home() / "Downloads"
            if not downloads_dir.exists():
                print(f"  WARNING: Downloads folder not found at {downloads_dir}. Skipping copy.")
                return
            
            destination = downloads_dir / Path(file_path).name
            shutil.copy2(file_path, destination)
            print(f"  Copied output file to {destination}")
        except Exception as e:
            print(f"  WARNING: Could not copy file to Downloads: {e}")
    
    def _print_summary(self, df: pd.DataFrame, output_file: str):
        """Print final summary statistics"""
        print("\n" + "="*70)
        print("FINAL CSV COMPILATION COMPLETE")
        print("="*70)
        print(f"Total validated contacts: {len(df)}")
        print(f"Unique schools: {df['School Name'].nunique()}")
        print(f"\nData completeness:")
        print(f"  First Name: {df['First Name'].ne('').sum()} ({df['First Name'].ne('').sum()/len(df)*100:.1f}%)")
        print(f"  Last Name: {df['Last Name'].ne('').sum()} ({df['Last Name'].ne('').sum()/len(df)*100:.1f}%)")
        print(f"  Phone: {df['Phone'].ne('').sum()} ({df['Phone'].ne('').sum()/len(df)*100:.1f}%)")
        print(f"\nConfidence scores:")
        print(f"  High (80-100): {len(df[df['Confidence Score'] >= 80])}")
        print(f"  Medium (60-79): {len(df[(df['Confidence Score'] >= 60) & (df['Confidence Score'] < 80)])}")
        print(f"  Low (0-59): {len(df[df['Confidence Score'] < 60])}")
        print(f"\nAverage confidence: {df['Confidence Score'].mean():.1f}")
        print(f"\nOutput file: {output_file}")
        print("="*70)
        
        # Show top schools
        print("\nTop 10 schools by contacts:")
        top_schools = df.groupby('School Name').size().sort_values(ascending=False).head(10)
        for school, count in top_schools.items():
            print(f"  {school[:40]:40} | {count} contacts")
        
        # Show title distribution
        print("\nTop 10 titles:")
        title_counts = df['Title'].value_counts().head(10)
        for title, count in title_counts.items():
            print(f"  {title[:40]:40} | {count}")
    
    def _create_quality_report(self, df: pd.DataFrame, report_file: str):
        """Create detailed quality report"""
        with open(report_file, 'w') as f:
            f.write("="*70 + "\n")
            f.write("CONTACT DATA QUALITY REPORT\n")
            f.write("="*70 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write(f"Total Contacts: {len(df)}\n")
            f.write(f"Unique Schools: {df['School Name'].nunique()}\n\n")
            
            f.write("DATA COMPLETENESS\n")
            f.write("-" * 70 + "\n")
            for col in ['First Name', 'Last Name', 'Phone']:
                complete = df[col].ne('').sum()
                pct = complete / len(df) * 100
                f.write(f"{col:15} {complete:5} / {len(df):5} ({pct:5.1f}%)\n")
            
            f.write("\n" + "CONFIDENCE DISTRIBUTION\n")
            f.write("-" * 70 + "\n")
            bins = [(90, 100), (80, 89), (70, 79), (60, 69), (0, 59)]
            for low, high in bins:
                count = len(df[(df['Confidence Score'] >= low) & (df['Confidence Score'] <= high)])
                pct = count / len(df) * 100
                f.write(f"{low:2}-{high:3}: {count:5} ({pct:5.1f}%)\n")
            
            f.write("\n" + "SCHOOLS BY CONTACT COUNT\n")
            f.write("-" * 70 + "\n")
            school_counts = df.groupby('School Name').size().sort_values(ascending=False)
            for school, count in school_counts.items():
                f.write(f"{school[:50]:50} {count:3}\n")
            
            f.write("\n" + "TITLE DISTRIBUTION\n")
            f.write("-" * 70 + "\n")
            title_counts = df['Title'].value_counts()
            for title, count in title_counts.items():
                f.write(f"{title[:50]:50} {count:3}\n")
        
        print(f"\nQuality report saved: {report_file}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Compile final validated CSV')
    parser.add_argument('--input', required=True, help='Input CSV from Step 4')
    parser.add_argument('--output', default='final_contacts.csv', help='Output CSV filename')
    
    args = parser.parse_args()
    
    compiler = FinalCompiler()
    compiler.compile_final_csv(args.input, args.output)
