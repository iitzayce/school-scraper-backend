# School Contact Scraper Pipeline

A 5-step Python pipeline for discovering Christian schools and extracting decision-maker contact information (Principals, IT Directors, Facilities Directors, Security Directors, etc.) for security grant applications.

## 🎯 Overview

This pipeline automates the discovery and extraction of school administrator contacts from school websites. It's designed for organizations like [Nonprofit Security Advisors](https://www.nonprofitsecurityadvisors.com/) that need to identify decision-makers for security grant applications.

### Pipeline Flow

```
Step 1: School Discovery (Google Places API)
    ↓
Step 2: Page Discovery (Web Crawling)
    ↓
Step 3: Content Collection (Beautiful Soup + Selenium)
    ↓
Step 4: LLM Parsing (OpenAI GPT-3.5-turbo)
    ↓
Step 5: Final Validation & Compilation
```

## 📋 Requirements

### Python Dependencies

```bash
pip install pandas requests beautifulsoup4 selenium openai
```

Or create a `requirements.txt`:

```
pandas>=2.0.0
requests>=2.31.0
beautifulsoup4>=4.12.0
selenium>=4.15.0
openai>=1.0.0
urllib3>=2.0.0
```

### API Keys Required

1. **Google Places API Key** - For Step 1 (school discovery)
   - Get from: [Google Cloud Console](https://console.cloud.google.com/)
   - Enable: Places API (New)

2. **OpenAI API Key** - For Step 4 (LLM parsing)
   - Get from: [OpenAI Platform](https://platform.openai.com/api-keys)

### System Requirements

- Python 3.9+
- Chrome/Chromium browser (for Selenium)
- ChromeDriver (automatically managed by Selenium 4+)

## 🚀 Quick Start

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd school-contact-scraper
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run Pipeline

#### Step 1: Discover Schools

```bash
python3 step1.py \
  --api-key YOUR_GOOGLE_PLACES_API_KEY \
  --state Texas \
  --max-counties 1 \
  --max-search-terms 2 \
  --max-api-calls 10 \
  --output step1_schools.csv
```

**Options:**
- `--state`: State to search (Texas, Iowa, etc.)
- `--max-counties`: Limit number of counties (for cost control)
- `--max-search-terms`: Limit search queries per county
- `--max-api-calls`: Total API call limit (cost control)

#### Step 2: Discover Pages

```bash
python3 step2.py \
  --input step1_schools.csv \
  --output step2_pages.csv \
  --max-pages-per-school 30 \
  --top-pages-limit 5
```

**Options:**
- `--max-pages-per-school`: Max pages to discover per school (default: 30)
- `--top-pages-limit`: Final filter - keep only top N pages by priority (default: 5)
- `--max-depth`: Crawl depth (default: 2)

#### Step 3: Collect Content

```bash
python3 step3.py \
  --input step2_pages.csv \
  --output step3_content.csv
```

**Features:**
- Fallback approach: Beautiful Soup first, then Selenium for dynamic content
- Handles click/hover reveals for hidden emails
- Collects HTML content for LLM parsing

#### Step 4: Parse with LLM

```bash
python3 step4.py \
  --input step3_content.csv \
  --output step4_parsed_contacts.csv \
  --api-key YOUR_OPENAI_API_KEY \
  --model gpt-3.5-turbo
```

**Features:**
- Extracts ALL contacts from HTML
- Filters by target titles (Principal, IT Director, Facilities, Security, etc.)
- Uses fuzzy matching for title variations
- Excludes non-relevant roles (Athletic Director, etc.)

#### Step 5: Final Validation

```bash
python3 step5.py \
  --input step4_parsed_contacts.csv \
  --output step5_final_contacts.csv
```

**Features:**
- Email validation
- Name cleaning (removes generic text)
- Phone formatting
- Deduplication
- Confidence scoring

## Output Files

- `step1_schools.csv` - Discovered schools with websites
- `step2_pages.csv` - Discovered pages per school (prioritized)
- `step3_content.csv` - Collected HTML content from pages
- `step4_parsed_contacts.csv` - LLM-extracted contacts (filtered by title)
- `step5_final_contacts.csv` - Final validated contacts
- `step5_final_contacts_quality_report.txt` - Quality metrics report

## 🎯 Target Titles (Step 4 Filtering)

The pipeline filters for these decision-maker roles:

- **Administration**: Principal, Superintendent, Assistant Principal, Head of School, Acting Principal, Vice Principal, School Administrator, etc.
- **Finance/Operations**: Director of Finance, Business Manager, CFO, Director of Operations, COO
- **IT**: IT Director, Technology Director, CTO, Chief Technology Officer
- **Facilities**: Facilities Director, Facilities Manager, Director of Facilities
- **Security**: Security Director, Security Manager, Director of Security, Chief Security Officer

**Excluded roles**: Athletic Director, Coach, Academic Dean, Teacher, Librarian, etc.

## 💰 Cost Optimization

### Google Places API
- **Cost**: ~$0.017 per request
- **Controls**: Use `--max-counties`, `--max-search-terms`, `--max-api-calls`
- **Example**: 10 API calls = ~$0.17

### OpenAI API (GPT-3.5-turbo)
- **Cost**: ~$0.001 per page (varies by content size)
- **Controls**: Limit pages per school (`--top-pages-limit`)
- **Example**: 100 pages = ~$0.10

### Projected Costs
- **5 schools**: ~$0.02 (LLM) + ~$0.17 (Places API) = **~$0.19**
- **100 schools**: ~$0.46 (LLM) + ~$1.70 (Places API) = **~$2.16**
- **1000 schools**: ~$4.60 (LLM) + ~$17.00 (Places API) = **~$21.60**

## Deployment

### Google Cloud Setup

1. **Create a Cloud Function or Cloud Run service**

```bash
# Install gcloud CLI
gcloud init
gcloud auth login

# Create a new project
gcloud projects create school-scraper --name="School Scraper"

# Set the project
gcloud config set project school-scraper

# Enable required APIs
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable run.googleapis.com
```

2. **Deploy as Cloud Function** (recommended for scheduled runs)

```bash
# Create function
gcloud functions deploy school-scraper \
  --runtime python39 \
  --trigger-http \
  --entry-point main \
  --memory 2GB \
  --timeout 540s \
  --set-env-vars GOOGLE_PLACES_API_KEY=YOUR_KEY,OPENAI_API_KEY=YOUR_KEY
```

3. **Or deploy as Cloud Run** (for web interface)

```bash
# Build and deploy
gcloud run deploy school-scraper \
  --source . \
  --platform managed \
  --region us-central1 \
  --set-env-vars GOOGLE_PLACES_API_KEY=YOUR_KEY,OPENAI_API_KEY=YOUR_KEY
```

### Vercel Setup (Frontend Dashboard)

1. **Install Vercel CLI**

```bash
npm i -g vercel
```

2. **Deploy**

```bash
cd frontend  # If you have a frontend folder
vercel
```

3. **Environment Variables** (set in Vercel dashboard)
   - `NEXT_PUBLIC_API_URL` - Your Google Cloud function/run URL

## 🔧 Configuration

### Adjusting Scope

**For more contacts:**
```bash
# Step 2: Increase pages per school
--top-pages-limit 10  # Instead of 5

# Step 1: Process more schools
--max-counties 5  # Instead of 1
--max-api-calls 50  # Instead of 10
```

**For cost control:**
```bash
# Step 2: Reduce pages
--top-pages-limit 3  # Instead of 5

# Step 1: Limit API calls
--max-api-calls 5  # Very restrictive
```

### Customizing Target Titles

Edit `step4.py` - modify the `target_titles` list in the `LLMParser.__init__` method:

```python
self.target_titles = [
    'principal', 'superintendent', 'it director', ...
]
```

## 🐛 Troubleshooting

### Selenium Issues
- Ensure Chrome/Chromium is installed
- ChromeDriver is auto-managed by Selenium 4+
- For headless mode, add to `step3.py`: `options.add_argument('--headless')`

### JSON Parse Errors
- Step 4 now includes automatic recovery for truncated responses
- Large pages (>20 emails) automatically get higher token limits

### API Rate Limits
- Google Places API: 100 requests/second (usually not an issue)
- OpenAI API: Rate limits depend on your tier
- Both steps include retry logic with exponential backoff

## 📈 Performance Metrics

Based on 5-school test:
- **Success Rate**: 60% of schools yield contacts
- **Pages per School**: 4.6 pages (with top-5 filter)
- **Email Collection**: 56.5% of pages have emails
- **Contact Extraction**: 84.6% of pages with emails yield contacts
- **Average Contacts per School**: 2.0 (3.3 for schools with contacts)

## 📝 License

[Add your license here]

## 🤝 Contributing

[Add contribution guidelines if needed]

## 📧 Support

For issues or questions, please open a GitHub issue.

