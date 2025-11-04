# School Scraper Backend

FastAPI backend for scraping school contact information with Selenium.

## Features

- Upload CSV of schools to scrape
- Background job processing
- Real-time progress tracking
- Export results as CSV
- AI-powered contact parsing (coming soon)

## Setup Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
uvicorn app.main:app --reload
```

Server will run at `http://localhost:8000`

## API Endpoints

### Upload Schools
```
POST /api/upload-schools
Body: CSV file with columns: name, website
Returns: job_id
```

### Start Scraping
```
POST /api/start-scraping/{job_id}
Returns: Status message
```

### Check Progress
```
GET /api/job-status/{job_id}
Returns: { status, progress, completed, total }
```

### Get Results
```
GET /api/results/{job_id}
Returns: Array of contact objects
```

## Deployment

This backend is designed to run on Railway, Render, or similar platforms that support long-running processes.

### Railway Deploy

1. Connect this GitHub repo to Railway
2. Railway will auto-detect the `Procfile`
3. Deploy!

### Environment Variables

None required for basic operation. Add these for production:

- `DATABASE_URL` - PostgreSQL connection (optional)
- `REDIS_URL` - Redis for job queue (optional)
- `OPENAI_API_KEY` - For AI parsing (optional)

## Tech Stack

- **FastAPI** - Modern Python web framework
- **Selenium** - Web scraping
- **BeautifulSoup** - HTML parsing
- **Pandas** - Data processing
