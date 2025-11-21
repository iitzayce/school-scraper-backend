# CURRENT PIPELINE ISSUES & TECHNICAL DEBT
## Summary for Giorgio - Technical Review

### Date: November 20, 2024
### Pipeline Status: Functional but needs optimization

---

## 1. STEP 2: PAGE DISCOVERY ISSUES

### Problem: Some schools exceed 3-page limit
- **Current Behavior**: Despite `max_pages_per_school=3` and `top_pages_limit=3`, some schools have 4-11 pages
- **Evidence**: From Step 2 analysis, 5.3% of schools exceed 3 pages (4-11 pages found)
- **Impact**: Unnecessary crawling, increased costs, slower processing
- **Location**: `step2.py` - `discover_pages()` method
- **Priority**: HIGH

### Problem: Low-priority pages being crawled
- **Current Behavior**: 26.6% of discovered pages have priority score < 20
- **Evidence**: 557 low-priority pages out of 2,094 total
- **Impact**: Wasted resources on contact pages, calendar pages, etc.
- **Location**: `step2.py` - priority scoring and filtering logic
- **Priority**: MEDIUM

---

## 2. STEP 3: CONTENT COLLECTION ISSUES

### Problem: Excessive retry logic
- **Current Behavior**: 4 retries for failed page loads
- **User Requirement**: Only 1 retry (not 4)
- **Impact**: Slower processing, unnecessary retries
- **Location**: `step3.py` - `safe_get()` and retry mechanisms
- **Priority**: MEDIUM
- **Status**: NOTED - not yet implemented

### Problem: Wrong pages being collected
- **Current Behavior**: Sometimes collects sports pages instead of faculty/admin pages
- **Evidence**: User feedback on specific schools (Texas Christian School, Texas Christian High School)
- **Impact**: Missing valuable contacts, extracting wrong data
- **Location**: `step2.py` (page selection) and `step3.py` (content collection)
- **Priority**: HIGH

---

## 3. STEP 4: LLM PARSING ISSUES

### Problem: Missing contacts without emails
- **Current Behavior**: Very few contacts without emails being captured
- **Expected**: Similar number of contacts with and without emails
- **Evidence**: Recent run had only 1 contact without email vs many with emails
- **Impact**: Missing valuable leads for enrichment
- **Location**: `step4.py` - LLM prompt and validation logic
- **Priority**: HIGH

### Problem: Some valid contacts being missed
- **Current Behavior**: Known contacts from specific pages not being extracted
- **Evidence**: User provided examples (ccagranbury.com/administration-staff, joshuachristianacademy.org/contact)
- **Impact**: Incomplete data collection
- **Location**: `step4.py` - HTML reduction, chunking, or LLM extraction
- **Priority**: HIGH

---

## 4. COST & PERFORMANCE OPTIMIZATION

### Problem: Places API costs
- **Current Cost**: ~$0.017 per call, $12.51 for 736 calls (50 counties)
- **User Concern**: Cost optimization needed for scaling to all 254 Texas counties
- **Location**: `step1.py` - API usage patterns
- **Priority**: MEDIUM

### Problem: Pipeline bottlenecks
- **Current Behavior**: Step 2 and Step 3 can be slow for large batches
- **Evidence**: User reported 6 hours for 158 of 1807 pages in Step 2
- **Impact**: Long runtimes, potential Cloud Run timeouts
- **Location**: `step2.py`, `step3.py` - crawling and content collection
- **Priority**: MEDIUM

---

## 5. CODE QUALITY & ARCHITECTURE

### Problem: Retry logic inconsistency
- **Current Behavior**: Different retry counts across steps (some 3, some 5)
- **User Requirement**: Standardize to 1 retry for page loads
- **Location**: Multiple files (`step2.py`, `step3.py`)
- **Priority**: LOW

### Problem: Page limit enforcement
- **Current Behavior**: `max_pages_per_school` not strictly enforced
- **Evidence**: Schools with 4-11 pages when limit is 3
- **Location**: `step2.py` - `discover_pages()` method
- **Priority**: HIGH

---

## 6. DATA QUALITY ISSUES

### Problem: Page priority scoring needs refinement
- **Current Behavior**: Some high-value pages get low scores, some low-value pages get high scores
- **Evidence**: Sports pages sometimes prioritized over admin pages
- **Impact**: Wrong pages selected for content collection
- **Location**: `step2.py` - `score_page_priority()` and `score_page_content()`
- **Priority**: HIGH

---

## KNOWN WORKING FEATURES

✅ Step 1: School discovery with Google Places API
✅ Step 1.5: School filtering (churches, camps, etc.)
✅ Step 2: Page discovery with priority scoring (mostly working)
✅ Step 3: HTML content collection with Beautiful Soup + Selenium fallback
✅ Step 4: LLM parsing with HTML reduction and chunking
✅ Step 5: Final compilation and deduplication
✅ Cloud Run deployment pipeline
✅ Vercel frontend integration

---

## RECOMMENDED FOCUS AREAS FOR GIORGIO

1. **Step 2 page limit enforcement** - Ensure strict 3-page limit per school
2. **Step 2 priority scoring** - Improve to avoid sports/calendar pages
3. **Step 3 retry logic** - Reduce to 1 retry as requested
4. **Step 4 contact extraction** - Ensure contacts without emails are captured
5. **Performance optimization** - Identify and fix bottlenecks

---

## CODEBASE LOCATION

- **GitHub**: https://github.com/iitzayce/school-scraper-backend
- **Local Path**: /Users/koenullrich/Documents/Altira/Cursor
- **Main Files**:
  - `step1.py` - School discovery
  - `step1.5.py` - School filtering
  - `step2.py` - Page discovery
  - `step3.py` - Content collection
  - `step4.py` - LLM parsing
  - `step5.py` - Final compilation
  - `api.py` - Cloud Run backend
  - `frontend/app/page.tsx` - Vercel frontend

---

## TEST DATA AVAILABLE

- `step1_schools_batch1.csv` - 903 schools from 50 counties
- `step1.5_schools_filtered_batch1.csv` - Filtered schools
- `step2_pages_batch1.csv` - 2,094 pages discovered
- `step2_pages_top500.csv` - Top 500 highest-value pages (for testing)
- Various Step 3, 4, 5 outputs from test runs

---

## ENVIRONMENT VARIABLES NEEDED

- `GOOGLE_PLACES_API_KEY` - For Step 1
- `OPENAI_API_KEY` - For Step 4
- `NEXT_PUBLIC_API_URL` - For frontend (Vercel)

---

## DEPLOYMENT STATUS

- **Cloud Run**: Deployed and functional
- **Vercel**: Deployed and functional
- **Current Test**: 10 counties (reduced from 50 for testing)
