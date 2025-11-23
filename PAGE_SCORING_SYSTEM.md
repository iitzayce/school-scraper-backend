# PAGE SCORING SYSTEM (Step 3)

## Overview
The scoring system determines which pages are most likely to contain administrative contact information. Each page gets a priority score based on URL patterns and content analysis.

## Scoring Components

### 1. URL-Based Scoring (`score_page_priority`)

**Base Score:** Starts at 0

**High-Value Keywords (+25 each):**
- `staff`
- `faculty`
- `directory`
- `administration`
- `admin`
- `team`
- `leadership`
- `our-team`
- `who-we-are`
- `meet-our`
- `personnel`
- `board`
- `principal`
- `superintendent`

**Support Keywords (+10 each):**
- `about`
- `mission`
- `vision`
- `history`

**Low-Value Keywords (+5 each):**
- `contact`
- `info`
- `location`

**Penalties (-25 each):**
- `calendar`
- `athletic`
- `sports`
- `admission`
- `apply`
- `enroll`
- `event`
- `news`
- `blog`
- `lunch`
- `menu`
- `forms`
- `download`
- `linktr.ee`
- `facebook.com`
- `instagram.com`
- `twitter.com`
- `youtube.com`
- `vimeo.com`
- `docs.google.com`
- `drive.google.com`

**Bad Domain Penalty (-40):**
- If domain contains any of: `linktr.ee`, `facebook.com`, `instagram.com`, `twitter.com`, `youtube.com`, `vimeo.com`, `docs.google.com`, `drive.google.com`

**Hash Fragment Boost (+20):**
- If URL contains `#` with any of: `team`, `staff`, `faculty`, `leadership`, `directory`, `admin`

**ZERO PRIORITY (Immediate Return 0 - checked first):**
- `contact`, `contact-us`, `contactus`, `contact_us`
- `admission`, `admissions`, `apply`, `enrollment`, `enroll`
- `home`, `index`
- `mission`, `vision`, `history`
- `calendar`, `event`, `events`
- `athletic`, `athletics`, `sports`
- `news`, `blog`
- `lunch`, `menu`, `cafeteria`, `dining`
- `forms`, `download`, `downloads`
- `employment`, `jobs`, `careers`, `hiring`
- Social media domains: `linktr.ee`, `facebook.com`, `instagram.com`, `twitter.com`, `youtube.com`, `vimeo.com`, `docs.google.com`, `drive.google.com`

**MINIMUM THRESHOLD:**
- Pages must score >= 20 to be included in results
- Pages below 20 are automatically excluded

### 2. Content-Based Scoring (`score_page_content`)

**Email Count:**
- 5+ emails: +40
- 2-4 emails: +25
- 1 email: +10

**Name Pattern Detection:**
- 10+ name matches (pattern: `[A-Z][a-z]+ [A-Z][a-z]+`): +30
- 5-9 name matches: +15

**Heading Keywords:**
- If headings (h1, h2, h3) contain high-value keywords: +10

## Final Priority Score

**Total Score = URL Score + Content Score**

## Filtering

- Pages with score = 0 are filtered out (checked first, immediate return 0)
- Pages with score < 20 are filtered out (minimum threshold)
- Pages are sorted by score (highest first)
- Top 3 pages per school are kept (highest scores that meet threshold)

## Current Issues

1. **Login pages getting through** (should be 0 score)
2. **Employment pages getting through** (should be lower score)
3. **Board pages getting high scores** (95) when they should be lower or filtered

