# COLLABORATION PLAN: Giorgio Code Review & Changes

## OBJECTIVE
Give Giorgio access to review code, identify issues, and make recommended changes to optimize the pipeline.

---

## OPTION 1: GitHub Collaborator Access (RECOMMENDED)

### Steps:
1. **Invite Giorgio as Collaborator**
   - Go to: https://github.com/iitzayce/school-scraper-backend/settings/access
   - Click "Add people" → Search for Giorgio's GitHub username
   - Grant "Write" access (allows him to create branches and make changes)

2. **Create Feature Branch for Giorgio**
   ```bash
   git checkout -b giorgio-review-optimizations
   git push origin giorgio-review-optimizations
   ```

3. **Share Access**
   - Repository: https://github.com/iitzayce/school-scraper-backend
   - Branch: `giorgio-review-optimizations`
   - Issues Document: `ISSUES_SUMMARY.md` (in repo)

### Advantages:
- ✅ Full code access
- ✅ Can create branches and make changes
- ✅ Easy to review changes via Pull Requests
- ✅ Version control for all changes
- ✅ Can revert if needed

---

## OPTION 2: Code Review via Pull Request

### Steps:
1. **Giorgio Forks Repository**
   - Giorgio forks: https://github.com/iitzayce/school-scraper-backend
   - Makes changes in his fork
   - Creates Pull Request to your main branch

### Advantages:
- ✅ No need to grant direct access
- ✅ Full review process
- ✅ You approve all changes

---

## OPTION 3: Shared Branch with Protected Main

### Steps:
1. **Create Shared Branch**
   ```bash
   git checkout -b giorgio-changes
   git push origin giorgio-changes
   ```

2. **Giorgio Clones & Works on Branch**
   ```bash
   git clone https://github.com/iitzayce/school-scraper-backend.git
   cd school-scraper-backend
   git checkout giorgio-changes
   ```

3. **Giorgio Makes Changes & Pushes**
   ```bash
   # Make changes
   git add .
   git commit -m "Fix: [description]"
   git push origin giorgio-changes
   ```

4. **You Review & Merge**
   - Review changes on GitHub
   - Merge via Pull Request or directly

### Advantages:
- ✅ Main branch protected
- ✅ Easy collaboration
- ✅ Clear separation of changes

---

## RECOMMENDED WORKFLOW

### Phase 1: Review & Analysis (Giorgio)
1. Giorgio reviews `ISSUES_SUMMARY.md`
2. Giorgio examines codebase structure
3. Giorgio identifies root causes
4. Giorgio creates prioritized list of fixes

### Phase 2: Implementation (Giorgio)
1. Giorgio creates feature branch: `giorgio-fixes-[issue-name]`
2. Makes changes with clear commit messages
3. Tests changes locally (if possible)
4. Pushes branch and creates Pull Request

### Phase 3: Review & Testing (You)
1. Review Pull Request on GitHub
2. Test changes locally or in Cloud Run
3. Approve and merge if satisfied
4. Deploy to production

---

## FILES TO SHARE WITH GIORGIO

### Essential Files:
- ✅ `ISSUES_SUMMARY.md` - Complete issues list
- ✅ All Python scripts (`step1.py` through `step5.py`, `step1.5.py`)
- ✅ `api.py` - Cloud Run backend
- ✅ `requirements.txt` - Dependencies
- ✅ `README.md` - Project documentation

### Test Data (Optional):
- `step2_pages_batch1.csv` - Example Step 2 output
- `step2_pages_top500.csv` - Filtered high-value pages
- Sample Step 3/4/5 outputs for testing

### Configuration:
- `.gitignore` - What's excluded
- Environment variable requirements

---

## COMMUNICATION PLAN

### Initial Handoff:
1. Share GitHub repository link
2. Share `ISSUES_SUMMARY.md`
3. Grant access (Option 1) or share fork instructions (Option 2)
4. Schedule brief call to discuss priorities

### During Review:
- Giorgio creates GitHub Issues for each fix
- You can comment/prioritize on Issues
- Giorgio updates Issues with progress

### After Changes:
- Giorgio creates Pull Request with description
- You review and test
- Merge when satisfied
- Deploy to Cloud Run/Vercel

---

## PRIORITY FIXES (For Giorgio's Focus)

### HIGH PRIORITY:
1. **Step 2: Enforce 3-page limit** - Critical for cost control
2. **Step 2: Improve priority scoring** - Avoid sports/calendar pages
3. **Step 4: Capture contacts without emails** - Missing valuable leads
4. **Step 4: Fix missing contacts** - Known pages not being extracted

### MEDIUM PRIORITY:
5. **Step 3: Reduce retries to 1** - Performance optimization
6. **Performance bottlenecks** - Identify and fix slow areas

### LOW PRIORITY:
7. **Code cleanup** - Standardize retry logic, improve comments

---

## TESTING STRATEGY

### For Giorgio's Changes:
1. Test on small dataset first (`step2_pages_top500.csv`)
2. Verify page limits are enforced
3. Verify priority scoring improves page selection
4. Verify contacts without emails are captured
5. Run full pipeline on 10 counties to validate

### Before Merging:
- Run Step 1-5 locally on test data
- Verify no regressions
- Check output quality
- Review performance improvements

---

## NEXT STEPS

1. ✅ **DONE**: Created `ISSUES_SUMMARY.md`
2. **TODO**: Choose collaboration option (recommend Option 1)
3. **TODO**: Invite Giorgio to GitHub or share fork instructions
4. **TODO**: Create initial branch for Giorgio
5. **TODO**: Share repository and issues document
6. **TODO**: Schedule brief handoff call

---

## QUESTIONS FOR GIORGIO

1. Which collaboration method do you prefer?
2. Do you have a GitHub account? (username?)
3. What's your preferred workflow for code changes?
4. Do you need access to test data or can you work with sample data?
5. Timeline expectations for review and fixes?

