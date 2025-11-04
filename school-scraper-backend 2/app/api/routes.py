from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import uuid
import io
from app.scraper.scraper import run_scraper_job

router = APIRouter()

# In-memory job storage (use Redis/Database in production)
jobs = {}

@router.post("/upload-schools")
async def upload_schools(file: UploadFile = File(...)):
    """
    Upload a CSV file containing schools to scrape.
    Expected columns: name, website
    """
    try:
        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        
        # Validate columns
        if 'website' not in df.columns:
            raise HTTPException(status_code=400, detail="CSV must have 'website' column")
        
        if 'name' not in df.columns:
            # Use website as name if name column doesn't exist
            df['name'] = df['website']
        
        # Create job
        job_id = str(uuid.uuid4())
        
        jobs[job_id] = {
            "status": "pending",
            "total": len(df),
            "completed": 0,
            "schools": df.to_dict('records'),
            "results": []
        }
        
        return {
            "job_id": job_id,
            "schools_count": len(df),
            "message": "Schools uploaded successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")


@router.post("/start-scraping/{job_id}")
async def start_scraping(job_id: str, background_tasks: BackgroundTasks):
    """
    Start scraping job in the background.
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if jobs[job_id]["status"] == "running":
        raise HTTPException(status_code=400, detail="Job already running")
    
    # Start scraping in background
    background_tasks.add_task(run_scraper_job, job_id, jobs)
    jobs[job_id]["status"] = "running"
    
    return {
        "message": "Scraping started",
        "job_id": job_id
    }


@router.get("/job-status/{job_id}")
async def get_job_status(job_id: str):
    """
    Get the status and progress of a scraping job.
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    
    return {
        "job_id": job_id,
        "status": job["status"],
        "progress": (job["completed"] / job["total"] * 100) if job["total"] > 0 else 0,
        "completed": job["completed"],
        "total": job["total"],
        "results_count": len(job.get("results", []))
    }


@router.get("/results/{job_id}")
async def get_results(job_id: str):
    """
    Get the scraping results for a job.
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    
    return {
        "job_id": job_id,
        "status": job["status"],
        "total_results": len(job.get("results", [])),
        "results": job.get("results", [])
    }


@router.get("/download/{job_id}")
async def download_results(job_id: str):
    """
    Download results as CSV file.
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    results = job.get("results", [])
    
    if not results:
        raise HTTPException(status_code=404, detail="No results available")
    
    # Convert to DataFrame and CSV
    df = pd.DataFrame(results)
    
    # Create CSV in memory
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    
    # Return as downloadable file
    response = StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv"
    )
    response.headers["Content-Disposition"] = f"attachment; filename=school_contacts_{job_id}.csv"
    
    return response


@router.delete("/job/{job_id}")
async def delete_job(job_id: str):
    """
    Delete a job and its results.
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    del jobs[job_id]
    
    return {"message": "Job deleted successfully"}


@router.get("/jobs")
async def list_jobs():
    """
    List all jobs (for debugging).
    """
    return {
        "total_jobs": len(jobs),
        "jobs": [
            {
                "job_id": job_id,
                "status": job["status"],
                "completed": job["completed"],
                "total": job["total"]
            }
            for job_id, job in jobs.items()
        ]
    }
