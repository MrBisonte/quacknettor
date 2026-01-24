from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
import uuid
import time
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from duckel.config import load_config
from duckel.runner import PipelineRunner
from api.schemas import (
    PipelineSummary, 
    PipelineRunRequest, 
    PipelineRunResponse, 
    ExecutionResult,
    ConnectionTestRequest,
    ConnectionTestResponse
)

app = FastAPI(title="DuckEL API", version="2.0.0")

# Enable CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory store for background jobs (simulating a database)
jobs = {}

@app.get("/api/pipelines", response_model=List[PipelineSummary])
async def list_pipelines():
    """List all available pipeline configurations."""
    try:
        pipelines_path = os.path.join(project_root, "configs", "pipelines.yml")
        configs = load_config(pipelines_path)
        return [
            PipelineSummary(
                name=name, 
                source_type=cfg.source.type, 
                target_type=cfg.target.type
            ) for name, cfg in configs.items()
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/pipelines/run", response_model=PipelineRunResponse)
async def run_pipeline_task(request: PipelineRunRequest, background_tasks: BackgroundTasks):
    """Trigger a pipeline execution in the background."""
    job_id = str(uuid.uuid4())
    
    try:
        pipelines_path = os.path.join(project_root, "configs", "pipelines.yml")
        configs = load_config(pipelines_path)
        
        if request.pipeline_name not in configs:
            raise HTTPException(status_code=404, detail="Pipeline not found")
            
        pipeline_cfg = configs[request.pipeline_name]
        
        # Initialize job state
        jobs[job_id] = {
            "status": "running",
            "pipeline": request.pipeline_name,
            "start_time": time.time(),
            "result": None
        }
        
        # Define background task
        def execute():
            try:
                runner = PipelineRunner(pipeline_cfg, request.overrides or {}, pipeline_name=request.pipeline_name)
                result = runner.run()
                jobs[job_id]["status"] = "success"
                jobs[job_id]["result"] = result
            except Exception as e:
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = str(e)
        
        background_tasks.add_task(execute)
        
        return PipelineRunResponse(
            job_id=job_id,
            status="started",
            message=f"Pipeline {request.pipeline_name} started in background"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jobs/{job_id}", response_model=Dict[str, Any])
async def get_job_status(job_id: str):
    """Retrieve the status and results of a background job."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]

@app.post("/api/test-connection", response_model=ConnectionTestResponse)
async def test_connection(request: ConnectionTestRequest):
    """Verify connectivity to a data source or target."""
    from duckel.adapters import create_source_adapter
    import duckdb
    
    try:
        config = request.model_dump()
        adapter = create_source_adapter(config)
        con = duckdb.connect()
        adapter.attach(con)
        return ConnectionTestResponse(success=True, message="Connection successful")
    except Exception as e:
        return ConnectionTestResponse(success=False, message=str(e))

@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
