from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime

class PipelineSummary(BaseModel):
    name: str
    source_type: str
    target_type: str

class PipelineRunRequest(BaseModel):
    pipeline_name: str
    overrides: Optional[Dict[str, Any]] = None

class PipelineRunResponse(BaseModel):
    job_id: str
    status: str
    message: str

class ExecutionMetric(BaseModel):
    rows: int
    total_s: float
    write_s: float
    count_s: float
    summary_s: float

class ExecutionResult(BaseModel):
    pipeline: str
    status: str
    rows: int
    duration_s: float
    timestamp: datetime
    metrics: Optional[ExecutionMetric] = None
    sample: Optional[List[Dict[str, Any]]] = None
    summary: Optional[List[Dict[str, Any]]] = None
    write_sql: Optional[str] = None

class ConnectionTestRequest(BaseModel):
    type: str
    conn: str
    name: Optional[str] = None

class ConnectionTestResponse(BaseModel):
    success: bool
    message: str
