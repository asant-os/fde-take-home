from fastapi import FastAPI, Depends, HTTPException

from datetime import date
from app.models import HealthResponse, RunRequest, RunResponse, RunStatusResponse, PreviewResponse, AlertRecord
from app.dependencies import get_config, get_repo, get_slack, get_email
from app.service import AlertService

app = FastAPI(title="Risk Alert Service")

@app.get("/health", response_model=HealthResponse)
def health(repo=Depends(get_repo)):
    return HealthResponse(ok=True, db=repo.health_check())

@app.post("/runs", response_model=RunResponse)
def create_run(
    req: RunRequest,
    config=Depends(get_config),
    repo=Depends(get_repo),
    notifier=Depends(get_slack),
    email_notifier=Depends(get_email),
):
    service = AlertService(repo=repo, config=config, notifier=notifier, email_notifier=email_notifier)
    try:
        run_id = service.run(
            source_uri=req.source_uri,
            target_month=date.fromisoformat(req.month),
            dry_run=req.dry_run,
        )
        return RunResponse(run_id=run_id)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/runs/{run_id}", response_model=RunStatusResponse)
def get_run(run_id: str, repo=Depends(get_repo), config=Depends(get_config)):
    try:
        result = repo.get_run_status(run_id, sample_limit=config.sample_limit)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/preview", response_model=PreviewResponse)
def preview(
    req: RunRequest,
    config=Depends(get_config),
    repo=Depends(get_repo),
):
    service = AlertService(repo=repo, config=config, notifier=None, email_notifier=None)
    try:
        alerts = service.preview(
            source_uri=req.source_uri,
            target_month=date.fromisoformat(req.month),
        )
        return PreviewResponse(
            month=req.month,
            alert_count=len(alerts),
            alerts=alerts[:config.sample_limit],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))