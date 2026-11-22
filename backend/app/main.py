
from fastapi import FastAPI
from backend.app.api.jobs import router as jobs_router

app = FastAPI(
    title="TaskFlow API",
    description="A distributed job scheduling platform",
    version="0.1.0",
)
app.include_router(jobs_router)

@app.get("/")
def root():
    return {
        "message": "Welcome to TaskFlow",
        "project": "Distributed Job Scheduling Platform",
        "version": "0.1.0",
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "taskflow-api",
    }
