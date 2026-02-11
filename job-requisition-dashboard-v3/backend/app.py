"""
Job Requisition Dashboard API - FastAPI Application
Main application entry point
Updated: Added Filled Analytics routes
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Import routes
from routes import analytics, requisitions, exports, filled_analytics, candidate_stage

app = FastAPI(
    title="Job Requisition Dashboard API",
    description="API for Taylor Morrison Job Requisition Analytics Dashboard",
    version="3.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class HealthCheckResponse(BaseModel):
    """Health check response model"""
    status: str
    version: str
    service: str


@app.get("/api/health", response_model=HealthCheckResponse, tags=["System"])
async def health_check():
    """
    Health check endpoint
    Returns the API status and version
    """
    return HealthCheckResponse(
        status="healthy",
        version="3.0.0",
        service="job-requisition-dashboard-api"
    )


# Include routers
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])
app.include_router(filled_analytics.router, prefix="/api/filled-analytics", tags=["Filled Analytics"])
app.include_router(candidate_stage.router, prefix="/api/candidate-stage", tags=["Candidate Stage"])
app.include_router(requisitions.router, prefix="/api/requisitions", tags=["Requisitions"])
app.include_router(exports.router, prefix="/api/exports", tags=["Exports"])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
