"""
Export API Routes
Endpoints for exporting data to CSV
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
import csv
import io
from models.requisition import ExportRequest, RequisitionFilters
from services.requisition_service import RequisitionService

router = APIRouter()


@router.post("/csv")
async def export_to_csv(request: Optional[ExportRequest] = None):
    """
    Export requisition data to CSV

    Request Body (optional):
        - filters: Optional filters to apply before export
          - status: Filter by status
          - department: Filter by department
          - location: Filter by location
          - min_days_open: Minimum days open
          - max_days_open: Maximum days open

    Returns:
        - CSV file with requisition data

    Example:
        POST /api/exports/csv
        {
            "filters": {
                "status": "Open",
                "min_days_open": 30
            }
        }
    """
    try:
        # Get filters from request or use defaults
        filters = request.filters if request and request.filters else None

        # Get all requisitions matching filters (no pagination)
        requisitions = RequisitionService.get_all_requisitions_for_export(filters)

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow([
            'Requisition ID',
            'Job Title',
            'Department',
            'Location',
            'Status',
            'Days Open',
            'Start Date',
            'Target Hire Date',
            'Candidate Stage'
        ])

        # Write data
        for req in requisitions:
            writer.writerow([
                req.requisition,
                req.job_title,
                req.department,
                req.location,
                req.status,
                req.days_open,
                req.start_date or 'N/A',
                req.target_hire_date or 'N/A',
                req.candidate_stage or 'N/A'
            ])

        # Prepare response
        output.seek(0)

        # Generate filename with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"requisitions_export_{timestamp}.csv"

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error exporting to CSV: {str(e)}"
        )
