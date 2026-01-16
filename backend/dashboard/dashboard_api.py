"""
STEP 6.3 — Dashboard API Router

Exposes dashboard queries as HTTP endpoints for UI consumption.
"""

from fastapi import APIRouter, HTTPException
from typing import Optional
from datetime import date

from dashboard.dashboard_query_service import (
    get_farm_overview,
    list_today_activities,
    list_in_progress_activities,
    list_missed_activities,
    list_recent_alerts,
)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/farms/{farm_id}/overview")
def get_farm_overview_endpoint(farm_id: str):
    """
    Get farm overview with activity and alert counts.
    """
    try:
        result = get_farm_overview(farm_id)
        if not result:
            raise HTTPException(status_code=404, detail="Farm not found")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/farms/{farm_id}/activities/today")
def list_today_activities_endpoint(farm_id: str, activity_date: Optional[date] = None):
    """
    List activities for a specific date (defaults to today).
    """
    if not activity_date:
        from datetime import date
        activity_date = date.today()
    
    try:
        return list_today_activities(farm_id, activity_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/farms/{farm_id}/activities/in-progress")
def list_in_progress_activities_endpoint(farm_id: str):
    """
    List currently in-progress activities.
    """
    try:
        return list_in_progress_activities(farm_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/farms/{farm_id}/activities/missed")
def list_missed_activities_endpoint(farm_id: str, limit: int = 20):
    """
    List missed activities.
    """
    try:
        return list_missed_activities(farm_id, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/farms/{farm_id}/alerts/recent")
def list_recent_alerts_endpoint(farm_id: str, limit: int = 20):
    """
    List recent alerts with context.
    """
    try:
        return list_recent_alerts(farm_id, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

