from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union

class FilterCondition(BaseModel):
    """Individual filter condition"""
    field: str = Field(..., description="Field name (e.g., 'age', 'location', 'subscription_plan')")
    operator: str = Field(..., description="Comparison operator: 'eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in', 'like'")
    value: Union[str, int, float, List[Union[str, int, float]]] = Field(..., description="Value to compare against")

class EventCondition(BaseModel):
    """Event-based filter condition"""
    event_name: str = Field(..., description="Name of the event (e.g., 'LOGIN', 'PURCHASE_MADE')")
    operator: str = Field(default="gte", description="Comparison operator for event count: 'eq', 'ne', 'gt', 'gte', 'lt', 'lte'")
    count: int = Field(default=1, description="Minimum/exact count of events")
    time_range_days: Optional[int] = Field(default=None, description="Filter events within last N days")

class SegmentationRequest(BaseModel):
    """Request model for user segmentation"""
    user_filters: Optional[List[FilterCondition]] = Field(default=[], description="Filters on user attributes")
    event_filters: Optional[List[EventCondition]] = Field(default=[], description="Filters on user events")
    logic_operator: str = Field(default="AND", description="Logic operator between filters: 'AND' or 'OR'")
    limit: Optional[int] = Field(default=1000, description="Maximum number of results to return")

class SegmentationResponse(BaseModel):
    """Response model for segmentation results"""
    user_ids: List[int]
    total_count: int
    filters_applied: Dict[str, Any]
