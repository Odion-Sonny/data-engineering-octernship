from fastapi import APIRouter, HTTPException
from ..models import SegmentationRequest, SegmentationResponse
from ..database import get_db_connection
from ..query_builder import build_segmentation_query

router = APIRouter()

@router.post("/segment", response_model=SegmentationResponse)
def segment_users(request: SegmentationRequest):
    """
    Segment users based on attribute and event filters
    """
    try:
        # Generate SQL query
        query = build_segmentation_query(request)
        
        # Execute query
        conn = get_db_connection()
        result = conn.execute(query).fetchall()
        conn.close()
        
        # Extract user IDs
        user_ids = [row[0] for row in result]
        
        return SegmentationResponse(
            user_ids=user_ids,
            total_count=len(user_ids),
            filters_applied={
                "user_filters": [filter.model_dump() for filter in request.user_filters],
                "event_filters": [filter.model_dump() for filter in request.event_filters],
                "logic_operator": request.logic_operator
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Segmentation failed: {str(e)}")

@router.get("/examples")
def get_examples():
    """Get example JSON payloads for different segmentation scenarios"""
    
    examples = {
        "age_segment_25_34": {
            "description": "Users aged 25-34",
            "payload": {
                "user_filters": [
                    {"field": "age", "operator": "gte", "value": 25},
                    {"field": "age", "operator": "lte", "value": 34}
                ],
                "logic_operator": "AND"
            }
        },
        "california_login_users": {
            "description": "California users who have logged in at least once",
            "payload": {
                "user_filters": [
                    {"field": "location", "operator": "eq", "value": "California"}
                ],
                "event_filters": [
                    {"event_name": "LOGIN", "operator": "gte", "count": 1}
                ],
                "logic_operator": "AND"
            }
        },
        "premium_active_users": {
            "description": "Premium users who made a purchase in the last 30 days",
            "payload": {
                "user_filters": [
                    {"field": "subscription_plan", "operator": "eq", "value": "Premium"}
                ],
                "event_filters": [
                    {"event_name": "PURCHASE_MADE", "operator": "gte", "count": 1, "time_range_days": 30}
                ],
                "logic_operator": "AND"
            }
        },
        "mobile_cart_abandoners": {
            "description": "Mobile users who added to cart but never purchased",
            "payload": {
                "user_filters": [
                    {"field": "device_type", "operator": "eq", "value": "Mobile"}
                ],
                "event_filters": [
                    {"event_name": "ADDED_TO_CART", "operator": "gte", "count": 1},
                    {"event_name": "PURCHASE_MADE", "operator": "eq", "count": 0}
                ],
                "logic_operator": "AND"
            }
        }
    }
    
    return examples
