from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union
import duckdb
import uvicorn

app = FastAPI(title="DuckMart User Segmentation API", version="1.0.0")

# Pydantic models for request/response

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

# Database connection
def get_db_connection():
    return duckdb.connect('duckmart.db')

# SQL generation functions
def generate_user_filter_sql(filter_condition: FilterCondition) -> str:
    """Generate SQL for user attribute filters"""
    field = filter_condition.field
    operator = filter_condition.operator
    value = filter_condition.value
    
    # Validate field names to prevent SQL injection
    valid_user_fields = ['user_id', 'name', 'age', 'gender', 'location', 'signup_date', 'subscription_plan', 'device_type']
    if field not in valid_user_fields:
        raise ValueError(f"Invalid field: {field}")
    
    if operator == "eq":
        return f"{field} = '{value}'" if isinstance(value, str) else f"{field} = {value}"
    elif operator == "ne":
        return f"{field} != '{value}'" if isinstance(value, str) else f"{field} != {value}"
    elif operator == "gt":
        return f"{field} > {value}"
    elif operator == "gte":
        return f"{field} >= {value}"
    elif operator == "lt":
        return f"{field} < {value}"
    elif operator == "lte":
        return f"{field} <= {value}"
    elif operator == "in":
        if isinstance(value, list):
            value_list = "', '".join(map(str, value)) if all(isinstance(v, str) for v in value) else ", ".join(map(str, value))
            return f"{field} IN ('{value_list}')" if all(isinstance(v, str) for v in value) else f"{field} IN ({value_list})"
        else:
            raise ValueError("'in' operator requires a list of values")
    elif operator == "not_in":
        if isinstance(value, list):
            value_list = "', '".join(map(str, value)) if all(isinstance(v, str) for v in value) else ", ".join(map(str, value))
            return f"{field} NOT IN ('{value_list}')" if all(isinstance(v, str) for v in value) else f"{field} NOT IN ({value_list})"
        else:
            raise ValueError("'not_in' operator requires a list of values")
    elif operator == "like":
        return f"{field} LIKE '%{value}%'"
    else:
        raise ValueError(f"Unsupported operator: {operator}")

def generate_event_filter_sql(event_condition: EventCondition) -> str:
    """Generate SQL for event-based filters"""
    event_name = event_condition.event_name
    operator = event_condition.operator
    count = event_condition.count
    time_range_days = event_condition.time_range_days
    
    # Base event count subquery
    time_filter = ""
    if time_range_days:
        time_filter = f"AND timestamp >= CURRENT_DATE - INTERVAL '{time_range_days}' DAY"
    
    count_subquery = f"""
        (SELECT COUNT(*) FROM user_events ue2 
         WHERE ue2.user_id = ua.user_id 
         AND ue2.event_name = '{event_name}' 
         {time_filter})
    """
    
    if operator == "eq":
        return f"{count_subquery} = {count}"
    elif operator == "ne":
        return f"{count_subquery} != {count}"
    elif operator == "gt":
        return f"{count_subquery} > {count}"
    elif operator == "gte":
        return f"{count_subquery} >= {count}"
    elif operator == "lt":
        return f"{count_subquery} < {count}"
    elif operator == "lte":
        return f"{count_subquery} <= {count}"
    else:
        raise ValueError(f"Unsupported event operator: {operator}")

def build_segmentation_query(request: SegmentationRequest) -> str:
    """Build complete SQL query from segmentation request"""
    
    where_conditions = []
    
    # Add user attribute filters
    for user_filter in request.user_filters:
        condition_sql = generate_user_filter_sql(user_filter)
        where_conditions.append(f"({condition_sql})")
    
    # Add event filters
    for event_filter in request.event_filters:
        condition_sql = generate_event_filter_sql(event_filter)
        where_conditions.append(f"({condition_sql})")
    
    # Combine conditions
    if where_conditions:
        logic_op = f" {request.logic_operator} "
        where_clause = f"WHERE {logic_op.join(where_conditions)}"
    else:
        where_clause = ""
    
    # Build final query
    query = f"""
        SELECT DISTINCT ua.user_id
        FROM user_attributes ua
        {where_clause}
        ORDER BY ua.user_id
    """
    
    if request.limit:
        query += f" LIMIT {request.limit}"
    
    return query

# API endpoints

@app.get("/")
def root():
    return {"message": "DuckMart User Segmentation API"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.post("/segment", response_model=SegmentationResponse)
def segment_users(request: SegmentationRequest):
    """
    Segment users based on attribute and event filters
    
    Example JSON payload:
    ```json
    {
        "user_filters": [
            {
                "field": "age",
                "operator": "gte",
                "value": 25
            },
            {
                "field": "age", 
                "operator": "lte",
                "value": 34
            }
        ],
        "event_filters": [
            {
                "event_name": "LOGIN",
                "operator": "gte", 
                "count": 1
            }
        ],
        "logic_operator": "AND",
        "limit": 1000
    }
    ```
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
                "user_filters": [filter.dict() for filter in request.user_filters],
                "event_filters": [filter.dict() for filter in request.event_filters],
                "logic_operator": request.logic_operator
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Segmentation failed: {str(e)}")

@app.get("/examples")
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)