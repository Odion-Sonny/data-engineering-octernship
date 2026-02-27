from typing import List
from .models import FilterCondition, EventCondition, SegmentationRequest

def generate_user_filter_sql(filter_condition: FilterCondition) -> str:
    """Generate SQL for user attribute filters"""
    field = filter_condition.field
    operator = filter_condition.operator
    value = filter_condition.value
    
    # Validate field names to prevent SQL injection
    valid_user_fields = ['user_id', 'name', 'age', 'gender', 'location', 'signup_date', 'subscription_plan', 'device_type']
    if field not in valid_user_fields:
        raise ValueError(f"Invalid field: {field}")
    
    if isinstance(value, str):
        value = value.replace("'", "''")
    elif isinstance(value, list):
        value = [v.replace("'", "''") if isinstance(v, str) else v for v in value]
    
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
        raise ValueError("'in' operator requires a list of values")
    elif operator == "not_in":
        if isinstance(value, list):
            value_list = "', '".join(map(str, value)) if all(isinstance(v, str) for v in value) else ", ".join(map(str, value))
            return f"{field} NOT IN ('{value_list}')" if all(isinstance(v, str) for v in value) else f"{field} NOT IN ({value_list})"
        raise ValueError("'not_in' operator requires a list of values")
    elif operator == "like":
        return f"{field} LIKE '%{value}%'"
    else:
        raise ValueError(f"Unsupported operator: {operator}")

def generate_event_filter_sql(event_condition: EventCondition) -> str:
    """Generate SQL for event-based filters"""
    event_name = event_condition.event_name.replace("'", "''")
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
