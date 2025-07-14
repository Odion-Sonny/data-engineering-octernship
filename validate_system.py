import duckdb
import requests
import json
import time

def validate_database():
    """Validate that the database setup is correct"""
    print("🔍 Validating database setup...")
    
    try:
        conn = duckdb.connect('duckmart.db')
        
        # Check table existence and row counts
        user_count = conn.execute("SELECT COUNT(*) FROM user_attributes").fetchone()[0]
        event_count = conn.execute("SELECT COUNT(*) FROM user_events").fetchone()[0]
        
        print(f"✅ Database validation passed:")
        print(f"   - User attributes: {user_count} records")
        print(f"   - User events: {event_count} records")
        
        # Validate required segmentation queries
        age_segment = conn.execute("""
            SELECT COUNT(*) FROM user_attributes 
            WHERE age >= 25 AND age <= 34
        """).fetchone()[0]
        
        ca_login_segment = conn.execute("""
            SELECT COUNT(DISTINCT ua.user_id)
            FROM user_attributes ua
            INNER JOIN user_events ue ON ua.user_id = ue.user_id
            WHERE ua.location = 'California' AND ue.event_name = 'LOGIN'
        """).fetchone()[0]
        
        print(f"   - Age segment (25-34): {age_segment} users")
        print(f"   - CA login segment: {ca_login_segment} users")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database validation failed: {str(e)}")
        return False

def validate_api():
    """Validate that the API is working correctly"""
    print("\n🔍 Validating API endpoints...")
    
    base_url = "http://localhost:8000"
    
    try:
        # Test health endpoint
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code != 200:
            print("❌ Health endpoint failed")
            return False
        
        print("✅ Health endpoint working")
        
        # Test examples endpoint
        response = requests.get(f"{base_url}/examples", timeout=5)
        if response.status_code != 200:
            print("❌ Examples endpoint failed")
            return False
        
        print("✅ Examples endpoint working")
        
        # Test segmentation endpoint - Age segment
        age_payload = {
            "user_filters": [
                {"field": "age", "operator": "gte", "value": 25},
                {"field": "age", "operator": "lte", "value": 34}
            ],
            "logic_operator": "AND",
            "limit": 10
        }
        
        response = requests.post(f"{base_url}/segment", json=age_payload, timeout=10)
        if response.status_code != 200:
            print(f"❌ Age segmentation failed: {response.text}")
            return False
        
        result = response.json()
        print(f"✅ Age segmentation working: {result['total_count']} users found")
        
        # Test segmentation endpoint - Location + Event segment
        location_event_payload = {
            "user_filters": [
                {"field": "location", "operator": "eq", "value": "California"}
            ],
            "event_filters": [
                {"event_name": "LOGIN", "operator": "gte", "count": 1}
            ],
            "logic_operator": "AND",
            "limit": 10
        }
        
        response = requests.post(f"{base_url}/segment", json=location_event_payload, timeout=10)
        if response.status_code != 200:
            print(f"❌ Location+Event segmentation failed: {response.text}")
            return False
        
        result = response.json()
        print(f"✅ Location+Event segmentation working: {result['total_count']} users found")
        
        return True
        
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API - make sure server is running on port 8000")
        return False
    except Exception as e:
        print(f"❌ API validation failed: {str(e)}")
        return False

def main():
    """Run complete system validation"""
    print("=" * 60)
    print("🚀 DuckMart Segmentation System Validation")
    print("=" * 60)
    
    # Validate database
    db_valid = validate_database()
    
    if not db_valid:
        print("\n❌ System validation failed - database issues")
        return False
    
    # Give API server time to start if needed
    print("\n⏳ Waiting for API server...")
    time.sleep(3)
    
    # Validate API
    api_valid = validate_api()
    
    if not api_valid:
        print("\n❌ System validation failed - API issues")
        print("💡 Make sure to start the API server with: python segmentation_api.py")
        return False
    
    print("\n" + "=" * 60)
    print("🎉 System validation completed successfully!")
    print("=" * 60)
    print("\n📋 Summary:")
    print("✅ Database schema created and populated")
    print("✅ Required segmentation queries working")
    print("✅ API endpoints functioning correctly")
    print("✅ JSON-to-SQL conversion working")
    print("\n🔗 API Documentation: http://localhost:8000/docs")
    print("🧪 Test endpoints: http://localhost:8000/examples")
    
    return True

if __name__ == "__main__":
    main()