import requests
import json

def test_segmentation_api():
    """Test the segmentation API with example requests"""
    
    base_url = "http://localhost:8000"
    
    # Test cases
    test_cases = [
        {
            "name": "Age Segment (25-34)",
            "payload": {
                "user_filters": [
                    {"field": "age", "operator": "gte", "value": 25},
                    {"field": "age", "operator": "lte", "value": 34}
                ],
                "logic_operator": "AND",
                "limit": 100
            }
        },
        {
            "name": "California Login Users", 
            "payload": {
                "user_filters": [
                    {"field": "location", "operator": "eq", "value": "California"}
                ],
                "event_filters": [
                    {"event_name": "LOGIN", "operator": "gte", "count": 1}
                ],
                "logic_operator": "AND",
                "limit": 100
            }
        },
        {
            "name": "Premium Users",
            "payload": {
                "user_filters": [
                    {"field": "subscription_plan", "operator": "eq", "value": "Premium"}
                ],
                "limit": 50
            }
        },
        {
            "name": "Multiple Locations",
            "payload": {
                "user_filters": [
                    {"field": "location", "operator": "in", "value": ["California", "New York", "Texas"]}
                ],
                "limit": 200
            }
        }
    ]
    
    print("Testing Segmentation API...")
    print("=" * 50)
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['name']}")
        print(f"Payload: {json.dumps(test_case['payload'], indent=2)}")
        
        try:
            response = requests.post(f"{base_url}/segment", json=test_case['payload'])
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Success: Found {result['total_count']} users")
                print(f"Sample user IDs: {result['user_ids'][:10]}...")
            else:
                print(f"❌ Failed: {response.status_code} - {response.text}")
                
        except requests.exceptions.ConnectionError:
            print("❌ Connection failed - Make sure the API server is running")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
        
        print("-" * 30)

def get_api_examples():
    """Get example payloads from the API"""
    
    try:
        response = requests.get("http://localhost:8000/examples")
        if response.status_code == 200:
            examples = response.json()
            print("\nAPI Examples:")
            print("=" * 50)
            for key, example in examples.items():
                print(f"\n{key}:")
                print(f"Description: {example['description']}")
                print(f"Payload: {json.dumps(example['payload'], indent=2)}")
        else:
            print(f"Failed to get examples: {response.status_code}")
    except Exception as e:
        print(f"Error getting examples: {str(e)}")

if __name__ == "__main__":
    test_segmentation_api()
    get_api_examples()