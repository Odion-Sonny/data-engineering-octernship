import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

def generate_user_attributes(num_users=10000):
    """Generate dummy user attributes data"""
    
    subscription_plans = ['Basic', 'Premium', 'Enterprise', 'Free']
    device_types = ['Desktop', 'Mobile', 'Tablet']
    locations = ['California', 'New York', 'Texas', 'Florida', 'Illinois', 'Pennsylvania', 'Ohio', 'Georgia', 'North Carolina', 'Michigan']
    
    users = []
    for i in range(1, num_users + 1):
        # Generate signup date between 2 years ago and now
        signup_date = fake.date_between(start_date='-2y', end_date='today')
        
        user = {
            'user_id': i,
            'name': fake.name(),
            'age': random.randint(18, 70),
            'gender': random.choice(['Male', 'Female', 'Other']),
            'location': random.choice(locations),
            'signup_date': signup_date.strftime('%Y-%m-%d'),
            'subscription_plan': random.choice(subscription_plans),
            'device_type': random.choice(device_types)
        }
        users.append(user)
    
    return users

def generate_user_events(user_ids, num_events=50000):
    """Generate dummy user events data"""
    
    event_names = [
        'LOGIN', 'LOGOUT', 'PURCHASE_MADE', 'ADDED_TO_CART', 'REMOVED_FROM_CART',
        'VIEW_PRODUCT', 'SEARCH', 'PROFILE_UPDATE', 'PASSWORD_CHANGE', 'EMAIL_OPENED'
    ]
    
    events = []
    for i in range(num_events):
        # Generate timestamp between 1 year ago and now
        timestamp = fake.date_time_between(start_date='-1y', end_date='now')
        
        event = {
            'user_id': random.choice(user_ids),
            'event_name': random.choice(event_names),
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S')
        }
        events.append(event)
    
    return events

def save_to_csv(data, filename, fieldnames):
    """Save data to CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    print("Generating dummy data...")
    
    # Generate user attributes
    users = generate_user_attributes(10000)
    user_ids = [user['user_id'] for user in users]
    
    # Generate user events
    events = generate_user_events(user_ids, 50000)
    
    # Save to CSV files
    save_to_csv(
        users, 
        'user_attributes.csv',
        ['user_id', 'name', 'age', 'gender', 'location', 'signup_date', 'subscription_plan', 'device_type']
    )
    
    save_to_csv(
        events,
        'user_events.csv', 
        ['user_id', 'event_name', 'timestamp']
    )
    
    print(f"Generated {len(users)} users and {len(events)} events")
    print("Files saved: user_attributes.csv, user_events.csv")