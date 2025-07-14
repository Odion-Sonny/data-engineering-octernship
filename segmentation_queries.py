import duckdb

def run_age_segmentation(conn):
    """Segment users by age group (25-34 years)"""
    
    query = """
    SELECT user_id, name, age 
    FROM user_attributes 
    WHERE age >= 25 AND age <= 34
    ORDER BY user_id
    """
    
    result = conn.execute(query).fetchall()
    user_ids = [row[0] for row in result]
    
    print(f"Age-based segmentation (25-34 years):")
    print(f"Total users in segment: {len(result)}")
    print(f"User IDs: {user_ids[:20]}...")  # Show first 20 IDs
    
    return user_ids

def run_location_event_segmentation(conn):
    """Segment users by location='California' and have logged in at least once"""
    
    query = """
    SELECT DISTINCT ua.user_id, ua.name, ua.location
    FROM user_attributes ua
    INNER JOIN user_events ue ON ua.user_id = ue.user_id
    WHERE ua.location = 'California' 
    AND ue.event_name = 'LOGIN'
    ORDER BY ua.user_id
    """
    
    result = conn.execute(query).fetchall()
    user_ids = [row[0] for row in result]
    
    print(f"\nLocation + Event segmentation (California + LOGIN):")
    print(f"Total users in segment: {len(result)}")
    print(f"User IDs: {user_ids[:20]}...")  # Show first 20 IDs
    
    return user_ids

def analyze_segments(conn):
    """Additional analysis of the segments"""
    
    # Age distribution
    print("\n=== Age Distribution Analysis ===")
    age_dist = conn.execute("""
        SELECT 
            CASE 
                WHEN age < 25 THEN '18-24'
                WHEN age >= 25 AND age <= 34 THEN '25-34'
                WHEN age >= 35 AND age <= 44 THEN '35-44'
                WHEN age >= 45 AND age <= 54 THEN '45-54'
                ELSE '55+'
            END as age_group,
            COUNT(*) as user_count
        FROM user_attributes
        GROUP BY age_group
        ORDER BY age_group
    """).fetchall()
    
    for group, count in age_dist:
        print(f"{group}: {count} users")
    
    # Location distribution for California users with LOGIN events
    print("\n=== California Users with LOGIN Events ===")
    ca_login_stats = conn.execute("""
        SELECT 
            COUNT(DISTINCT ua.user_id) as total_ca_users_with_login,
            COUNT(ue.event_name) as total_login_events
        FROM user_attributes ua
        INNER JOIN user_events ue ON ua.user_id = ue.user_id
        WHERE ua.location = 'California' AND ue.event_name = 'LOGIN'
    """).fetchall()
    
    for stats in ca_login_stats:
        print(f"Total CA users with LOGIN: {stats[0]}")
        print(f"Total LOGIN events by CA users: {stats[1]}")

def main():
    # Connect to the existing DuckDB database
    conn = duckdb.connect('duckmart.db')
    
    try:
        print("Running segmentation queries...")
        
        # Run the required segmentations
        age_segment_users = run_age_segmentation(conn)
        location_event_users = run_location_event_segmentation(conn)
        
        # Additional analysis
        analyze_segments(conn)
        
        # Save results to files for reference
        with open('age_segment_users.txt', 'w') as f:
            f.write("Users in age group 25-34:\n")
            f.write(str(age_segment_users))
        
        with open('location_event_users.txt', 'w') as f:
            f.write("California users who have logged in:\n")
            f.write(str(location_event_users))
        
        print(f"\nResults saved to age_segment_users.txt and location_event_users.txt")
        
    except Exception as e:
        print(f"Query execution failed: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()