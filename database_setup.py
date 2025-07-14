import duckdb
import pandas as pd
import os

def create_database_schema(conn):
    """Create the database schema for user attributes and events"""
    
    # Create user_attributes table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_attributes (
            user_id INTEGER PRIMARY KEY,
            name VARCHAR,
            age INTEGER,
            gender VARCHAR,
            location VARCHAR,
            signup_date DATE,
            subscription_plan VARCHAR,
            device_type VARCHAR
        )
    """)
    
    # Create user_events table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_events (
            user_id INTEGER,
            event_name VARCHAR,
            timestamp TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES user_attributes(user_id)
        )
    """)
    
    # Create index for better query performance
    conn.execute("CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_user_events_event_name ON user_events(event_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_user_attributes_age ON user_attributes(age)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_user_attributes_location ON user_attributes(location)")
    
    print("Database schema created successfully")

def load_csv_data(conn, csv_file, table_name):
    """Load CSV data into DuckDB table"""
    
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found!")
        return False
    
    try:
        # Read CSV using pandas
        df = pd.read_csv(csv_file)
        
        # Convert DataFrame to DuckDB table
        conn.execute(f"DELETE FROM {table_name}")  # Clear existing data
        conn.register('temp_df', df)
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Loaded {row_count} rows into {table_name}")
        return True
        
    except Exception as e:
        print(f"Error loading {csv_file}: {str(e)}")
        return False

def verify_data_load(conn):
    """Verify that data was loaded correctly"""
    
    # Check user_attributes table
    user_count = conn.execute("SELECT COUNT(*) FROM user_attributes").fetchone()[0]
    print(f"Total users: {user_count}")
    
    # Check user_events table  
    event_count = conn.execute("SELECT COUNT(*) FROM user_events").fetchone()[0]
    print(f"Total events: {event_count}")
    
    # Show sample data
    print("\nSample user attributes:")
    sample_users = conn.execute("SELECT * FROM user_attributes LIMIT 3").fetchall()
    for user in sample_users:
        print(user)
    
    print("\nSample user events:")
    sample_events = conn.execute("SELECT * FROM user_events LIMIT 3").fetchall()
    for event in sample_events:
        print(event)

def main():
    # Connect to DuckDB (creates file if doesn't exist)
    conn = duckdb.connect('duckmart.db')
    
    try:
        # Create schema
        create_database_schema(conn)
        
        # Load data
        print("\nLoading data...")
        user_success = load_csv_data(conn, 'user_attributes.csv', 'user_attributes')
        event_success = load_csv_data(conn, 'user_events.csv', 'user_events')
        
        if user_success and event_success:
            print("\nData loading completed successfully!")
            verify_data_load(conn)
        else:
            print("Data loading failed!")
            
    except Exception as e:
        print(f"Database setup failed: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()