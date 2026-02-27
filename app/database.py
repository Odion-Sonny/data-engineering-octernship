import duckdb

def get_db_connection(db_path: str = 'duckmart.db'):
    return duckdb.connect(db_path)
