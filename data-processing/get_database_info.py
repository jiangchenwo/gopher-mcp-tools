import sqlite3

def full_database_analysis(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    print(f"Analyzing database: {db_file}")
    print("=" * 60)
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    
    print(f"\nFound {len(tables)} tables:")
    for table in tables:
        print(f"  - {table}")
    
    print("\nDetailed Table Information:")
    print("=" * 60)
    
    relationships = []
    
    for table_name in tables:
        print(f"\nTable: {table_name}")
        print("-" * 40)
        
        # Column information
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        # Row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        
        print(f"Rows: {row_count}")
        print("Columns:")
        
        for col in columns:
            col_id, name, data_type, not_null, default_val, primary_key = col
            
            indicators = []
            if primary_key:
                indicators.append("PK")
            if not_null:
                indicators.append("NOT NULL")
            if default_val is not None:
                indicators.append(f"DEFAULT: {default_val}")
            
            indicator_str = f" ({', '.join(indicators)})" if indicators else ""
            print(f"  • {name}: {data_type}{indicator_str}")
        
        # Foreign key relationships
        cursor.execute(f"PRAGMA foreign_key_list({table_name});")
        foreign_keys = cursor.fetchall()
        
        if foreign_keys:
            print("Foreign Key Relationships:")
            for fk in foreign_keys:
                id_num, seq, table, from_col, to_col, on_update, on_delete, match = fk
                relationship = f"{table_name}.{from_col} -> {table}.{to_col}"
                relationships.append(relationship)
                print(f"  {relationship}")
    
    # Summary of all relationships
    if relationships:
        print(f"\nAll Database Relationships:")
        print("-" * 40)
        for rel in relationships:
            print(f"  {rel}")
    else:
        print("\n(No foreign key relationships found)")
    
    conn.close()

# Usage example
if __name__ == "__main__":
    db_file = "../data/gopherGrades.db"
    full_database_analysis(db_file)