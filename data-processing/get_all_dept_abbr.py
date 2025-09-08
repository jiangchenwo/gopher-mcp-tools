import sqlite3, json

def get_all_dept_abbr_name(db_file: str) -> list:
    """
    Retrieve all unique department abbreviations and corresponding full department name from the courses table in the specified SQLite database.

    Args:
        db_file: Path to the SQLite database file.

    Returns:
        A dict mapping unique department abbreviations to full names.
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT dept_abbr, dept_name FROM departmentdistribution;")
    results = cursor.fetchall()
    dept_abbr_name = {abbr: name for abbr, name in results}
    
    conn.close()
    return dept_abbr_name

if __name__ == "__main__":
    db_file = "../data/gopherGrades.db"  # Replace with your database file path
    dept_abbr_name = get_all_dept_abbr_name(db_file)
    for abbr, name in dept_abbr_name.items():
        print(f"{abbr}: {name}")

    # Save to a json file
    with open("../data/deptAbbrName.json", "w") as f:
        json.dump(dept_abbr_name, f, indent=4)
