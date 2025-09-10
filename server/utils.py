import json
import sqlite3
from typing import Any, Dict, List, Literal


# Utility functions
def parse_json_field(value: Any) -> Any:
    """Parse JSON fields from database"""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value

def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    """Convert a database row to a dictionary, parsing JSON fields"""
    result = dict(row)
    # Parse known JSON fields
    json_fields = ['grades', 'total_grades', 'srt_vals', 'libEds']
    for field in json_fields:
        if field in result:
            result[field] = parse_json_field(result[field])
    return result

def calculate_grades_stats(grades: Dict[str, int]) -> Dict[str, Any]:
    """
    Calculate comprehensive GPA and grade statistics from a grade distribution

    Args:
        grades: Dictionary mapping letter grades to student counts

    Returns:
        Comprehensive grade statistics including 
            average GPA, number of students, number of graded students, number of A-F students, 
            pass rate, withdrawal rate, and grade level breakdowns (A, B, C, D, F rates). 
    """
    # Validate input
    if not grades or not isinstance(grades, dict):
        return {
            "average_gpa": None,
            "total_students": 0,
            "total_graded_students": 0,
            "total_af_students": 0,
            "pass_rate": None,
            "withdrawal_rate": None,
            "a_rate": None,
            "b_rate": None,
            "c_rate": None,
            "d_rate": None,
            "f_rate": None,
            "grade_distribution": {}
        }

    # GPA mapping for letter grades
    gpa_map = {
        "A+": 4.333, "A": 4.0, "A-": 3.667,
        "B+": 3.333, "B": 3.0, "B-": 2.667,
        "C+": 2.333, "C": 2.0, "C-": 1.667,
        "D+": 1.333, "D": 1.0, "D-": 0.667,
        "F": 0.0
    }

    # Calculate totals
    total_points = 0
    total_af_students = 0  # Students with A-F grades (for GPA calculation)
    total_students = sum(grades.values())

    # Calculate average GPA from A-F grades only
    for grade, count in grades.items():
        if grade in gpa_map:
            total_points += gpa_map[grade] * count
            total_af_students += count

    # Calculate average GPA
    average_gpa = (total_points / total_af_students) if total_af_students > 0 else None

    # Define grade categories
    passing_grades = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-", "S", "P"]
    failing_grades = ["F", "N"]
    withdrawn_grades = ["W"]
    
    # Grade level breakdowns
    a_grades = ["A+", "A", "A-"]
    b_grades = ["B+", "B", "B-"]
    c_grades = ["C+", "C", "C-"]
    d_grades = ["D+", "D", "D-"]
    f_grades = ["F"]

    # Calculate counts
    passed = sum(grades.get(g, 0) for g in passing_grades)
    failed = sum(grades.get(g, 0) for g in failing_grades)
    withdrawn = sum(grades.get(g, 0) for g in withdrawn_grades)
    
    # Grade level counts
    a_count = sum(grades.get(g, 0) for g in a_grades)
    b_count = sum(grades.get(g, 0) for g in b_grades)
    c_count = sum(grades.get(g, 0) for g in c_grades)
    d_count = sum(grades.get(g, 0) for g in d_grades)
    f_count = sum(grades.get(g, 0) for g in f_grades)

    # Students with pass/fail grades (excluding withdrawals and other special grades)
    total_graded_students = passed + failed
    
    # Calculate rates
    pass_rate = (passed / total_graded_students) * 100 if total_graded_students > 0 else None
    withdrawal_rate = (withdrawn / total_students) * 100 if total_students > 0 else None
    
    # Grade level rates (as percentage of graded students)
    a_rate = (a_count / total_graded_students) * 100 if total_graded_students > 0 else None
    b_rate = (b_count / total_graded_students) * 100 if total_graded_students > 0 else None
    c_rate = (c_count / total_graded_students) * 100 if total_graded_students > 0 else None
    d_rate = (d_count / total_graded_students) * 100 if total_graded_students > 0 else None
    f_rate = (f_count / total_graded_students) * 100 if total_graded_students > 0 else None

    return {
        "average_gpa": round(average_gpa, 3) if average_gpa is not None else None,
        "total_students": total_students,
        "total_graded_students": total_graded_students,
        "total_af_students": total_af_students,
        "pass_rate": round(pass_rate, 1) if pass_rate is not None else None,
        "withdrawal_rate": round(withdrawal_rate, 1) if withdrawal_rate is not None else None,
        "grade_rates": {
            "a_rate": round(a_rate, 1) if a_rate is not None else None,
            "b_rate": round(b_rate, 1) if b_rate is not None else None,
            "c_rate": round(c_rate, 1) if c_rate is not None else None,
            "d_rate": round(d_rate, 1) if d_rate is not None else None,
            "f_rate": round(f_rate, 1) if f_rate is not None else None,
        },
        "grade_counts": {
            "a_count": a_count,
            "b_count": b_count,
            "c_count": c_count,
            "d_count": d_count,
            "f_count": f_count,
            "withdrawn_count": withdrawn,
            "passed_count": passed,
            "failed_count": failed
        }
    }

def get_prefixes_for_level(level: List[Literal["undergraduate", "master", "doctoral"]]) -> List[str]:
    """Get course number prefixes for a given course level."""
    prefixes = []
    if "undergraduate" in level:
        prefixes.extend([str(i) for i in range(1, 5)])  # 1000-4999
    if "master" in level:
        prefixes.extend([str(i) for i in range(5, 7)])  # 5000-6999
    if "doctoral" in level:
        prefixes.extend([str(i) for i in range(7, 10)]) # 7000-9999
    return prefixes

def term_to_name(term: int):
    retVal = ""
    if term % 10 == 5:
        retVal += "Summer "
    elif term % 10 == 9:
        retVal += "Fall "
    elif term % 10 == 3:
        retVal += "Spring "
    else:
        return "Invalid Term"

    term //= 10
    retVal += f"{1900 + (term)}"
    return retVal