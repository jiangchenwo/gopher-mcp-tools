"""
Gopher Grades
Provides tools for accessing the GopherGrades SQLite database
"""

import os
import sqlite3
from typing import Optional, List, Dict, Any
import json
from contextlib import contextmanager
from fastmcp import FastMCP

# Initialize FastMCP server
app = FastMCP("Gopher Grades")

# Database path - adjust this to your actual database location
TOOL_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(TOOL_DIR, "ProcessedData.db")

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

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

@app.tool()
def search_courses(query: str, campus: str = "UMNTC", limit: int = 20) -> Dict[str, Any]:
    """
    Search for courses by department code, course number, or description
    
    Args:
        query: Search term (e.g., "CSCI", "5511", "Machine Learning")
        campus: Campus code (default: UMNTC for Twin Cities)
        limit: Maximum number of results to return
    
    Returns:
        Dictionary with matching courses
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        search_pattern = f"%{query.replace(' ', '')}%"
        
        cursor.execute("""
            SELECT 
                id,
                campus,
                dept_abbr,
                course_num,
                class_desc,
                total_students,
                total_grades,
                onestop,
                onestop_desc,
                cred_min,
                cred_max,
                srt_vals
            FROM classdistribution
            WHERE campus = ? AND (
                dept_abbr || course_num LIKE ? OR
                REPLACE(class_desc, ' ', '') LIKE ? OR
                dept_abbr LIKE ?
            )
            ORDER BY total_students DESC
            LIMIT ?
        """, (campus, search_pattern, search_pattern, search_pattern, limit))
        
        courses = [row_to_dict(row) for row in cursor.fetchall()]
        
        return {
            "count": len(courses),
            "courses": courses
        }

@app.tool()
def get_course_details(dept_abbr: str, course_num: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get detailed information about a specific course including grade distributions
    
    Args:
        dept_abbr: Department abbreviation (e.g., "CSCI")
        course_num: Course number (e.g., "5511")
        campus: Campus code (default: UMNTC)
    
    Returns:
        Detailed course information including distributions by professor
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get course info
        cursor.execute("""
            SELECT * FROM classdistribution
            WHERE campus = ? AND dept_abbr = ? AND course_num = ?
        """, (campus, dept_abbr.upper(), course_num))
        
        course_row = cursor.fetchone()
        if not course_row:
            return {"error": "Course not found"}
        
        course = row_to_dict(course_row)
        
        # Get distributions by professor
        cursor.execute("""
            SELECT 
                d.id as distribution_id,
                p.id as professor_id,
                p.name as professor_name,
                p.RMP_score,
                p.RMP_diff,
                p.RMP_link,
                t.term,
                t.students,
                t.grades
            FROM distribution d
            LEFT JOIN professor p ON d.professor_id = p.id
            LEFT JOIN termdistribution t ON d.id = t.dist_id
            WHERE d.class_id = ?
            ORDER BY t.term DESC
        """, (course['id'],))
        
        distributions = [row_to_dict(row) for row in cursor.fetchall()]
        
        # Get associated liberal education requirements
        cursor.execute("""
            SELECT l.name
            FROM libedAssociationTable lat
            JOIN libed l ON lat.left_id = l.id
            WHERE lat.right_id = ?
        """, (course['id'],))
        
        libeds = [row['name'] for row in cursor.fetchall()]
        
        course['libeds'] = libeds
        course['distributions'] = distributions
        
        return course

@app.tool()
def search_professors(query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Search for professors by name to get their database IDs and RMP scores
    
    Args:
        query: Professor name or partial name
        limit: Maximum number of results
    
    Returns:
        List of matching professors with their database IDs, names, and RMP scores
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        search_pattern = f"%{query.replace(' ', '')}%"
        
        cursor.execute("""
            SELECT 
                p.id,
                p.name,
                p.RMP_score,
                p.RMP_diff,
                p.RMP_link,
                p.x500,
                COUNT(DISTINCT d.class_id) as num_courses
            FROM professor p
            LEFT JOIN distribution d ON p.id = d.professor_id
            WHERE REPLACE(p.name, ' ', '') LIKE ?
            GROUP BY p.id
            ORDER BY p.RMP_score DESC NULLS LAST
            LIMIT ?
        """, (search_pattern, limit))
        
        return [row_to_dict(row) for row in cursor.fetchall()]

@app.tool()
def get_professor_courses(professor_id: int) -> Dict[str, Any]:
    """
    Get all courses taught by a specific professor
    
    Args:
        professor_id: The professor's database ID
    
    Returns:
        Professor information and list of courses taught
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get professor info
        cursor.execute("""
            SELECT * FROM professor WHERE id = ?
        """, (professor_id,))
        
        prof_row = cursor.fetchone()
        if not prof_row:
            return {"error": "Professor not found"}
        
        professor = row_to_dict(prof_row)
        
        # Get courses taught
        cursor.execute("""
            SELECT 
                c.dept_abbr,
                c.course_num,
                c.class_desc,
                c.campus,
                t.term,
                t.students,
                t.grades
            FROM distribution d
            JOIN classdistribution c ON d.class_id = c.id
            JOIN termdistribution t ON d.id = t.dist_id
            WHERE d.professor_id = ?
            ORDER BY t.term DESC
        """, (professor_id,))
        
        courses = [row_to_dict(row) for row in cursor.fetchall()]
        
        professor['courses'] = courses
        
        return professor

@app.tool()
def get_department_courses(dept_abbr: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get all courses in a department
    
    Args:
        dept_abbr: Department abbreviation (e.g., "CSCI")
        campus: Campus code (default: UMNTC)
    
    Returns:
        Department information and list of courses
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get department info
        cursor.execute("""
            SELECT * FROM departmentdistribution
            WHERE campus = ? AND dept_abbr = ?
        """, (campus, dept_abbr.upper()))
        
        dept_row = cursor.fetchone()
        if not dept_row:
            return {"error": "Department not found"}
        
        department = row_to_dict(dept_row)
        
        # Get courses in department
        cursor.execute("""
            SELECT 
                id,
                course_num,
                class_desc,
                total_students,
                total_grades,
                cred_min,
                cred_max,
                srt_vals
            FROM classdistribution
            WHERE campus = ? AND dept_abbr = ?
            ORDER BY course_num ASC
        """, (campus, dept_abbr.upper()))
        
        courses = [row_to_dict(row) for row in cursor.fetchall()]
        
        department['courses'] = courses
        
        return department

def calculate_gpa(grades: Dict[str, int]) -> Dict[str, Any]:
    """
    Calculate GPA and grade statistics including passing rate and withdrawal rate from a grade distribution
    
    Args:
        grades: Dictionary mapping letter grades to student counts
    
    Returns:
        GPA and grade statistics including passing rate and withdrawal rate
    """
    gpa_map = {
        "A+": 4.333, "A": 4.0, "A-": 3.667,
        "B+": 3.333, "B": 3.0, "B-": 2.667,
        "C+": 2.333, "C": 2.0, "C-": 1.667,
        "D+": 1.333, "D": 1.0, "D-": 0.667,
        "F": 0.0
    }
    
    total_points = 0
    total_af_students = 0
    total_students = sum(grades.values())
    
    for grade, count in grades.items():
        if grade in gpa_map:
            total_points += gpa_map[grade] * count
            total_af_students += count
    
    if total_af_students == 0:
        return {
            "gpa": None,
            "total_students": total_students,
            "total_af_students": 0,
            "pass_rate": None
        }
    
    gpa = total_points / total_af_students
    
    # Calculate pass rate (C- or better)
    passing_grades = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "S"]
    passed = sum(grades.get(g, 0) for g in passing_grades)
    failed_grades = ["F", "D+", "D", "N"]
    failed = sum(grades.get(g, 0) for g in failed_grades)
    total_graded_students = passed + failed
    pass_rate = (passed / total_graded_students) * 100 if total_graded_students > 0 else None

    # Calculate withdrawal rate
    withdrawn_grades = ["W"]
    withdrawn = sum(grades.get(g, 0) for g in withdrawn_grades)
    withdrawal_rate = (withdrawn / (total_graded_students + withdrawn)) * 100 if total_graded_students + withdrawn > 0 else None
    
    return {
        "gpa": round(gpa, 3),
        "total_students": total_students,
        "total_graded_students": total_graded_students,
        "total_af_students": total_af_students,
        "pass_rate": round(pass_rate, 1) if pass_rate else None,
        "withdrawal_rate": round(withdrawal_rate, 1) if withdrawal_rate else None,
        "grade_distribution": grades
    }

@app.tool()
def get_grade_trends(dept_abbr: str, course_num: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get grade distribution trends over time for a course
    
    Args:
        dept_abbr: Department abbreviation
        course_num: Course number
        campus: Campus code
    
    Returns:
        Grade trends by term
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get course ID
        cursor.execute("""
            SELECT id, class_desc FROM classdistribution
            WHERE campus = ? AND dept_abbr = ? AND course_num = ?
        """, (campus, dept_abbr.upper(), course_num))
        
        course_row = cursor.fetchone()
        if not course_row:
            return {"error": "Course not found"}
        
        course_id = course_row['id']
        course_desc = course_row['class_desc']
        
        # Get grade distributions by term
        cursor.execute("""
            SELECT 
                t.term,
                SUM(t.students) as total_students,
                GROUP_CONCAT(t.grades) as combined_grades,
                GROUP_CONCAT(p.name) as professors
            FROM distribution d
            JOIN termdistribution t ON d.id = t.dist_id
            LEFT JOIN professor p ON d.professor_id = p.id
            WHERE d.class_id = ?
            GROUP BY t.term
            ORDER BY t.term DESC
        """, (course_id,))
        
        trends = []
        for row in cursor.fetchall():
            # Combine grades from multiple professors in same term
            combined = {}
            if row['combined_grades']:
                for grades_json in row['combined_grades'].split(','):
                    grades = parse_json_field(grades_json)
                    if grades:
                        for grade, count in grades.items():
                            combined[grade] = combined.get(grade, 0) + count
            
            stats = calculate_gpa(combined)
            trends.append({
                "term": row['term'],
                "term_name": term_to_name(row['term']),
                "total_students": row['total_students'],
                "professors": row['professors'].split(',') if row['professors'] else [],
                "gpa": stats['gpa'],
                "pass_rate": stats['pass_rate'],
                "grades": combined
            })
        
        return {
            "course": f"{dept_abbr.upper()} {course_num}",
            "description": course_desc,
            "trends": trends
        }

def term_to_name(term: int) -> str:
    """Convert term code to readable name"""
    year = 1900 + (term // 10)
    semester = term % 10
    
    if semester == 3:
        return f"Spring {year}"
    elif semester == 5:
        return f"Summer {year}"
    elif semester == 9:
        return f"Fall {year}"
    else:
        return f"Unknown {year}"

@app.tool()
def get_liberal_education_courses(libed_name: str, campus: str = "UMNTC", limit: int = 50) -> Dict[str, Any]:
    """
    Get courses that fulfill a specific liberal education requirement
    
    Args:
        libed_name: Name of the liberal education requirement
        campus: Campus code
        limit: Maximum number of results
    
    Returns:
        List of courses fulfilling the requirement
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get libed ID
        cursor.execute("""
            SELECT id FROM libed WHERE name LIKE ?
        """, (f"%{libed_name}%",))
        
        libed_row = cursor.fetchone()
        if not libed_row:
            return {"error": "Liberal education requirement not found"}
        
        libed_id = libed_row['id']
        
        # Get courses
        cursor.execute("""
            SELECT 
                c.dept_abbr,
                c.course_num,
                c.class_desc,
                c.total_students,
                c.total_grades,
                c.cred_min,
                c.cred_max
            FROM libedAssociationTable lat
            JOIN classdistribution c ON lat.right_id = c.id
            WHERE lat.left_id = ? AND c.campus = ?
            ORDER BY c.total_students DESC
            LIMIT ?
        """, (libed_id, campus, limit))
        
        courses = [row_to_dict(row) for row in cursor.fetchall()]
        
        return {
            "libed": libed_name,
            "count": len(courses),
            "courses": courses
        }

@app.tool()
def get_professor_grade_statistics(professor_id: int) -> Dict[str, Any]:
    """
    Get comprehensive grade statistics for a professor across all courses
    
    Args:
        professor_id: The professor's database ID
    
    Returns:
        Detailed grade statistics including GPA, pass rates, and grade distributions
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get professor info
        cursor.execute("SELECT * FROM professor WHERE id = ?", (professor_id,))
        prof_row = cursor.fetchone()
        if not prof_row:
            return {"error": "Professor not found"}
        
        professor = row_to_dict(prof_row)
        
        # Get all grades given by this professor
        cursor.execute("""
            SELECT 
                c.dept_abbr,
                c.course_num,
                c.class_desc,
                t.term,
                t.students,
                t.grades
            FROM distribution d
            JOIN termdistribution t ON d.id = t.dist_id
            JOIN classdistribution c ON d.class_id = c.id
            WHERE d.professor_id = ? AND c.campus = 'UMNTC'
        """, (professor_id,))
        
        all_grades = {}
        total_students = 0
        courses_taught = set()
        terms_taught = set()
        course_grades = {}
        
        for row in cursor.fetchall():
            grades = parse_json_field(row['grades'])
            if grades:
                course_key = f"{row['dept_abbr']} {row['course_num']}"
                courses_taught.add(course_key)
                terms_taught.add(row['term'])
                
                # Aggregate all grades
                for grade, count in grades.items():
                    all_grades[grade] = all_grades.get(grade, 0) + count
                    total_students += count
                
                # Track grades by course
                if course_key not in course_grades:
                    course_grades[course_key] = {"grades": {}, "students": 0, "description": row['class_desc']}
                
                for grade, count in grades.items():
                    course_grades[course_key]["grades"][grade] = course_grades[course_key]["grades"].get(grade, 0) + count
                    course_grades[course_key]["students"] += count
        
        # Calculate overall statistics
        overall_stats = calculate_gpa(all_grades)
        
        # Calculate per-course statistics
        course_stats = []
        for course, data in course_grades.items():
            stats = calculate_gpa(data["grades"])
            course_stats.append({
                "course": course,
                "description": data["description"],
                "total_students": data["students"],
                "gpa": stats["gpa"],
                "pass_rate": stats["pass_rate"]
            })
        
        # Sort courses by GPA
        course_stats.sort(key=lambda x: x["gpa"] if x["gpa"] else 0, reverse=True)
        
        # Calculate grade distribution percentages
        grade_percentages = {}
        if total_students > 0:
            for grade, count in all_grades.items():
                grade_percentages[grade] = round((count / total_students) * 100, 2)
        
        return {
            "professor": professor,
            "overall_statistics": {
                "total_students_taught": total_students,
                "unique_courses": len(courses_taught),
                "terms_taught": len(terms_taught),
                "overall_gpa": overall_stats["gpa"],
                "overall_pass_rate": overall_stats["pass_rate"],
                "grade_distribution": all_grades,
                "grade_percentages": grade_percentages
            },
            "by_course": course_stats,
            "easiest_course": course_stats[0] if course_stats else None,
            "hardest_course": course_stats[-1] if course_stats else None
        }

@app.tool()
def compare_professor_grades(professor_ids: List[int]) -> Dict[str, Any]:
    """
    Compare grade statistics between multiple professors
    
    Args:
        professor_ids: List of professor database IDs to compare
    
    Returns:
        Comparative statistics for the professors
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        comparisons = []
        
        for prof_id in professor_ids:
            # Get professor info and grades
            cursor.execute("""
                SELECT 
                    p.id,
                    p.name,
                    p.RMP_score,
                    p.RMP_diff,
                    GROUP_CONCAT(t.grades) as all_grades,
                    SUM(t.students) as total_students,
                    COUNT(DISTINCT d.class_id) as num_courses
                FROM professor p
                LEFT JOIN distribution d ON p.id = d.professor_id
                LEFT JOIN termdistribution t ON d.id = t.dist_id
                WHERE p.id = ?
                GROUP BY p.id
            """, (prof_id,))
            
            row = cursor.fetchone()
            if row:
                # Combine all grades
                combined_grades = {}
                if row['all_grades']:
                    for grades_json in row['all_grades'].split(','):
                        grades = parse_json_field(grades_json)
                        if grades:
                            for grade, count in grades.items():
                                combined_grades[grade] = combined_grades.get(grade, 0) + count
                
                stats = calculate_gpa(combined_grades)
                
                comparisons.append({
                    "id": row['id'],
                    "name": row['name'],
                    "rmp_score": row['RMP_score'],
                    "rmp_difficulty": row['RMP_diff'],
                    "total_students": row['total_students'],
                    "num_courses": row['num_courses'],
                    "average_gpa": stats['gpa'],
                    "pass_rate": stats['pass_rate'],
                    "grade_distribution": combined_grades
                })
        
        # Sort by GPA
        comparisons.sort(key=lambda x: x['average_gpa'] if x['average_gpa'] else 0, reverse=True)
        
        return {
            "comparison": comparisons,
            "easiest_grader": comparisons[0] if comparisons else None,
            "hardest_grader": comparisons[-1] if comparisons else None
        }

@app.tool()
def get_course_grade_statistics(dept_abbr: str, course_num: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get comprehensive grade statistics for a course across all professors and terms
    
    Args:
        dept_abbr: Department abbreviation
        course_num: Course number
        campus: Campus code
    
    Returns:
        Detailed grade statistics including by professor and by term breakdowns
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get course info
        cursor.execute("""
            SELECT * FROM classdistribution
            WHERE campus = ? AND dept_abbr = ? AND course_num = ?
        """, (campus, dept_abbr.upper(), course_num))
        
        course_row = cursor.fetchone()
        if not course_row:
            return {"error": "Course not found"}
        
        course = row_to_dict(course_row)
        
        # Get grades by professor
        cursor.execute("""
            SELECT 
                p.id as professor_id,
                p.name as professor_name,
                p.RMP_score,
                p.RMP_diff,
                GROUP_CONCAT(t.grades) as all_grades,
                SUM(t.students) as total_students,
                COUNT(DISTINCT t.term) as num_terms,
                MIN(t.term) as first_term,
                MAX(t.term) as last_term
            FROM distribution d
            LEFT JOIN professor p ON d.professor_id = p.id
            LEFT JOIN termdistribution t ON d.id = t.dist_id
            WHERE d.class_id = ?
            GROUP BY p.id
        """, (course['id'],))
        
        professor_stats = []
        for row in cursor.fetchall():
            # Combine grades from all terms
            combined_grades = {}
            if row['all_grades']:
                for grades_json in row['all_grades'].split(','):
                    grades = parse_json_field(grades_json)
                    if grades:
                        for grade, count in grades.items():
                            combined_grades[grade] = combined_grades.get(grade, 0) + count
            
            stats = calculate_gpa(combined_grades)
            
            professor_stats.append({
                "professor_id": row['professor_id'],
                "professor_name": row['professor_name'],
                "rmp_score": row['RMP_score'],
                "rmp_difficulty": row['RMP_diff'],
                "total_students": row['total_students'],
                "num_terms_taught": row['num_terms'],
                "first_term": term_to_name(row['first_term']) if row['first_term'] else None,
                "last_term": term_to_name(row['last_term']) if row['last_term'] else None,
                "average_gpa": stats['gpa'],
                "pass_rate": stats['pass_rate'],
                "grade_distribution": combined_grades
            })
        
        # Sort by GPA
        professor_stats.sort(key=lambda x: x['average_gpa'] if x['average_gpa'] else 0, reverse=True)
        
        # Calculate overall course statistics
        overall_stats = calculate_gpa(course['total_grades'])
        
        # Get grade trends by year
        cursor.execute("""
            SELECT 
                CAST(t.term / 10 AS INTEGER) + 1900 as year,
                GROUP_CONCAT(t.grades) as all_grades,
                SUM(t.students) as total_students
            FROM distribution d
            JOIN termdistribution t ON d.id = t.dist_id
            WHERE d.class_id = ?
            GROUP BY year
            ORDER BY year DESC
        """, (course['id'],))
        
        yearly_trends = []
        for row in cursor.fetchall():
            combined_grades = {}
            if row['all_grades']:
                for grades_json in row['all_grades'].split(','):
                    grades = parse_json_field(grades_json)
                    if grades:
                        for grade, count in grades.items():
                            combined_grades[grade] = combined_grades.get(grade, 0) + count
            
            stats = calculate_gpa(combined_grades)
            yearly_trends.append({
                "year": row['year'],
                "total_students": row['total_students'],
                "average_gpa": stats['gpa'],
                "pass_rate": stats['pass_rate']
            })
        
        return {
            "course": course,
            "overall_statistics": {
                "average_gpa": overall_stats['gpa'],
                "pass_rate": overall_stats['pass_rate'],
                "total_students": course['total_students'],
                "grade_distribution": course['total_grades']
            },
            "by_professor": professor_stats,
            "easiest_professor": professor_stats[0] if professor_stats else None,
            "hardest_professor": professor_stats[-1] if professor_stats else None,
            "yearly_trends": yearly_trends
        }

@app.tool()
def find_easy_courses(dept_abbr: Optional[str] = None, min_gpa: float = 3.5, 
                      min_students: int = 50, campus: str = "UMNTC", limit: int = 20) -> List[Dict[str, Any]]:
    """
    Find courses with high average GPAs (easier courses)
    
    Args:
        dept_abbr: Optional department filter
        min_gpa: Minimum average GPA (default: 3.5)
        min_students: Minimum number of students to consider (default: 50)
        campus: Campus code
        limit: Maximum number of results
    
    Returns:
        List of courses sorted by average GPA
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        query = """
            SELECT 
                dept_abbr,
                course_num,
                class_desc,
                total_students,
                total_grades,
                cred_min,
                cred_max
            FROM classdistribution
            WHERE campus = ? AND total_students >= ?
        """
        params = [campus, min_students]
        
        if dept_abbr:
            query += " AND dept_abbr = ?"
            params.append(dept_abbr.upper())
        
        cursor.execute(query, params)
        
        courses_with_gpa = []
        for row in cursor.fetchall():
            course = row_to_dict(row)
            stats = calculate_gpa(course['total_grades'])
            
            if stats['gpa'] and stats['gpa'] >= min_gpa:
                courses_with_gpa.append({
                    "course": f"{course['dept_abbr']} {course['course_num']}",
                    "description": course['class_desc'],
                    "average_gpa": stats['gpa'],
                    "pass_rate": stats['pass_rate'],
                    "total_students": course['total_students'],
                    "credits": f"{course['cred_min']}-{course['cred_max']}" if course['cred_max'] != course['cred_min'] else str(course['cred_min'])
                })
        
        # Sort by GPA descending
        courses_with_gpa.sort(key=lambda x: x['average_gpa'], reverse=True)
        
        return courses_with_gpa[:limit]

@app.tool()
def find_challenging_courses(dept_abbr: Optional[str] = None, max_gpa: float = 2.5,
                            min_students: int = 50, campus: str = "UMNTC", limit: int = 20) -> List[Dict[str, Any]]:
    """
    Find courses with low average GPAs (more challenging courses)
    
    Args:
        dept_abbr: Optional department filter
        max_gpa: Maximum average GPA (default: 2.5)
        min_students: Minimum number of students to consider (default: 50)
        campus: Campus code
        limit: Maximum number of results
    
    Returns:
        List of courses sorted by average GPA (ascending)
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        query = """
            SELECT 
                dept_abbr,
                course_num,
                class_desc,
                total_students,
                total_grades,
                cred_min,
                cred_max
            FROM classdistribution
            WHERE campus = ? AND total_students >= ?
        """
        params = [campus, min_students]
        
        if dept_abbr:
            query += " AND dept_abbr = ?"
            params.append(dept_abbr.upper())
        
        cursor.execute(query, params)
        
        courses_with_gpa = []
        for row in cursor.fetchall():
            course = row_to_dict(row)
            stats = calculate_gpa(course['total_grades'])
            
            if stats['gpa'] and stats['gpa'] <= max_gpa:
                courses_with_gpa.append({
                    "course": f"{course['dept_abbr']} {course['course_num']}",
                    "description": course['class_desc'],
                    "average_gpa": stats['gpa'],
                    "pass_rate": stats['pass_rate'],
                    "total_students": course['total_students'],
                    "credits": f"{course['cred_min']}-{course['cred_max']}" if course['cred_max'] != course['cred_min'] else str(course['cred_min'])
                })
        
        # Sort by GPA ascending (hardest first)
        courses_with_gpa.sort(key=lambda x: x['average_gpa'])
        
        return courses_with_gpa[:limit]

@app.tool()
def get_grade_distribution_percentiles(dept_abbr: str, course_num: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get percentile breakdown of grades for a course (what percentage got A, B, C, etc.)
    
    Args:
        dept_abbr: Department abbreviation
        course_num: Course number
        campus: Campus code
    
    Returns:
        Percentile breakdown and cumulative percentages
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT class_desc, total_students, total_grades
            FROM classdistribution
            WHERE campus = ? AND dept_abbr = ? AND course_num = ?
        """, (campus, dept_abbr.upper(), course_num))
        
        row = cursor.fetchone()
        if not row:
            return {"error": "Course not found"}
        
        course = row_to_dict(row)
        grades = course['total_grades']
        
        # Calculate percentages
        total = sum(grades.values())
        if total == 0:
            return {"error": "No grade data available"}
        
        percentages = {}
        cumulative = {}
        cumulative_sum = 0
        
        # Order grades from best to worst
        grade_order = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-", "F", "W", "S", "N", "P"]
        
        for grade in grade_order:
            count = grades.get(grade, 0)
            percentage = (count / total) * 100
            percentages[grade] = round(percentage, 2)
            cumulative_sum += count
            cumulative[grade] = round((cumulative_sum / total) * 100, 2)
        
        # Calculate key percentiles
        a_range = grades.get("A+", 0) + grades.get("A", 0) + grades.get("A-", 0)
        b_range = grades.get("B+", 0) + grades.get("B", 0) + grades.get("B-", 0)
        c_range = grades.get("C+", 0) + grades.get("C", 0) + grades.get("C-", 0)
        d_range = grades.get("D+", 0) + grades.get("D", 0) + grades.get("D-", 0)
        
        return {
            "course": f"{dept_abbr.upper()} {course_num}",
            "description": course['class_desc'],
            "total_students": total,
            "grade_percentages": percentages,
            "cumulative_percentages": cumulative,
            "summary": {
                "a_range_percentage": round((a_range / total) * 100, 2),
                "b_range_percentage": round((b_range / total) * 100, 2),
                "c_range_percentage": round((c_range / total) * 100, 2),
                "d_range_percentage": round((d_range / total) * 100, 2),
                "f_percentage": round((grades.get("F", 0) / total) * 100, 2),
                "withdrawal_rate": round((grades.get("W", 0) / total) * 100, 2)
            }
        }

@app.tool()
def find_grade_inflated_courses(min_a_percentage: float = 50.0, min_students: int = 100,
                               campus: str = "UMNTC", limit: int = 20) -> List[Dict[str, Any]]:
    """
    Find courses where a high percentage of students receive A grades
    
    Args:
        min_a_percentage: Minimum percentage of A grades (default: 50%)
        min_students: Minimum number of students to consider
        campus: Campus code
        limit: Maximum number of results
    
    Returns:
        List of potentially grade-inflated courses
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                dept_abbr,
                course_num,
                class_desc,
                total_students,
                total_grades
            FROM classdistribution
            WHERE campus = ? AND total_students >= ?
        """, (campus, min_students))
        
        inflated_courses = []
        
        for row in cursor.fetchall():
            course = row_to_dict(row)
            grades = course['total_grades']
            total = sum(grades.values())
            
            if total > 0:
                a_grades = grades.get("A+", 0) + grades.get("A", 0) + grades.get("A-", 0)
                a_percentage = (a_grades / total) * 100
                
                if a_percentage >= min_a_percentage:
                    stats = calculate_gpa(grades)
                    inflated_courses.append({
                        "course": f"{course['dept_abbr']} {course['course_num']}",
                        "description": course['class_desc'],
                        "a_grade_percentage": round(a_percentage, 2),
                        "average_gpa": stats['gpa'],
                        "total_students": total,
                        "grade_breakdown": {
                            "A+": grades.get("A+", 0),
                            "A": grades.get("A", 0),
                            "A-": grades.get("A-", 0)
                        }
                    })
        
        # Sort by A percentage descending
        inflated_courses.sort(key=lambda x: x['a_grade_percentage'], reverse=True)
        
        return inflated_courses[:limit]

@app.tool()
def get_professor_course_comparison(professor_id: int, dept_abbr: str, course_num: str) -> Dict[str, Any]:
    """
    Compare a professor's grading in a specific course to the course average
    
    Args:
        professor_id: Professor's database ID
        dept_abbr: Department abbreviation
        course_num: Course number
    
    Returns:
        Comparison of professor's grading to course average
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get course ID and overall stats
        cursor.execute("""
            SELECT id, class_desc, total_students, total_grades
            FROM classdistribution
            WHERE campus = 'UMNTC' AND dept_abbr = ? AND course_num = ?
        """, (dept_abbr.upper(), course_num))
        
        course_row = cursor.fetchone()
        if not course_row:
            return {"error": "Course not found"}
        
        course = row_to_dict(course_row)
        overall_stats = calculate_gpa(course['total_grades'])
        
        # Get professor's stats for this course
        cursor.execute("""
            SELECT 
                p.name,
                p.RMP_score,
                GROUP_CONCAT(t.grades) as all_grades,
                SUM(t.students) as total_students
            FROM distribution d
            JOIN professor p ON d.professor_id = p.id
            JOIN termdistribution t ON d.id = t.dist_id
            WHERE d.class_id = ? AND p.id = ?
            GROUP BY p.id
        """, (course['id'], professor_id))
        
        prof_row = cursor.fetchone()
        if not prof_row:
            return {"error": "Professor has not taught this course"}
        
        # Combine professor's grades
        prof_grades = {}
        if prof_row['all_grades']:
            for grades_json in prof_row['all_grades'].split(','):
                grades = parse_json_field(grades_json)
                if grades:
                    for grade, count in grades.items():
                        prof_grades[grade] = prof_grades.get(grade, 0) + count
        
        prof_stats = calculate_gpa(prof_grades)
        
        # Calculate differences
        gpa_difference = (prof_stats['gpa'] - overall_stats['gpa']) if prof_stats['gpa'] and overall_stats['gpa'] else None
        pass_rate_difference = (prof_stats['pass_rate'] - overall_stats['pass_rate']) if prof_stats['pass_rate'] and overall_stats['pass_rate'] else None
        
        return {
            "course": f"{dept_abbr.upper()} {course_num}",
            "description": course['class_desc'],
            "professor": {
                "name": prof_row['name'],
                "rmp_score": prof_row['RMP_score'],
                "students_taught": prof_row['total_students'],
                "average_gpa": prof_stats['gpa'],
                "pass_rate": prof_stats['pass_rate'],
                "grade_distribution": prof_grades
            },
            "course_average": {
                "total_students": course['total_students'],
                "average_gpa": overall_stats['gpa'],
                "pass_rate": overall_stats['pass_rate'],
                "grade_distribution": course['total_grades']
            },
            "comparison": {
                "gpa_difference": round(gpa_difference, 3) if gpa_difference else None,
                "pass_rate_difference": round(pass_rate_difference, 2) if pass_rate_difference else None,
                "easier_than_average": gpa_difference > 0 if gpa_difference else None
            }
        }

# @app.tool()
# def get_database_stats() -> Dict[str, Any]:
#     """
#     Get overall statistics about the database 
    
#     Returns:
#         Database statistics including counts of courses, professors, etc.
#     """
#     with get_db_connection() as conn:
#         cursor = conn.cursor()
        
#         stats = {}
        
#         # Count courses
#         cursor.execute("SELECT COUNT(*) as count FROM classdistribution WHERE campus = 'UMNTC'")
#         stats['total_courses'] = cursor.fetchone()['count']
        
#         # Count professors
#         cursor.execute("SELECT COUNT(*) as count FROM professor")
#         stats['total_professors'] = cursor.fetchone()['count']
        
#         # Count departments
#         cursor.execute("SELECT COUNT(*) as count FROM departmentdistribution WHERE campus = 'UMNTC'")
#         stats['total_departments'] = cursor.fetchone()['count']
        
#         # Count terms
#         cursor.execute("SELECT COUNT(DISTINCT term) as count FROM termdistribution")
#         stats['total_terms'] = cursor.fetchone()['count']
        
#         # Get term range
#         cursor.execute("SELECT MIN(term) as min_term, MAX(term) as max_term FROM termdistribution")
#         row = cursor.fetchone()
#         if row['min_term'] and row['max_term']:
#             stats['earliest_term'] = term_to_name(row['min_term'])
#             stats['latest_term'] = term_to_name(row['max_term'])
        
#         # Total students graded
#         cursor.execute("SELECT SUM(total_students) as total FROM classdistribution WHERE campus = 'UMNTC'")
#         stats['total_students_graded'] = cursor.fetchone()['total']
        
#         return stats


if __name__ == "__main__":
    app.run()