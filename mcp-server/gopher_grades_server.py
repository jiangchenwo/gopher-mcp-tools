"""
Gopher Grades
Provides tools for accessing the GopherGrades SQLite database
"""

import os
import sqlite3
import logging
from typing import Optional, List, Dict, Any
import json
from contextlib import contextmanager
from fastmcp import FastMCP, Context
import re # string manipulation

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



class Database:
    """Database class with async connect/disconnect and query support."""

    def __init__(self):
        TOOL_DIR = os.path.dirname(os.path.abspath(__file__))
        self.DB_PATH = os.path.join(os.path.dirname(TOOL_DIR), "ProcessedData.db")
        logger.info(f"Database path set to: {self.DB_PATH}")
        self.conn = None

    async def connect(self) -> "Database":
        """Connect to database (runs in thread for async)."""
        try:
            logger.info(f"Connecting to database at {self.DB_PATH}")
            loop = asyncio.get_running_loop()
            self.conn = await loop.run_in_executor(
                None, lambda: sqlite3.connect(self.DB_PATH)
            )
            self.conn.row_factory = sqlite3.Row
            logger.info("Database connection established successfully")
            return self
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from database (runs in thread for async)."""
        try:
            if self.conn:
                logger.info("Disconnecting from database")
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.conn.close)
                logger.info("Database disconnected successfully")
        except Exception as e:
            logger.error(f"Error disconnecting from database: {e}")

    async def query(self, query: str, params: tuple = None, type: str = "all", context: Optional['DbContext'] = None):
        """Execute a query asynchronously."""
        def _query():
            cursor = self.conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if type == "one":
                return cursor.fetchone()
            elif type == "all":
                return cursor.fetchall()
            else:
                raise ValueError("Invalid query type specified.")
        try:
            # Log query details if context is provided
            if context:
                formatted_query = query
                if params:
                    formatted_query = f"{query} -- params: {params}"
                context.last_query = formatted_query
                context.query_history.append(formatted_query)
                logger.info(f"Executing query: {formatted_query}")
            
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, _query)
            
            # Log result summary if context is provided
            if context:
                result_summary = {
                    "type": type,
                    "row_count": len(result) if result and hasattr(result, '__len__') else (1 if result else 0),
                    "query": formatted_query
                }
                context.last_result = result_summary
                logger.info(f"Query completed: {result_summary}")
            
            return result
        except Exception as e:
            if context:
                error_info = {
                    "error": str(e),
                    "query": query,
                    "params": params
                }
                context.last_result = error_info
                logger.error(f"Query failed: {error_info}")
            logger.error(f"Database async query failed: {e}")
            raise


@dataclass
class DbContext:
    """ Database context for managing queries and results."""
    db: Database
    last_query: Optional[str] = None
    last_result: Optional[Dict[str, Any]] = None
    query_history: List[str] = None
    
    def __post_init__(self):
        if self.query_history is None:
            self.query_history = []
    

@asynccontextmanager
async def db_lifespan(server: FastMCP) -> AsyncIterator[DbContext]:
    """Manage database lifespan."""
    # Initialize on startup
    logger.info("Starting database lifespan context")
    db = None
    try:
        logger.info("Initializing database connection")
        db = await Database().connect()
        context = DbContext(db=db)
        logger.info("Database context created successfully")
        yield context
    except Exception as e:
        logger.error(f"Error in database lifespan: {e}")
        raise
    finally:
        # Cleanup on shutdown
        logger.info("Cleaning up database connection")
        if db is not None:
            await db.disconnect()
        logger.info("Database lifespan context ended")

# Initialize FastMCP server
logger.info("Initializing FastMCP server: Gopher Grades")
app = FastMCP("Gopher Grades", lifespan=db_lifespan)

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
async def search_courses(ctx: Context, query: str, campus: str = "UMNTC", limit: int = 20) -> Dict[str, Any]:
    """
    Search for courses by department code, course number, or description
    
    Args:
        query: Search term (e.g., "CSCI", "5511", "Machine Learning")
        campus: Campus code (default: UMNTC for Twin Cities)
        limit: Maximum number of results to return
    
    Returns:
        Dictionary with information of matching courses
    """
    # Access the database from lifespan context
    db_context = ctx.request_context.lifespan_context

    search_pattern = f"%{query.replace(' ', '')}%"
    query_str = """
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
        """
    query_params = (campus, search_pattern, search_pattern, search_pattern, limit)

    courses = [row_to_dict(row) for row in await db_context.db.query(query_str, query_params, context=db_context)]
    return {
        "count": len(courses),
            "courses": courses
        }

@app.tool()
async def get_course_details(ctx: Context, dept_abbr: str, course_num: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get detailed information about a specific course including grade distributions
    
    Args:
        dept_abbr: Department abbreviation (e.g., "CSCI")
        course_num: Course number (e.g., "5511")
        campus: Campus code (default: UMNTC)
    
    Returns:
        Detailed course information including distributions by professor
    """
    db_context = ctx.request_context.lifespan_context
        
    # Get course info
    course_row = await db_context.db.query("""
            SELECT * FROM classdistribution
            WHERE campus = ? AND dept_abbr = ? AND course_num = ?
        """, (campus, dept_abbr.upper(), course_num), type="one", context=db_context)

    if not course_row:
        return {"error": "Course not found"}

    course = row_to_dict(course_row)

    # Get distributions by professor
    query_str = """
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
        """
    query_params = (course['id'],)

    distributions = [row_to_dict(row) for row in await db_context.db.query(query_str, query_params, context=db_context)]

    # Get associated liberal education requirements
    libed_query_str = """
        SELECT l.name
        FROM libedAssociationTable lat
        JOIN libed l ON lat.left_id = l.id
        WHERE lat.right_id = ?
    """
    libed_query_params = (course['id'],)
    libeds = [row['name'] for row in await db_context.db.query(libed_query_str, libed_query_params, context=db_context)]
    
    course['libeds'] = libeds
    course['distributions'] = distributions
    
    return course

@app.tool()
async def search_professors(ctx: Context, query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Search for professors by name to get their database IDs and RMP scores
    
    Args:
        query: Professor name or partial name
        limit: Maximum number of results
    
    Returns:
        List of matching professors with their database IDs, names, and RMP scores
    """
    db_context = ctx.request_context.lifespan_context

    search_pattern = f"%{query.replace(' ', '')}%"
    query_str = """
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
        """
    query_params = (search_pattern, limit)

    return [row_to_dict(row) for row in await db_context.db.query(query_str, query_params, context=db_context)]


@app.tool()
async def get_professor_courses(ctx: Context, professor_id: int) -> Dict[str, Any]:
    """
    Get all courses taught by a specific professor
    
    Args:
        professor_id: The professor's database ID
    
    Returns:
        Professor information and list of courses taught
    """
    db_context = ctx.request_context.lifespan_context

    # Get professor info
    prof_row = await db_context.db.query("""
        SELECT * FROM professor WHERE id = ?
    """, (professor_id,), type="one", context=db_context)

    if not prof_row:
        return {"error": "Professor not found"}

    professor = row_to_dict(prof_row)

    # Get courses taught
    courses_query_str = """
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
    """
    courses_query_params = (professor_id,)

    professor['courses'] = [row_to_dict(row) for row in await db_context.db.query(courses_query_str, courses_query_params, context=db_context)]

    return professor

@app.tool()
async def get_query_logs(ctx: Context) -> Dict[str, Any]:
    """
    Get the current query logs from the database context
    
    Returns:
        Dictionary containing query history and last query details
    """
    db_context = ctx.request_context.lifespan_context
    
    return {
        "last_query": db_context.last_query,
        "last_result": db_context.last_result,
        "query_history": db_context.query_history[-10:] if db_context.query_history else [],  # Last 10 queries
        "total_queries": len(db_context.query_history) if db_context.query_history else 0
    }

@app.tool()
async def get_department_courses(ctx: Context, dept_abbr: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get all courses in a department
    
    Args:
        dept_abbr: Department abbreviation (e.g., "CSCI")
        campus: Campus code (default: UMNTC)
    
    Returns:
        Department information and list of courses
    """
    db_context = ctx.request_context.lifespan_context

    # Get department info
    dept_row = await db_context.db.query(
        """
        SELECT * FROM departmentdistribution
        WHERE campus = ? AND dept_abbr = ?
        """,
        (campus, dept_abbr.upper()),
        type="one",
        context=db_context
    )

    if not dept_row:
        return {"error": "Department not found"}

    department = row_to_dict(dept_row)

    # Get courses in department
    courses = [
        row_to_dict(row)
        for row in await db_context.db.query(
            """
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
            """,
            (campus, dept_abbr.upper()),
            context=db_context
        )
    ]

    department['courses'] = courses

    return department


@app.tool()
async def get_grade_trends(ctx: Context, dept_abbr: str, course_num: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get grade distribution trends over time for a course
    
    Args:
        dept_abbr: Department abbreviation
        course_num: Course number
        campus: Campus code
    
    Returns:
        Grade trends by term
    """
    db_context = ctx.request_context.lifespan_context

    # Get course ID
    course_row = await db_context.db.query(
        """
        SELECT id, class_desc FROM classdistribution
        WHERE campus = ? AND dept_abbr = ? AND course_num = ?
        """,
        (campus, dept_abbr.upper(), course_num),
        type="one",
        context=db_context
    )

    if not course_row:
        return {"error": "Course not found"}

    course_id = course_row['id']
    course_desc = course_row['class_desc']

    # Get grade distributions by term
    trends = [
        {
            "term": row['term'],
            "total_students": row['total_students'],
            "combined_grades": row['combined_grades'],
            "professors": row['professors']
        }
        for row in await db_context.db.query(
            """
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
            """,
            (course_id,),
            context=db_context
        )
    ]

    return {
        "course": f"{dept_abbr.upper()} {course_num}",
        "description": course_desc,
        "trends": trends
    }


@app.tool()
async def get_liberal_education_courses(ctx: Context, libed_name: str, campus: str = "UMNTC", limit: int = 50) -> Dict[str, Any]:
    """
    Get courses that fulfill a specific liberal education requirement
    
    Args:
        libed_name: Name of the liberal education requirement
        campus: Campus code
        limit: Maximum number of results
    
    Returns:
        List of courses fulfilling the requirement
    """
    db_context = ctx.request_context.lifespan_context

    # Get libed ID
    libed_row = await db_context.db.query(
        """
        SELECT id FROM libed WHERE name LIKE ?
        """,
        (f"%{libed_name}%",),
        type="one",
        context=db_context
    )

    if not libed_row:
        return {"error": "Liberal education requirement not found"}

    libed_id = libed_row['id']

    # Get courses
    courses = [
        row_to_dict(row)
        for row in await db_context.db.query(
            """
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
            """,
            (libed_id, campus, limit),
            context=db_context
        )
    ]

    return {
        "libed": libed_name,
        "count": len(courses),
        "courses": courses
    }

@app.tool()
async def get_professor_grade_statistics(ctx: Context, professor_id: int) -> Dict[str, Any]:
    """
    Get comprehensive grade statistics for a professor across all courses
    
    Args:
        professor_id: The professor's database ID
    
    Returns:
        Detailed grade statistics including GPA, pass rates, and grade distributions
    """
    db_context = ctx.request_context.lifespan_context

    # Get professor info
    prof_row = await db_context.db.query(
        "SELECT * FROM professor WHERE id = ?",
        (professor_id,),
        type="one",
        context=db_context
    )

    if not prof_row:
        return {"error": "Professor not found"}

    professor = row_to_dict(prof_row)

    # Get all grades given by this professor
    all_grades = {}
    total_students = 0
    courses_taught = set()
    terms_taught = set()
    course_grades = {}

    for row in await db_context.db.query(
        """
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
        """,
        (professor_id,),
        context=db_context
    ):
        grades = parse_json_field(row['grades'])
        total_students += row['students']
        courses_taught.add(f"{row['dept_abbr']} {row['course_num']}")
        terms_taught.add(row['term'])

        for grade, count in grades.items():
            all_grades[grade] = all_grades.get(grade, 0) + count

        course_key = f"{row['dept_abbr']} {row['course_num']}"
        if course_key not in course_grades:
            course_grades[course_key] = {"grades": {}, "students": 0}

        for grade, count in grades.items():
            course_grades[course_key]["grades"][grade] = course_grades[course_key]["grades"].get(grade, 0) + count

        course_grades[course_key]["students"] += row['students']

    # Calculate overall statistics
    overall_stats = calculate_grades_stats(all_grades)

    # Calculate per-course statistics
    course_stats = []
    for course, data in course_grades.items():
        stats = calculate_grades_stats(data["grades"])
        stats["course"] = course
        stats["students"] = data["students"]
        course_stats.append(stats)

    # Sort courses by GPA
    course_stats.sort(key=lambda x: x["gpa"] if x["gpa"] else 0, reverse=True)

    # Calculate grade distribution percentages
    grade_percentages = {}
    if total_students > 0:
        for grade, count in all_grades.items():
            grade_percentages[grade] = round((count / total_students) * 100, 1)

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
async def compare_professor_grades(ctx: Context, professor_ids: List[int]) -> Dict[str, Any]:
    """
    Compare grade statistics between multiple professors
    
    Args:
        professor_ids: List of professor database IDs to compare
    
    Returns:
        Comparative statistics for the professors
    """
    db_context = ctx.request_context.lifespan_context
    
    comparisons = []
    
    for prof_id in professor_ids:
        # Get professor info and grades
        row = await db_context.db.query("""
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
        """, (prof_id,), type="one", context=db_context)
        
        if row:
            # Combine all grades
            combined_grades = {}
            if row['all_grades']:
                for grades_json in re.findall(r'{[^}]*}', row['all_grades']):
                    grades = parse_json_field(grades_json)
                    if grades:
                        for grade, count in grades.items():
                            combined_grades[grade] = combined_grades.get(grade, 0) + count
            
            stats = calculate_grades_stats(combined_grades)
            
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
async def get_course_grade_statistics(ctx: Context, dept_abbr: str, course_num: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get comprehensive grade statistics for a course across all professors and terms
    
    Args:
        dept_abbr: Department abbreviation
        course_num: Course number
        campus: Campus code
    
    Returns:
        Detailed grade statistics including by professor and by term breakdowns
    """
    db_context = ctx.request_context.lifespan_context
    
    # Get course info
    course_row = await db_context.db.query("""
        SELECT * FROM classdistribution
        WHERE campus = ? AND dept_abbr = ? AND course_num = ?
    """, (campus, dept_abbr.upper(), course_num), type="one", context=db_context)
    
    if not course_row:
        return {"error": "Course not found"}
    
    course = row_to_dict(course_row)
    
    # Get grades by professor
    professor_rows = await db_context.db.query("""
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
    """, (course['id'],), context=db_context)
    
    professor_stats = []
    for row in professor_rows:
        # Combine grades from all terms
        combined_grades = {}
        if row['all_grades']:
            for grades_json in re.findall(r'{[^}]*}', row['all_grades']):
                logger.debug(f"Parsing grades JSON: {grades_json}")
                grades = parse_json_field(grades_json)
                logger.debug(f"Parsed grades: {grades}")
                if grades:
                    for grade, count in grades.items():
                        combined_grades[grade] = combined_grades.get(grade, 0) + count
        
        stats = calculate_grades_stats(combined_grades)
        
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
    overall_stats = calculate_grades_stats(course['total_grades'])
    
    # Get grade trends by year
    yearly_rows = await db_context.db.query("""
        SELECT 
            CAST(t.term / 10 AS INTEGER) + 1900 as year,
            GROUP_CONCAT(t.grades) as all_grades,
            SUM(t.students) as total_students
        FROM distribution d
        JOIN termdistribution t ON d.id = t.dist_id
        WHERE d.class_id = ?
        GROUP BY year
        ORDER BY year DESC
    """, (course['id'],), context=db_context)
    
    yearly_trends = []
    for row in yearly_rows:
        combined_grades = {}
        if row['all_grades']:
            for grades_json in re.findall(r'{[^}]*}', row['all_grades']):
                grades = parse_json_field(grades_json)
                if grades:
                    for grade, count in grades.items():
                        combined_grades[grade] = combined_grades.get(grade, 0) + count
        
        stats = calculate_grades_stats(combined_grades)
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
async def find_easy_courses(ctx: Context, dept_abbr: Optional[str] = None, min_gpa: float = 3.5, 
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
    db_context = ctx.request_context.lifespan_context
    
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
    
    rows = await db_context.db.query(query, tuple(params), context=db_context)
    
    courses_with_gpa = []
    for row in rows:
        course = row_to_dict(row)
        stats = calculate_grades_stats(course['total_grades'])
        
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
async def find_challenging_courses(ctx: Context, dept_abbr: Optional[str] = None, max_gpa: float = 2.5,
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
    db_context = ctx.request_context.lifespan_context
    
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
    
    rows = await db_context.db.query(query, tuple(params), context=db_context)
    
    courses_with_gpa = []
    for row in rows:
        course = row_to_dict(row)
        stats = calculate_grades_stats(course['total_grades'])
        
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
async def get_grade_distribution_percentiles(ctx: Context, dept_abbr: str, course_num: str, campus: str = "UMNTC") -> Dict[str, Any]:
    """
    Get percentile breakdown of grades for a course (what percentage got A, B, C, etc.)
    
    Args:
        dept_abbr: Department abbreviation
        course_num: Course number
        campus: Campus code
    
    Returns:
        Percentile breakdown and cumulative percentages
    """
    db_context = ctx.request_context.lifespan_context
    
    row = await db_context.db.query("""
        SELECT class_desc, total_students, total_grades
        FROM classdistribution
        WHERE campus = ? AND dept_abbr = ? AND course_num = ?
    """, (campus, dept_abbr.upper(), course_num), type="one", context=db_context)
    
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
async def find_grade_inflated_courses(ctx: Context, min_a_percentage: float = 50.0, min_students: int = 100,
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
    db_context = ctx.request_context.lifespan_context
    
    rows = await db_context.db.query("""
        SELECT 
            dept_abbr,
            course_num,
            class_desc,
            total_students,
            total_grades
        FROM classdistribution
        WHERE campus = ? AND total_students >= ?
    """, (campus, min_students), context=db_context)
    
    inflated_courses = []
    
    for row in rows:
        course = row_to_dict(row)
        grades = course['total_grades']
        total = sum(grades.values())
        
        if total > 0:
            a_grades = grades.get("A+", 0) + grades.get("A", 0) + grades.get("A-", 0)
            a_percentage = (a_grades / total) * 100
            
            if a_percentage >= min_a_percentage:
                stats = calculate_grades_stats(grades)
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
async def get_professor_course_comparison(ctx: Context, professor_id: int, dept_abbr: str, course_num: str) -> Dict[str, Any]:
    """
    Compare a professor's grading in a specific course to the course average
    
    Args:
        professor_id: Professor's database ID
        dept_abbr: Department abbreviation
        course_num: Course number
    
    Returns:
        Comparison of professor's grading to course average
    """
    db_context = ctx.request_context.lifespan_context
    
    # Get course ID and overall stats
    course_row = await db_context.db.query("""
        SELECT id, class_desc, total_students, total_grades
        FROM classdistribution
        WHERE campus = 'UMNTC' AND dept_abbr = ? AND course_num = ?
    """, (dept_abbr.upper(), course_num), type="one", context=db_context)
    
    if not course_row:
        return {"error": "Course not found"}
    
    course = row_to_dict(course_row)
    overall_stats = calculate_grades_stats(course['total_grades'])
    
    # Get professor's stats for this course
    prof_row = await db_context.db.query("""
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
    """, (course['id'], professor_id), type="one", context=db_context)
    
    if not prof_row:
        return {"error": "Professor has not taught this course"}
    
    # Combine professor's grades
    prof_grades = {}
    if prof_row['all_grades']:
        for grades_json in re.findall(r'{[^}]*}', prof_row['all_grades']):
            grades = parse_json_field(grades_json)
            if grades:
                for grade, count in grades.items():
                    prof_grades[grade] = prof_grades.get(grade, 0) + count
    
    prof_stats = calculate_grades_stats(prof_grades)
    
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

@app.tool()
async def get_database_stats(ctx: Context) -> Dict[str, Any]:
    """
    Get overall statistics about the database 

    Returns:
        Database statistics including counts of courses, professors, etc.
    """
    db_context = ctx.request_context.lifespan_context

    stats = {}

    # Get counts of courses
    stats['total_courses'] = await db_context.db.query(
        "SELECT COUNT(*) as count FROM classdistribution",
        type="one",
        context=db_context
    )['count']

    # Get counts of professors
    stats['total_professors'] = await db_context.db.query(
        "SELECT COUNT(*) as count FROM professor",
        type="one",
        context=db_context
    )['count']

    # Get counts of departments
    stats['total_departments'] = await db_context.db.query(
        "SELECT COUNT(*) as count FROM departmentdistribution",
        type="one",
        context=db_context
    )['count']

    return stats


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


if __name__ == "__main__":
    app.run()