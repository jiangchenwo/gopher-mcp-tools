"""
Gopher Grades
Provides tools for accessing the GopherGrades SQLite database
"""

import os
import sqlite3
import logging
from typing import List, Dict, Any, Literal, Set
import json
from dataclasses import dataclass
import re # string manipulation

from fastmcp import FastMCP, Context

import asyncio
import aiofiles
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

async def get_grades_stats(grades: Dict[str, int]) -> Dict[str, Any]:
    """
    Calculate comprehensive GPA and grade statistics from a grade distribution

    Args:
        grades: Dictionary mapping letter grades to student counts

    Returns:
        Comprehensive grade statistics including GPA, pass rates, withdrawal rate, and grade breakdowns
    """
    return calculate_grades_stats(grades)



# Define Lifespan and Database classes
class Database:
    """Database class with async connect/disconnect and query support."""

    def __init__(self):
        TOOL_DIR = os.path.dirname(os.path.abspath(__file__))
        self.DB_PATH = os.path.join(os.path.dirname(TOOL_DIR), "data/gopherGrades.db")
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

    async def query(self, query: str, params: tuple = None, type: str = "all", context: "DbContext" = None):
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
    last_query: str = None
    last_result: Dict[str, Any] = None
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
app = FastMCP(
    "Gopher Grades", 
    lifespan=db_lifespan
)



@app.tool()
async def search_courses(
    ctx: Context, 
    search_term: str = "", 
    campus: str = "UMNTC", 
    limit: int = 20,
    dept_abbr: str = "",
    course_num: str = "",
    level: Set[Literal["undergraduate", "master", "doctoral"]] = {"undergraduate", "master", "doctoral"},
    min_gpa: float = -1,
    max_gpa: float = 5
) -> Dict[str, Any]:
    """
    Search for courses based on various criteria, including department abbreviation, course number, course level, average GPA range (minimum average gpa or maximum average gpa), and a general search term. Get course details and grade statistics.

    Examples:
        - Search by department abbreviation: dept_abbr="CSCI"
        - Search by course number: course_num="5511"
        - Search by general term: search_term="Machine Learning"
        - Filter by average GPA range: min_gpa=3.0, max_gpa=4.0
            - Find easy courses (with high average GPA): min_gpa=3.5
            - Find hard courses (with low average GPA): max_gpa=2.5
        - Filter by course level: level={"undergraduate", "master"}

    Args:
        search_term: A general search term to match department codes, course numbers, or descriptions (e.g., "CSCI", "5511", "Machine Learning").
        campus: The campus code to filter courses (default: UMNTC for Twin Cities).
        limit: The maximum number of results to return (default: 20).
        dept_abbr: An optional filter for department abbreviation (e.g., "CSCI").
        course_num: An optional filter for course number (e.g., "5511").
        course_level: An optional filter for course level (undergraduate, master, doctoral).
        min_gpa: An optional filter for the minimum average GPA of courses.
        max_gpa: An optional filter for the maximum average GPA of courses.

    Returns:
        A dictionary containing the count of matching courses and corresponding course details including 
            department abbrevation, 
            course number, 
            course name, 
            number of total student,
            aggregated grades distribution of all terms and professors,
            onestop link,
            course description,
            credit range,
            student ratings,
            GPA statistics.
    """
    # Access the database from lifespan context
    db_context = ctx.request_context.lifespan_context

    # Build dynamic query conditions
    conditions = ["campus = ?"]
    query_params = [campus]
    
    # Add department filter if provided
    if dept_abbr:
        conditions.append("dept_abbr = ?")
        query_params.append(dept_abbr.upper())
    
    # Add course number filter if provided
    if course_num:
        conditions.append("course_num = ?")
        query_params.append(course_num)

    # Add course level filter if not ALL_LEVELS
    # Filter based on the first digit of course_num
    if level and level != {"undergraduate", "master", "doctoral"}:
        prefixes = get_prefixes_for_level(level)
        if prefixes:
            placeholders = ','.join('?' for _ in prefixes)
            conditions.append(f"SUBSTR(course_num, 1, 1) IN ({placeholders})")
            query_params.extend(prefixes)
    
    # Add search term conditions if provided
    if search_term:
        search_pattern = f"%{search_term.replace(' ', '')}%"
        search_conditions = [
            "dept_abbr || course_num LIKE ?",
            "REPLACE(class_desc, ' ', '') LIKE ?",
            "dept_abbr LIKE ?"
        ]
        conditions.append(f"({' OR '.join(search_conditions)})")
        query_params.extend([search_pattern, search_pattern, search_pattern])
    
    # Build the complete query
    where_clause = " AND ".join(conditions)
    query_str = f"""
        SELECT 
            id,
            campus,
            dept_abbr,
            course_num,
            class_desc as course_name,
            total_students,
            total_grades,
            onestop as onestop_link,
            onestop_desc as course_description,
            cred_min,
            cred_max,
            srt_vals as student_ratings
        FROM classdistribution
        WHERE {where_clause}
        ORDER BY total_students DESC
    """

    # Execute query to get courses
    course_rows = await db_context.db.query(query_str, query_params, context=db_context)
    courses = [row_to_dict(row) for row in course_rows]

    # Calculate GPA statistics
    for course in courses:
        # Calculate GPA for the course
        grades = course['total_grades']
        if grades:
            stats = calculate_grades_stats(grades)
            course['grades_stats'] = stats
    
    # Filter by GPA range if specified
    if min_gpa > 0 or max_gpa < 5:
        filtered_courses = []
        for course in courses:
            grades = course['total_grades']
            if grades:
                stats = course['grades_stats']
                gpa = stats['average_gpa']
                # Apply GPA filters
                if gpa is not None:
                    if min_gpa > 0 and gpa < min_gpa:
                        continue
                    if max_gpa < 5 and gpa > max_gpa:
                        continue
                
            filtered_courses.append(course)
        
        courses = filtered_courses
    
    # Apply limit after filtering
    courses = courses[:limit] if limit else courses
    
    return {
        "count": len(courses),
        "courses": courses
    }

@app.tool(name="get_course_grades_by_professor_and_term")
async def get_course_details(
    ctx: Context, 
    dept_abbr: str, 
    course_num: str, 
    campus: str = "UMNTC"
) -> Dict[str, Any]:
    """
    Get detailed information about a specific course including grade distributions by professor and term and associated liberal education requirements.
    
    Args:
        dept_abbr: Department abbreviation (e.g., "CSCI")
        course_num: Course number (e.g., "5511")
        campus: Campus code (default: UMNTC)
    
    Returns:
        Detailed course information including 
            course details,
            grade distributions and corresponding statistics by professor and term,
            Rate My Professor (RMP) score, difficulty rating, and link of each professor,
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

    # Get distributions by professor and term
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

    course['distributions'] = distributions

    # Calculate grade statistics for each distribution
    for dist in course['distributions']:
        grades = dist['grades']
        if grades:
            dist['grades_stats'] = calculate_grades_stats(grades)

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
    
    
    return course

@app.tool()
async def search_professors(
    ctx: Context, 
    query: str, 
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Search for professors by name to get their database IDs and RMP scores
    
    Args:
        query: Professor name or partial name
        limit: Maximum number of results
    
    Returns:
        List of matching professors with their 
            database IDs, 
            names, 
            Rate My Professor (RMP) scores, difficulty ratings, and links,
            number of courses taught.
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
async def get_professor_courses_grades(
    ctx: Context, 
    professor_id: int
) -> Dict[str, Any]:
    """
    Get all courses taught by a specific professor and corresponding details about each course including grade distributions and statistics by term.
    
    Args:
        professor_id: The professor's database ID
    
    Returns:
        Professor information and list of courses taught and their details
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

    # Calculate grade statistics for each course
    for course in professor['courses']:
        grades = course['grades']
        if grades:
            course['grades_stats'] = calculate_grades_stats(grades)

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
async def get_liberal_education_courses(
    ctx: Context, 
    libed_name: str, 
    campus: str = "UMNTC", 
    limit: int = 50
) -> Dict[str, Any]:
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
async def get_professor_grade_statistics(
    ctx: Context, 
    professor_id: int
) -> Dict[str, Any]:
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
    course_stats.sort(key=lambda x: x["average_gpa"] if x["average_gpa"] else 0, reverse=True)

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
            "overall_gpa": overall_stats["average_gpa"],
            "overall_pass_rate": overall_stats["pass_rate"],
            "grade_distribution": all_grades,
            "grade_percentages": grade_percentages
        },
        "by_course": course_stats,
        "easiest_course": course_stats[0] if course_stats else None,
        "hardest_course": course_stats[-1] if course_stats else None
    }

# @app.tool()
# async def compare_professor_grades(
#     ctx: Context, 
#     professor_ids: List[int]
# ) -> Dict[str, Any]:
#     """
#     Compare grade statistics between multiple professors
    
#     Args:
#         professor_ids: List of professor database IDs to compare
    
#     Returns:
#         Comparative statistics for the professors
#     """
#     db_context = ctx.request_context.lifespan_context
    
#     comparisons = []
    
#     for prof_id in professor_ids:
#         # Get professor info and grades
#         row = await db_context.db.query("""
#             SELECT 
#                 p.id,
#                 p.name,
#                 p.RMP_score,
#                 p.RMP_diff,
#                 GROUP_CONCAT(t.grades) as all_grades,
#                 SUM(t.students) as total_students,
#                 COUNT(DISTINCT d.class_id) as num_courses
#             FROM professor p
#             LEFT JOIN distribution d ON p.id = d.professor_id
#             LEFT JOIN termdistribution t ON d.id = t.dist_id
#             WHERE p.id = ?
#             GROUP BY p.id
#         """, (prof_id,), type="one", context=db_context)
        
#         if row:
#             # Combine all grades
#             combined_grades = {}
#             if row['all_grades']:
#                 for grades_json in re.findall(r'{[^}]*}', row['all_grades']):
#                     grades = parse_json_field(grades_json)
#                     if grades:
#                         for grade, count in grades.items():
#                             combined_grades[grade] = combined_grades.get(grade, 0) + count
            
#             stats = calculate_grades_stats(combined_grades)
            
#             comparisons.append({
#                 "id": row['id'],
#                 "name": row['name'],
#                 "rmp_score": row['RMP_score'],
#                 "rmp_difficulty": row['RMP_diff'],
#                 "total_students": row['total_students'],
#                 "num_courses": row['num_courses'],
#                 "average_gpa": stats['gpa'],
#                 "pass_rate": stats['pass_rate'],
#                 "grade_distribution": combined_grades
#             })
    
#     # Sort by GPA
#     comparisons.sort(key=lambda x: x['average_gpa'] if x['average_gpa'] else 0, reverse=True)
    
#     return {
#         "comparison": comparisons,
#         "easiest_grader": comparisons[0] if comparisons else None,
#         "hardest_grader": comparisons[-1] if comparisons else None
#     }



# @app.tool()
# async def get_professor_course_comparison(
#     ctx: Context, 
#     professor_id: int, 
#     dept_abbr: str, 
#     course_num: str
# ) -> Dict[str, Any]:
#     """
#     Compare a professor's grading in a specific course to the course average
    
#     Args:
#         professor_id: Professor's database ID
#         dept_abbr: Department abbreviation
#         course_num: Course number
    
#     Returns:
#         Comparison of professor's grading to course average
#     """
#     db_context = ctx.request_context.lifespan_context
    
#     # Get course ID and overall stats
#     course_row = await db_context.db.query("""
#         SELECT id, class_desc, total_students, total_grades
#         FROM classdistribution
#         WHERE campus = 'UMNTC' AND dept_abbr = ? AND course_num = ?
#     """, (dept_abbr.upper(), course_num), type="one", context=db_context)
    
#     if not course_row:
#         return {"error": "Course not found"}
    
#     course = row_to_dict(course_row)
#     overall_stats = calculate_grades_stats(course['total_grades'])
    
#     # Get professor's stats for this course
#     prof_row = await db_context.db.query("""
#         SELECT 
#             p.name,
#             p.RMP_score,
#             GROUP_CONCAT(t.grades) as all_grades,
#             SUM(t.students) as total_students
#         FROM distribution d
#         JOIN professor p ON d.professor_id = p.id
#         JOIN termdistribution t ON d.id = t.dist_id
#         WHERE d.class_id = ? AND p.id = ?
#         GROUP BY p.id
#     """, (course['id'], professor_id), type="one", context=db_context)
    
#     if not prof_row:
#         return {"error": "Professor has not taught this course"}
    
#     # Combine professor's grades
#     prof_grades = {}
#     if prof_row['all_grades']:
#         for grades_json in re.findall(r'{[^}]*}', prof_row['all_grades']):
#             grades = parse_json_field(grades_json)
#             if grades:
#                 for grade, count in grades.items():
#                     prof_grades[grade] = prof_grades.get(grade, 0) + count
    
#     prof_stats = calculate_grades_stats(prof_grades)
    
#     # Calculate differences
#     gpa_difference = (prof_stats['gpa'] - overall_stats['gpa']) if prof_stats['gpa'] and overall_stats['gpa'] else None
#     pass_rate_difference = (prof_stats['pass_rate'] - overall_stats['pass_rate']) if prof_stats['pass_rate'] and overall_stats['pass_rate'] else None
    
#     return {
#         "course": f"{dept_abbr.upper()} {course_num}",
#         "description": course['class_desc'],
#         "professor": {
#             "name": prof_row['name'],
#             "rmp_score": prof_row['RMP_score'],
#             "students_taught": prof_row['total_students'],
#             "average_gpa": prof_stats['gpa'],
#             "pass_rate": prof_stats['pass_rate'],
#             "grade_distribution": prof_grades
#         },
#         "course_average": {
#             "total_students": course['total_students'],
#             "average_gpa": overall_stats['gpa'],
#             "pass_rate": overall_stats['pass_rate'],
#             "grade_distribution": course['total_grades']
#         },
#         "comparison": {
#             "gpa_difference": round(gpa_difference, 3) if gpa_difference else None,
#             "pass_rate_difference": round(pass_rate_difference, 2) if pass_rate_difference else None,
#             "easier_than_average": gpa_difference > 0 if gpa_difference else None
#         }
#     }

# @app.tool()
# async def get_database_stats(ctx: Context) -> Dict[str, Any]:
#     """
#     Get overall statistics about the database 

#     Returns:
#         Database statistics including counts of courses, professors, etc.
#     """
#     db_context = ctx.request_context.lifespan_context

#     stats = {}

#     # Get counts of courses
#     stats['total_courses'] = await db_context.db.query(
#         "SELECT COUNT(*) as count FROM classdistribution",
#         type="one",
#         context=db_context
#     )['count']

#     # Get counts of professors
#     stats['total_professors'] = await db_context.db.query(
#         "SELECT COUNT(*) as count FROM professor",
#         type="one",
#         context=db_context
#     )['count']

#     # Get counts of departments
#     stats['total_departments'] = await db_context.db.query(
#         "SELECT COUNT(*) as count FROM departmentdistribution",
#         type="one",
#         context=db_context
#     )['count']

#     return stats

@app.resource("info://abbreviations-and-terms")
async def resource_abbreviations_and_terms(ctx: Context) -> Dict[str, Any]:
    """Get abbreviations, department code and academic terms"""
    
    # load data from JSON files
    FILE_DIR =  os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(os.path.dirname(FILE_DIR), "data/abbreviationsAndTerms.json")
    async with aiofiles.open(file_path, mode='r') as f:
        content = await f.read()
        data = json.loads(content)
        return data
    
@app.tool(enabled=False)
async def get_abbreviations_and_terms(ctx: Context) -> Dict[str, Any]:
    """Get abbreviations, department code and academic terms"""
    
    # load data from JSON files
    FILE_DIR =  os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(os.path.dirname(FILE_DIR), "data/abbreviationsAndTerms.json")
    async with aiofiles.open(file_path, mode='r') as f:
        content = await f.read()
        data = json.loads(content)
        return data
    
@app.tool(enabled=False) # Disabled to reduce tool list clutter



if __name__ == "__main__":
    app.run()
