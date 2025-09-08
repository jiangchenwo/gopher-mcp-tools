"""
Gopher Grades
Provides tools for accessing the GopherGrades SQLite database
"""

import os
import logging
from typing import List, Dict, Any, Literal, Set
import json
from dataclasses import dataclass
import re # string manipulation

from utils import parse_json_field, row_to_dict, calculate_grades_stats, term_to_name, get_prefixes_for_level

from fastmcp import FastMCP, Context

# Async operations
import aiosqlite
import aiofiles
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator



# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



# Define Lifespan and Database classes
class Database:
    """Database class with async support using aiosqlite."""

    def __init__(self):
        TOOL_DIR = os.path.dirname(os.path.abspath(__file__))
        self.DB_PATH = os.path.join(os.path.dirname(TOOL_DIR), "data/gopherGrades.db")
        logger.info(f"Database path set to: {self.DB_PATH}")
        self.conn = None

    async def connect(self) -> "Database":
        """Connect to database using aiosqlite."""
        try:
            logger.info(f"Connecting to database at {self.DB_PATH}")
            self.conn = await aiosqlite.connect(self.DB_PATH)
            self.conn.row_factory = aiosqlite.Row 
            logger.info("Database connection established successfully")
            return self
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from database."""
        try:
            if self.conn:
                logger.info("Disconnecting from database")
                await self.conn.close()
                logger.info("Database disconnected successfully")
        except Exception as e:
            logger.error(f"Error disconnecting from database: {e}")

    async def query(self, query: str, params: tuple | None = None, type: str = "all", context: 'DbContext' = None):
        """Execute a query using aiosqlite."""
        try:
            # Log query details if context is provided
            if context:
                formatted_query = query
                if params:
                    formatted_query = f"{query} -- params: {params}"
                context.last_query = formatted_query
                context.query_history.append(formatted_query)
                logger.info(f"Executing query: {formatted_query}")
            
            # Execute query with aiosqlite
            if params:
                cursor = await self.conn.execute(query, params)
            else:
                cursor = await self.conn.execute(query)
            
            if type == "one":
                result = await cursor.fetchone()
            elif type == "all":
                result = await cursor.fetchall()
            else:
                raise ValueError("Invalid query type specified.")
            
            await cursor.close()
            
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
    last_query: str = ""
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



# Define tools
@app.tool()
async def search_courses(
    ctx: Context, 
    search_term: str = "", 
    campus: str = "UMNTC", 
    limit: int = 20,
    dept_abbr: str = "",
    course_num: str = "",
    level: Set[Literal[1,2,3,4,5,6,7,8,9]] | Set[Literal["undergraduate", "master", "doctoral"]] | None = None,
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
        - Filter by course level: 
            - level={"undergraduate", "master"} for both undergraduate and master level courses
            - level={3, 4} for 3000-4999 level courses

    Args:
        search_term: A general search term to match department codes, course numbers, or descriptions (e.g., "CSCI", "5511", "Machine Learning").
        campus: The campus code to filter courses (default: UMNTC for Twin Cities).
        limit: The maximum number of results to return (default: 20).
        dept_abbr: An optional filter for department abbreviation (e.g., "CSCI").
        course_num: An optional filter for course number (e.g., "5511").
        course_level: An optional filter for course level.
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
            student ratings containing Deep Understanding, Stimulated Interest, Technical Effectiveness, Activities Supported Learning, Effort Reasonable, Grading Standards, Recommend, Number of Responses,
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
    if level:
        # See what type of level input we have
        if all(isinstance(lv, int) for lv in level):
            # Convert numeric levels to corresponding prefixes
            prefixes = [str(lv) for lv in level if 1 <= lv <= 9]
        else:
            # Assume string levels
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

@app.tool(name="get_course_grades_of_each_professor_and_term")
async def get_course_details(
    ctx: Context, 
    dept_abbr: str, 
    course_num: str, 
    campus: str = "UMNTC"
) -> Dict[str, Any]:
    """
    Get detailed information about a specific course including grade distribution and statistics of each professor and term and associated liberal education requirements.
    
    Args:
        dept_abbr: Department abbreviation (e.g., "CSCI")
        course_num: Course number (e.g., "5511")
        campus: Campus code (default: UMNTC)
    
    Returns:
        Detailed course information including 
            course details,
            grade distributions and corresponding statistics by professor and term,
            rate my professor score, difficulty rating, and link of each professor,
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
            p.RMP_score as rate_my_professor_score,
            p.RMP_diff as rate_my_professor_difficulty_ratings,
            p.RMP_link as rate_my_professor_link,
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

    for dist in course['distributions']:
        # Calculate grade statistics for each distribution
        grades = dist['grades']
        if grades:
            dist['grades_stats'] = calculate_grades_stats(grades)
        # Rename term field to academic term
        dist['term'] = term_to_name(dist['term'])


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
    professor_name: str = "",
    professor_id: int | None = None,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Search for professors by name or database ID to get their IDs, Rate My Professor (RMP) scores, difficulty ratings, and links, list of courses taught.
    
    Args:
        professor_name: Professor name or partial name
        professor_id: Professor database ID (if known)
        limit: Maximum number of results
    
    Returns:
        List of matching professors with their 
            database IDs, 
            names, 
            Rate My Professor scores, difficulty ratings, and links,
            list of courses taught (including department abbreviation, course number, course name, and all terms taught)
    """
    db_context = ctx.request_context.lifespan_context

    # Build dynamic query conditions
    conditions = []
    query_params = []

    if professor_id:
        conditions.append("p.id = ?")
        query_params.append(professor_id)

    if professor_name:
        search_pattern = f"%{professor_name.replace(' ', '')}%"
        conditions.append("REPLACE(p.name, ' ', '') LIKE ?")
        query_params.append(search_pattern)

    if not conditions:
        return {"error": "Please provide either professor_name or professor_id."}
    
    where_clause = " AND ".join(conditions)
    query_str = f"""
        SELECT
            p.id as professor_id,
            p.name as professor_name,
            p.RMP_score as rate_my_professor_score,
            p.RMP_diff as rate_my_professor_difficulty_ratings,
            p.RMP_link as rate_my_professor_link,
            p.x500
        FROM professor p
        WHERE {where_clause}
        GROUP BY p.id
        ORDER BY p.RMP_score DESC NULLS LAST
        LIMIT ?
    """
    query_params.append(limit)

    # Execute query to get professors
    prof_rows = await db_context.db.query(query_str,query_params, context=db_context)
    professors = [row_to_dict(row) for row in prof_rows]

    # Get list of courses taught by each professor
    # For each course, include department abbreviation, course number, course name, and all terms taught
    for prof in professors:
        courses_query_str = """
            SELECT 
                c.dept_abbr,
                c.course_num,
                c.class_desc as course_name,
                GROUP_CONCAT(DISTINCT t.term) as terms_taught
            FROM distribution d
            JOIN classdistribution c ON d.class_id = c.id
            JOIN termdistribution t ON d.id = t.dist_id
            WHERE d.professor_id = ? AND c.campus = 'UMNTC'
            GROUP BY c.id
            ORDER BY c.dept_abbr, c.course_num
        """
        courses_query_params = (prof['professor_id'],)
        courses = [row_to_dict(row) for row in await db_context.db.query(courses_query_str, courses_query_params, context=db_context)]

        for course in courses:
            # Convert term numbers to academic term names
            if course['terms_taught']:
                term_numbers = course['terms_taught'].split(',')
                term_names = [term_to_name(int(tn)) for tn in term_numbers if tn.isdigit()]
                course['terms_taught'] = term_names
            else:
                course['terms_taught'] = []
        
        prof['courses_taught'] = courses
    
    return professors

@app.tool(name="get_courses_grade_statistics_of_professor")
async def get_professor_details(
    ctx: Context, 
    professor_id: int
) -> Dict[str, Any]:
    """
    Get professor details, overall grade statistics of all the courses taught by the professor, and individual grade statistics per course per term.
    
    Args:
        professor_id: The professor's database ID
    
    Returns:
        A dictionary containing
            "professor" as key for professor details including database ID, name, Rate My Professor score, difficulty rating, and link,
            "overall_statistics" as key for overall statistics including total number of students taught, number of unique courses taught, overall grade distribution, and overall grade statistics,
            "details_per_course" as key for a list of courses taught with their respective course details, total number of students, aggregated grade distribution, overall course grade statistics, and per-term grade statistics with term details.
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
    all_grades_professor = {}
    total_students = 0
    per_course = {}

    for row in await db_context.db.query(
        """
        SELECT 
            c.dept_abbr,
            c.course_num,
            c.class_desc as course_name,
            c.onestop as onestop_link,
            c.onestop_desc as course_description,
            c.cred_min,
            c.cred_max,
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
        course_key = f"{row['dept_abbr']} {row['course_num']}"
        term_key = term_to_name(row['term'])
        grades = parse_json_field(row['grades']) or {}

        if course_key not in per_course:
            per_course[course_key] = {
                "dept_abbr": row['dept_abbr'],
                "course_num": row['course_num'],
                "course_name": row['course_name'],
                "onestop_link": row['onestop_link'],
                "course_description": row['course_description'],
                "cred_min": row['cred_min'],
                "cred_max": row['cred_max'],
                "all_grades_course": {},
                "students": 0,
                "grades_per_term": {}
            }
        if term_key not in per_course[course_key]:
            per_course[course_key]['grades_per_term'][term_key] = {
                "students": row['students'],
                "grades": grades,
                "stats": calculate_grades_stats(grades)
            }

        total_students += row['students']
        per_course[course_key]['students'] += row['students']
        for grade, count in grades.items():
            all_grades_professor[grade] = all_grades_professor.get(grade, 0) + count
            per_course[course_key]['all_grades_course'][grade] = per_course[course_key]['all_grades_course'].get(grade, 0) + count

    # Calculate overall statistics
    overall_stats = calculate_grades_stats(all_grades_professor)
    overall_stats.update({
        "unique_courses": len(per_course),
        "overall_grade_distribution": all_grades_professor
    })

    # Calculate per-course and per-course-term statistics
    for course_key, data in per_course.items():
        data['course_grade_stats'] = calculate_grades_stats(data['all_grades_course'])
        for term, term_data in data['grades_per_term'].items():
            term_data['stats'] = calculate_grades_stats(term_data['grades'])

    return {
        "professor": professor,
        "overall_statistics": overall_stats,
        "details_per_course": per_course,
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
async def get_grades_stats(grades: Dict[str, int]) -> Dict[str, Any]:
    """
    Calculate comprehensive GPA and grade statistics from a grade distribution

    Args:
        grades: Dictionary mapping letter grades to student counts

    Returns:
        Comprehensive grade statistics including GPA, pass rates, withdrawal rate, and grade breakdowns
    """
    return calculate_grades_stats(grades)

@app.tool(enabled=False) # Not used by llm 
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


if __name__ == "__main__":
    app.run()
