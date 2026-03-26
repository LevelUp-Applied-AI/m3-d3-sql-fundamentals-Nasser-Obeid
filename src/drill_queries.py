import sqlite3


def top_departments(db_path):
    """
    Returns the top 3 departments by total salary expenditure.

    Returns:
        List of tuples [(dept_name, total_salary), ...] sorted descending by total salary.
    """
    query = """
        SELECT
            d.name        AS dept_name,
            SUM(e.salary) AS total_salary
        FROM employees e
        JOIN departments d ON e.dept_id = d.dept_id
        GROUP BY d.dept_id, d.name
        ORDER BY total_salary DESC
        LIMIT 3
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(query)
        return cursor.fetchall()


def employees_with_projects(db_path):
    """
    Returns all employees assigned to at least one project.

    Traverses: employees → project_assignments → projects

    Returns:
        List of tuples [(employee_name, project_name), ...]
    """
    query = """
        SELECT
            e.name AS employee_name,
            p.name AS project_name
        FROM employees e
        INNER JOIN project_assignments pa ON e.emp_id = pa.emp_id
        INNER JOIN projects p             ON pa.project_id = p.project_id
        ORDER BY e.name, p.name
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(query)
        return cursor.fetchall()


def salary_rank_by_department(db_path):
    """
    Ranks employees by salary within each department using a window function.

    Rank is computed with RANK() OVER (PARTITION BY department_id ORDER BY salary DESC),
    so tied salaries receive the same rank and the next rank is skipped accordingly.

    Returns:
        List of tuples [(employee_name, dept_name, salary, rank), ...]
        ordered by department name, then rank.
    """
    query = """
        SELECT
            e.name AS employee_name,
            d.name AS dept_name,
            e.salary,
            RANK() OVER (
                PARTITION BY e.dept_id
                ORDER BY e.salary DESC
            ) AS salary_rank
        FROM employees e
        JOIN departments d ON e.dept_id = d.dept_id
        ORDER BY dept_name, salary_rank
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(query)
        return cursor.fetchall()