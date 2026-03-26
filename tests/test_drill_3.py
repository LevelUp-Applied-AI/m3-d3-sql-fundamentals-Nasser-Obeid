"""Autograder tests for Module 3 Core Skills Drill."""
import sqlite3
from pathlib import Path


def test_drill_queries_file_exists():
    assert Path("drill_queries.py").exists(), "drill_queries.py not found"


def test_top_departments():
    from src.drill_queries import top_departments
    result = top_departments("drill.db")
    assert isinstance(result, list), "top_departments should return a list"
    assert len(result) == 3, f"Expected 3 departments, got {len(result)}"
    assert len(result[0]) == 2, "Each tuple should have (dept_name, total_salary)"
    # Results should be sorted descending by total salary
    assert result[0][1] >= result[1][1], "Results not sorted descending by total salary"
    assert result[1][1] >= result[2][1], "Results not sorted descending by total salary"


def test_employees_with_projects():
    from src.drill_queries import employees_with_projects
    result = employees_with_projects("drill.db")
    assert isinstance(result, list), "employees_with_projects should return a list"
    assert len(result) > 0, "Expected at least one employee-project pair"
    assert len(result[0]) == 2, "Each tuple should have (employee_name, project_name)"
    # All names should be non-empty strings
    for emp_name, proj_name in result:
        assert emp_name and proj_name, "Names should not be empty"


def test_salary_rank_by_department():
    from src.drill_queries import salary_rank_by_department
    result = salary_rank_by_department("drill.db")
    assert isinstance(result, list), "salary_rank_by_department should return a list"
    assert len(result) > 0, "Expected at least one row"
    assert len(result[0]) == 4, "Each tuple should have (name, dept, salary, rank)"
    # Check that rank 1 exists
    ranks = [r[3] for r in result]
    assert 1 in ranks, "Expected rank 1 to appear in results"
