This Python script is designed to help gradually synchronize model definitions in Tortoise ORM with the actual tables in a PostgreSQL database that may have diverged over time. The key components and their functions are as follows:

1.	Schema Inspection Class:
	•	InspectedSchema: A class that processes the database schema structure and provides methods to retrieve table and column information.
2.	Database Inspection Function:
	•	inspect_db(dsn): Uses `tbls` utility to inspect the database schema and returns an InspectedSchema instance.
3.	Schema Comparison Class:
	•	Comparator: A class that compares two database schemas (expected and actual) and prints differences in tables, columns, types, comments, indexes, and constraints.

This is a very niche tool that solves a rather rare problem.
