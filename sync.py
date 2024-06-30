from tmodels import *
from tortoise import Tortoise, fields, run_async
from inspect import cleandoc
import subprocess
import json
import re
from typing import List


async def reset():
    sql = cleandoc(
        """
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        GRANT ALL ON SCHEMA public TO postgres;
        GRANT ALL ON SCHEMA public TO public;
        COMMENT ON SCHEMA public IS 'standard public schema';
    """
    )
    await Tortoise.get_connection("default").execute_script(sql)


async def restore(dump_file):
    with open(dump_file, "r") as file:
        sql = file.read()

    await Tortoise.get_connection("default").execute_script(sql)


def run_shell_command_and_get_output(command):
    return (
        subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        .stdout.read()
        .decode("utf-8")
    )


class InspectedSchema:
    def __init__(self, data):
        self.data = data
        self.table_name_to_table = {table["name"]: table for table in data["tables"]}
        self.table_name_to_column_name_to_column = {
            table["name"]: {column["name"]: column for column in table["columns"]}
            for table in data["tables"]
        }

    def table_names(self):
        return [table["name"] for table in self.data["tables"]]

    def has_table(self, table_name):
        return table_name in self.table_name_to_table

    def get_table(self, table_name):
        return self.table_name_to_table.get(table_name)

    def column_names(self, table_name):
        return [
            column["name"] for column in self.get_table(table_name).get("columns", [])
        ]

    def has_column(self, table_name, column_name):
        return column_name in self.table_name_to_column_name_to_column.get(
            table_name, {}
        )

    def get_column(self, table_name, column_name):
        return self.table_name_to_column_name_to_column.get(table_name, {}).get(
            column_name
        )


def inspect_db(dsn) -> InspectedSchema:
    output = run_shell_command_and_get_output(f"tbls out --sort -t json '{dsn}'")
    return InspectedSchema(json.loads(output))


class Comparator:
    def __init__(self, expected: InspectedSchema, actual: InspectedSchema, equivalent_types: List[List[str]] = []):
        self.expected = expected
        self.actual = actual
        self.equivalent_types = equivalent_types
        self.type_and_size_re = re.compile(r"^(?P<type>\w+)\((?P<size>.+)\)$")

    def compare(self):
        table_names = sorted(
            set(self.expected.table_names() + self.actual.table_names())
        )
        for table_name in table_names:
            self.compare_table(table_name)

    def _compare_table_type(self, table_name):
        expected_table_type = self.expected.get_table(table_name).get("type")
        actual_table_type = self.actual.get_table(table_name).get("type")
        if expected_table_type != actual_table_type:
            print(
                f"{table_name}: type mismatch: {expected_table_type} != {actual_table_type}"
            )

    def _compare_table_comment(self, table_name):
        expected = self.expected.get_table(table_name).get("comment")
        actual = self.actual.get_table(table_name).get("comment")
        if expected != actual:
            print(f"{table_name}: comment mismatch: {actual} != {expected}")

    def _compare_table_columns(self, table_name):
        expected_columns = self.expected.column_names(table_name)
        actual_columns = self.actual.column_names(table_name)
        column_names = sorted(set(expected_columns + actual_columns))
        for column_name in column_names:
            self._compare_column(table_name, column_name)

    def _compare_column(self, table_name, column_name):
        expected = self.expected.get_column(table_name, column_name)
        if expected is None:
            print(f"{table_name}.{column_name}: column not expected")
            return

        actual = self.actual.get_column(table_name, column_name)
        if actual is None:
            print(f"{table_name}.{column_name}: column not found")
            return

        self._compare_column_type(table_name, column_name)
        self._compare_column_comment(table_name, column_name)

    def _compare_column_type(self, table_name, column_name):
        expected = self.expected.get_column(table_name, column_name).get("type")
        actual = self.actual.get_column(table_name, column_name).get("type")

        m1 = self.type_and_size_re.fullmatch(expected)
        expected_type = m1['type'] if m1 else expected
        expected_size = m1['size'] if m1 else None

        m2 = self.type_and_size_re.fullmatch(actual)
        actual_type = m2['type'] if m2 else actual
        actual_size = m2['size'] if m2 else None

        for types in self.equivalent_types:
            if expected_type in types and actual_type in types:
                return

        if expected_type != actual_type:
            print(f"{table_name}.{column_name}: type mismatch: {actual} != {expected}")
        elif expected_size != actual_size:
            print(f"{table_name}.{column_name}: type size mismatch: {actual} != {expected}")

    def _normalize_index_definition(self, definition):
        def1_re = re.compile(r"^CREATE(?P<unique> UNIQUE|) INDEX \w+ ON (?P<table>\S+) USING (?P<algorithm>btree|gin) \((?P<columns>.*)\)$")

        if m := def1_re.fullmatch(definition):
            return f"CREATE{m['unique']} INDEX ... ON {m['table']} USING {m['algorithm']} ({m['columns']})"

        raise NotImplementedError(f"Cannot parse index definition: {definition}")

    def _normalize_constraint_definition(self, definition):
        def1_re = re.compile(r"^(?P<main>FOREIGN KEY \(.+\) REFERENCES [^(]+\(.*\))(?P<extra>.+)?$")

        if m := def1_re.fullmatch(definition):
            return (m['main'], m['extra'])

        def2_re = re.compile(r"^(?P<main>PRIMARY KEY \(.+\))$")

        if m := def2_re.fullmatch(definition):
            return (m['main'], None)

        def3_re = re.compile(r"^(?P<main>UNIQUE \(.+\))$")

        if m := def3_re.fullmatch(definition):
            return (m['main'], None)

        raise NotImplementedError(f"Cannot parse constraint definition: {definition}")

    def _compare_table_indexes(self, table_name):
        expected = [
            self._normalize_index_definition(index.get("def"))
            for index in self.expected.get_table(table_name).get("indexes", [])
        ]
        actual = [
            self._normalize_index_definition(index.get("def"))
            for index in self.actual.get_table(table_name).get("indexes", [])
        ]

        all = sorted(set(expected + actual))
        for index in all:
            if index not in expected:
                print(f"{table_name}: index not expected: {index}")
            elif index not in actual:
                print(f"{table_name}: index not found: {index}")

    def _compare_table_constraints(self, table_name):
        expected = dict([
            self._normalize_constraint_definition(constraint.get("def"))
            for constraint in self.expected.get_table(table_name).get("constraints", [])
        ])
        actual = dict([
            self._normalize_constraint_definition(constraint.get("def"))
            for constraint in self.actual.get_table(table_name).get("constraints", [])
        ])

        keys = sorted(set(expected.keys()) | set(actual.keys()))

        for key in keys:
            if key not in expected:
                print(f"{table_name}: constraint not expected: {key}")
            elif key not in actual:
                print(f"{table_name}: constraint not found: {key}")
            elif expected[key] != actual[key]:
                print(f"{table_name}: constraint {key} mismatch: {actual[key]} != {expected[key]}")

    def _compare_column_comment(self, table_name, column_name):
        expected = self.expected.get_column(table_name, column_name).get("comment")
        actual = self.actual.get_column(table_name, column_name).get("comment")
        if expected != actual:
            print(
                f"{table_name}.{column_name}: comment mismatch: {actual} != {expected}"
            )

    def compare_table(self, table_name):
        if not self.expected.has_table(table_name):
            print(f"{table_name}: table not expected")
            return

        if not self.actual.has_table(table_name):
            print(f"{table_name}: table not found")
            return

        self._compare_table_type(table_name)
        self._compare_table_comment(table_name)
        self._compare_table_columns(table_name)
        self._compare_table_indexes(table_name)
        self._compare_table_constraints(table_name)


async def run(dsn):
    await Tortoise.init(
        db_url=dsn,
        modules={"models": ["tmodels"]},
    )

    await reset()
    await restore("dump.sql")
    expected = inspect_db(dsn)

    await reset()
    await Tortoise.generate_schemas()
    actual = inspect_db(dsn)

    Comparator(
        expected,
        actual,
    ).compare()


run_async(run("postgres://postgres@localhost:5432/postgres"))
