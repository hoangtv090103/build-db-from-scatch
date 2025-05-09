from typing import List
from dbms.buffer.buffer_pool_manager import BufferPoolManager
from dbms.catalog.catalog import Catalog
from dbms.parser.sql_parser import CommandType, ParsedCommand


class ExecutionEngine:
    """
    The ExecutionEngine is responsible for executing parsed SQL commands (CREATE TABLE, INSERT, SELECT)
    by interacting with the catalog, buffer pool, and operator tree. It handles command dispatch,
    serialization, predicate construction, and projection logic.
    """

    def __init__(self, catalog: Catalog, buffer_pool_manager: BufferPoolManager):
        """
        Initialize the ExecutionEngine.

        Args:
            catalog (Catalog): The database catalog containing metadata.
            buffer_pool_manager (BufferPoolManager): The buffer pool manager for page management.
        """
        self.catalog = catalog
        self.buffer_pool_manager = buffer_pool_manager

    def execute(self, parsed_command: ParsedCommand) -> List[tuple] | str:
        """
        Execute a parsed SQL command.

        Args:
            parsed_command (ParsedCommand): The parsed SQL command object.

        Returns:
            List[tuple]: Query results for SELECT.
            str: Status message for CREATE TABLE and INSERT.
        """
        cmd = parsed_command
        if cmd.command_type == CommandType.CREATE_TABLE:
            table_meta = self.catalog.create_table(cmd.table_name, cmd.columns)
            if table_meta is None:
                return f"Failed to create table '{cmd.table_name}'"

            return f"Table '{cmd.table_name} created successfully'"

        elif cmd.command_type == CommandType.INSERT:
            table_meta = self.catalog.get_table_metadata(cmd.table_name)

            if table_meta is None:
                return f"Error: Table '{cmd.table_name}' not found."

            table_heap = self.catalog.get_table_heap(cmd.table_name)
            if table_heap is None:
                return f"Error: Could not get TableHeap for '{cmd.table_name}'."

            try:
                serialized_record_data = self._serialize_insert_values(
                    cmd.values, table_meta.schema
                )
            except Exception as e:
                return f"Error: Invalid data for table schema. {e}"

            rid = table_heap.insert_record(serialized_record_data)
            if rid is not None:
                return "1 row inserted."
            else:
                return "Failed to insert row."

        elif parsed_command.command_type == CommandType.SELECT:
            from dbms.parser.sql_parser import SelectCommand
            from dbms.execution.operators import (
                SeqScanOperator,
                FilterOperator,
                ProjectionOperator,
            )

            cmd = parsed_command  # Already a SelectCommand

            table_meta = self.catalog.get_table_metadata(cmd.table_name)
            if table_meta is None:
                return f"Error: Table '{cmd.table_name}' not found."

            table_heap = self.catalog.get_table_heap(cmd.table_name)
            if table_heap is None:
                return f"Error: Could not get TableHeap for '{cmd.table_name}'."

            # 1. Build operator tree
            current_operator = SeqScanOperator(table_heap.iterator(), table_meta.schema)

            # 2. Add FilterOperator if WHERE
            if cmd.filter_condition is not None:
                predicate_func = self._build_predicate(
                    cmd.filter_condition, table_meta.schema
                )
                current_operator = FilterOperator(current_operator, predicate_func)

            # 3. Add ProjectionOperator
            column_indices = self._get_projection_indices(
                cmd.select_columns, table_meta.schema
            )
            current_operator = ProjectionOperator(current_operator, column_indices)

            # 4. Execute and collect results
            results = []
            current_operator.open()
            try:
                while True:
                    record = current_operator.next()
                    if record is None:
                        break
                    results.append(record)
            finally:
                current_operator.close()
            return results

        else:
            return "Error: Command type not yet supported by ExecutionEngine."

    def _serialize_insert_values(self, values, schema):
        """
        Serialize a list of values into bytes according to the table schema.

        Args:
            values (list): The values to serialize.
            schema (TableSchema): The schema describing column types.

        Returns:
            bytes: The serialized record.

        Raises:
            ValueError: If value count does not match schema or value too long.
            NotImplementedError: If column type is unsupported.
        """
        import struct

        result = bytearray()
        if len(values) != len(schema.columns):
            raise ValueError("Value count does not match table schema")
        for val, col in zip(values, schema.columns):
            col_type = col.column_type.upper()
            if col_type == "INTEGER":
                result += struct.pack("i", int(val))
            elif col_type == "BOOLEAN":
                result += struct.pack("?", bool(val))
            elif col_type.startswith("VARCHAR"):
                encoded = str(val).encode("utf-8")
                if len(encoded) > int(col_type.split("(")[1].split(")")[0]):
                    raise ValueError(f"Value too long for column {col.column_name}")
                result += struct.pack("H", len(encoded))
                result += encoded
            else:
                raise NotImplementedError(f"Unsupported column type: {col_type}")
        return bytes(result)

    def _build_predicate(self, condition, schema):
        """
        Build a predicate function for filtering records based on a simple condition.

        Args:
            condition (FilterCondition): The filter condition (column, operator, value).
            schema (TableSchema): The table schema.

        Returns:
            Callable[[tuple], bool]: A predicate function for filtering.

        Raises:
            ValueError: If the column is not found in the schema.
            NotImplementedError: If the operator is unsupported.
        """
        col_idx = None
        for idx, col in enumerate(schema.columns):
            if col.column_name == condition.column:
                col_idx = idx
                break
        if col_idx is None:
            raise ValueError(f"Column {condition.column} not found in schema")
        op = condition.operator
        val = condition.value
        if op == "=":
            return lambda record: record[col_idx] == val
        elif op == ">":
            return lambda record: record[col_idx] > val
        elif op == "<":
            return lambda record: record[col_idx] < val
        elif op == ">=":
            return lambda record: record[col_idx] >= val
        elif op == "<=":
            return lambda record: record[col_idx] <= val
        elif op == "!=":
            return lambda record: record[col_idx] != val
        else:
            raise NotImplementedError(f"Unsupported operator: {op}")

    def _get_projection_indices(self, select_cols, schema):
        """
        Get the indices of columns to project for SELECT queries.

        Args:
            select_cols (list[str]): List of column names to select, or ["*"] for all.
            schema (TableSchema): The table schema.

        Returns:
            list[int]: Indices of columns to project.

        Raises:
            ValueError: If a column is not found in the schema.
        """
        if select_cols == ["*"]:
            return list(range(len(schema.columns)))
        indices = []
        for col in select_cols:
            found = False
            for idx, schema_col in enumerate(schema.columns):
                if schema_col.column_name == col:
                    indices.append(idx)
                    found = True
                    break
            if not found:
                raise ValueError(f"Column {col} not found in schema")
        return indices
