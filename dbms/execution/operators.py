from abc import ABC, abstractmethod
from typing import Callable, List

from dbms.catalog.catalog import TableSchema
from dbms.storage.table_heap import TableIterator


class ExecutionOperator(ABC):
    """
    Abstract base class for all execution operators in the query execution engine.
    Defines the interface for producing tuples, opening, and closing the operator.
    """

    @abstractmethod
    def next(self) -> tuple | None:
        """
        Produce the next tuple from this operator.
        Returns:
            tuple: The next record as a tuple, or None if no more records.
        """
        pass

    @abstractmethod
    def open(self):
        """
        Prepare the operator for execution (e.g., initialize state, open child operators).
        """
        pass

    @abstractmethod
    def close(self):
        """
        Release any resources held by the operator (e.g., close child operators).
        """
        pass


class SeqScanOperator(ExecutionOperator):
    """
    Sequential scan operator. Iterates over all records in a table using a TableIterator,
    deserializing each record according to the table schema.
    """

    def __init__(self, table_heap_iterator: TableIterator, table_schema: TableSchema):
        """
        Args:
            table_heap_iterator (TableIterator): Iterator over the table's records.
            table_schema (TableSchema): Schema describing the table's columns.
        """
        self.table_heap_iterator = table_heap_iterator
        self.table_schema = table_schema

        self._deserialize_tuple()

    def next(self) -> tuple | None:
        """
        Fetch the next record from the table, deserialized as a tuple.
        Returns:
            tuple: The next record as a tuple, or None if no more records.
        """
        rid_record_pair = self.table_heap_iterator.next()
        if rid_record_pair is None:
            return None

        rid, record_data_byte = rid_record_pair

        # Deserialize record_data_byte into a tuple of Python values using the schema
        return self._deserialize_tuple(record_data_byte)

    def _deserialize_tuple(self, record_bytes: bytes) -> tuple:
        """
        Deserialize a record's bytes into a tuple of values according to the table schema.
        Assumes fixed-length types (e.g., INTEGER, BOOLEAN) and VARCHAR(N) is stored as length-prefixed.

        Args:
            record_bytes (bytes): The raw bytes of the record.

        Returns:
            tuple: The deserialized values as a tuple.
        """
        import struct

        values = []
        offset = 0
        for col in self.table_schema.columns:
            col_type = col.column_type.upper()
            if col_type == "INTEGER":
                val = struct.unpack_from("i", record_bytes, offset)[0]
                offset += 4
            elif col_type == "BOOLEAN":
                val = struct.unpack_from("?", record_bytes, offset)[0]
                offset += 1
            elif col_type.startswith("VARCHAR"):
                # Expect VARCHAR(N), stored as: [2 bytes length][data]
                strlen = struct.unpack_from("H", record_bytes, offset)[0]
                offset += 2
                val = record_bytes[offset : offset + strlen].decode("utf-8")
                offset += strlen
            else:
                raise NotImplementedError(f"Unsupported column type: {col_type}")
            values.append(val)
        return tuple(values)

    def open(self):
        """
        Open the underlying table iterator.
        """
        self.table_heap_iterator.open()

    def close(self):
        """
        Close the underlying table iterator.
        """
        self.table_heap_iterator.close()


class FilterOperator(ExecutionOperator):
    """
    Filter operator. Consumes tuples from a child operator and yields only those
    that satisfy a given predicate.
    """

    def __init__(
        self, child_operator: "ExecutionOperator", predicate: "Callable[[tuple], bool]"
    ):
        """
        Args:
            child_operator (ExecutionOperator): The child operator to fetch tuples from.
            predicate (Callable[[tuple], bool]): Predicate function to filter tuples.
        """
        self.child_operator = child_operator
        self.predicate = predicate

    def next(self) -> tuple | None:
        """
        Fetch the next tuple from the child that satisfies the predicate.
        Returns:
            tuple: The next matching tuple, or None if no more matches.
        """
        while True:
            record_tuple = self.child_operator.next()
            if record_tuple is None:
                return None

            if self.predicate(record_tuple):
                return record_tuple

    def open(self):
        """
        Open the child operator.
        """
        self.child_operator.open()

    def close(self):
        """
        Close the child operator.
        """
        self.child_operator.close()


class ProjectionOperator(ExecutionOperator):
    """
    Projection operator. Consumes tuples from a child operator and yields tuples
    containing only the specified columns.
    """

    def __init__(
        self, child_operator: ExecutionOperator, columns_indices_to_project: List[int]
    ):
        """
        Args:
            child_operator (ExecutionOperator): The child operator to fetch tuples from.
            columns_indices_to_project (List[int]): Indices of columns to project.
        """
        self.child_operator = child_operator
        self.columns_indices_to_project = columns_indices_to_project

    def next(self) -> tuple | None:
        """
        Fetch the next tuple from the child and project the specified columns.
        Returns:
            tuple: The projected tuple, or None if no more records.
        """
        record_tuple = self.child_operator.next()
        if record_tuple is None:
            return None

        projected_values = []
        for index in self.columns_indices_to_project:
            projected_values.append(record_tuple[index])
        return tuple(projected_values)

    def open(self):
        """
        Open the child operator.
        """
        self.child_operator.open()

    def close(self):
        """
        Close the child operator.
        """
        self.child_operator.close()
