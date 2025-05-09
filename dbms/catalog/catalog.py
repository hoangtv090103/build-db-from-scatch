import json
import threading
from typing import List
from dbms.buffer.buffer_pool_manager import BufferPoolManager
from dbms.common.config import PAGE_SIZE
from dbms.storage.slotted_page import SlottedPageWrapper
from dbms.storage.table_heap import TableHeap


class ColumnInfo:
    """
    Stores information about a single column in a table.

    Attributes:
        column_name (str): The name of the column.
        column_type (str): The data type of the column (e.g., "INTEGER", "VARCHAR(255)", "BOOLEAN").
    """

    def __init__(self, column_name: str, column_type: str):
        self.column_name = column_name
        self.column_type = column_type


class TableSchema:
    """
    Represents the schema of a table, which is a list of columns.

    Attributes:
        columns (List[ColumnInfo]): List of ColumnInfo objects describing each column in the table.
    """

    def __init__(self, columns: List[ColumnInfo]):
        self.columns = columns


class TableMetadata:
    """
    Stores metadata for a table, including its name, schema, and all page IDs used by the table.

    Attributes:
        table_name (str): The name of the table.
        schema (TableSchema): The schema of the table.
        all_page_ids (List[int]): List of all page IDs managed by this table's TableHeap.
    """

    def __init__(self, table_name: str, schema: TableSchema, all_page_ids: List[int]):
        """
        Initializes TableMetadata.

        Args:
            table_name (str): The name of the table.
            schema (TableSchema): The schema of the table.
            all_page_ids (List[int]): List of all page IDs that TableHeap of the table manages.
        """
        self.table_name = table_name
        self.schema = schema
        self.all_page_ids = all_page_ids


class Catalog:
    """
    Catalog is responsible for managing all metadata about the database.

    This includes information about tables, such as table names, schemas (column names and data types),
    and the data pages that each table uses.

    The Catalog itself is persisted to disk using the BufferPoolManager. When changes are made
    (e.g., creating a new table), the Catalog serializes its in-memory data structures to bytes
    and writes them to fixed pages on disk. When the DBMS starts up, the Catalog reads this data
    to restore its state.

    Key components:
    - Stores metadata for tables including name, schema, and page IDs
    - Persists metadata to disk for durability
    - Provides methods to create tables, retrieve table information, etc.

    Data structures:
    - ColumnInfo: Stores column name and data type
    - TableSchema: Contains a list of columns
    - TableMetadata: Holds table name, schema, and list of page IDs for the table's data
    - _tables: Dictionary mapping table names to their TableMetadata
    """

    CATALOG_ROOT_PAGE_ID = 0

    def __init__(self, buffer_pool_manager: BufferPoolManager, root_page_id: int):
        self.buffer_pool_manager = buffer_pool_manager
        self.catalog_data_page_id = root_page_id
        self._tables: dict[str, TableMetadata] = {}
        self._lock = threading.Lock()

        self._load_catalog_from_disk()

    def _serialize_catalog_data(self) -> bytes:
        import json

        def table_metadata_to_dict(table_metadata):
            return {
                "schema": {
                    "columns": [
                        {"name": col.name, "type": col.type}
                        for col in table_metadata.schema.columns
                    ]
                },
                "all_page_ids": table_metadata.all_page_ids,
            }

        catalog_dict = {
            table_name: table_metadata_to_dict(table_metadata)
            for table_name, table_metadata in self._tables.items()
        }
        return json.dumps(catalog_dict).encode("utf-8")

    def _deserialize_catalog_data(self, data_bytes: bytes):
        if not data_bytes:
            self._tables = {}
            return

        catalog_dict = json.loads(data_bytes.decode("utf-8"))
        self._tables = {}
        for table_name, table_info in catalog_dict.items():
            columns = [
                ColumnInfo(col["name"], col["type"])
                for col in table_info["schema"]["columns"]
            ]
            schema = TableSchema(columns)
            all_page_ids = table_info["all_page_ids"]
            self._tables[table_name] = TableMetadata(
                name=table_name, schema=schema, all_page_ids=all_page_ids
            )

    def _load_catalog_from_disk(self):
        with self._lock:
            catalog_page = self.buffer_pool_manager.fetch_page(
                self.catalog_data_page_id
            )
            if catalog_page is None:
                print(
                    "Error: Could not fetch catalog page. Initializing empty catalog."
                )
                self._tables = {}
                return

            # Assume the catalog data is stored as a single JSON blob at the start of the page.
            # We don't know the real length, so try to decode until we hit an error or get valid JSON.
            raw_data_bytes = bytes(catalog_page.data)
            try:
                # Try to find the end of the JSON object (could be padded with zeros)
                # Remove trailing zeros for safety
                trimmed = raw_data_bytes.rstrip(b"\x00")
                self._deserialize_catalog_data(trimmed)
            except Exception as e:
                print(
                    "Warning: Could not deserialize catalog data. Starting with empty catalog."
                )
                self._tables = {}

            self.buffer_pool_manager.unpin_page(
                self.catalog_data_page_id, is_dirty=False
            )

    def _persist_catalog_to_disk(self):
        with self._lock:
            catalog_page = self.buffer_pool_manager.fetch_page(
                self.catalog_data_page_id
            )
            if catalog_page is None:
                print(
                    "FATAL: Could not fetch catalog page to persist data. Metadata changes might be lost."
                )
                return False  # or raise Exception

            serialized_bytes = self._serialize_catalog_data()

            if len(serialized_bytes) > PAGE_SIZE:
                print(
                    "FATAL: Serialized catalog data is larger than PAGE_SIZE. Not implemented for multi-page catalog yet."
                )
                self.buffer_pool_manager.unpin_page(
                    self.catalog_data_page_id, is_dirty=False
                )
                return False  # or raise Exception

            # Write data to page, zero out the rest
            page_data_buffer = bytearray(PAGE_SIZE)
            page_data_buffer[0 : len(serialized_bytes)] = serialized_bytes
            catalog_page.data = page_data_buffer  # Overwrite entire page

            # Mark page as dirty (if your Page class doesn't do this automatically)
            if hasattr(catalog_page, "mark_dirty"):
                catalog_page.mark_dirty()
            else:
                # fallback: set dirty flag if needed
                if hasattr(catalog_page, "_is_dirty"):
                    catalog_page._is_dirty = True

            success = self.buffer_pool_manager.unpin_page(
                self.catalog_data_page_id, is_dirty=True
            )
            if not success:
                print("Warning: Failed to unpin catalog page after persisting.")
            return success

    def create_table(
        self, table_name: str, schema_definition: List[tuple[str, str]]
    ) -> TableMetadata | None:
        with self._lock:
            if table_name in self._tables:
                print(f"Error: Table '{table_name}' already exists")
                return None

            # Create a schema object
            parsed_columns = []
            for col_name, col_type in schema_definition:
                # TODO: Validate col_type against supported types
                parsed_columns.append(ColumnInfo(col_name, col_type))

            table_schema = TableSchema(parsed_columns)

            # Allocate the first page for TableHeap of this table
            first_page_for_table = self.buffer_pool_manager.new_page()
            if first_page_for_table is None:
                print(f"Error: Could not allocate a new page for table '{table_name}")

            first_page_id = first_page_for_table.page_id

            # Initialize this page as an empty slotted page
            slotted_wrapper = SlottedPageWrapper(first_page_for_table)
            slotted_wrapper.initialize()

            # Important: Unpin this first page. It's initialize and mark dirty if necessary.
            # TableHeap will fetch itself when created
            self.buffer_pool_manager.unpin_page(
                first_page_id, is_dirty=first_page_for_table.is_dirty
            )

            # Create Metadata
            table_meta = TableMetadata(
                table_name, table_schema, all_page_ids=[first_page_id]
            )
            # Update catalog in memory and persist
            self._tables[table_name] = table_meta
            if not self._persist_catalog_to_disk():
                print(
                    f"Error: Failed to persist catalog after creating table '{table_name}'. Rolling back."
                )
                del self._tables[table_name]  # Rollback in-memory change
                # TODO: Need a mechanism to deallocate first_page_id if persist fails
                self.buffer_pool_manager.delete_page(first_page_id)  # Attempt cleanup
                return None

            return table_meta

    def get_table_metadata(self, table_name: str) -> TableMetadata | None:
        with self._lock:
            return self._tables.get(table_name)

    def get_table_heap(self, table_name: str) -> "TableHeap | None":
        table_meta = self.get_table_metadata(table_name)
        if table_meta is None:
            return None
        # Always pass a copy of the page id list to TableHeap to avoid accidental mutation
        page_ids_for_heap = list(table_meta.all_page_ids)
        return TableHeap(self.buffer_pool_manager, page_ids_for_heap)

    def list_tables(self) -> List[str]:
        with self._lock:
            return list(self._tables.keys())
