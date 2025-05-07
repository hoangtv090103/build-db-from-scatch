import json
import os


class InMemoryStore:
    def __init__(self):
        """
        A simple in-memory key-value store that can persist data to a JSON file.
        The data is structured as:
        {
            "table_name_1": {
                "key1": {"col1": "val1", "col2": "val2"},
                "key2": {"col1": "val3", "col2": "val4"}
            },
            "table_name_2": { ... }
        }
        """
        self._data: dict[str, dict[str, dict]] = {}

    def put(self, table_name: str, key: str, value_object: dict) -> None:
        if table_name not in self._data:
            self._data[table_name] = {}

        self._data[table_name][key] = value_object
        print(f"Put: table='{table_name}', key='{key}', value={value_object}")

    def get(self, table_name: str, key: str) -> dict | None:
        table = self._data.get(table_name)
        if not table:
            print(
                f"Get: table='{table_name}', key='{key}', result=None (table not found)")
            return None

        value = table.get(key)
        print(f"Get: table='{table_name}', key='{key}', result={value}")
        return value

    def delete(self, table_name: str, key: str) -> bool:
        if table_name not in self._data or key not in self._data.get(table_name):
            print(
                f"Delete: table='{table_name}', key='{key}', success=False (not found)")
            return False

        del self._data.get(table_name)[key]
        print(f"Delete: table='{table_name}', key='{key}', success=True")
        if not self._data[table_name]:  # Remove table if empty
            del self._data[table_name]
            print(f"Table '{table_name}' removed as it became empty.")
        return True

    def save_to_file(self, filepath: str) -> bool:
        try:
            dir_path = os.path.dirname(filepath)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self._data, f, indent=4)
            print(f"Data saved to '{filepath}'")
            return True
        except IOError as e:
            print(f"Error saving data to '{filepath}': {e}")
            return False
        except Exception as e:
            print(f"An unexpected error occurred during save: {e}")
            return False

    def load_from_file(self, filepath: str) -> bool:
        try:
            if not os.path.exists(filepath):
                print(
                    f"File '{filepath}' not found. Initializing with empty data.")
                self._data = {}
                return False  # Or True, depending on if "not found" is an error or just initial state

            with open(filepath, 'r', encoding='utf-8') as f:
                self._data = json.load(f)

            print(f"Data loaded from '{filepath}'")
            return True
        except IOError as e:
            print(f"Error loading data from '{filepath}'")
            self._data = {}
            return False
        except json.JSONDecodeError as e:
            print(
                f"Error decoding JSON from '{filepath}': {e}. Initializing with empty data.")
            self._data = {}  # Reset data on error
            return False
        except Exception as e:
            print(f"An unexpected error occurred during load: {e}")
            self._data = {}  # Reset data on error
            return False

    def table_exists(self, table_name: str) -> bool:
        """Checks if a table exists."""
        return table_name in self._data

    def create_table(self, table_name: str) -> bool:
        """
        Creates a new empty table.
        Returns True if created, False if it already exists.
        """
        if table_name in self._data:
            print(f"Table '{table_name}' already exists.")
            return False
        self._data[table_name] = {}
        print(f"Table '{table_name}' created.")
        return True
