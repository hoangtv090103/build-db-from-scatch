from dbms.storage.memory_store import InMemoryStore


def parse_put_command(args_str: str) -> tuple[str, str, dict] | None:
    parts = args_str.split(maxsplit=2)
    if len(parts) < 2:
        # Must have at least table_name and key
        print("Error: PUT command needs at least a table name and a key.")
        return None

    table_name = parts[0]
    key = parts[1]

    value_object = {}
    if len(parts) == 3:  # If there are column assignments
        col_assignments_str = parts[2]
        try:
            assignments = col_assignments_str.split()
            for assign in assignments:
                if '=' not in assign:
                    print(
                        f"Error: Invalid column assignment '{assign}'. Expected format 'col=val'.")
                    return None
                col_name, col_value = assign.split('=', 1)
                value_object[col_name.strip()] = col_value.strip()
        except ValueError:
            print(
                "Error: Invalid format for column assignments. Expected 'col1=val1 col2=val2 ...'")
            return None

    if not value_object:  # If no columns specified, treat it as an empty object for the key
        print(
            f"Warning: PUT command for table '{table_name}', key '{key}' has no column values. Storing empty object.")

    return table_name, key, value_object


def main():
    store = InMemoryStore()
    db_file = "my_simple_db.json"  # Default database file

    print("Simple DB CLI. Type 'HELP' for commands, 'EXIT' to quit.")

    while True:
        try:
            raw_input = input("> ").strip()
            if not raw_input:
                continue

            command_parts = raw_input.split(maxsplit=1)
            command = command_parts[0].upper()
            args_str = command_parts[1] if len(command_parts) > 1 else ""

            if command == "EXIT":
                print("Exiting...")
                break
            elif command == "HELP":
                print("\nAvailable commands:")
                print("  CREATE_TABLE <table_name>          - Creates a new table.")
                print("  PUT <table_name> <key> [col1=val1 col2=val2 ...]")
                print("                                       - Inserts/updates a record. If no cols, stores an empty object for the key.")
                print("  GET <table_name> <key>             - Retrieves a record.")
                print("  DELETE <table_name> <key>          - Deletes a record.")
                print(
                    "  SAVE [filepath]                    - Saves the current database to a file (default: my_simple_db.json).")
                print(
                    "  LOAD [filepath]                    - Loads the database from a file (default: my_simple_db.json). Overwrites current data.")
                print(
                    "  LIST_TABLES                      - Shows all current table names.")
                print("  EXIT                               - Quits the CLI.\n")

            elif command == "CREATE_TABLE":
                if not args_str:
                    print("Error: CREATE_TABLE needs a table name.")
                    continue
                store.create_table(args_str)

            elif command == "PUT":
                if not args_str:
                    print(
                        "Error: PUT command needs arguments: <table_name> <key> [col1=val1 ...]")
                    continue
                parsed_put = parse_put_command(args_str)
                if parsed_put:
                    table_name, key, value_object = parsed_put
                    store.put(table_name, key, value_object)

            elif command == "GET":
                if not args_str:
                    print("Error: GET command needs arguments: <table_name> <key>")
                    continue
                parts = args_str.split(maxsplit=1)
                if len(parts) != 2:
                    print("Error: GET command requires exactly <table_name> and <key>.")
                    continue
                table_name, key = parts
                result = store.get(table_name, key)
                if result is None:
                    print(
                        f"No record found for key '{key}' in table '{table_name}'.")
                # else: The get method in InMemoryStore already prints.

            elif command == "DELETE":
                if not args_str:
                    print("Error: DELETE command needs arguments: <table_name> <key>")
                    continue
                parts = args_str.split(maxsplit=1)
                if len(parts) != 2:
                    print(
                        "Error: DELETE command requires exactly <table_name> and <key>.")
                    continue
                table_name, key = parts
                store.delete(table_name, key)
                # The delete method in InMemoryStore already prints.

            elif command == "SAVE":
                current_db_file = args_str if args_str else db_file
                if store.save_to_file(current_db_file):
                    db_file = current_db_file  # Update default if save was to a new file
                # else: save_to_file already prints error

            elif command == "LOAD":
                current_db_file = args_str if args_str else db_file
                if store.load_from_file(current_db_file):
                    db_file = current_db_file  # Update default if load was from a new file
                # else: load_from_file already prints error or info

            elif command == "LIST_TABLES":
                if not store._data:  # Accessing protected member for simplicity here
                    print("No tables exist.")
                else:
                    print("Tables:")
                    for table_name in store._data.keys():
                        print(f"  - {table_name}")

            else:
                print(
                    f"Unknown command: '{command}'. Type 'HELP' for a list of commands.")

        except KeyboardInterrupt:
            print("\nExiting due to KeyboardInterrupt...")
            break
        except Exception as e:
            print(f"An unexpected error occurred in CLI: {e}")
            # Potentially save unsaved data here or log the error
            # For now, just print and continue or break
            # break


if __name__ == "__main__":
    # Ensure the dbms/storage directory exists (or is in PYTHONPATH)
    # For this example, we assume the structure is correct
    # and InMemoryStore can be imported.

    # Create dummy directories if they don't exist, so import works
    import os
    if not os.path.exists("dbms/storage"):
        os.makedirs("dbms/storage", exist_ok=True)
    # Create an empty __init__.py in dbms and dbms/storage if they don't exist
    # to make them packages
    if not os.path.exists("dbms/__init__.py"):
        open("dbms/__init__.py", 'w').close()
    if not os.path.exists("dbms/storage/__init__.py"):
        open("dbms/storage/__init__.py", 'w').close()

    # Now try to run main
    main()
