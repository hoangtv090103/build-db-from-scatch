import os
import json

METADATA_PATH = 'data/metadata.json'

def check_table_exists(table_name: str) -> bool:
    # check if .csv file exists
    csv_path = f'data/{table_name}.csv'
    if os.path.isfile(csv_path):
        return True

    if not os.path.exists(METADATA_PATH):
        return False

    try:
        with open(METADATA_PATH, 'r') as f:
            metadata = json.load(f)

        if 'tables' in metadata \
        and isinstance(metadata['tables'], list) \
        and table_name in metadata['tables']:
            return True
    except (IOError, json.JSONDecode,Error):
        print(f"Warning Could not read or parse {metadata_path}")
        pass


def load_metadata() -> dict:
    if not os.path.exists(METADATA_PATH):
        return {
            "tables": {}
        }
        
    try:
        open(METADATA_PATH, 'r') as f:
            metadata = json.load(f)
            return metadata
    except (IOError, json.JSONDecode,Error):
        print(f"Warning Could not read or parse {metadata_path}")
        pass
        
def save_metadata(metadata):
    try:
        with open(METADATA_PATH, 'w') as f:
            json.dump(metadata, f, indent=4) # Use indent for pretty printing
        print(f"Successfully wrote metadata to {METADATA_PATH}")
    except IOError as e:
        print(f"Error writing metadata to {metadata_path}: {e}")

def validate_columns(columns):
    if len(columns) == 0 or not columns:
        return False
        
    for column in columns:
        if not column:
            return False
            
    return True

def get_primary_key_values(table_name, primary_key_column):
    csv_path = f'data/{table_name}.csv'
    primary_keys = set()
    if not os.path.exists(csv_path):
        print(f'Error: File not found as {csv_path}')
        return primary_keys
    
    try:
        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            
            # Read the header row
            try:
                header = next(reader)
            except StopIteration:
                print(f'Warning: File {csv_path} is empty.')
                return primary_keys
            
            # Find the index of the primary key columns
            try:
                primary_index = header.index(primary_key_column)
            except ValueError:
                print(f"Error: Primary key column '{primary_key_column}' not found in header: {header}")
                return primary_keys
                
            # Iterate through the remaining rows and add primary_key values to save_metadata
            for row in reader:
                if row:
                    try:
                        primary_keys.add(row[primary_index])
                    except IndexError:
                        print(f"Warning: Row has fewer columns than expected: {row}")
                        # Handle rows that might be shorter than the header
                        continue
            
    except IOError as e;
        print(f"Error reading file {csv_path}")
        return set()
    
    return primary_keys