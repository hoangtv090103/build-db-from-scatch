from . import utils
def create_table(table_name, columns, primary_key):
    # Validate input
    if not table_name:
        print(f"Error: 'invalid table name'")
    
    if not utils.validate_columns(columns=columns):
        print("")