from enum import Enum
import re
from typing import Any, List


class CommandType(Enum):
    """Enumeration of supported SQL command types."""
    CREATE_TABLE = 1
    INSERT = 2
    SELECT = 3


class FilterCondition:
    """
    Represents a simple WHERE condition in a SQL statement.

    Attributes:
        column (str): The column name to filter on.
        operator (str): The comparison operator (e.g., '=', '>', '<').
        value (Any): The value to compare against.
    """

    def __init__(self, column_name: str, operator: str, value: Any):
        """
        Initialize a FilterCondition.

        Args:
            column_name (str): The column name to filter on.
            operator (str): The comparison operator.
            value (Any): The value to compare against.
        """
        self.column = column_name
        self.operator = operator
        self.value = value


class ParsedCommand:
    """
    Base class for parsed SQL commands.

    Attributes:
        command_type (CommandType): The type of SQL command.
    """

    def __init__(self, command_type: CommandType):
        """
        Initialize a ParsedCommand.

        Args:
            command_type (CommandType): The type of SQL command.
        """
        self.command_type = command_type


class CreateTableCommand(ParsedCommand):
    """
    Represents a parsed CREATE TABLE command.

    Attributes:
        table_name (str): The name of the table to create.
        columns (List[tuple[str, str]]): List of (column name, column type) pairs.
    """

    def __init__(self, table_name: str, columns: List[tuple[str, str]]):
        """
        Initialize a CreateTableCommand.

        Args:
            table_name (str): The name of the table to create.
            columns (List[tuple[str, str]]): List of (column name, column type) pairs.
        """
        super().__init__(CommandType.CREATE_TABLE)
        self.table_name = table_name
        self.columns = columns


class InsertCommand(ParsedCommand):
    """
    Represents a parsed INSERT command.

    Attributes:
        table_name (str): The name of the table to insert into.
        values (list[Any]): List of values to insert, in column order.
    """

    def __init__(self, table_name: str, values: list[Any]):
        """
        Initialize an InsertCommand.

        Args:
            table_name (str): The name of the table to insert into.
            values (list[Any]): List of values to insert, in column order.
        """
        super().__init__(CommandType.INSERT)
        self.table_name = table_name
        self.values = values  # List of values to insert, in column order


class SelectCommand(ParsedCommand):
    """
    Represents a parsed SELECT command.

    Attributes:
        table_name (str): The name of the table to select from.
        select_columns (list[str]): List of column names to select, or ["*"] for all.
        filter_condition (FilterCondition | None): WHERE condition, if any.
    """

    def __init__(
        self,
        table_name: str,
        select_columns: list[str],
        filter_condition: FilterCondition | None = None,
    ):
        """
        Initialize a SelectCommand.

        Args:
            table_name (str): The name of the table to select from.
            select_columns (list[str]): List of column names to select, or ["*"] for all.
            filter_condition (FilterCondition | None): WHERE condition, if any.
        """
        super().__init__(CommandType.SELECT)
        self.table_name = table_name
        self.select_columns = select_columns  # List of column names or ["*"]
        self.filter_condition = filter_condition  # WHERE condition or None


class SQLParser:
    """
    SQLParser parses SQL strings into command objects.

    Methods:
        parse(sql_string: str) -> ParsedCommand | None:
            Parses a SQL string and returns a ParsedCommand object or None on error.
    """

    def parse(self, sql_string: str) -> ParsedCommand | None:
        """
        Parse a SQL string and return a ParsedCommand object.

        Args:
            sql_string (str): The SQL statement to parse.

        Returns:
            ParsedCommand | None: The parsed command object, or None if parsing fails.
        """
        normalized_sql = sql_string.trim().upper()

        if normalized_sql.startswith("CREATE TABLE"):
            return self._parse_create_table(sql_string)

        elif normalized_sql.startswith("INSERT INTO"):
            return self._parse_insert(sql_string)

        elif normalized_sql.startswith("SELECT"):
            return self._parse_select(sql_string)

        else:
            print("Error: Unknown SQL command.")
            return None

    def _parse_create_table(self, sql_string: str) -> CreateTableCommand | None:
        """
        Parse a CREATE TABLE statement.

        Args:
            sql_string (str): The CREATE TABLE SQL statement.

        Returns:
            CreateTableCommand | None: The parsed command or None if syntax is invalid.
        """
        # Regex to match: CREATE TABLE table_name (col1 type1, col2 type2, ...)
        pattern = r"CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.+)\)"
        match = re.match(pattern, sql_string.strip(), re.IGNORECASE)
        if not match:
            print("Error: Invalid CREATE TABLE syntax.")
            return None

        table_name = match.group(1)
        columns_str = match.group(2)

        # Split columns by comma, but handle commas inside parentheses (e.g., VARCHAR(50))
        def smart_split_columns(s):
            """
            Split a string of column definitions by commas, ignoring commas inside parentheses.

            Args:
                s (str): The string to split.

            Returns:
                list[str]: List of column definition strings.
            """
            cols = []
            buf = ""
            paren = 0
            for c in s:
                if c == "(":
                    paren += 1
                elif c == ")":
                    paren -= 1

                if c == "," and paren == 0:
                    cols.append(buf.strip())
                    buf = ""
                else:
                    buf += c
            if buf.strip():
                cols.append(buf.strip())
            return cols

        columns = []
        for col_def in smart_split_columns(columns_str):
            # Split by first space
            parts = col_def.split(None, 1)
            if len(parts) != 2:
                print(f"Error: Invalid column definition '{col_def}'")
                return None
            col_name, col_type = parts[0], parts[1]
            columns.append((col_name, col_type))

        return CreateTableCommand(table_name, columns)

    def _parse_insert(self, sql_string: str) -> InsertCommand | None:
        """
        Parse an INSERT statement.

        Args:
            sql_string (str): The INSERT SQL statement.

        Returns:
            InsertCommand | None: The parsed command or None if syntax is invalid.
        """
        # Example: INSERT INTO users VALUES (1, 'Alice', TRUE)
        pattern = r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+VALUES\s*\((.*)\)"
        match = re.match(pattern, sql_string.strip(), re.IGNORECASE)
        if not match:
            print("Error: Invalid INSERT syntax.")
            return None

        table_name = match.group(1)
        values_str = match.group(2)

        def smart_split_values(s):
            """
            Split a string of values by commas, ignoring commas inside quoted strings.

            Args:
                s (str): The string to split.

            Returns:
                list[str]: List of value strings.
            """
            vals = []
            buf = ""
            in_str = False
            str_char = ""
            i = 0
            while i < len(s):
                c = s[i]
                if in_str:
                    buf += c
                    if c == str_char:
                        # Check for escaped quote
                        if i + 1 < len(s) and s[i + 1] == str_char:
                            buf += str_char
                            i += 1
                        else:
                            in_str = False
                    i += 1
                    continue
                if c in ("'", '"'):
                    in_str = True
                    str_char = c
                    buf += c
                    i += 1
                    continue
                if c == ",":
                    vals.append(buf.strip())
                    buf = ""
                    i += 1
                    continue
                buf += c
                i += 1
            if buf.strip():
                vals.append(buf.strip())
            return vals

        def parse_value(val):
            """
            Parse a string value into its appropriate Python type.

            Args:
                val (str): The value string.

            Returns:
                Any: The parsed value.
            """
            # Try int
            if re.match(r"^-?\d+$", val):
                return int(val)
            # Try float
            if re.match(r"^-?\d+\.\d+$", val):
                return float(val)
            # Try boolean
            if val.upper() == "TRUE":
                return True
            if val.upper() == "FALSE":
                return False
            # Try string (single or double quoted)
            if (val.startswith("'") and val.endswith("'")) or (
                val.startswith('"') and val.endswith('"')
            ):
                # Remove quotes and handle escaped quotes
                v = val[1:-1].replace("\\'", "'").replace('\\"', '"')
                return v
            # Fallback: treat as string
            return val

        try:
            values = [parse_value(v) for v in smart_split_values(values_str)]
        except Exception as e:
            print(f"Error: Failed to parse values: {e}")
            return None

        return InsertCommand(table_name, values)


    def _parse_select(self, sql_string: str) -> "SelectCommand | None":
        """
        Parse a SELECT statement.

        Args:
            sql_string (str): The SELECT SQL statement.

        Returns:
            SelectCommand | None: The parsed command or None if syntax is invalid.
        """
        import re

        # Remove leading/trailing whitespace and collapse multiple spaces
        sql = sql_string.strip()
        sql = re.sub(r"\s+", " ", sql)

        # Regex to match SELECT ... FROM ... [WHERE ...]
        select_regex = re.compile(
            r"^SELECT\s+(?P<columns>[\w\*,\s]+)\s+FROM\s+(?P<table>\w+)(?:\s+WHERE\s+(?P<where>.+))?$",
            re.IGNORECASE,
        )
        m = select_regex.match(sql)
        if not m:
            print("Error: Invalid SELECT syntax.")
            return None

        # Parse columns
        columns_str = m.group("columns").strip()
        if columns_str == "*":
            select_columns = ["*"]
        else:
            select_columns = [col.strip() for col in columns_str.split(",") if col.strip()]

        # Parse table name
        table_name = m.group("table").strip()

        # Parse WHERE clause (if any)
        where_str = m.group("where")
        filter_condition = None
        if where_str:
            # Simple: only support single condition: col op value
            # e.g. age > 20, name = 'Alice'
            where_regex = re.compile(
                r"^(?P<col>\w+)\s*(?P<op>=|!=|<>|<|>|<=|>=)\s*(?P<val>.+)$", re.IGNORECASE
            )
            wm = where_regex.match(where_str.strip())
            if not wm:
                print("Error: Invalid WHERE clause.")
                return None
            col = wm.group("col")
            op = wm.group("op")
            val = wm.group("val").strip()
            # Try to parse value (reuse parse_value from above if available)
            def parse_value(val):
                """
                Parse a string value from a WHERE clause into its appropriate Python type.

                Args:
                    val (str): The value string.

                Returns:
                    Any: The parsed value.
                """
                if re.match(r"^-?\d+$", val):
                    return int(val)
                if re.match(r"^-?\d+\.\d+$", val):
                    return float(val)
                if val.upper() == "TRUE":
                    return True
                if val.upper() == "FALSE":
                    return False
                if (val.startswith("'") and val.endswith("'")) or (
                    val.startswith('"') and val.endswith('"')
                ):
                    v = val[1:-1].replace("\\'", "'").replace('\\"', '"')
                    return v
                return val
            parsed_val = parse_value(val)
            filter_condition = FilterCondition(column_name=col, operator=op, value=parsed_val)

        return SelectCommand(
            select_columns=select_columns,
            table_name=table_name,
            filter_condition=filter_condition,
        )