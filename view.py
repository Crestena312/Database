import re
from datetime import datetime

class Interface:
    def __init__(self):
        self.menu_displayed = False

    def show_menu(self):
        if not self.menu_displayed:
            print(
                "\nSelect an option:\n"
                " 1. Display table names\n"
                " 2. Display column names\n"
                " 3. Add new record\n"
                " 4. Update record\n"
                " 5. Delete record\n"
                " 6. Generate random data (SQL)\n"
                " 7. Multi-attribute search (join up to 2 tables)\n"
                " 8. Run prepared reports (join/group by)\n"
                " 9. Exit\n"
            )
            self.menu_displayed = True
        return input("Enter option number: ").strip()

    def show_message(self, message):
        print(message)

    def get_input(self, prompt):
        return input(prompt)

    def get_table_name(self, prompt="Enter table name: ", allow_empty=False):
        while True:
            t = input(prompt).strip()
            if t == "" and allow_empty:
                return ""
            if t:
                return t
            print("Table name cannot be empty.")

    def display_tables(self, tables):
        print("\nTables:")
        for t in tables:
            print("-", t)
        print()

    def display_columns(self, columns, table):
        print(f"\nColumns in {table}:")
        for col_name, data_type in columns:
            print(f" - {col_name} ({data_type})")
        print()

    def get_values_for_columns(self, col_types):
        """
        Ask user values for each column. Skip serial PKs (user may press Enter to leave NULL)
        Returns dict {column: value_string}
        """
        result = {}
        print("Provide values for columns. Leave empty to set NULL or skip (if default/serial).")
        for col, dtype in col_types.items():
            prompt = f"{col} ({dtype}): "
            val = input(prompt)
            if val.strip() == "":
                continue
            result[col] = val
        return result

    def get_single_value(self, column, data_type):
        val = input(f"Enter new value for {column} ({data_type}): ")
        return val

    def get_update_identifier(self):
        table = input("Table name: ").strip()
        column = input(f"Column name in {table}: ").strip()
        row_id = input(f"{table} primary key value (ID): ").strip()
        return table, column, row_id

    def get_id(self, table):
        return input(f"Enter {table} primary key value (ID): ").strip()

    def get_row_count(self):
        return input("Enter number of rows to generate: ").strip()

    def confirm(self, prompt):
        ans = input(prompt).strip().lower()
        return ans in ('y','yes')

    def get_number_of_filters(self):
        while True:
            v = input("How many attribute filters do you want to apply? (0-10): ").strip()
            try:
                n = int(v)
                if 0 <= n <= 10:
                    return n
            except:
                pass
            print("Enter a number between 0 and 10.")

    def choose_table_for_filter(self, tables):
        print("Available tables for filter:")
        for i, t in enumerate(tables, start=1):
            print(f"{i}. {t}")
        while True:
            v = input("Choose table number: ").strip()
            try:
                idx = int(v) - 1
                return tables[idx]
            except:
                print("Invalid choice.")

    def choose_column(self, table, col_types):
        print(f"Columns in {table}:")
        cols = list(col_types.keys())
        for i, c in enumerate(cols, start=1):
            print(f"{i}. {c} ({col_types[c]})")
        while True:
            v = input("Choose column number: ").strip()
            try:
                idx = int(v) - 1
                return cols[idx]
            except:
                print("Invalid choice.")

    def build_filter_clause(self, table, column, data_type):
        dt = data_type.lower()
        col_ref = f'"{table}"."{column}"'
        if dt.startswith('integer') or dt in ('bigint','smallint','numeric','double precision','real'):
            lo = input("Enter lower bound (or press Enter to skip): ").strip()
            hi = input("Enter upper bound (or press Enter to skip): ").strip()
            clauses = []
            params = []
            if lo != "":
                clauses.append(f"{col_ref} >= {self._literal_for_numeric(lo)}")
            if hi != "":
                clauses.append(f"{col_ref} <= {self._literal_for_numeric(hi)}")
            return " AND ".join(clauses) if clauses else "TRUE"
        if 'character' in dt or dt == 'text':
            pattern = input("Enter pattern (SQL LIKE, use % as wildcard, e.g. 'Ann%'): ").strip()
            if pattern == "":
                return "TRUE"
            return f"{col_ref} ILIKE {self._quote_literal(pattern)}"
        if dt == 'boolean':
            val = input("Enter boolean (true/false): ").strip().lower()
            if val in ('true','t','1','yes','y'):
                return f"{col_ref} = TRUE"
            if val in ('false','f','0','no','n'):
                return f"{col_ref} = FALSE"
            return "TRUE"
        if 'date' in dt or 'timestamp' in dt:
            lo = input("Start date/time (ISO format) (or press Enter to skip): ").strip()
            hi = input("End date/time (ISO format) (or press Enter to skip): ").strip()
            clauses = []
            if lo:
                clauses.append(f"{col_ref} >= {self._quote_literal(lo)}")
            if hi:
                clauses.append(f"{col_ref} <= {self._quote_literal(hi)}")
            return " AND ".join(clauses) if clauses else "TRUE"
        val = input(f"Enter value to match for {column}: ").strip()
        if val == "":
            return "TRUE"
        return f"{col_ref} = {self._quote_literal(val)}"

    def _literal_for_numeric(self, s):
        try:
            float(s)
            return s
        except:
            return "0"

    def _quote_literal(self, s):
        return "'" + s.replace("'", "''") + "'"
    def display_query_result(self, columns, rows):
        if not rows:
            print("No rows returned.\n")
            return
        print("\nQuery result:")
        print(" | ".join(columns))
        print("-" * (len(" | ".join(columns)) + 4))
        for r in rows:
            print(" | ".join(str(x) for x in r))
        print()

    def ask_parameters_for_template(self, template_sql):
        """
        Very simple parameter extraction: count %s placeholders and ask user that many values.
        The template_sql uses %s placeholders (psycopg2 parameter style).
        """
        num = template_sql.count("%s")
        params = []
        for i in range(num):
            val = input(f"Enter value for parameter {i+1} : ")
            v = self._try_cast_param(val)
            params.append(v)
        return params

    def _try_cast_param(self, val):
        s = val.strip()
        if s == "":
            return None
        if re.fullmatch(r"-?\d+", s):
            return int(s)
        if re.fullmatch(r"-?\d+\.\d+", s):
            return float(s)
        if s.lower() in ('true','false'):
            return True if s.lower() == 'true' else False
        return s
