from model import Database
from view import Interface
from psycopg2 import DatabaseError
import time

class AppController:
    def __init__(self):
        self.model = Database()
        self.view = Interface()

    def run(self):
        while True:
            choice = self.view.show_menu()
            actions = {
                "1": self.show_tables,
                "2": self.show_columns,
                "3": self.add_entry,
                "4": self.update_entry,
                "5": self.delete_entry,
                "6": self.generate_data_sql,
                "7": self.multi_attribute_search,
                "8": self.run_prepared_reports,
                "9": self.exit_app
            }
            action = actions.get(choice)
            if action:
                try:
                    action()
                except Exception as e:
                    self.view.show_message(f"An error occurred: {str(e)}\n")
            else:
                self.view.show_message("Incorrect input. Please try again.\n")

    def show_tables(self):
        tables = self.model.get_tables()
        self.view.display_tables(tables)

    def show_columns(self):
        table = self.view.get_table_name()
        columns = self.model.get_columns(table)
        if columns is None:
            self.view.show_message(f"Table '{table}' not found.\n")
            return
        self.view.display_columns(columns, table)

    def add_entry(self):
        table = self.view.get_table_name()
        col_types = self.model.get_column_types(table)
        if col_types is None:
            self.view.show_message(f"Table '{table}' not found.\n")
            return

        values = self.view.get_values_for_columns(col_types)
        try:
            self.model.insert_entry_validated(table, values, col_types)
            self.view.show_message("Data successfully added.\n")
        except DatabaseError as e:
            self.view.show_message(f"Database error while inserting: {e.pgerror or str(e)}\n")
        except ValueError as ve:
            self.view.show_message(f"Validation error: {str(ve)}\n")

    def update_entry(self):
        table, column, row_id = self.view.get_update_identifier()
        col_types = self.model.get_column_types(table)
        if col_types is None or column not in col_types:
            self.view.show_message("Table/column not found.\n")
            return
        new_value = self.view.get_single_value(column, col_types[column])
        try:
            updated = self.model.update_entry_validated(table, column, row_id, new_value, col_types)
            if updated:
                self.view.show_message("Data successfully updated.\n")
            else:
                self.view.show_message("No rows updated (check ID).\n")
        except DatabaseError as e:
            self.view.show_message(f"Database error while updating: {e.pgerror or str(e)}\n")
        except ValueError as ve:
            self.view.show_message(f"Validation error: {str(ve)}\n")

    def delete_entry(self):
        table = self.view.get_table_name()
        row_id = self.view.get_id(table)
        children = self.model.get_child_tables(table)
        if children:
            self.view.show_message(f"Warning: There are child tables referencing {table}: {', '.join(children)}")
            ok = self.view.confirm(f"Do you really want to delete {table} row {row_id}? This may cascade or fail. (yes/no): ")
            if not ok:
                self.view.show_message("Delete cancelled.\n")
                return
        try:
            deleted = self.model.delete_entry(table, row_id)
            if deleted:
                self.view.show_message("Data successfully deleted.\n")
            else:
                self.view.show_message("No rows deleted (check ID).\n")
        except DatabaseError as e:
            self.view.show_message(f"Database error while deleting: {e.pgerror or str(e)}\n")

    def generate_data_sql(self):
        table = self.view.get_table_name()
        count = self.view.get_row_count()
        try:
            count = int(count)
            if count <= 0:
                raise ValueError("Count must be positive")
        except ValueError:
            self.view.show_message("Invalid number of rows.\n")
            return

        try:
            t0 = time.perf_counter()
            inserted = self.model.generate_rows_sql(table, count)
            t1 = time.perf_counter()
            self.view.show_message(f"Inserted approximately {inserted} rows into '{table}' in {int((t1-t0)*1000)} ms.\n")
        except Exception as e:
            self.view.show_message(f"Error during generation: {str(e)}\n")

    def multi_attribute_search(self):
        """
        Build a query that can JOIN up to two tables and apply multiple attribute filters.
        For each selected attribute the view asks what kind of filter to apply depending on its type.
        """
        self.view.show_message("Multi-attribute search. You may provide filters for attributes across two tables.")
        table1 = self.view.get_table_name(prompt="Enter primary table name: ")
        if not self.model.table_exists(table1):
            self.view.show_message(f"Table '{table1}' does not exist.\n")
            return

        table2 = self.view.get_table_name(prompt="Enter secondary table name to JOIN : ", allow_empty=True)
        if table2 and not self.model.table_exists(table2):
            self.view.show_message(f"Table '{table2}' does not exist. Aborting.\n")
            return

        filters = []
        col_types1 = self.model.get_column_types(table1)
        col_types2 = self.model.get_column_types(table2) if table2 else {}

        num = self.view.get_number_of_filters()
        for i in range(num):
            tbl_choice = self.view.choose_table_for_filter([table1] + ([table2] if table2 else []))
            if tbl_choice == table1:
                col = self.view.choose_column(table1, col_types1)
                ctype = col_types1[col]
            else:
                col = self.view.choose_column(table2, col_types2)
                ctype = col_types2[col]
            clause = self.view.build_filter_clause(tbl_choice, col, ctype)
            filters.append(clause)

        if table2:
            join_expr = self.model.find_join_expression(table1, table2)
            if not join_expr:
                self.view.show_message("No foreign key relation found between chosen tables. Using CROSS JOIN.\n")
                from_clause = f'"{table1}" CROSS JOIN "{table2}"'
                join_condition = ""
            else:
                from_clause = f'"{table1}" JOIN "{table2}" ON {join_expr}'
                join_condition = ""
        else:
            from_clause = f'"{table1}"'
        where_sql = " AND ".join(filters) if filters else "TRUE"
        sql_query = f'SELECT * FROM {from_clause} WHERE {where_sql} LIMIT 1000;'

        try:
            t0 = time.perf_counter()
            rows, cols = self.model.execute_raw_select(sql_query)
            t1 = time.perf_counter()
            self.view.display_query_result(cols, rows)
            self.view.show_message(f"Query executed in {int((t1-t0)*1000)} ms.\n")
        except Exception as e:
            self.view.show_message(f"Error executing search: {str(e)}\n")

    def run_prepared_reports(self):
        """
        Provide 3 prepared queries that join and group data across tables.
        Allow user to supply parameters for WHERE filters and display execution time.
        """
        reports = self.model.get_prepared_reports_templates()
        self.view.show_message("Prepared reports:")
        for i, (title, _) in enumerate(reports, start=1):
            self.view.show_message(f"{i}. {title}")
        choice = self.view.get_input("Choose report number: ")
        try:
            choice = int(choice)
            if not (1 <= choice <= len(reports)):
                raise ValueError
        except ValueError:
            self.view.show_message("Invalid choice.\n")
            return

        title, template = reports[choice - 1]
        params = self.view.ask_parameters_for_template(template)
        try:
            t0 = time.perf_counter()
            rows, cols = self.model.execute_prepared_select(template, params)
            t1 = time.perf_counter()
            self.view.display_query_result(cols, rows)
            self.view.show_message(f"Report '{title}' executed in {int((t1-t0)*1000)} ms.\n")
        except Exception as e:
            self.view.show_message(f"Error running report: {str(e)}\n")

    def exit_app(self):
        self.view.show_message("Goodbye!")
        self.model.close()
        raise SystemExit
