import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

class Database:
    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=os.getenv("PG_DB", "postgres"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASS", "1234"),
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432"))
        )
        self.connection.autocommit = False

    def close(self):
        self.connection.close()
    def table_exists(self, table):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=%s
                );
            """, (table,))
            return cur.fetchone()[0]

    def get_tables(self):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            return [r[0] for r in cur.fetchall()]

    def get_columns(self, table):
        if not self.table_exists(table):
            return None
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name = %s
                ORDER BY ordinal_position;
            """, (table,))
            return cur.fetchall()

    def get_column_types(self, table):
        cols = self.get_columns(table)
        if cols is None:
            return None
        return {col: dtype for col, dtype in cols}

    def get_child_tables(self, parent_table):
        """Return list of tables with FKs referencing parent_table."""
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT tc.table_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                   ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                   ON ccu.constraint_name = tc.constraint_name
                WHERE constraint_type = 'FOREIGN KEY' AND ccu.table_name = %s;
            """, (parent_table,))
            return [r[0] for r in cur.fetchall()]
    def insert_entry_validated(self, table, values_dict, col_types):
        cols, vals, placeholders = [], [], []
        for col, dtype in col_types.items():
            if col not in values_dict:
                continue
            val = values_dict[col]
            if val is None or val == "":
                python_val = None
            else:
                python_val = self._cast_value_from_input(val, dtype)
            cols.append(sql.Identifier(col))
            vals.append(python_val)
            placeholders.append("%s")

        if not cols:
            raise ValueError("No columns to insert.")

        query = sql.SQL("INSERT INTO {table} ({cols}) VALUES ({ph})").format(
            table=sql.Identifier(table),
            cols=sql.SQL(', ').join(cols),
            ph=sql.SQL(', ').join(sql.SQL(p) for p in placeholders)
        )

        with self.connection.cursor() as cur:
            cur.execute(query, vals)
        self.connection.commit()
        return True

    def update_entry_validated(self, table, column, row_id, new_value, col_types):
        if column not in col_types:
            raise ValueError("Unknown column")
        dtype = col_types[column]
        casted = self._cast_value_from_input(new_value, dtype)
        id_col = self._find_pk_column(table)
        if id_col is None:
            raise ValueError("Primary key column not found for table")
        query = sql.SQL('UPDATE {table} SET {col} = %s WHERE {idcol} = %s').format(
            table=sql.Identifier(table),
            col=sql.Identifier(column),
            idcol=sql.Identifier(id_col)
        )
        with self.connection.cursor() as cur:
            cur.execute(query, (casted, row_id))
            affected = cur.rowcount
        self.connection.commit()
        return affected > 0

    def delete_entry(self, table, row_id):
        id_col = self._find_pk_column(table)
        if id_col is None:
            raise ValueError("Primary key not found")
        query = sql.SQL('DELETE FROM {table} WHERE {idcol} = %s').format(
            table=sql.Identifier(table),
            idcol=sql.Identifier(id_col)
        )
        with self.connection.cursor() as cur:
            cur.execute(query, (row_id,))
            affected = cur.rowcount
        self.connection.commit()
        return affected > 0

    def _find_pk_column(self, table):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tco
                JOIN information_schema.key_column_usage kcu
                  ON kcu.constraint_name = tco.constraint_name
                WHERE tco.constraint_type = 'PRIMARY KEY' AND kcu.table_name = %s;
            """, (table,))
            r = cur.fetchone()
            return r[0] if r else None

    def _cast_value_from_input(self, value, data_type):
        if value is None:
            return None
        dt = data_type.lower()
        s = value.strip()
        if s == "":
            return None
        if 'character' in dt or 'text' in dt:
            return s
        if dt.startswith('integer') or dt in ('bigint', 'smallint'):
            return int(s)
        if dt.startswith('numeric') or dt.startswith('double') or dt.startswith('real'):
            return float(s)
        if dt == 'boolean':
            if s.lower() in ('true', 't', '1', 'yes', 'y'):
                return True
            if s.lower() in ('false', 'f', '0', 'no', 'n'):
                return False
            raise ValueError("Invalid boolean value")
        if 'date' in dt or 'timestamp' in dt:
            return s
        return s

    def generate_rows_sql(self, table, count):
        """
        Mass generation of rows in a table via SQL taking into account FK and serial PK.
        Serial PK and columns with DEFAULT are skipped.
        """
        cols = self.get_columns(table)
        if not cols:
            raise ValueError("Table not found or has no columns")

        fk_map = {} 
        insertable_cols = []

        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' AND kcu.table_name = %s;
            """, (table,))
            for col, ftable, fcol in cur.fetchall():
                fk_map[col] = (ftable, fcol)

            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public'
                AND table_name=%s
                AND is_nullable='NO'
                AND column_default IS NULL;
            """, (table,))
            not_null_cols = [r[0] for r in cur.fetchall()]

        col_names = []
        select_exprs = []

        for col_name, data_type in cols:
            dt = data_type.lower()
            if col_name not in not_null_cols:
                continue
            col_names.append(sql.Identifier(col_name))
            if col_name in fk_map:
                ftable, fcol = fk_map[col_name]
                expr = sql.SQL("(SELECT {fcol} FROM {ft} ORDER BY random() LIMIT 1)").format(
                    fcol=sql.Identifier(fcol),
                    ft=sql.Identifier(ftable)
                )
            elif 'email' in col_name.lower():
                expr = sql.SQL("substring(md5(random()::text),1,7) || '@gmail.com'")
            elif dt.startswith('integer') or dt in ('bigint','smallint'):
                expr = sql.SQL("floor(random()*10000)::int")
            elif dt.startswith('numeric') or dt in ('real','double precision'):
                expr = sql.SQL("round(random()*10000::numeric,2)")
            elif dt == 'boolean':
                expr = sql.SQL("(random() > 0.5)")
            elif 'timestamp' in dt:
                expr = sql.SQL("timestamp '2020-01-01' + random() * (timestamp '2025-12-31' - timestamp '2020-01-01')")
            elif 'date' in dt:
                expr = sql.SQL("date '2020-01-01' + (random() * 2000)::int")
            elif 'character' in dt or 'text' in dt:
                expr = sql.SQL("substring(md5(random()::text),1,10)")
            else:
                expr = sql.SQL("NULL")

            select_exprs.append(expr)

        if not col_names:
            raise ValueError("No columns to insert (all are PK/DEFAULT)")

        query = sql.SQL("INSERT INTO {table} ({cols}) SELECT {exprs} FROM generate_series(1, %s)").format(
            table=sql.Identifier(table),
            cols=sql.SQL(", ").join(col_names),
            exprs=sql.SQL(", ").join(select_exprs)
        )

        try:
            with self.connection.cursor() as cur:
                cur.execute(query, (count,))
            self.connection.commit()
            return count
        except Exception as e:
            self.connection.rollback()
            raise e

    def find_join_expression(self, t1, t2):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                  kcu.table_name AS child_table,
                  kcu.column_name AS child_column,
                  ccu.table_name AS parent_table,
                  ccu.column_name AS parent_column
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND ((kcu.table_name = %s AND ccu.table_name = %s)
                       OR (kcu.table_name = %s AND ccu.table_name = %s));
            """, (t1, t2, t2, t1))
            r = cur.fetchone()
            if not r:
                return None
            child_table, child_col, parent_table, parent_col = r
            return f'"{child_table}"."{child_col}" = "{parent_table}"."{parent_col}"'

    def execute_raw_select(self, query):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
            cols = list(rows[0].keys()) if rows else []
            return [tuple(r.values()) for r in rows], cols

    def get_prepared_reports_templates(self):
        reports = []
        sql1 = """
            SELECT 
                r.route_id,
                r.departurep,
                r.arrivalp,
                COUNT(b.booking_id) AS total_bookings
            FROM "route" r
            JOIN "booking" b ON b.route_id = r.route_id
            WHERE TO_DATE(b.bookingdate, 'DD.MM.YYYY') BETWEEN %s AND %s
            GROUP BY r.route_id, r.departurep, r.arrivalp
            ORDER BY total_bookings DESC
            LIMIT 100;
        """

        reports.append(("Number of bookings for routes in a given date range", sql1))

        sql2 = """
            SELECT 
                bu.buyer_id,
                bu."Surname",
                COUNT(DISTINCT b.booking_id) AS total_bookings,
                COUNT(p.payment_id) AS payments_count,
                SUM(COALESCE(CAST(p.sum AS numeric), 0)) AS total_paid
            FROM "buyer" bu
            JOIN "booking" b ON b.buyer_id = bu.buyer_id
            LEFT JOIN "payment" p ON p.booking_id = b.booking_id
            WHERE CAST(p.sum AS numeric) >= %s
            GROUP BY bu.buyer_id, bu."Surname"
            HAVING SUM(COALESCE(CAST(p.sum AS numeric), 0)) > %s
            ORDER BY total_paid DESC
            LIMIT 100;
        """

        reports.append(("Buyers with payments exceeding a certain amount", sql2))

        sql3 = """
            SELECT 
                r.route_id,
                r.departurep,
                r.arrivalp,
                COUNT(DISTINCT b.booking_id) AS bookings,
                SUM(COALESCE(CAST(p.sum AS numeric), 0)) AS total_sum,
                AVG(COALESCE(CAST(p.sum AS numeric), 0)) AS avg_payment
            FROM "route" r
            JOIN "booking" b ON b.route_id = r.route_id
            LEFT JOIN "payment" p ON p.booking_id = b.booking_id
            WHERE r.departurep ILIKE %s
            AND r.arrivalp ILIKE %s
            AND CAST(p.sum AS numeric) >= %s
            GROUP BY r.route_id, r.departurep, r.arrivalp
            ORDER BY avg_payment DESC
            LIMIT 100;
        """
        reports.append(("Average payments by route by departure/arrival filters", sql3))

        return reports

    def execute_prepared_select(self, template_sql, params):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(template_sql, params)
            rows = cur.fetchall()
            cols = list(rows[0].keys()) if rows else []
            return [tuple(r.values()) for r in rows], cols
