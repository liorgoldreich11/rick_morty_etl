import snowflake.connector
import os
from scripts import ingest

DRY_RUN = False
conn = snowflake.connector.connect(
    user=os.environ.get('SF_USER'),
    password=os.environ.get('SF_PASSWORD'),
    account=os.environ.get('SF_ACCOUNT'),
    warehouse="RICKMORTY_WH",
    database="RICKMORTY_DB",
)


def run_sql_file(conn, path, dry_run=False):
    """Execute a SQL script file, splitting statements by semicolon."""
    try:
        with open(path, "r") as f:
            sql = f.read()
        statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
        print(f"Executing {len(statements)} statements from {path}...")
        with conn.cursor() as cur:
            for stmt in statements:
                if dry_run:
                    print(stmt)
                else:
                    cur.execute(stmt)
        conn.commit()
        print(f"Finished executing {path}")
    except Exception as e:
        conn.rollback()
        print(f"Error executing {path}: {e}")
        raise


if __name__ == '__main__':
    try:
        print("=== Step 1: Run DDL to create warehouse, schemas, and tables ===")
        run_sql_file(conn=conn, path="sql/ddl.sql", dry_run=DRY_RUN)

        print("=== Step 2: Ingest raw JSON from API ===")
        ingest.ingest_endpoint(conn, ingest.URL_CHAR, "CHARACTERS_RAW")
        ingest.ingest_endpoint(conn, ingest.URL_EP, "EPISODES_RAW")

        print("=== Step 3: Transform and load staging -> MODEL tables ===")
        run_sql_file(conn=conn, path="sql/transform.sql", dry_run=DRY_RUN)

        print("=== Step 4: Run Tests ===")
        run_sql_file(conn=conn, path="sql/tests.sql", dry_run=DRY_RUN)

        print("🎉 Pipeline completed successfully!")
    finally:
        conn.close()
