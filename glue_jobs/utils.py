import traceback
import psycopg2
import logging

log = logging.getLogger("omniroute-processor")

def execute_postgres_upsert(df, jdbc_url, staging_table, upsert_sql, pg_connect_kwargs, jdbc_props):
    """
    Shared utility for the 3-step idempotent Postgres write pattern:
    1. Truncate staging table
    2. Spark JDBC append to staging
    3. Execute UPSERT (INSERT ... ON CONFLICT) from staging to target
    """
    if df.rdd.isEmpty():
        print(f"No rows to sync for {staging_table}.")
        return

    # Step 1: Truncate staging
    conn, cur = None, None
    try:
        conn = psycopg2.connect(**pg_connect_kwargs)
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {staging_table};")
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"[execute_postgres_upsert] TRUNCATE of staging table '{staging_table}' failed: {e}"
        print(error_msg)
        log.error(error_msg)
        print("Detailed Error Traceback:")
        print(traceback.format_exc())
        log.error(traceback.format_exc())
        raise
    finally:
        if cur: cur.close()
        if conn: conn.close()

    # Step 2: Spark JDBC append to staging
    df.write.mode("append").jdbc(url=jdbc_url, table=staging_table, properties=jdbc_props)

    # Step 3: UPSERT staging -> target
    conn, cur = None, None
    try:
        conn = psycopg2.connect(**pg_connect_kwargs)
        cur = conn.cursor()
        cur.execute(upsert_sql)
        conn.commit()
        print(f"Postgres UPSERT complete for {staging_table}")
    except Exception as e:
        if conn: conn.rollback()
        error_msg = f"Postgres UPSERT failed for {staging_table}: {e}"
        print(error_msg)
        log.error(error_msg)
        print("Detailed Error Traceback:")
        print(traceback.format_exc())
        log.error(traceback.format_exc())
        raise
    finally:
        if cur: cur.close()
        if conn: conn.close()
