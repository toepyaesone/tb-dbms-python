# #!/usr/bin/env python3
# """
# Bi-Directional Sync Script: Synchronizes local NocoDB operational tables 
# with Cloud Supabase using the Supabase Service Role Key (bypassing auth network issues).
# """

# from __future__ import annotations

# import json
# import os
# from typing import Any

# import psycopg2
# from psycopg2.extras import RealDictCursor, execute_values
# from dotenv import load_dotenv
# from supabase import Client, create_client

# load_dotenv()

# # Use Service Role Key for server-side ETL & sync (bypasses RLS login issues)
# SUPABASE_URL = os.getenv("SUPABASE_URL", "")
# SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

# # Local Docker PostgreSQL Credentials
# PG_HOST = os.getenv("LOCAL_PG_HOST", "local-postgres")
# PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
# PG_DB = os.getenv("LOCAL_PG_DB", "tb_local_data")
# PG_USER = os.getenv("POSTGRES_USER", "tb_admin")
# PG_PASS = os.getenv("POSTGRES_PASSWORD", "StrongLocalPassword123!")

# DHIS2_TABLES: list[tuple[str, str]] = [
#     ("dhis2_registration_screening", "upi"),
#     ("dhis2_diagnostic_evaluation", "record_id"),
#     ("dhis2_tb_surveillance", "record_id"),
#     ("dhis2_contact_tpt", "record_id"),
#     ("dhis2_ygntbpro_summary", "upi"),
# ]

# LOCAL_OPERATIONAL_TABLES: list[str] = [
#     "local_tb_register",
#     "local_dots_tracking",
#     "local_ci_records",
#     "local_treatment_outcomes",
#     "local_health_education",
# ]


# def get_local_pg_conn():
#     return psycopg2.connect(
#         host=PG_HOST,
#         port=PG_PORT,
#         dbname=PG_DB,
#         user=PG_USER,
#         password=PG_PASS,
#     )


# def upsert_local_postgres(pg_conn, table_name: str, pk_col: str, rows: list[dict[str, Any]]) -> int:
#     if not rows:
#         return 0
#     cols = list(rows[0].keys())
#     col_names_sql = ", ".join(f'"{c}"' for c in cols)
#     update_cols_sql = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk_col)

#     values = [
#         tuple(json.dumps(r[c]) if isinstance(r[c], (dict, list)) else r[c] for c in cols)
#         for r in rows
#     ]

#     sql = f"""
#         INSERT INTO public.{table_name} ({col_names_sql})
#         VALUES %s
#         ON CONFLICT ("{pk_col}") DO UPDATE SET {update_cols_sql};
#     """
#     with pg_conn.cursor() as cur:
#         execute_values(cur, sql, values, page_size=250)
#     pg_conn.commit()
#     return len(rows)


# def pull_from_supabase_to_local(supabase: Client, pg_conn) -> None:
#     print("⬇️ Step 1: Pulling DHIS2 records from Cloud Supabase to Local NocoDB...")
#     for table_name, pk_col in DHIS2_TABLES:
#         try:
#             res = supabase.table(table_name).select("*").execute()
#             rows = res.data or []
#             count = upsert_local_postgres(pg_conn, table_name, pk_col, rows)
#             print(f"  ✓ Pulled {count} rows into local table: {table_name}")
#         except Exception as e:
#             print(f"  ⚠️ Warning when pulling {table_name}: {e}")


# def push_pending_local_to_supabase(supabase: Client, pg_conn) -> None:
#     print("⬆️ Step 2: Pushing pending Local NocoDB operational records to Cloud Supabase...")
#     for table_name in LOCAL_OPERATIONAL_TABLES:
#         try:
#             with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
#                 cur.execute(f"SELECT * FROM public.{table_name} WHERE sync_status = 'pending';")
#                 pending_rows = [dict(r) for r in cur.fetchall()]

#             if not pending_rows:
#                 print(f"  • {table_name}: 0 pending records")
#                 continue

#             payload: list[dict[str, Any]] = []
#             ids_to_mark_synced: list[str] = []

#             for r in pending_rows:
#                 row_copy = {}
#                 for k, v in r.items():
#                     if hasattr(v, "isoformat"):
#                         row_copy[k] = v.isoformat()
#                     elif v is not None and not isinstance(v, (str, int, float, bool, dict, list)):
#                         row_copy[k] = str(v)
#                     else:
#                         row_copy[k] = v
#                 row_copy["sync_status"] = "synced"
#                 payload.append(row_copy)
#                 ids_to_mark_synced.append(str(r["id"]))

#             # Upsert to Cloud Supabase using Service Role Key
#             supabase.table(table_name).upsert(payload, on_conflict="id").execute()

#             # Mark as 'synced' locally
#             with pg_conn.cursor() as cur:
#                 cur.execute(
#                     f"UPDATE public.{table_name} SET sync_status = 'synced' WHERE id::text = ANY(%s);",
#                     (ids_to_mark_synced,),
#                 )
#             pg_conn.commit()
#             print(f"  ✓ Pushed & marked synced: {len(payload)} rows from {table_name}")
#         except Exception as e:
#             print(f"  ⚠️ Warning when pushing {table_name}: {e}")


# def main() -> None:
#     if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
#         raise RuntimeError("Please configure SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in your .env file.")

#     print(f"🔐 Initializing Supabase Client with Service Role Key...")
#     supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

#     pg_conn = get_local_pg_conn()
#     try:
#         pull_from_supabase_to_local(supabase, pg_conn)
#         push_pending_local_to_supabase(supabase, pg_conn)
#     finally:
#         pg_conn.close()

#     print("🎉 Local NocoDB <-> Cloud Supabase Sync Complete!")


# if __name__ == "__main__":
#     main()
