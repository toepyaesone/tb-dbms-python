# #!/usr/bin/env python3
# """
# Connects to the DHIS2 server, retrieves live metadata (Programs, Tracked Entity
# Attributes, Program Stages, Data Elements, Value Types, and Option Sets), and
# generates PostgreSQL table schemas for both Cloud Supabase and Local NocoDB.
# """

# from __future__ import annotations

# import json
# import os
# import re
# from typing import Any
# import pandas as pd
# import requests
# import urllib3
# from requests.adapters import HTTPAdapter
# from urllib3.util import Retry

# # Suppress SSL warnings for MOH training/production servers
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# # --- DHIS2 Server Credentials & Target Programs ---
# RAW_URL = os.getenv("DHIS2_BASE_URL", "https://mbdrtraining.moh.gov.mm/train/dhis-web-login/")
# USERNAME = os.getenv("DHIS2_USERNAME", "NTP_YTP_Central_S2")
# PASSWORD = os.getenv("DHIS2_PASSWORD", "District@1")

# PROGRAM_MAP: dict[str, dict[str, str]] = {
#     "UZF0HrTlps0": {
#         "name": "Registration & Screening",
#         "table_name": "dhis2_registration_screening",
#         "pk_mode": "upi",  # Deduplicated 1 row per Unique ID (UPI)
#     },
#     "GvywHD6crky": {
#         "name": "Diagnostic Evaluation",
#         "table_name": "dhis2_diagnostic_evaluation",
#         "pk_mode": "record_id",  # Multi-episode rows kept per UPI
#     },
#     "Lt6P15ps7f6": {
#         "name": "TB Surveillance",
#         "table_name": "dhis2_tb_surveillance",
#         "pk_mode": "record_id",  # Multi-episode rows kept per UPI
#     },
#     "cQsXTtAJ3HW": {
#         "name": "TB Contact Tracing and TPT",
#         "table_name": "dhis2_contact_tpt",
#         "pk_mode": "record_id",  # Multi-episode rows kept per UPI
#     },
# }

# # Map DHIS2 valueType -> PostgreSQL Data Type
# # Note: If multi-event pipe-concatenation ("val1 | val2") is used, TEXT is safest,
# # so we include both strict SQL types and TEXT-safe compatibility.
# DHIS2_TO_PG_TYPE: dict[str, str] = {
#     "TEXT": "TEXT",
#     "LONG_TEXT": "TEXT",
#     "LETTER": "VARCHAR(10)",
#     "PHONE_NUMBER": "VARCHAR(50)",
#     "EMAIL": "VARCHAR(255)",
#     "BOOLEAN": "TEXT",           # Kept TEXT-safe for "Yes | No" or uncoded labels
#     "TRUE_ONLY": "TEXT",
#     "DATE": "TEXT",              # Kept TEXT-safe for pipe-concatenated multi-event dates
#     "DATETIME": "TEXT",
#     "TIME": "TEXT",
#     "NUMBER": "TEXT",
#     "UNIT_INTERVAL": "TEXT",
#     "PERCENTAGE": "TEXT",
#     "INTEGER": "TEXT",
#     "INTEGER_POSITIVE": "TEXT",
#     "INTEGER_NEGATIVE": "TEXT",
#     "INTEGER_ZERO_OR_POSITIVE": "TEXT",
#     "ORGANISATION_UNIT": "TEXT",
#     "AGE": "TEXT",
# }


# def normalize_dhis2_url(url: str) -> str:
#     """Strips login paths like /dhis-web-login/ or /api to get the clean base URL."""
#     clean = (url or "").strip().rstrip("/")
#     clean = re.sub(r"/dhis-web-.*$", "", clean)
#     if clean.endswith("/api"):
#         clean = clean[:-4]
#     return clean


# def create_session(username: str, password: str) -> requests.Session:
#     session = requests.Session()
#     session.auth = (username, password)
#     session.headers.update({"Accept": "application/json"})
#     retries = Retry(
#         total=3,
#         backoff_factor=0.5,
#         status_forcelist=[429, 500, 502, 503, 504],
#     )
#     adapter = HTTPAdapter(max_retries=retries)
#     session.mount("https://", adapter)
#     session.mount("http://", adapter)
#     return session


# def safe_get(session: requests.Session, url: str, **kwargs) -> requests.Response:
#     try:
#         return session.get(url, timeout=30, **kwargs)
#     except requests.exceptions.SSLError:
#         session.verify = False
#         return session.get(url, verify=False, timeout=30, **kwargs)


# def to_snake_case(label: str, prefix: str = "", used_names: set[str] | None = None) -> str:
#     """Converts a DHIS2 displayName into a valid, unique PostgreSQL column identifier (<= 63 chars)."""
#     s = label.lower().strip()
#     # Replace common symbols with words
#     s = s.replace(">", "_gt_").replace("<", "_lt_").replace("%", "_pct_")
#     s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
#     if prefix:
#         p = re.sub(r"[^a-z0-9]+", "_", prefix.lower()).strip("_")
#         s = f"{p}_{s}"
#     if not s or s[0].isdigit():
#         s = f"col_{s}"

#     # PostgreSQL identifier limit is 63 bytes
#     base_col = s[:58]
#     final_col = base_col
#     if used_names is not None:
#         counter = 2
#         while final_col in used_names:
#             suffix = f"_{counter}"
#             final_col = f"{base_col[:63 - len(suffix)]}{suffix}"
#             counter += 1
#         used_names.add(final_col)
#     return final_col


# def make_stage_prefix(stage_name: str) -> str:
#     """Creates a short, readable table column prefix for each Program Stage."""
#     clean = re.sub(r"^\d+\.\s*", "", stage_name.strip())
#     words = [w for w in re.split(r"[^A-Za-z0-9]+", clean) if w]
#     if not words:
#         return "stg"
#     if len(words) == 1:
#         return words[0][:4].lower()
#     return "".join(w[0].lower() for w in words[:4])


# def fetch_program_schemas(base_url: str, username: str, password: str) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
#     session = create_session(username, password)

#     # 1. Verify authentication
#     me_res = safe_get(session, f"{base_url}/api/me.json?fields=id,username,name")
#     if me_res.status_code in (401, 403):
#         raise RuntimeError(f"Authentication failed ({me_res.status_code}). Check username/password.")
#     me_res.raise_for_status()
#     user_info = me_res.json()
#     print(f"✅ Authenticated to {base_url} as: {user_info.get('name')} ({user_info.get('username')})")

#     # 2. Fetch full metadata for the 4 target programs in a single API call
#     prog_ids = ",".join(PROGRAM_MAP.keys())
#     fields_param = (
#         "id,displayName,programType,"
#         "programTrackedEntityAttributes[mandatory,valueType,trackedEntityAttribute["
#         "id,code,displayName,valueType,unique,optionSet[id,displayName,options[code,displayName]]]],"
#         "programStages[id,displayName,repeatable,sortOrder,"
#         "programStageDataElements[compulsory,dataElement["
#         "id,code,displayName,valueType,optionSet[id,displayName,options[code,displayName]]]]]"
#     )
#     url = (
#         f"{base_url}/api/programs.json"
#         f"?filter=id:in:[{prog_ids}]"
#         f"&fields={fields_param}"
#         f"&paging=false"
#     )

#     res = safe_get(session, url)
#     res.raise_for_status()
#     programs_data = res.json().get("programs", [])
#     session.close()

#     if not programs_data:
#         raise RuntimeError("No matching programs found on the DHIS2 server.")

#     dictionary_rows: list[dict[str, Any]] = []
#     column_mapping: dict[str, dict[str, str]] = {}
#     sql_blocks: list[str] = [
#         "-- ============================================================================",
#         "-- AUTO-GENERATED POSTGRESQL SCHEMA FROM LIVE DHIS2 SERVER METADATA",
#         f"-- Server: {base_url}",
#         "-- Compatible with Cloud Supabase & Local NocoDB (PostgreSQL)",
#         "-- ============================================================================",
#         'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";\n',
#     ]

#     # Sort programs in clinical cascade order
#     prog_order = list(PROGRAM_MAP.keys())
#     programs_data.sort(key=lambda x: prog_order.index(x.get("id")) if x.get("id") in prog_order else 99)

#     for prog in programs_data:
#         prog_id = prog.get("id", "")
#         prog_cfg = PROGRAM_MAP.get(prog_id, {})
#         prog_name = prog_cfg.get("name") or prog.get("displayName") or prog_id
#         table_name = prog_cfg.get("table_name") or to_snake_case(prog_name, prefix="dhis2")
#         pk_mode = prog_cfg.get("pk_mode", "record_id")

#         used_sql_cols: set[str] = {
#             "record_id", "upi", "tracked_entity_instance", "enrollment_id",
#             "program_id", "program_name", "enrollment_date", "enrollment_status",
#             "org_unit_id", "org_unit_name", "created", "last_updated",
#             "stage_data_json", "synced_at",
#         }
#         prog_col_map: dict[str, str] = {
#             "Unique ID (UPI)": "upi",
#             "trackedEntityInstance": "tracked_entity_instance",
#             "enrollment_id": "enrollment_id",
#             "program_id": "program_id",
#             "program_name": "program_name",
#             "enrollment_date": "enrollment_date",
#             "enrollment_status": "enrollment_status",
#             "orgUnit_id": "org_unit_id",
#             "Org Unit Name": "org_unit_name",
#             "created": "created",
#             "lastUpdated": "last_updated",
#         }

#         sql_cols_ddl: list[str] = []
#         if pk_mode == "upi":
#             sql_cols_ddl.append("    upi VARCHAR(128) PRIMARY KEY, -- Unique ID (UPI)")
#         else:
#             sql_cols_ddl.append("    record_id VARCHAR(160) PRIMARY KEY, -- upi || '_' || enrollment_id")
#             sql_cols_ddl.append("    upi VARCHAR(128) NOT NULL, -- Unique ID (UPI)")

#         sql_cols_ddl.extend([
#             "    tracked_entity_instance VARCHAR(64),",
#             "    enrollment_id VARCHAR(64),",
#             f"    program_id VARCHAR(64) DEFAULT '{prog_id}',",
#             f"    program_name TEXT DEFAULT '{prog_name}',",
#             "    enrollment_date TEXT,",
#             "    enrollment_status VARCHAR(50),",
#             "    org_unit_id VARCHAR(64),",
#             "    org_unit_name TEXT,",
#             "    created TEXT,",
#             "    last_updated TEXT,",
#         ])

#         # --- Process Tracked Entity Attributes (Demographics / Registration Info) ---
#         ptea_list = prog.get("programTrackedEntityAttributes", []) or []
#         if ptea_list:
#             sql_cols_ddl.append("\n    -- Tracked Entity Attributes (Demographics & Identifiers)")

#         for ptea in ptea_list:
#             tea = ptea.get("trackedEntityAttribute") or {}
#             tea_id = tea.get("id", "")
#             tea_name = (tea.get("displayName") or tea_id).strip()
#             val_type = tea.get("valueType") or ptea.get("valueType") or "TEXT"
#             pg_type = DHIS2_TO_PG_TYPE.get(val_type, "TEXT")

#             # Map Unique ID (UPI) directly to the primary `upi` column
#             if tea_name == "Unique ID (UPI)":
#                 sql_col = "upi"
#             else:
#                 sql_col = to_snake_case(tea_name, prefix="attr", used_names=used_sql_cols)
#                 comment = tea_name.replace("'", "")
#                 sql_cols_ddl.append(f"    {sql_col} {pg_type}, -- {comment} ({tea_id} | {val_type})")

#             prog_col_map[tea_name] = sql_col

#             opt_set = tea.get("optionSet") or {}
#             options_preview = ", ".join(
#                 f"{o.get('code')}={o.get('displayName')}"
#                 for o in (opt_set.get("options") or [])[:10]
#             )

#             dictionary_rows.append({
#                 "program_id": prog_id,
#                 "program_name": prog_name,
#                 "table_name": table_name,
#                 "component_type": "TrackedEntityAttribute",
#                 "stage_id": "",
#                 "stage_name": "Registration Attributes",
#                 "uid": tea_id,
#                 "app_py_column_name": tea_name,
#                 "sql_column_name": sql_col,
#                 "dhis2_value_type": val_type,
#                 "postgres_type": pg_type,
#                 "mandatory": bool(ptea.get("mandatory")),
#                 "option_set_name": opt_set.get("displayName", ""),
#                 "options_sample": options_preview,
#             })

#         # --- Process Program Stages & Data Elements ---
#         stages = sorted(
#             prog.get("programStages", []) or [],
#             key=lambda s: s.get("sortOrder", 999),
#         )

#         for stage in stages:
#             stage_id = stage.get("id", "")
#             stage_name = (stage.get("displayName") or f"Stage_{stage_id}").strip()
#             stg_prefix = make_stage_prefix(stage_name)
#             psde_list = stage.get("programStageDataElements", []) or []

#             if psde_list:
#                 sql_cols_ddl.append(f"\n    -- Stage: [{stage_name}] ({stage_id})")

#             for psde in psde_list:
#                 de = psde.get("dataElement") or {}
#                 de_id = de.get("id", "")
#                 de_name = (de.get("displayName") or f"Element_{de_id}").strip()
#                 val_type = de.get("valueType") or "TEXT"
#                 pg_type = DHIS2_TO_PG_TYPE.get(val_type, "TEXT")

#                 # Exact column format produced by app.py: "[Stage Name] Data Element Name"
#                 app_py_col = f"[{stage_name}] {de_name}"
#                 sql_col = to_snake_case(de_name, prefix=stg_prefix, used_names=used_sql_cols)
#                 prog_col_map[app_py_col] = sql_col

#                 comment = app_py_col.replace("'", "")
#                 sql_cols_ddl.append(f"    {sql_col} {pg_type}, -- {comment} ({de_id} | {val_type})")

#                 opt_set = de.get("optionSet") or {}
#                 options_preview = ", ".join(
#                     f"{o.get('code')}={o.get('displayName')}"
#                     for o in (opt_set.get("options") or [])[:10]
#                 )

#                 dictionary_rows.append({
#                     "program_id": prog_id,
#                     "program_name": prog_name,
#                     "table_name": table_name,
#                     "component_type": "DataElement",
#                     "stage_id": stage_id,
#                     "stage_name": stage_name,
#                     "uid": de_id,
#                     "app_py_column_name": app_py_col,
#                     "sql_column_name": sql_col,
#                     "dhis2_value_type": val_type,
#                     "postgres_type": pg_type,
#                     "mandatory": bool(psde.get("compulsory")),
#                     "option_set_name": opt_set.get("displayName", ""),
#                     "options_sample": options_preview,
#                 })

#         sql_cols_ddl.extend([
#             "\n    -- Dynamic fallback JSONB & Sync Timestamp",
#             "    stage_data_json JSONB DEFAULT '{}'::jsonb,",
#             "    synced_at TIMESTAMPTZ DEFAULT NOW()",
#         ])

#         table_ddl = (
#             f"-- ============================================================================\n"
#             f"-- PROGRAM: {prog_name} (UID: {prog_id})\n"
#             f"-- Table: public.{table_name} ({len(prog_col_map)} mapped columns)\n"
#             f"-- ============================================================================\n"
#             f"CREATE TABLE IF NOT EXISTS public.{table_name} (\n"
#             + "\n".join(sql_cols_ddl)
#             + "\n);\n"
#             f"CREATE INDEX IF NOT EXISTS idx_{table_name}_upi ON public.{table_name}(upi);\n"
#             f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ou ON public.{table_name}(org_unit_id);\n"
#         )
#         sql_blocks.append(table_ddl)
#         column_mapping[table_name] = prog_col_map

#     return dictionary_rows, "\n".join(sql_blocks), column_mapping


# def main() -> None:
#     base_url = normalize_dhis2_url(RAW_URL)
#     print(f"🔍 Connecting to DHIS2 server: {base_url} ...")

#     out_dir = os.path.dirname(os.path.abspath(__file__))
#     sql_out_path = os.path.join(out_dir, "dhis2_auto_schema.sql")
#     json_out_path = os.path.join(out_dir, "dhis2_column_mapping.json")
#     csv_out_path = os.path.join(out_dir, "dhis2_schema_dictionary.csv")

#     dictionary_rows, sql_ddl, column_mapping = fetch_program_schemas(base_url, USERNAME, PASSWORD)

#     # Save SQL DDL
#     with open(sql_out_path, "w", encoding="utf-8") as f:
#         f.write(sql_ddl)

#     # Save JSON Mapping (app.py column -> SQL column per table)
#     with open(json_out_path, "w", encoding="utf-8") as f:
#         json.dump(column_mapping, f, indent=2, ensure_ascii=False)

#     # Save CSV Data Dictionary
#     df_dict = pd.DataFrame(dictionary_rows)
#     df_dict.to_csv(csv_out_path, index=False, encoding="utf-8-sig")

#     print("\n📊 Schema Extraction Summary by Program Module:")
#     summary = (
#         df_dict.groupby(["program_id", "program_name", "table_name", "component_type"])
#         .size()
#         .unstack(fill_value=0)
#         .reset_index()
#     )
#     print(summary.to_string(index=False))

#     print("\n✅ Generated Files:")
#     print(f"  1. SQL Table Schema (Supabase & NocoDB): {sql_out_path}")
#     print(f"  2. Column Map JSON (app.py -> Postgres): {json_out_path}")
#     print(f"  3. Full Metadata Dictionary (CSV):       {csv_out_path}")


# if __name__ == "__main__":
#     main()
