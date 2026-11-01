# #!/usr/bin/env python3
# """
# Optimized Automated ETL Script: Extracts raw DHIS2 instances using parallel page and
# program workers, decodes metadata using uncoded caching, maps columns via schema_mapper,
# and batch upserts into Cloud Supabase and Local NocoDB with robust timeout handling.
# """

# from __future__ import annotations

# import json
# import os
# import re
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from datetime import datetime, timezone
# from typing import Any

# import requests
# import urllib3
# from dotenv import load_dotenv
# from requests.adapters import HTTPAdapter
# from supabase import Client, create_client
# from urllib3.util import Retry

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# load_dotenv()

# RAW_URL = os.getenv("DHIS2_BASE_URL", "https://mbdrtraining.moh.gov.mm/train")
# USERNAME = os.getenv("DHIS2_USERNAME", "NTP_YTP_Central_S2")
# PASSWORD = os.getenv("DHIS2_PASSWORD", "District@1")

# SUPABASE_URL = os.getenv("SUPABASE_URL", "")
# SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

# REGISTRATION_PROGRAM_ID = "UZF0HrTlps0"
# DIAGNOSIS_PROGRAM_ID = "GvywHD6crky"
# SURVEILLANCE_PROGRAM_ID = "Lt6P15ps7f6"
# CONTACT_PROGRAM_ID = "cQsXTtAJ3HW"

# PROGRAM_MAP: dict[str, str] = {
#     "Registration & Screening": REGISTRATION_PROGRAM_ID,
#     "Diagnostic Evaluation": DIAGNOSIS_PROGRAM_ID,
#     "TB Surveillance": SURVEILLANCE_PROGRAM_ID,
#     "TB Contact Tracing and TPT": CONTACT_PROGRAM_ID,
# }
# PROGRAM_ID_TO_LABEL: dict[str, str] = {v: k for k, v in PROGRAM_MAP.items()}

# TOWNSHIP_OU_MAP: dict[str, list[str]] = {
#     "12. YTP/Pyi Gyi Khin (Hlaing)": ["YGCPqlBamwD", "KqjORlUe8Yc"],
#     "12. YTP/Pyi Gyi Khin (Kyeemyindaing)": ["dE0upZMc4xG", "rTTCKrLpxTB"],
#     "12. YTP/Pyi Gyi Khin (Dagon Myothit (South))": ["mCgl8tnCaee", "XHz6CPxTAbR"],
#     "12. YTP/Pyi Khin (Thingangyun)": ["lJ9AHubJTjg", "aBfPB9AwbF5"],
#     "12. YTP/Pyi Gyi Khin (South Okkalapa)": ["KSRVZsryAKz", "OeZsFpNKLP5"],
#     "12. YTP/Pyi Gyi Khin (Mayangone)": ["bctwifUcioE", "mPwLv1cjror"],
#     "12. YTP/Pyi Khin (Shwepyithar)": ["Z7Qf4wgtHz3", "aMAEOgli6W8"],
# }

# UID_REGEX = re.compile(r"^[A-Za-z][A-Za-z0-9]{10}$")
# NULL_STRINGS = {"", "nan", "None", "NaN", "<NA>", "null"}

# LEGACY_FIELDS = (
#     "trackedEntityInstance,orgUnit,orgUnitName,created,lastUpdated,"
#     "attributes[attribute,displayName,value],"
#     "enrollments[enrollment,program,orgUnit,orgUnitName,enrolledAt,enrollmentDate,occurredAt,status,"
#     "events[event,programStage,occurredAt,eventDate,created,status,dataValues[dataElement,value]]]"
# )

# MODERN_FIELDS = (
#     "trackedEntity,orgUnit,orgUnitName,createdAt,updatedAt,"
#     "attributes[attribute,displayName,value],"
#     "enrollments[enrollment,program,orgUnit,orgUnitName,enrolledAt,occurredAt,status,"
#     "events[event,programStage,occurredAt,createdAt,status,dataValues[dataElement,value]]]"
# )


# def normalize_base_url(url: str) -> str:
#     clean = (url or "").strip().rstrip("/")
#     if clean.endswith("/api"):
#         clean = clean[:-4]
#     return clean

# # Alias so both function names work seamlessly
# normalize_dhis2_url = normalize_base_url


# def create_http_session(username: str, password: str, verify_ssl: bool = True) -> requests.Session:
#     session = requests.Session()
#     session.auth = (username, password)
#     session.verify = verify_ssl
#     session.headers.update({"Accept": "application/json", "User-Agent": "DHIS2-ETL-Script/8.0"})
#     retries = Retry(total=3, connect=2, read=2, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
#     adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=retries)
#     session.mount("https://", adapter)
#     session.mount("http://", adapter)
#     return session


# def safe_get(session: requests.Session, url: str, **kwargs) -> requests.Response:
#     kwargs.setdefault("timeout", 60)
#     try:
#         return session.get(url, **kwargs)
#     except requests.exceptions.SSLError:
#         session.verify = False
#         kwargs["verify"] = False
#         return session.get(url, **kwargs)


# def fetch_metadata(base_url: str, username: str, password: str) -> tuple[dict[str, str], dict[str, dict[str, str]], bool, bool]:
#     session = create_http_session(username, password, verify_ssl=True)
#     meta_map: dict[str, str] = {}
#     option_value_map: dict[str, dict[str, str]] = {}

#     try:
#         me_res = safe_get(session, f"{base_url}/api/me.json?fields=id,username", timeout=30)
#         if me_res.status_code in (401, 403):
#             raise RuntimeError(f"Authentication failed ({me_res.status_code}). Check DHIS2 username and password.")
#     except requests.exceptions.RequestException as e:
#         print(f"  ⚠️ Warning: /api/me.json check skipped due to timeout/network lag ({e}). Proceeding with API extraction...")

#     verify_ssl = bool(session.verify)

#     for t_name, uids in TOWNSHIP_OU_MAP.items():
#         for uid in uids:
#             meta_map[uid] = t_name

#     endpoints = {
#         "trackedEntityAttributes": f"{base_url}/api/trackedEntityAttributes.json?fields=id,displayName,optionSet[id,options[code,name,displayName]]&paging=false",
#         "dataElements": f"{base_url}/api/dataElements.json?fields=id,displayName,optionSet[id,options[code,name,displayName]]&paging=false",
#         "programStages": f"{base_url}/api/programStages.json?fields=id,displayName&paging=false",
#         "programs": f"{base_url}/api/programs.json?fields=id,displayName&paging=false",
#         "organisationUnits": f"{base_url}/api/organisationUnits.json?fields=id,code,displayName&paging=false",
#     }

#     def _fetch_meta(resource_url: tuple[str, str]) -> tuple[str, list[dict]]:
#         resource, url = resource_url
#         try:
#             res = safe_get(session, url, timeout=60)
#             if res.ok:
#                 return resource, res.json().get(resource, [])
#         except Exception:
#             pass
#         return resource, []

#     with ThreadPoolExecutor(max_workers=3) as executor:
#         for resource, items in executor.map(_fetch_meta, endpoints.items()):
#             for item in items:
#                 uid = item.get("id")
#                 if not uid:
#                     continue
#                 display_name = item.get("displayName") or item.get("code") or uid
#                 meta_map[uid] = display_name
#                 opt_set = item.get("optionSet")
#                 if opt_set and isinstance(opt_set, dict):
#                     field_opts: dict[str, str] = {}
#                     for opt in opt_set.get("options", []) or []:
#                         if not isinstance(opt, dict):
#                             continue
#                         opt_code = str(opt.get("code", "")).strip()
#                         opt_label = opt.get("displayName") or opt.get("name") or opt_code
#                         if opt_code:
#                             field_opts[opt_code] = opt_label
#                     if field_opts:
#                         option_value_map[uid] = field_opts

#     is_legacy = True
#     try:
#         test_res = safe_get(session, f"{base_url}/api/trackedEntityInstances.json", params={"program": REGISTRATION_PROGRAM_ID, "ouMode": "ACCESSIBLE", "pageSize": 1}, timeout=30)
#         if test_res.status_code in (404, 410):
#             is_legacy = False
#     except Exception:
#         pass

#     session.close()
#     return meta_map, option_value_map, is_legacy, verify_ssl


# def _fetch_single_page(session: requests.Session, active_endpoint: str, is_legacy: bool, prog: str, ou: str | None, ou_mode: str, page: int, page_size: int = 500) -> list[dict]:
#     params = {"program": prog, "ouMode": ou_mode, "pageSize": page_size, "page": page, "totalPages": "false", "fields": LEGACY_FIELDS if is_legacy else MODERN_FIELDS}
#     if ou:
#         params["ou" if is_legacy else "orgUnit"] = ou
#     try:
#         response = safe_get(session, active_endpoint, params=params, timeout=60)
#         if not response.ok:
#             return []
#         data = response.json()
#         instances = data.get("trackedEntityInstances") or data.get("instances") or data.get("trackedEntities") or []
#         for inst in instances:
#             inst["_queried_program"] = prog
#         return instances
#     except Exception:
#         return []


# def fetch_raw_program_pages(session: requests.Session, active_endpoint: str, is_legacy: bool, prog: str, ou: str | None = None, ou_mode: str = "ACCESSIBLE") -> list[dict]:
#     page_size = 500
#     first_page = _fetch_single_page(session, active_endpoint, is_legacy, prog, ou, ou_mode, 1, page_size)
#     if not first_page:
#         return []
#     if len(first_page) < page_size:
#         return first_page

#     all_instances = list(first_page)
#     _fetch_page = partial(_fetch_single_page, session, active_endpoint, is_legacy, prog, ou, ou_mode, page_size=page_size)

#     batch_size = 3
#     page = 2
#     while True:
#         pages_in_batch = list(range(page, page + batch_size))
#         with ThreadPoolExecutor(max_workers=batch_size) as page_executor:
#             batch_results = list(page_executor.map(_fetch_page, pages_in_batch))

#         got_last_page = False
#         for result in batch_results:
#             if not result:
#                 got_last_page = True
#                 break
#             all_instances.extend(result)
#             if len(result) < page_size:
#                 got_last_page = True
#                 break
#         if got_last_page:
#             break
#         page += batch_size

#     return all_instances


# def build_uncoded_df_dhis2(data_dhis: list[dict], meta_map: dict[str, str], option_value_map: dict[str, dict[str, str]]) -> pd.DataFrame:
#     local_meta = dict(meta_map)
#     uncoded_rows: list[dict[str, Any]] = []
#     _prog_label = PROGRAM_ID_TO_LABEL
#     _meta_get = local_meta.get
#     _opt_map = option_value_map
#     _uid_match = UID_REGEX.match

#     for instance in data_dhis:
#         prog = instance.get("_queried_program") or ""
#         prog_name = _prog_label.get(prog) or _meta_get(prog, prog)
#         tei_id = instance.get("trackedEntityInstance") or instance.get("trackedEntity")
#         tei_ou_id = instance.get("orgUnit")
#         if tei_ou_id and (ou_name_raw := instance.get("orgUnitName")):
#             local_meta[tei_ou_id] = ou_name_raw
#         tei_ou_name = instance.get("orgUnitName") or _meta_get(tei_ou_id, tei_ou_id)

#         base_attrs: dict[str, Any] = {}
#         for attr in instance.get("attributes", []) or []:
#             attr_id = attr.get("attribute") or ""
#             val = attr.get("value")
#             if val is None:
#                 continue
#             val_str = str(val).strip()
#             if not val_str or val_str in NULL_STRINGS:
#                 continue
#             if attr_id in _opt_map:
#                 decoded_val = _opt_map[attr_id].get(val_str, val_str)
#             elif _uid_match(val_str) and val_str in local_meta:
#                 decoded_val = local_meta[val_str]
#             else:
#                 decoded_val = val_str
#             col_name = attr.get("displayName") or _meta_get(attr_id) or attr_id
#             base_attrs[col_name] = decoded_val

#         enrollments = [enr for enr in (instance.get("enrollments") or []) if enr.get("program") == prog]
#         inst_created = instance.get("created") or instance.get("createdAt")
#         inst_updated = instance.get("lastUpdated") or instance.get("updatedAt")

#         if not enrollments:
#             row = {
#                 **base_attrs,
#                 "program_id": prog,
#                 "program_name": prog_name,
#                 "trackedEntityInstance": tei_id,
#                 "enrollment_id": f"no_enr_{tei_id}",
#                 "orgUnit_id": tei_ou_id,
#                 "tei_orgUnit_id": tei_ou_id,
#                 "Org Unit Name": _meta_get(tei_ou_id, tei_ou_name),
#                 "created": inst_created,
#                 "lastUpdated": inst_updated,
#             }
#             uncoded_rows.append(row)
#         else:
#             for enrollment in enrollments:
#                 enr_id = enrollment.get("enrollment")
#                 enr_ou_id = enrollment.get("orgUnit") or tei_ou_id
#                 enr_ou_name = enrollment.get("orgUnitName") or _meta_get(enr_ou_id) or _meta_get(tei_ou_id) or enr_ou_id
#                 raw_enr_date = enrollment.get("enrolledAt") or enrollment.get("enrollmentDate") or enrollment.get("occurredAt")
#                 enr_date_clean = str(raw_enr_date)[:10] if raw_enr_date else None

#                 row = {
#                     **base_attrs,
#                     "program_id": prog,
#                     "program_name": prog_name,
#                     "trackedEntityInstance": tei_id,
#                     "enrollment_id": enr_id,
#                     "orgUnit_id": enr_ou_id,
#                     "tei_orgUnit_id": tei_ou_id,
#                     "Org Unit Name": enr_ou_name,
#                     "created": inst_created,
#                     "lastUpdated": inst_updated,
#                     "enrollment_date": enr_date_clean,
#                     "enrollment_status": enrollment.get("status"),
#                 }

#                 for event in enrollment.get("events") or []:
#                     stage_id = event.get("programStage") or ""
#                     stage_name = _meta_get(stage_id, f"Stage_{stage_id}")
#                     for dv in event.get("dataValues") or []:
#                         de_id = dv.get("dataElement") or ""
#                         val = dv.get("value")
#                         if val is None:
#                             continue
#                         val_str = str(val).strip()
#                         if not val_str or val_str in NULL_STRINGS:
#                             continue
#                         if de_id in _opt_map:
#                             decoded_val = _opt_map[de_id].get(val_str, val_str)
#                         elif _uid_match(val_str) and val_str in local_meta:
#                             decoded_val = local_meta[val_str]
#                         else:
#                             decoded_val = val_str
#                         de_name = _meta_get(de_id, f"Element_{de_id}")
#                         col_key = f"[{stage_name}] {de_name}"
#                         row[col_key] = decoded_val

#                 uncoded_rows.append(row)

#     return pd.DataFrame(uncoded_rows)


# def batch_upsert_supabase(supabase: Client, table_name: str, pk_col: str, records: list[dict[str, Any]], batch_size: int = 250) -> int:
#     if not records:
#         return 0
#     total = 0
#     for i in range(0, len(records), batch_size):
#         chunk = records[i : i + batch_size]
#         supabase.table(table_name).upsert(chunk, on_conflict=pk_col).execute()
#         total += len(chunk)
#     return total


# def main() -> None:
#     if not SUPABASE_URL or not SUPABASE_SERVICE_KEY or "your-project-id" in SUPABASE_URL:
#         raise RuntimeError("Please configure SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in ~/tb-dbms/.env")

#     base_url = normalize_dhis2_url(RAW_URL)
#     mapping_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dhis2_column_mapping.json")
#     if not os.path.exists(mapping_path):
#         raise FileNotFoundError(f"Missing {mapping_path}. Run generate_dhis2_schema.py first.")

#     with open(mapping_path, "r", encoding="utf-8") as f:
#         full_column_mapping = json.load(f)

#     print(f"🔄 Connecting and fetching metadata from {base_url}...")
#     meta_map, option_value_map, is_legacy, verify_ssl = fetch_metadata(base_url, USERNAME, PASSWORD)
#     active_endpoint = f"{base_url}/api/trackedEntityInstances.json" if is_legacy else f"{base_url}/api/tracker/trackedEntities.json"

#     session = create_http_session(USERNAME, PASSWORD, verify_ssl=verify_ssl)
#     supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

#     all_configured_ous = list({ou for ous in TOWNSHIP_OU_MAP.values() for ou in ous})

#     program_configs = [
#         (REGISTRATION_PROGRAM_ID, "dhis2_registration_screening", "upi"),
#         (DIAGNOSIS_PROGRAM_ID, "dhis2_diagnostic_evaluation", "record_id"),
#         (SURVEILLANCE_PROGRAM_ID, "dhis2_tb_surveillance", "record_id"),
#         (CONTACT_PROGRAM_ID, "dhis2_contact_tpt", "record_id"),
#     ]

#     for prog_id, table_name, pk_col in program_configs:
#         print(f"📥 Extracting program {prog_id} into {table_name}...")
#         prog_instances = []
#         for ou in all_configured_ous:
#             ou_instances = fetch_raw_program_pages(session, active_endpoint, is_legacy, prog_id, ou=ou, ou_mode="DESCENDANTS")
#             if ou_instances:
#                 prog_instances.extend(ou_instances)

#         if not prog_instances:
#             print(f"  • No records found for program {prog_id}")
#             continue

#         df_prog = build_uncoded_df_dhis2(prog_instances, meta_map, option_value_map)
#         col_map = full_column_mapping.get(table_name, {})

#         records = []
#         for row in df_prog.to_dict(orient="records"):
#             sql_row = {}
#             extra_stage = {}
#             for k, v in row.items():
#                 if v is None or pd.isna(v) or str(v).strip() in NULL_STRINGS:
#                     continue
#                 v_str = str(v).strip()
#                 sql_col = col_map.get(k)
#                 if sql_col:
#                     sql_row[sql_col] = v_str
#                 elif k.startswith("["):
#                     extra_stage[k] = v_str

#             upi = sql_row.get("upi")
#             if not upi:
#                 continue

#             sql_row["stage_data_json"] = extra_stage
#             if pk_col == "upi":
#                 records.append(sql_row)
#             else:
#                 enr_id = sql_row.get("enrollment_id") or "1"
#                 sql_row["record_id"] = f"{upi}_{enr_id}"[:160]
#                 records.append(sql_row)

#         upserted = batch_upsert_supabase(supabase, table_name, pk_col, records)
#         print(f"✅ Upserted {upserted} records into public.{table_name}")

#     session.close()
#     print("🎉 Optimized DHIS2 -> Cloud Supabase ETL completed successfully!")


# if __name__ == "__main__":
#     main()
