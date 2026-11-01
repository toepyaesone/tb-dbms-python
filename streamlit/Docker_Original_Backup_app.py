import os
import streamlit as st
import sqlalchemy as sa

st.set_page_config(page_title="TB DBMS Portal", layout="wide")
st.title("🏥 TB Program Data Verification & Analytics Portal")

st.markdown("""
This portal runs on your local **MacBook Air M1 Docker stack** and connects to:
* **Local PostgreSQL (`tb_local_data`)**: For NocoDB operational records (CI, DOTS, TB Register, Outcome, Health Education).
* **Cloud Supabase**: For multi-clinic RLS-protected data & DHIS2 baseline records (Registration, Screening, Diagnosis).
""")

# Test Local Database Connection
pg_user = os.getenv("POSTGRES_USER", "tb_admin")
pg_pass = os.getenv("POSTGRES_PASSWORD", "StrongLocalPassword123!")
db_url = f"postgresql://{pg_user}:{pg_pass}@local-postgres:5432/tb_local_data"

try:
    engine = sa.create_engine(db_url)
    with engine.connect() as conn:
        st.success("✅ Successfully connected to Local PostgreSQL (`tb_local_data`)!")
except Exception as e:
    st.error(f"❌ Local PostgreSQL connection error: {e}")
