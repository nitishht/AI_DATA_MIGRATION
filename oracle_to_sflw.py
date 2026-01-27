import os
import json
import re
import time
from typing import List, Tuple, Optional

import oracledb
import pandas as pd

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from openai import OpenAI

from dotenv import load_dotenv
load_dotenv()

# ----------------------------
# CONFIG
# ----------------------------

# Oracle (same as your current script)
ORACLE_CLIENT_PATH = r"C:\Users\AD739BB\Downloads\instantclient-basic-windows.x64-23.26.0.0.0\instantclient_23_0"

SRC_DB = {
    "user": "SRC_DW1",
    "password": "SRC_DW1",
    "dsn": "127.0.0.1:1521/XE"
}

# Snowflake target
SNOWFLAKE = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
    "database": os.getenv("SNOWFLAKE_DATABASE", ""),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
}

# LLM
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
# Put your incubator key in env var (do not hardcode)
# export OPENAI_API_KEY="..."
# OpenAI SDK reads OPENAI_API_KEY by default.  [1](https://pypi.org/project/openai/)

BATCH_SIZE = 5000

# If true: create/replace tables each run (drops prior data)
CREATE_OR_REPLACE = True

# Optional: only migrate a subset of tables
TABLE_FILTER: Optional[List[str]] = None


# ----------------------------
# INIT ORACLE CLIENT (THICK)
# ----------------------------
oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_PATH)


# ----------------------------
# HELPERS
# ----------------------------

IDENT_OK = re.compile(r"^[A-Z_][A-Z0-9_]*$")

def sf_ident(name: str) -> str:
    """
    Safe Snowflake identifier.
    If simple uppercase identifier -> unquoted (case-insensitive).
    Otherwise -> quoted.
    """
    name_u = name.upper()
    if IDENT_OK.match(name_u):
        return name_u
    # escape quotes if needed
    return f'"{name.replace(chr(34), chr(34)*2)}"'

def oracle_columns_metadata(src_cur, table: str) -> List[Tuple]:
    """
    Returns list of:
    (column_name, data_type, data_length, data_precision, data_scale, nullable)
    """
    src_cur.execute("""
        SELECT column_name,
               data_type,
               data_length,
               data_precision,
               data_scale,
               nullable
        FROM user_tab_columns
        WHERE table_name = :tbl
        ORDER BY column_id
    """, tbl=table)
    return src_cur.fetchall()

def oracle_tables(src_cur) -> List[str]:
    src_cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
    tables = [r[0] for r in src_cur.fetchall()]
    if TABLE_FILTER:
        allow = set([t.upper() for t in TABLE_FILTER])
        tables = [t for t in tables if t.upper() in allow]
    return tables

def fallback_oracle_to_snowflake_type(dtype: str, length, precision, scale) -> str:
    """
    Deterministic fallback mapping aligned with Snowflake’s Oracle type equivalences. [3](https://docs.snowflake.com/en/migrations/snowconvert-docs/translation-references/oracle/basic-elements-of-oracle-sql/data-types/README)
    """
    dtype = (dtype or "").upper()

    if dtype in ("VARCHAR2", "NVARCHAR2", "VARCHAR", "CHAR", "NCHAR"):
        # Snowflake VARCHAR/CHAR; keep length if available
        if length:
            return f"VARCHAR({int(length)})"
        return "VARCHAR"

    if dtype == "NUMBER":
        if precision is not None:
            sc = 0 if scale is None else int(scale)
            return f"NUMBER({int(precision)},{sc})"
        return "NUMBER"

    if dtype == "FLOAT":
        return "FLOAT"

    if dtype == "DATE":
        # Oracle DATE includes time; Snowflake equivalent often TIMESTAMP_NTZ
        return "TIMESTAMP_NTZ"

    if dtype.startswith("TIMESTAMP"):
        # Map Oracle TIMESTAMP variants to Snowflake equivalents
        if "WITH TIME ZONE" in dtype:
            return "TIMESTAMP_TZ"
        if "WITH LOCAL TIME ZONE" in dtype:
            return "TIMESTAMP_LTZ"
        return "TIMESTAMP_NTZ"

    if dtype in ("CLOB", "NCLOB", "LONG"):
        return "VARCHAR"

    if dtype in ("BLOB", "RAW", "LONG RAW"):
        return "BINARY"

    # For JSON/XMLTYPE etc., default to VARIANT (common in Snowflake)
    if dtype in ("JSON", "XMLTYPE", "SYS.ANYDATA"):
        return "VARIANT"

    return "VARCHAR"


def build_llm_prompt(table: str, columns: List[Tuple]) -> str:
    """
    Provide Oracle metadata and guardrails to the model.
    We explicitly ask for JSON only.
    """
    # A small mapping guide consistent with Snowflake’s published equivalents. [3](https://docs.snowflake.com/en/migrations/snowconvert-docs/translation-references/oracle/basic-elements-of-oracle-sql/data-types/README)
    mapping_guide = """
Oracle→Snowflake type guidance (use these unless strong reason):
- VARCHAR2/NVARCHAR2/VARCHAR/CHAR/NCHAR -> VARCHAR(n)
- NUMBER(p,s) -> NUMBER(p,s); NUMBER with no p/s -> NUMBER
- DATE -> TIMESTAMP_NTZ
- TIMESTAMP -> TIMESTAMP_NTZ
- TIMESTAMP WITH TIME ZONE -> TIMESTAMP_TZ
- TIMESTAMP WITH LOCAL TIME ZONE -> TIMESTAMP_LTZ
- CLOB/NCLOB/LONG -> VARCHAR
- RAW/BLOB/LONG RAW -> BINARY
- JSON/XMLTYPE/SYS.ANYDATA -> VARIANT
"""

    cols = []
    for (name, dtype, length, prec, scale, nullable) in columns:
        cols.append({
            "name": name,
            "oracle_type": dtype,
            "data_length": length,
            "precision": prec,
            "scale": scale,
            "nullable": nullable
        })

    payload = {
        "table": table,
        "target_database": SNOWFLAKE["database"],
        "target_schema": SNOWFLAKE["schema"],
        "create_or_replace": CREATE_OR_REPLACE,
        "oracle_columns": cols,
        "mapping_guide": mapping_guide.strip(),
        "requirements": [
            "Return STRICT JSON only; no markdown, no explanations.",
            "JSON must contain keys: create_table_sql, column_list.",
            "create_table_sql must be valid Snowflake SQL.",
            "Use NOT NULL when nullable='N', else allow NULL.",
            "Prefer unquoted uppercase identifiers when possible; otherwise quote safely."
        ]
    }

    return json.dumps(payload, indent=2)


def llm_generate_snowflake_ddl(client: OpenAI, table: str, columns: List[Tuple]) -> Tuple[str, List[str]]:
    """
    Calls LLM to generate Snowflake CREATE TABLE DDL.
    Returns (ddl, column_list)
    """
    prompt = build_llm_prompt(table, columns)

    # Use Responses API (primary interface in OpenAI SDK). [1](https://pypi.org/project/openai/)
    resp = client.responses.create(
        model=OPENAI_MODEL,
        input=prompt,
        instructions=(
            "You are a database migration assistant. "
            "Convert Oracle table metadata into Snowflake CREATE TABLE SQL. "
            "Return JSON only."
        )
    )

    text = resp.output_text.strip()
    data = json.loads(text)
    return data["create_table_sql"], data["column_list"]


def deterministic_ddl(table: str, columns: List[Tuple]) -> Tuple[str, List[str]]:
    """
    Fallback DDL generator if LLM fails.
    """
    col_defs = []
    col_list = []
    for (name, dtype, length, prec, scale, nullable) in columns:
        sf_type = fallback_oracle_to_snowflake_type(dtype, length, prec, scale)
        null_sql = "NOT NULL" if nullable == "N" else ""
        col_defs.append(f"{sf_ident(name)} {sf_type} {null_sql}".strip())
        col_list.append(name)

    ddl_prefix = "CREATE OR REPLACE TABLE" if CREATE_OR_REPLACE else "CREATE TABLE"
    ddl = f"{ddl_prefix} {sf_ident(table)} (\n  " + ",\n  ".join(col_defs) + "\n);"
    return ddl, col_list


def sf_exec(cur, sql: str):
    cur.execute(sql)


def load_table_to_snowflake(src_cur, sf_conn, table: str, column_list: List[str]) -> int:
    """
    Batch fetch from Oracle -> pandas -> Snowflake write_pandas.
    Snowflake supports writing DataFrames using the connector. [2](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas)
    """
    # Pull data from Oracle
    src_cur.execute(f'SELECT * FROM "{table}"')

    total = 0
    while True:
        rows = src_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        df = pd.DataFrame(rows, columns=column_list)

        # Bulk upload via write_pandas (fast).
        # Note: the destination table must exist; write_pandas appends by default. [2](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas)
        success, nchunks, nrows, _ = write_pandas(
            conn=sf_conn,
            df=df,
            table_name=table,
            database=SNOWFLAKE["database"],
            schema=SNOWFLAKE["schema"],
            quote_identifiers=False  # we use uppercase + safe identifiers
        )

        if not success:
            raise RuntimeError(f"write_pandas reported failure for {table}")

        total += int(nrows)

    return total


# ----------------------------
# MAIN
# ----------------------------

def main():
    # LLM client
    llm = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    # Oracle connections
    src_conn = oracledb.connect(**SRC_DB)
    src_cur = src_conn.cursor()
    src_cur.arraysize = BATCH_SIZE

    # Snowflake connection
    sf_conn = snowflake.connector.connect(
        account=SNOWFLAKE["account"],
        user=SNOWFLAKE["user"],
        password=SNOWFLAKE["password"],
        warehouse=SNOWFLAKE["warehouse"],
        database=SNOWFLAKE["database"],
        schema=SNOWFLAKE["schema"],
    )
    sf_cur = sf_conn.cursor()

    print("✅ Connected to Oracle (source) and Snowflake (target)")

    # Ensure context (good practice)
    sf_exec(sf_cur, f'USE WAREHOUSE {sf_ident(SNOWFLAKE["warehouse"])};')
    sf_exec(sf_cur, f'USE DATABASE {sf_ident(SNOWFLAKE["database"])};')
    sf_exec(sf_cur, f'CREATE SCHEMA IF NOT EXISTS {sf_ident(SNOWFLAKE["schema"])};')
    sf_exec(sf_cur, f'USE SCHEMA {sf_ident(SNOWFLAKE["schema"])};')

    # Get tables from Oracle
    tables = oracle_tables(src_cur)
    print(f"📦 Found {len(tables)} tables in Oracle source")

    for table in tables:
        print(f"\n🚀 Migrating table: {table}")

        cols = oracle_columns_metadata(src_cur, table)

        # --- Step 2A: LLM generate Snowflake DDL (auto)
        try:
            ddl, column_list = llm_generate_snowflake_ddl(llm, table, cols)
            print("🤖 LLM generated DDL")
        except Exception as e:
            print(f"⚠️ LLM failed for {table}, using deterministic fallback. Reason: {e}")
            ddl, column_list = deterministic_ddl(table, cols)

        # --- Execute DDL in Snowflake (auto)
        try:
            sf_exec(sf_cur, ddl)
            print("🧱 Created/Updated table in Snowflake")
        except Exception as e:
            print(f"❌ Snowflake DDL execution failed for {table}\nDDL:\n{ddl}\nError: {e}")
            # Optionally stop here
            raise

        # --- Step 2B: Load data (auto)
        start = time.time()
        try:
            loaded = load_table_to_snowflake(src_cur, sf_conn, table, column_list)
            elapsed = time.time() - start
            print(f"📥 Loaded {loaded} rows into Snowflake table {table} in {elapsed:.2f}s")
        except Exception as e:
            print(f"❌ Data load failed for {table}: {e}")
            raise

        # --- Optional: lightweight validation (row count)
        try:
            src_cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            src_count = int(src_cur.fetchone()[0])

            sf_exec(sf_cur, f"SELECT COUNT(*) FROM {sf_ident(table)}")
            tgt_count = int(sf_cur.fetchone()[0])

            status = "✅" if src_count == tgt_count else "⚠️"
            print(f"{status} Rowcount check {table}: Oracle={src_count}, Snowflake={tgt_count}")
        except Exception as e:
            print(f"⚠️ Validation skipped/failed for {table}: {e}")

    # Cleanup
    src_cur.close()
    src_conn.close()
    sf_cur.close()
    sf_conn.close()
    print("\n🎯 Oracle → Snowflake migration completed")


if __name__ == "__main__":
    main()
repr()