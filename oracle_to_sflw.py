import os
import json
import re
import time
from typing import List, Tuple, Optional

import oracledb
import pandas as pd


try:
    import snowflake, snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    print("Imports OK")
    import inspect, sys
    print("Using interpreter:", sys.executable)
    print("snowflake from   :", snowflake.__file__)
except Exception as e:
    print("Import failed:", repr(e))


from openai import OpenAI
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()


ORACLE_CLIENT_PATH = r"C:\Users\QG165WL\Downloads\instantclient-basic-windows.x64-23.26.0.0.0\instantclient_23_0"
SRC_DB = {
    "user": "SRC_DW",
    "password": "SRC_DW",
    "dsn": "127.0.0.1:1521/XE"
}

SNOWFLAKE = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", ""),
    "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
    "database": os.getenv("SNOWFLAKE_DATABASE", ""),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
}



AZURE_OPENAI_ENDPOINT = (os.getenv("AZURE_OPENAI_ENDPOINT") or "").rstrip("/")
AZURE_OPENAI_API_KEY = (os.getenv("AZURE_OPENAI_API_KEY") or "").strip()

AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21")
AZURE_OPENAI_DEPLOYMENT = (os.getenv("AZURE_OPENAI_DEPLOYMENT") or "").strip()



BATCH_SIZE = 100000

CREATE_OR_REPLACE = True

TABLE_FILTER: Optional[List[str]] = None


oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_PATH)


IDENT_OK = re.compile(r"^[A-Z_][A-Z0-9_]*$")



def make_llm_client():
    """Return (client, is_azure, model_name_to_use).
    Using APIM Azure-style route: base_url points to /openai/deployments/{deployment}
    and we pass api-version via default_query.
    """
    if AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY and AZURE_OPENAI_DEPLOYMENT:
        client = OpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            base_url=f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{AZURE_OPENAI_DEPLOYMENT}",
            default_query={"api-version": AZURE_OPENAI_API_VERSION},
            default_headers={
                "api-key": AZURE_OPENAI_API_KEY,
                "Accept": "application/json",
            },
        )
        return client, True, AZURE_OPENAI_DEPLOYMENT
    else:
        raise RuntimeError(
            "No OpenAI credentials found. Set Azure vars "
            "(AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_DEPLOYMENT) "
            "or OPENAI_API_KEY for public OpenAI."
        )
    
def assert_openai_ready() -> None:
    """
    Validates Azure OpenAI or public OpenAI connectivity with a tiny Chat Completions call.
    """
    client, is_azure, model_name = make_llm_client()
    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "Return exactly the word: pong."},
                {"role": "user", "content": "ping"},
            ],
            
            extra_headers={
                "api-key": AZURE_OPENAI_API_KEY,
                "Accept": "application/json",
            },
            extra_query={
                "api-version": AZURE_OPENAI_API_VERSION
            },
        )
        text = (resp.choices[0].message.content or "").strip()
        if "pong" not in text.lower():
            print(f"OpenAI responded but not as expected: {text!r}")
        else:
            print("Azure/OpenAI key is valid and the service is reachable.")
    except Exception as e:
        raise RuntimeError(
            "Failed to validate Azure/OpenAI credentials.\n"
            "• Azure: check AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION.\n"
            "• Public OpenAI: check OPENAI_API_KEY.\n"
        ) from e
    

def sf_ident(name: str) -> str:
    """
    Safe Snowflake identifier.
    If simple uppercase identifier -> unquoted (case-insensitive).
    Otherwise -> quoted.
    """
    name_u = name.upper()
    if IDENT_OK.match(name_u):
        return name_u
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


def oracle_table_constraints(src_cur, table: str) -> List[dict]:
    """
    Returns a list of constraints for the given table.
    Each dict contains: type, name, columns, (for FKs: r_table, r_columns)
    """
    
    src_cur.execute("""
        SELECT c.constraint_type, c.constraint_name, cc.column_name, c.search_condition
        FROM user_constraints c
        JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
        WHERE c.table_name = :tbl
        AND c.constraint_type IN ('P', 'U', 'C')
        ORDER BY c.constraint_type, c.constraint_name, cc.position
    """, tbl=table)
    constraints = []
    for row in src_cur.fetchall():
        constraints.append({
            "type": row[0],
            "name": row[1],
            "column": row[2],
            "condition": row[3]
        })
    
    src_cur.execute("""
        SELECT c.constraint_name, cc.column_name, c.r_constraint_name, r.table_name, rcc.column_name
        FROM user_constraints c
        JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
        JOIN user_constraints r ON c.r_constraint_name = r.constraint_name
        JOIN user_cons_columns rcc ON r.constraint_name = rcc.constraint_name AND cc.position = rcc.position
        WHERE c.table_name = :tbl
        AND c.constraint_type = 'R'
        ORDER BY c.constraint_name, cc.position
    """, tbl=table)
    for row in src_cur.fetchall():
        constraints.append({
            "type": "R",
            "name": row[0],
            "column": row[1],
            "r_table": row[3],
            "r_column": row[4]
        })
    return constraints

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
        return "TIMESTAMP_NTZ"

    if dtype.startswith("TIMESTAMP"):
        if "WITH TIME ZONE" in dtype:
            return "TIMESTAMP_TZ"
        if "WITH LOCAL TIME ZONE" in dtype:
            return "TIMESTAMP_LTZ"
        return "TIMESTAMP_NTZ"

    if dtype in ("CLOB", "NCLOB", "LONG"):
        return "VARCHAR"

    if dtype in ("BLOB", "RAW", "LONG RAW"):
        return "BINARY"

    if dtype in ("JSON", "XMLTYPE", "SYS.ANYDATA"):
        return "VARIANT"

    return "VARCHAR"


def build_llm_prompt(table: str, columns: List[Tuple], constraints:List[dict]) -> str:
    """
    Provide Oracle metadata and guardrails to the model.
    We explicitly ask for JSON only.
    """
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
        "oracle_constraints": constraints,
        "mapping_guide": mapping_guide.strip(),
        "requirements": [
            "Return STRICT JSON only; no markdown, no explanations.",
            "JSON must contain keys: create_table_sql, column_list.",
            "create_table_sql must be valid Snowflake SQL.",
            "Use NOT NULL when nullable='N', else allow NULL.",
            "Prefer unquoted uppercase identifiers when possible; otherwise quote safely.",
            "Include primary key, unique, foreign key, and check constraints in the DDL if present.",
            "Do NOT include CHECK constraints; Snowflake does not support them."
        ]
    }

    return json.dumps(payload, indent=2)


# def llm_generate_snowflake_ddl(client: OpenAI, table: str, columns: List[Tuple]) -> Tuple[str, List[str]]:
#     """
#     Calls LLM to generate Snowflake CREATE TABLE DDL.
#     Returns (ddl, column_list)
#     """
#     prompt = build_llm_prompt(table, columns)

#     resp = client.responses.create(
#         model=OPENAI_MODEL,
#         input=prompt,
#         instructions=(
#             "You are a database migration assistant. "
#             "Convert Oracle table metadata into Snowflake CREATE TABLE SQL. "
#             "Return JSON only."
#         )
#     )

#     text = resp.output_text.strip()
#     data = json.loads(text)
#     return data["create_table_sql"], data["column_list"]

def llm_generate_snowflake_ddl(client: OpenAI, model_name: str, table: str, columns: List[Tuple], constraints: List[dict]) -> Tuple[str, List[str]]:
    """
    Uses Chat Completions to get strict JSON:
      { "create_table_sql": "...", "column_list": ["..."] }
    Works for both Azure (deployment name) and public OpenAI.
    """
    prompt = build_llm_prompt(table, columns, constraints)

    resp = client.chat.completions.create(
        model=model_name,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": (
                "You are a database migration assistant. Convert Oracle table metadata "
                "into Snowflake CREATE TABLE SQL. Return JSON only with keys: "
                "create_table_sql, column_list."
            )},
            {"role": "user", "content": prompt},
        ],
        
        extra_headers={
            "api-key": AZURE_OPENAI_API_KEY,
            "Accept": "application/json",
        },
        extra_query={
            "api-version": AZURE_OPENAI_API_VERSION
        },
    )

    text = (resp.choices[0].message.content or "").strip()
    data = json.loads(text)
    return data["create_table_sql"], data["column_list"]



def deterministic_ddl(table: str, columns: List[Tuple], constraints: List[dict]) -> Tuple[str, List[str]]:
    """
    Fallback DDL generator if LLM fails.
    """
    print("/nUsing fallback deterministic DDL generation logic/n")
    col_defs = []
    col_list = []
    for (name, dtype, length, prec, scale, nullable) in columns:
        sf_type = fallback_oracle_to_snowflake_type(dtype, length, prec, scale)
        null_sql = "NOT NULL" if nullable == "N" else ""
        col_defs.append(f"{sf_ident(name)} {sf_type} {null_sql}".strip())
        col_list.append(name)

    constraint_defs = []
    # Group columns by constraint name for multi-column constraints
    pk_cols = []
    unique_constraints = {}
    check_constraints = []

    for c in constraints:
        if c["type"] == "P":
            pk_cols.append(c["column"])
        elif c["type"] == "U":
            unique_constraints.setdefault(c["name"], []).append(c["column"])
        elif c["type"] == "C" and c["condition"]:
            check_constraints.append((c["name"], c["condition"]))

    if pk_cols:
        constraint_defs.append(f"PRIMARY KEY ({', '.join(sf_ident(col) for col in pk_cols)})")
    for name, cols in unique_constraints.items():
        constraint_defs.append(f"UNIQUE ({', '.join(sf_ident(col) for col in cols)})")
    # for name, cond in check_constraints:
    #     constraint_defs.append(f"CHECK ({cond})") # Snowflake supports CHECK but translating Oracle conditions to Snowflake syntax can be complex; skipping for now.

    all_defs = col_defs + constraint_defs
    ddl_prefix = "CREATE OR REPLACE TABLE" if CREATE_OR_REPLACE else "CREATE TABLE"
    ddl = f"{ddl_prefix} {sf_ident(table)} (\n  " + ",\n  ".join(all_defs) + "\n);"
    return ddl, col_list


def sf_exec(cur, sql: str):
    cur.execute(sql)


def load_table_to_snowflake(src_cur, sf_conn, table: str, column_list: List[str]) -> int:
    """
    Batch fetch from Oracle -> pandas -> Snowflake write_pandas.
    Snowflake supports writing DataFrames using the connector. [2](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas)
    """
    src_cur.execute(f'SELECT * FROM "{table}"')
    colnames = [d[0] for d in src_cur.description]
    total = 0
    while True:
        rows = src_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        df = pd.DataFrame(rows, columns=colnames)  

        success, nchunks, nrows, _ = write_pandas(
            conn=sf_conn,
            df=df,
            table_name=table,
            database=SNOWFLAKE["database"],
            schema=SNOWFLAKE["schema"],
            quote_identifiers=False
        )

        if not success:
            raise RuntimeError(f"write_pandas reported failure for {table}")

        total += int(nrows)

    return total


def main():

    
    assert_openai_ready()
    llm, IS_AZURE, MODEL_NAME = make_llm_client()

    # llm = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    src_conn = oracledb.connect(**SRC_DB)
    src_cur = src_conn.cursor()
    src_cur.arraysize = BATCH_SIZE

    sf_conn = snowflake.connector.connect(
        account=SNOWFLAKE["account"],
        user=SNOWFLAKE["user"],
        password=SNOWFLAKE["password"],
        warehouse=SNOWFLAKE["warehouse"],
        database=SNOWFLAKE["database"],
        schema=SNOWFLAKE["schema"],
    )
    sf_cur = sf_conn.cursor()

    print("Connected to Oracle (source) and Snowflake (target)")

    sf_exec(sf_cur, f'USE WAREHOUSE {sf_ident(SNOWFLAKE["warehouse"])};')
    sf_exec(sf_cur, f'USE DATABASE {sf_ident(SNOWFLAKE["database"])};')
    sf_exec(sf_cur, f'CREATE SCHEMA IF NOT EXISTS {sf_ident(SNOWFLAKE["schema"])};')
    sf_exec(sf_cur, f'USE SCHEMA {sf_ident(SNOWFLAKE["schema"])};')

    tables = oracle_tables(src_cur)
    print(f"Found {len(tables)} tables in Oracle source")

    for idx, tbl in enumerate(tables, 1):
        print(f"{idx}. {tbl}")

    selected = input("Enter the table name(s) to migrate (comma-separated, or just one): ").strip()
    selected_tables = [t.strip().upper() for t in selected.split(",") if t.strip()]

    for table in selected_tables:
        if table not in tables:
            print(f"Skipping table: {table}, not found in selection list")
            continue
        print(f"\nMigrating table: {table}")

        cols = oracle_columns_metadata(src_cur, table)
        constraints = oracle_table_constraints(src_cur, table)

        try:
            # ddl, column_list = llm_generate_snowflake_ddl(llm, table, cols)
            ddl, column_list = llm_generate_snowflake_ddl(llm, MODEL_NAME, table, cols, constraints)
            print("LLM generated DDL")
        except Exception as e:
            print(f"LLM failed for {table}, using deterministic fallback. Reason: {e}")
            ddl, column_list = deterministic_ddl(table, cols, constraints)

        try:
            sf_exec(sf_cur, ddl)
            print("Created/Updated table in Snowflake")
        except Exception as e:
            print(f"Snowflake DDL execution failed for {table}\nDDL:\n{ddl}\nError: {e}")
            raise

        start = time.time()
        try:
            loaded = load_table_to_snowflake(src_cur, sf_conn, table, column_list)
            elapsed = time.time() - start
            print(f"Loaded {loaded} rows into Snowflake table {table} in {elapsed:.2f}s")
        except Exception as e:
            print(f"Data load failed for {table}: {e}")
            raise

        try:
            src_cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            src_count = int(src_cur.fetchone()[0])

            sf_exec(sf_cur, f"SELECT COUNT(*) FROM {sf_ident(table)}")
            tgt_count = int(sf_cur.fetchone()[0])

            
            print(f"Rowcount check {table}: Oracle={src_count}, Snowflake={tgt_count}")
        except Exception as e:
            print(f" Validation skipped/failed for {table}: {e}")

    src_cur.close()
    src_conn.close()
    sf_cur.close()
    sf_conn.close()
    print("\nOracle → Snowflake migration completed")


if __name__ == "__main__":
    main()
