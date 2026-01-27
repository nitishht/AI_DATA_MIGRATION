# import oracledb
# from typing import List, Tuple

# # ----------------------------
# # CONFIG
# # ----------------------------

# ORACLE_CLIENT_PATH = r"C:\Users\QG165WL\Downloads\instantclient-basic-windows.x64-23.26.0.0.0\instantclient_23_0"

# SRC_DB = {
#     "user": "SRC_DW",
#     "password": "SRC_DW",
#     "dsn": "127.0.0.1:1521/XE"
# }

# TGT_DB = {
#     "user": "TGT_DW",
#     "password": "TGT_DW",
#     "dsn": "127.0.0.1:1521/XE"
# }

# BATCH_SIZE = 1000

# # ----------------------------
# # INIT ORACLE CLIENT (THICK)
# # ----------------------------

# oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_PATH)

# # ----------------------------
# # CONNECTIONS
# # ----------------------------

# src_conn = oracledb.connect(**SRC_DB)
# tgt_conn = oracledb.connect(**TGT_DB)

# src_cur = src_conn.cursor()
# tgt_cur = tgt_conn.cursor()

# print("✅ Connected to Source and Target databases")

# # ----------------------------
# # STEP 1: GET TABLE LIST FROM SOURCE
# # ----------------------------

# src_cur.execute("""
#     SELECT table_name
#     FROM user_tables
#     ORDER BY table_name
# """)

# tables = [r[0] for r in src_cur.fetchall()]
# print(f"📦 Found {len(tables)} tables in source")

# # ----------------------------
# # HELPER: MAP ORACLE TYPES
# # ----------------------------

# def map_oracle_type(
#     dtype: str,
#     length,
#     precision,
#     scale
# ) -> str:
#     if dtype in ("VARCHAR2", "CHAR"):
#         return f"{dtype}({length})"
#     elif dtype == "NUMBER":
#         if precision is not None:
#             return f"NUMBER({precision},{scale})"
#         return "NUMBER"
#     elif dtype == "DATE":
#         return "DATE"
#     elif dtype.startswith("TIMESTAMP"):
#         return dtype
#     else:
#         return dtype  # fallback

# # ----------------------------
# # STEP 2–4: CREATE + LOAD TABLES
# # ----------------------------

# for table in tables:
#     print(f"\n🚀 Migrating table: {table}")

#     # ---- Read column metadata
#     src_cur.execute("""
#         SELECT column_name,
#                data_type,
#                data_length,
#                data_precision,
#                data_scale,
#                nullable
#         FROM user_tab_columns
#         WHERE table_name = :tbl
#         ORDER BY column_id
#     """, tbl=table)

#     columns = src_cur.fetchall()

#     # ---- Build CREATE TABLE
#     ddl_cols = []
#     for col in columns:
#         name, dtype, length, prec, scale, nullable = col
#         col_type = map_oracle_type(dtype, length, prec, scale)
#         null_flag = "NULL" if nullable == "Y" else "NOT NULL"
#         ddl_cols.append(f"{name} {col_type} {null_flag}")

#     create_sql = f"""
#         CREATE TABLE {table} (
#             {", ".join(ddl_cols)}
#         )
#     """

#     # ---- Execute CREATE on TARGET
#     try:
#         tgt_cur.execute(create_sql)
#         tgt_conn.commit()
#         print(f"🧱 Table created: {table}")
#     except oracledb.DatabaseError as e:
#         print(f"⚠️ Skipping table creation ({table}): {e}")
#         continue

#     # ---- Extract data from SOURCE
#     src_cur.execute(f"SELECT * FROM {table}")

#     col_count = len(columns)
#     placeholders = ",".join([f":{i+1}" for i in range(col_count)])
#     insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"

#     total_rows = 0

#     while True:
#         rows = src_cur.fetchmany(BATCH_SIZE)
#         if not rows:
#             break

#         tgt_cur.executemany(insert_sql, rows)
#         tgt_conn.commit()
#         total_rows += len(rows)

#     print(f"📥 Loaded {total_rows} rows into {table}")

# # ----------------------------
# # CLEANUP
# # ----------------------------

# src_cur.close()
# tgt_cur.close()
# src_conn.close()
# tgt_conn.close()

# print("\n🎯 Migration completed successfully")



import oracledb
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple, Dict
import re
import sys
import time

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

ORACLE_CLIENT_PATH = r"C:\Users\AD739BB\Downloads\instantclient-basic-windows.x64-23.26.0.0.0\instantclient_23_0"
USE_THICK_CLIENT = True

SRC_DB = dict(user="SRC_DW1", password="SRC_DW1", dsn="127.0.0.1:1521/XE")
TGT_DB = dict(user="TGT_DW", password="TGT_DW", dsn="127.0.0.1:1521/XE")

BATCH_SIZE = 5000
COMMIT_EVERY_BATCH = True

# If True, will try to drop target objects before creating (best-effort).
DROP_IF_EXISTS = False

# If you want to migrate only a subset of tables, put names here (uppercase), else None.
TABLE_FILTER: Optional[List[str]] = None

# If True, migrate code objects (views, procedures, packages, triggers, etc.)
MIGRATE_CODE_OBJECTS = True


# ------------------------------------------------------------
# UTILITIES
# ------------------------------------------------------------

def banner(msg: str):
    print(f"\n{'='*80}\n{msg}\n{'='*80}")

def read_lob(val):
    """DBMS_METADATA returns CLOBs sometimes as LOB objects; normalize to str."""
    if val is None:
        return None
    try:
        # oracledb.LOB has read()
        return val.read() if hasattr(val, "read") else val
    except Exception:
        return str(val)

def normalize_ddl(ddl: str, src_schema: str, tgt_schema: str) -> str:
    """
    Make DDL more target-friendly:
    - Remove trailing "/" (common with PL/SQL from some tools)
    - Optionally remap schema names
    """
    if not ddl:
        return ddl

    ddl = ddl.strip()

    # remove trailing slash line (common in SQL*Plus scripts)
    ddl = re.sub(r"\n/\s*$", "", ddl, flags=re.MULTILINE)

    # Remap schema references if any appear (quoted/unquoted)
    # This is a simple string-level remap; works for most DBMS_METADATA DDL.
    # If you need exact remap semantics, you can switch to OPEN/FETCH with SET_REMAP_PARAM.
    ddl = re.sub(rf'("{src_schema}")\.', rf'"{tgt_schema}".', ddl, flags=re.IGNORECASE)
    ddl = re.sub(rf"\b{src_schema}\.", f"{tgt_schema}.", ddl, flags=re.IGNORECASE)

    return ddl

def execute_ddl(cur: oracledb.Cursor, ddl: str):
    """
    Execute a single DDL/PLSQL statement.
    DBMS_METADATA GET_DDL normally returns one statement per object.
    """
    ddl = ddl.strip()
    if not ddl:
        return
    cur.execute(ddl)

def ora_code(e: Exception) -> Optional[int]:
    try:
        return e.args[0].code  # DatabaseError often has .code
    except Exception:
        return None


# ------------------------------------------------------------
# DBMS_METADATA SETUP
# ------------------------------------------------------------

def set_metadata_transforms(cur: oracledb.Cursor):
    """
    Configure DBMS_METADATA to produce cleaner, more portable DDL.
    """
    plsql = """
    BEGIN
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY', TRUE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR', FALSE);

      -- Remove physical/storage noise
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES', FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE', FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE', FALSE);

      -- Keep constraints in output (we still apply in controlled order later)
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS', TRUE);
      DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'REF_CONSTRAINTS', TRUE);

      -- Optional:
      -- DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'OID', FALSE);
    END;
    """
    cur.execute(plsql)


def get_ddl(cur: oracledb.Cursor, obj_type: str, name: str, schema: str) -> Optional[str]:
    """
    Fetch DDL for a single object using DBMS_METADATA.GET_DDL.
    """
    sql = "SELECT DBMS_METADATA.GET_DDL(:t, :n, :s) FROM dual"
    cur.execute(sql, t=obj_type, n=name, s=schema)
    row = cur.fetchone()
    return read_lob(row[0]) if row else None


# ------------------------------------------------------------
# OBJECT LISTING (names) - lightweight dictionary queries
# (DDL still comes from DBMS_METADATA)
# ------------------------------------------------------------

def list_tables(cur: oracledb.Cursor) -> List[str]:
    cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
    return [r[0] for r in cur.fetchall()]

def list_sequences(cur: oracledb.Cursor) -> List[str]:
    cur.execute("SELECT sequence_name FROM user_sequences ORDER BY sequence_name")
    return [r[0] for r in cur.fetchall()]

def list_views(cur: oracledb.Cursor) -> List[str]:
    cur.execute("SELECT view_name FROM user_views ORDER BY view_name")
    return [r[0] for r in cur.fetchall()]

def list_indexes_for_table(cur: oracledb.Cursor, table: str) -> List[str]:
    # Skip LOB indexes and system-generated indexes if desired.
    cur.execute("""
        SELECT index_name
        FROM user_indexes
        WHERE table_name = :t
          AND index_type NOT LIKE 'LOB%'
        ORDER BY index_name
    """, t=table)
    return [r[0] for r in cur.fetchall()]

def list_constraints_for_table(cur: oracledb.Cursor, table: str, types: Tuple[str, ...]) -> List[str]:
    cur.execute(f"""
        SELECT constraint_name
        FROM user_constraints
        WHERE table_name = :t
          AND constraint_type IN ({",".join([f"'{x}'" for x in types])})
          AND generated = 'USER NAME'
        ORDER BY constraint_name
    """, t=table)
    return [r[0] for r in cur.fetchall()]

def list_code_objects(cur: oracledb.Cursor) -> List[Tuple[str, str]]:
    """
    Returns list of (object_type, object_name) for common code objects.
    We exclude TABLE/INDEX/SEQUENCE/CONSTRAINT/VIEW which are handled separately.
    """
    cur.execute("""
        SELECT object_type, object_name
        FROM user_objects
        WHERE object_type IN (
          'PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY'
        )
        ORDER BY object_type, object_name
    """)
    return [(r[0], r[1]) for r in cur.fetchall()]


# ------------------------------------------------------------
# TARGET DROP HELPERS (optional)
# ------------------------------------------------------------

def drop_object_if_exists(cur: oracledb.Cursor, obj_type: str, name: str):
    """
    Best-effort drop on target. Not exhaustive, but helpful.
    """
    ddl = None
    if obj_type == "TABLE":
        ddl = f'DROP TABLE "{name}" CASCADE CONSTRAINTS PURGE'
    elif obj_type == "SEQUENCE":
        ddl = f'DROP SEQUENCE "{name}"'
    elif obj_type == "VIEW":
        ddl = f'DROP VIEW "{name}"'
    elif obj_type in ("PROCEDURE","FUNCTION","PACKAGE","PACKAGE BODY","TRIGGER","TYPE","TYPE BODY"):
        # PACKAGE BODY is not dropped directly; dropping PACKAGE drops body too
        if obj_type == "PACKAGE BODY":
            return
        ddl = f'DROP {obj_type} "{name}"'
    elif obj_type == "INDEX":
        ddl = f'DROP INDEX "{name}"'
    elif obj_type == "CONSTRAINT":
        # constraints are dropped via ALTER TABLE; we avoid here
        return

    if ddl:
        try:
            cur.execute(ddl)
        except oracledb.DatabaseError:
            pass


# ------------------------------------------------------------
# DATA LOAD
# ------------------------------------------------------------

def load_table_data(src_cur: oracledb.Cursor, tgt_cur: oracledb.Cursor, table: str, batch_size: int) -> int:
    """
    Copy data from source table to target table using batched executemany.
    """
    src_cur.execute(f'SELECT * FROM "{table}"')
    col_count = len(src_cur.description)

    placeholders = ",".join([f":{i+1}" for i in range(col_count)])
    ins = f'INSERT INTO "{table}" VALUES ({placeholders})'

    total = 0
    while True:
        rows = src_cur.fetchmany(batch_size)
        if not rows:
            break
        tgt_cur.executemany(ins, rows)
        total += len(rows)
    return total


# ------------------------------------------------------------
# MAIN MIGRATION
# ------------------------------------------------------------

def migrate():
    if USE_THICK_CLIENT:
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_PATH)

    src_conn = oracledb.connect(**SRC_DB)
    tgt_conn = oracledb.connect(**TGT_DB)

    src_cur = src_conn.cursor()
    tgt_cur = tgt_conn.cursor()

    # Helps fetch performance
    src_cur.arraysize = BATCH_SIZE

    banner("Connected to Source and Target")

    # configure metadata transforms in SOURCE session
    set_metadata_transforms(src_cur)

    src_schema = SRC_DB["user"].upper()
    tgt_schema = TGT_DB["user"].upper()

    # 1) TABLES LIST
    tables = list_tables(src_cur)
    if TABLE_FILTER:
        allowed = set([t.upper() for t in TABLE_FILTER])
        tables = [t for t in tables if t.upper() in allowed]

    print(f"Found {len(tables)} tables in source schema {src_schema}")

    # 2) CREATE TABLES (structure first)
    banner("Creating tables on target (structure only)")
    for t in tables:
        try:
            if DROP_IF_EXISTS:
                drop_object_if_exists(tgt_cur, "TABLE", t)

            ddl = get_ddl(src_cur, "TABLE", t, src_schema)
            ddl = normalize_ddl(ddl, src_schema, tgt_schema)

            # DBMS_METADATA TABLE DDL may include constraints; that’s okay.
            # We'll still re-apply constraints later to ensure ordering (PK/UK/C then FK).
            execute_ddl(tgt_cur, ddl)
            tgt_conn.commit()
            print(f"✅ Created table: {t}")
        except oracledb.DatabaseError as e:
            tgt_conn.rollback()
            print(f"⚠️ Table create failed: {t} | {e}")
            # ORA-00955: name already used by existing object
            # if exists, continue; else raise
            if ora_code(e) not in (955,):
                # for other errors, you may want to stop
                # raise
                pass

    # 3) CREATE SEQUENCES (before code objects that might reference them)
    banner("Creating sequences on target")
    seqs = list_sequences(src_cur)
    for s in seqs:
        try:
            if DROP_IF_EXISTS:
                drop_object_if_exists(tgt_cur, "SEQUENCE", s)
            ddl = get_ddl(src_cur, "SEQUENCE", s, src_schema)
            ddl = normalize_ddl(ddl, src_schema, tgt_schema)
            execute_ddl(tgt_cur, ddl)
            tgt_conn.commit()
            print(f"✅ Created sequence: {s}")
        except oracledb.DatabaseError as e:
            tgt_conn.rollback()
            print(f"⚠️ Sequence create failed: {s} | {e}")

    # 4) LOAD DATA
    banner("Loading data (batched)")
    total_rows_all = 0
    for t in tables:
        try:
            start = time.time()
            rows = load_table_data(src_cur, tgt_cur, t, BATCH_SIZE)
            if COMMIT_EVERY_BATCH:
                tgt_conn.commit()
            elapsed = time.time() - start
            total_rows_all += rows
            print(f"📥 Loaded {rows} rows into {t} ({elapsed:.2f}s)")
        except oracledb.DatabaseError as e:
            tgt_conn.rollback()
            print(f"❌ Data load failed for {t} | {e}")
            # Decide whether to continue or stop
            # raise

    print(f"\n✅ Total loaded rows: {total_rows_all}")

    # 5) APPLY CONSTRAINTS in safe order (PK/UK/CHECK first, then FKs)
    # NOTE: Table DDL may already contain constraints. If so, these ALTER statements may fail with "already exists".
    # This phase is still useful when you set TABLE DDL transforms to exclude constraints or when DDL differs.
    banner("Applying constraints (PK/UK/CHECK then FK)")
    for t in tables:
        # First: P/U/C
        for ctype in [("P", "U", "C"), ("R",)]:
            cons = list_constraints_for_table(src_cur, t, ctype)
            for cname in cons:
                try:
                    ddl = get_ddl(src_cur, "CONSTRAINT", cname, src_schema)
                    ddl = normalize_ddl(ddl, src_schema, tgt_schema)
                    execute_ddl(tgt_cur, ddl)
                    tgt_conn.commit()
                    print(f"✅ Applied constraint {cname} on {t}")
                except oracledb.DatabaseError as e:
                    tgt_conn.rollback()
                    # ORA-02275: such a referential constraint already exists
                    # ORA-00955: name already used
                    if ora_code(e) in (2275, 955):
                        continue
                    print(f"⚠️ Constraint apply failed {cname} on {t} | {e}")

    # 6) APPLY INDEXES (after load for speed)
    banner("Creating indexes (post-load)")
    for t in tables:
        idxs = list_indexes_for_table(src_cur, t)
        for idx in idxs:
            try:
                if DROP_IF_EXISTS:
                    drop_object_if_exists(tgt_cur, "INDEX", idx)
                ddl = get_ddl(src_cur, "INDEX", idx, src_schema)
                ddl = normalize_ddl(ddl, src_schema, tgt_schema)
                execute_ddl(tgt_cur, ddl)
                tgt_conn.commit()
                print(f"✅ Created index: {idx}")
            except oracledb.DatabaseError as e:
                tgt_conn.rollback()
                if ora_code(e) in (955,):  # already exists
                    continue
                print(f"⚠️ Index create failed {idx} | {e}")

    # 7) VIEWS
    banner("Creating views")
    for v in list_views(src_cur):
        try:
            if DROP_IF_EXISTS:
                drop_object_if_exists(tgt_cur, "VIEW", v)
            ddl = get_ddl(src_cur, "VIEW", v, src_schema)
            ddl = normalize_ddl(ddl, src_schema, tgt_schema)
            execute_ddl(tgt_cur, ddl)
            tgt_conn.commit()
            print(f"✅ Created view: {v}")
        except oracledb.DatabaseError as e:
            tgt_conn.rollback()
            print(f"⚠️ View create failed {v} | {e}")

    # 8) CODE OBJECTS (optional)
    if MIGRATE_CODE_OBJECTS:
        banner("Creating code objects (procedures/functions/packages/triggers/types)")
        for obj_type, obj_name in list_code_objects(src_cur):
            try:
                if DROP_IF_EXISTS:
                    drop_object_if_exists(tgt_cur, obj_type, obj_name)
                ddl = get_ddl(src_cur, obj_type, obj_name, src_schema)
                ddl = normalize_ddl(ddl, src_schema, tgt_schema)
                execute_ddl(tgt_cur, ddl)
                tgt_conn.commit()
                print(f"✅ Created {obj_type}: {obj_name}")
            except oracledb.DatabaseError as e:
                tgt_conn.rollback()
                print(f"⚠️ {obj_type} create failed {obj_name} | {e}")

    # CLEANUP
    src_cur.close()
    tgt_cur.close()
    src_conn.close()
    tgt_conn.close()

    banner("Migration completed")


if __name__ == "__main__":
    migrate()
 