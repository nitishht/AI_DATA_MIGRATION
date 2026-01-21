import oracledb
from typing import List, Tuple

# ----------------------------
# CONFIG
# ----------------------------

ORACLE_CLIENT_PATH = r"C:\Users\QG165WL\Downloads\instantclient-basic-windows.x64-23.26.0.0.0\instantclient_23_0"

SRC_DB = {
    "user": "SRC_DW",
    "password": "SRC_DW",
    "dsn": "127.0.0.1:1521/XE"
}

TGT_DB = {
    "user": "TGT_DW",
    "password": "TGT_DW",
    "dsn": "127.0.0.1:1521/XE"
}

BATCH_SIZE = 1000

# ----------------------------
# INIT ORACLE CLIENT (THICK)
# ----------------------------

oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_PATH)

# ----------------------------
# CONNECTIONS
# ----------------------------

src_conn = oracledb.connect(**SRC_DB)
tgt_conn = oracledb.connect(**TGT_DB)

src_cur = src_conn.cursor()
tgt_cur = tgt_conn.cursor()

print("✅ Connected to Source and Target databases")

# ----------------------------
# STEP 1: GET TABLE LIST FROM SOURCE
# ----------------------------

src_cur.execute("""
    SELECT table_name
    FROM user_tables
    ORDER BY table_name
""")

tables = [r[0] for r in src_cur.fetchall()]
print(f"📦 Found {len(tables)} tables in source")

# ----------------------------
# HELPER: MAP ORACLE TYPES
# ----------------------------

def map_oracle_type(
    dtype: str,
    length,
    precision,
    scale
) -> str:
    if dtype in ("VARCHAR2", "CHAR"):
        return f"{dtype}({length})"
    elif dtype == "NUMBER":
        if precision is not None:
            return f"NUMBER({precision},{scale})"
        return "NUMBER"
    elif dtype == "DATE":
        return "DATE"
    elif dtype.startswith("TIMESTAMP"):
        return dtype
    else:
        return dtype  # fallback

# ----------------------------
# STEP 2–4: CREATE + LOAD TABLES
# ----------------------------

for table in tables:
    print(f"\n🚀 Migrating table: {table}")

    # ---- Read column metadata
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

    columns = src_cur.fetchall()

    # ---- Build CREATE TABLE
    ddl_cols = []
    for col in columns:
        name, dtype, length, prec, scale, nullable = col
        col_type = map_oracle_type(dtype, length, prec, scale)
        null_flag = "NULL" if nullable == "Y" else "NOT NULL"
        ddl_cols.append(f"{name} {col_type} {null_flag}")

    create_sql = f"""
        CREATE TABLE {table} (
            {", ".join(ddl_cols)}
        )
    """

    # ---- Execute CREATE on TARGET
    try:
        tgt_cur.execute(create_sql)
        tgt_conn.commit()
        print(f"🧱 Table created: {table}")
    except oracledb.DatabaseError as e:
        print(f"⚠️ Skipping table creation ({table}): {e}")
        continue

    # ---- Extract data from SOURCE
    src_cur.execute(f"SELECT * FROM {table}")

    col_count = len(columns)
    placeholders = ",".join([f":{i+1}" for i in range(col_count)])
    insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"

    total_rows = 0

    while True:
        rows = src_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        tgt_cur.executemany(insert_sql, rows)
        tgt_conn.commit()
        total_rows += len(rows)

    print(f"📥 Loaded {total_rows} rows into {table}")

# ----------------------------
# CLEANUP
# ----------------------------

src_cur.close()
tgt_cur.close()
src_conn.close()
tgt_conn.close()

print("\n🎯 Migration completed successfully")
