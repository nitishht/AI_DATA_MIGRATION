import oracledb

oracledb.init_oracle_client(
    lib_dir=r"C:\Users\QG165WL\Downloads\instantclient-basic-windows.x64-23.26.0.0.0\instantclient_23_0"
)

conn = oracledb.connect(
    user="TGT_STG",
    password="TGT_STG",
    dsn="127.0.0.1:1521/XE"
)

cur = conn.cursor()


cur.execute("SELECT sys_context('USERENV','SERVICE_NAME') FROM dual")
print("Connected to service:", cur.fetchone()[0])


cur.execute("""
    SELECT table_name
    FROM all_tables
    WHERE owner = 'SRC_DW'
      AND table_name NOT LIKE 'BIN$%'
""")

tables = [t[0] for t in cur.fetchall()]
print(f"Found {len(tables)} tables in SRC_DW")


for table in tables:
    print(f"\nMigrating table: {table}")


    cur.execute("""
        SELECT column_name, data_type, data_length,
               data_precision, data_scale, nullable
        FROM all_tab_columns
        WHERE owner = 'SRC_DW'
          AND table_name = :tbl
        ORDER BY column_id
    """, tbl=table)

    cols = cur.fetchall()

    ddl_cols = []
    for name, dtype, length, prec, scale, nullable in cols:
        if dtype in ("VARCHAR2", "CHAR"):
            col_type = f"{dtype}({length})"
        elif dtype == "NUMBER":
            col_type = (
                f"NUMBER({prec},{scale})" if prec is not None else "NUMBER"
            )
        else:
            col_type = dtype

        ddl_cols.append(
            f'"{name}" {col_type} {"NULL" if nullable == "Y" else "NOT NULL"}'
        )

    create_sql = f"""
        CREATE TABLE {table} (
            {", ".join(ddl_cols)}
        )
    """

    
    try:
        cur.execute(create_sql)
        conn.commit()
        print(f"Table {table} created")
    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            print(f"Table {table} already exists, skipping CREATE")
        else:
            raise

 
    insert_sql = f"""
        INSERT INTO {table}
        SELECT * FROM src_dw.{table}
    """

    cur.execute(insert_sql)
    conn.commit()

    print(f"Data copied for table {table}")


cur.close()
conn.close()

print("\nMigration completed successfully.")
