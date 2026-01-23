import json
import pandas as pd
import sqlalchemy
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, Boolean, BigInteger, inspect
from sqlalchemy.engine.url import URL
import argparse
import sys
import os
import re
import logger_config

# [2026-01-19] Anya-Corena: Phoenix SQL Importer (Hardened Edition)

# Configure Logging using centralized system
logger = logger_config.setup_logger("PhoenixImporter")

def clean_currency(val):
    """Attempts to clean currency fields like '28.00 EUR' to 28.00 (float)."""
    if isinstance(val, str) and 'EUR' in val:
        try:
            return float(val.replace(' EUR', '').replace(',', '.').strip())
        except Exception as e:
            logger.warning(f"Failed to clean currency '{val}': {e}")
            return val
    return val

def get_engine(user, password, host, port, dbname):
    url_object = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=dbname,
    )
    return create_engine(url_object)

def analyze_dataframe(df):
    """Analyzes DataFrame to determine optimal SQL type mapping. Detects JSONB columns."""
    dtype_map = {}
    for col in df.columns:
        sample_values = df[col].dropna().head(100).tolist()
        is_json = False
        if sample_values:
            if any(isinstance(x, (dict, list)) for x in sample_values):
                is_json = True
        
        if is_json:
            dtype_map[col] = JSONB
        else:
            if pd.api.types.is_integer_dtype(df[col]):
                dtype_map[col] = BigInteger
            elif pd.api.types.is_float_dtype(df[col]):
                dtype_map[col] = Float
            elif pd.api.types.is_bool_dtype(df[col]):
                dtype_map[col] = Boolean
            else:
                dtype_map[col] = String
    return dtype_map

def generate_sql_script(df, table_name, mode, pk_field=None):
    """Generates a complete PostgreSQL script for the given DataFrame."""
    sql_lines = []
    now_str = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    sql_lines.append(f"-- Phoenix SQL Pro - Auto-generated Export")
    sql_lines.append(f"-- Created: {now_str}")
    sql_lines.append(f"-- Source: JSON\n")

    dtype_map = analyze_dataframe(df)
    
    # 1. DROP TABLE
    if mode == 'nuke':
        sql_lines.append(f"DROP TABLE IF EXISTS \"{table_name}\";\n")

    # 2. CREATE TABLE (if nuke or if it might not exist)
    if mode == 'nuke':
        cols_sql = []
        for col, dtype in dtype_map.items():
            sql_type = "TEXT"
            if dtype == JSONB: sql_type = "JSONB"
            elif dtype == BigInteger: sql_type = "BIGINT"
            elif dtype == Float: sql_type = "NUMERIC"
            elif dtype == Boolean: sql_type = "BOOLEAN"
            
            pk_str = " PRIMARY KEY" if col == pk_field else ""
            cols_sql.append(f"    \"{col}\" {sql_type}{pk_str}")
        
        # Add id if no PK and not in columns
        if not pk_field and 'id' not in df.columns:
            cols_sql.append("    \"id\" SERIAL PRIMARY KEY")

        sql_lines.append(f"CREATE TABLE \"{table_name}\" (")
        sql_lines.append(",\n".join(cols_sql))
        sql_lines.append(");\n")

    # 3. INSERT / UPSERT Statements
    cols = [f"\"{c}\"" for c in df.columns]
    col_str = ", ".join(cols)
    
    # Helper for SQL Literal formatting
    def to_sql_literal(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "NULL"
        if isinstance(val, (dict, list)):
            return "'" + json.dumps(val).replace("'", "''") + "'"
        if isinstance(val, str):
            return "'" + val.replace("'", "''") + "'"
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        return str(val)

    batch_size = 100
    records = df.to_dict(orient='records')
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        values_list = []
        for rec in batch:
            row_vals = [to_sql_literal(rec.get(c)) for c in df.columns]
            values_list.append("(" + ", ".join(row_vals) + ")")
        
        insert_stmt = f"INSERT INTO \"{table_name}\" ({col_str})\nVALUES\n"
        insert_stmt += ",\n".join(values_list)
        
        if mode == 'upsert' and pk_field:
            update_sets = [f"\"{c}\" = EXCLUDED.\"{c}\"" for c in df.columns if c != pk_field]
            if update_sets:
                insert_stmt += f"\nON CONFLICT (\"{pk_field}\") DO UPDATE SET\n"
                insert_stmt += ",\n".join(update_sets)
            else:
                insert_stmt += f"\nON CONFLICT (\"{pk_field}\") DO NOTHING"
        
        insert_stmt += ";"
        sql_lines.append(insert_stmt + "\n")

    return "\n".join(sql_lines)

def process_data(json_path, table_name, engine, mode, pk_field=None, gui_callback=None, export_path=None):
    def log(msg, level="info"):
        if gui_callback: gui_callback(msg)
        if level == "info": logger.info(msg)
        elif level == "error": logger.error(msg)
        elif level == "warning": logger.warning(msg)
        elif level == "debug": logger.debug(msg)

    log(f"[*] Reading JSON: {json_path}...")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        log(f"[ERROR] Failed to read JSON: {e}", "error")
        raise ValueError(f"Failed to read JSON: {e}")

    if not isinstance(data, list):
        log("[ERROR] Root JSON must be a list of objects.", "error")
        raise ValueError("Root JSON must be a list of objects.")

    df = pd.DataFrame(data)
    log(f"[*] {len(df)} records loaded in memory.")

    # --- DUPLICATE CHECK ---
    if pk_field:
        if pk_field not in df.columns:
            log(f"[ERROR] The selected PK field '{pk_field}' does not exist in the data.", "error")
            raise ValueError(f"Field '{pk_field}' not found.")
            
        dupes = df[df.duplicated(subset=[pk_field], keep=False)]
        if not dupes.empty:
            num_dupes = len(dupes)
            dupe_values = dupes[pk_field].unique()[:5]
            log(f"[WARN] Data contains {num_dupes} rows with duplicate '{pk_field}' values!", "warning")
            log(f"[WARN] Examples: {list(dupe_values)}", "warning")
            log(f"[*] Cleaning data: Keeping only the LAST occurrence of each '{pk_field}'...")
            
            initial_count = len(df)
            df = df.drop_duplicates(subset=[pk_field], keep='last')
            final_count = len(df)
            log(f"[!] Cleaned: {initial_count - final_count} duplicate rows removed.")

    log("[*] Inferring SQL schema...")
    dtype_map = analyze_dataframe(df)

    # --- SQL EXPORT MODE ---
    if export_path:
        log(f"[*] Exporting SQL Script to: {export_path}...")
        try:
            sql_content = generate_sql_script(df, table_name, mode, pk_field)
            # Create directory if it doesn't exist
            export_dir = os.path.dirname(export_path)
            if export_dir:
                os.makedirs(export_dir, exist_ok=True)
            with open(export_path, 'w', encoding='utf-8') as f:
                f.write(sql_content)
            log(f"[SUCCESS] SQL Script saved successfully!", "info")
            return
        except Exception as e:
            log(f"[ERROR] SQL Export failed: {e}", "error")
            raise

    if not engine:
        raise ValueError("Database Engine is required for direct import mode.")
    
    inspector = inspect(engine)
    table_exists = inspector.has_table(table_name)

    if mode == 'nuke':
        log(f"[WARNING] NUKE Mode: Dropping table '{table_name}'...", "warning")
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()
        except Exception as e:
            log(f"[ERROR] Could not drop table: {e}", "error")
            raise

        log(f"[*] Creating table and dumping data...")
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_map)
        
        # Set PK
        pk_col = pk_field if pk_field else 'id'
        if pk_col == 'id' and 'id' not in df.columns:
            log("[*] Generating serial ID column...")
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN id SERIAL PRIMARY KEY;"))
                conn.commit()
        else:
            log(f"[*] Setting '{pk_col}' as PRIMARY KEY...")
            with engine.connect() as conn:
                try:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_col});"))
                    conn.commit()
                except Exception as e:
                    log(f"[WARN] Failed to set PK: {e}", "warning")

    elif mode == 'append':
        log(f"[*] APPEND Mode: Inserting into '{table_name}'...")
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_map)
        except Exception as e:
            log(f"[ERROR] Append failed. Possible duplicate or schema mismatch: {e}", "error")
            raise

    elif mode == 'upsert':
        if not pk_field:
            raise ValueError("UPSERT Mode requires selecting a Primary Key (PK).")
            
        if not table_exists:
            log(f"[*] Table does not exist. Initializing with NUKE...")
            process_data(json_path, table_name, engine, mode='nuke', pk_field=pk_field, gui_callback=gui_callback)
            return

        # --- CONSTRAINT VERIFICATION ---
        pk_constraint = inspector.get_pk_constraint(table_name)
        existing_pks = pk_constraint.get('constrained_columns', [])
        
        if pk_field not in existing_pks:
            log(f"[ERROR] Table '{table_name}' exists but does NOT have a PK on '{pk_field}'.", "error")
            log(f"[ERROR] UPSERT strategy depends on a UNIQUE/PK constraint. Use NUKE to rebuild.", "error")
            raise ValueError(f"Missing Unique/PK constraint on '{pk_field}' in database.")

        # Schema Evolution
        existing_cols = {c['name'] for c in inspector.get_columns(table_name)}
        new_cols = [c for c in df.columns if c not in existing_cols]
        if new_cols:
            log(f"[*] SCHEMA EVOLUTION: Adding {len(new_cols)} new columns.")
            with engine.connect() as conn:
                for col in new_cols:
                    sql_type = "TEXT"
                    col_dtype = dtype_map.get(col, sqlalchemy.String)
                    if col_dtype == JSONB: sql_type = "JSONB"
                    elif col_dtype == Integer or col_dtype == BigInteger: sql_type = "BIGINT"
                    elif col_dtype == Float: sql_type = "NUMERIC"
                    elif col_dtype == Boolean: sql_type = "BOOLEAN"
                    
                    try:
                        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN \"{col}\" {sql_type};"))
                        conn.commit()
                        log(f"    -> Added: {col}")
                    except Exception as e:
                        log(f"[WARN] Failed adding column {col}: {e}", "warning")

        # Batch Upsert
        records = df.to_dict(orient='records')
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        batch_size = 500
        total = len(records)
        
        log(f"[*] Upserting {total} records in batches of {batch_size}...")
        with engine.connect() as conn:
            for i in range(0, total, batch_size):
                batch = records[i:i+batch_size]
                stmt = insert(table).values(batch)
                
                # Identify columns to update (all except PK)
                update_cols = {c.name: c for c in stmt.excluded if c.name != pk_field}
                
                if update_cols:
                    stmt = stmt.on_conflict_do_update(index_elements=[pk_field], set_=update_cols)
                else:
                    stmt = stmt.on_conflict_do_nothing(index_elements=[pk_field])
                
                conn.execute(stmt)
                conn.commit()
                
                current_count = min(i + batch_size, total)
                log(f"    -> [BATCH] Processed {current_count}/{total} records (Mode: UPSERT)")

    log(f"[SUCCESS] Operation '{mode.upper()}' complete.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--json', default='catalogo_enriquecido.json')
    parser.add_argument('--table', default='catalogo_enriquecido')
    parser.add_argument('--mode', choices=['nuke', 'upsert', 'append'], default='upsert')
    parser.add_argument('--pk', default='sku')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default='5432')
    parser.add_argument('--db', default='')
    parser.add_argument('--user', default='postgres')
    parser.add_argument('--passw', default='')
    args = parser.parse_args()

    # (Simplified main for brevity - GUI is the primary usage)
    engine = get_engine(args.user, args.passw, args.host, args.port, args.db)
    process_data(args.json, args.table, engine, args.mode, args.pk)

if __name__ == "__main__":
    main()
