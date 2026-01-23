import json
import pandas as pd
import sqlalchemy
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, Boolean, BigInteger
from sqlalchemy.engine.url import URL
import argparse
import sys
import os
import re
import logging

# [2026-01-19] Anya-Corena: Phoenix SQL Importer (English + Logging Edition)

# Configure Logging
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phoenix_debug.log")
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PhoenixImporter")

def clean_currency(val):
    """
    Attempts to clean currency fields like '28.00 EUR' to 28.00 (float).
    """
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
    """
    Analyzes DataFrame to determine optimal SQL type mapping.
    Detects JSONB columns.
    """
    dtype_map = {}
    
    for col in df.columns:
        # Detect if column contains dicts or lists
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

def process_data(json_path, table_name, engine, mode, pk_field=None, gui_callback=None):
    def log(msg, level="info"):
        if gui_callback:
            gui_callback(msg)
        
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
        logger.exception("JSON Read Exception")
        raise ValueError(f"Failed to read JSON: {e}")

    if not isinstance(data, list):
        log("[ERROR] Root JSON must be a list of objects.", "error")
        raise ValueError("Root JSON must be a list of objects (not a dict or other type).")

    df = pd.DataFrame(data)
    log(f"[*] {len(df)} records loaded in memory.")

    log("[*] Inferring SQL schema...")
    dtype_map = analyze_dataframe(df)
    
    if mode == 'nuke':
        log(f"[WARNING] NUKE Mode: Dropping table '{table_name}'...", "warning")
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()
        except sqlalchemy.exc.OperationalError as e:
            log(f"[ERROR] Could not drop table. It might be locked by another process.", "error")
            logger.exception("Nuke Drop Exception")
            raise ValueError(f"Table locked or in use: {e}")
        
        log(f"[*] Creating table and dumping data...")
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_map)
        
        pk_col = pk_field if pk_field and pk_field in df.columns else 'id'
        if pk_col == 'id' and 'id' not in df.columns:
            log("[*] Generating serial ID column...")
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN id SERIAL PRIMARY KEY;"))
                conn.commit()
        else:
            log(f"[*] Setting '{pk_col}' as PRIMARY KEY...")
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()
        except sqlalchemy.exc.OperationalError as e:
            log(f"[ERROR] No se pudo eliminar la tabla. Posiblemente esté bloqueada por otro proceso.")
            raise ValueError(f"Tabla bloqueada o en uso: {e}")
        
        log(f"[*] Creando tabla y volcando datos...")
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_map)
        
        pk_col = pk_field if pk_field and pk_field in df.columns else 'id'
        if pk_col == 'id' and 'id' not in df.columns:
            log("[*] Generando columna ID serial...")
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
                    log(f"[WARN] Could not set PK: {e}", "warning")

    elif mode == 'append':
        log(f"[*] APPEND Mode: Inserting into '{table_name}'...")
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_map)
        except sqlalchemy.exc.IntegrityError as e:
            log("[ERROR] Duplicate records detected.", "error")
            logger.exception("Append Integrity Error")
            raise ValueError("Integrity Error: You are trying to insert records with a Primary Key that already exists. Use 'UPSERT' instead.")

    elif mode == 'upsert':
        if not pk_field:
            log("[ERROR] UPSERT Mode requires a PK", "error")
            raise ValueError("UPSERT Mode requires selecting a Primary Key (PK).")
            
        log(f"[*] UPSERT Mode (Key: {pk_field})...")
        
        # 1. Check Table Existence
        inspector = sqlalchemy.inspect(engine)
        if not inspector.has_table(table_name):
            log(f"[*] Table does not exist. Creating initial with NUKE...")
            process_data(json_path, table_name, engine, mode='nuke', pk_field=pk_field, gui_callback=gui_callback)
            return

        # 2. Schema Evolution
        existing_cols = {c['name'] for c in inspector.get_columns(table_name)}
        new_cols = [c for c in df.columns if c not in existing_cols]
        
        if new_cols:
            log(f"[*] SCHEMA EVOLUTION: Detected {len(new_cols)} new columns.")
            with engine.connect() as conn:
                for col in new_cols:
                    sql_type = "TEXT"
                    col_dtype = dtype_map.get(col, sqlalchemy.String)
                    
                    if col_dtype == JSONB: sql_type = "JSONB"
                    elif col_dtype == Integer or col_dtype == BigInteger: sql_type = "BIGINT"
                    elif col_dtype == Float: sql_type = "NUMERIC"
                    elif col_dtype == Boolean: sql_type = "BOOLEAN"
                    
                    log(f"    -> Adding column: {col} ({sql_type})")
                    try:
                        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN \"{col}\" {sql_type};"))
                        conn.commit()
                    except Exception as e:
                         log(f"[WARN] Failed adding column {col}: {e}", "warning")

        # 3. Upsert Logic
        records = df.to_dict(orient='records')
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        
        batch_size = 500
        total = len(records)
        
        log(f"[*] Processing {total} records in batches of {batch_size}...")
        
        with engine.connect() as conn:
            for i in range(0, total, batch_size):
                batch = records[i:i+batch_size]
                stmt = insert(table).values(batch)
                
                update_cols = {c.name: c for c in stmt.excluded if c.name != pk_field}
                
                if update_cols:
                    stmt = stmt.on_conflict_do_update(index_elements=[pk_field], set_=update_cols)
                else:
                    stmt = stmt.on_conflict_do_nothing(index_elements=[pk_field])
                
                conn.execute(stmt)
                conn.commit()
                log(f"    -> [BATCH] Records {i+1} to {min(i+batch_size, total)} processed.")

    log(f"[SUCCESS] Operation '{mode.upper()}' finished!")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--json', default='catalogo_enriquecido.json')
    parser.add_argument('--table', default='catalogo_enriquecido')
    parser.add_argument('--mode', choices=['nuke', 'upsert', 'append'], default='upsert')
    parser.add_argument('--pk', default='sku', help='Key field for upsert')
    
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default='5432')
    parser.add_argument('--db', default='')
    parser.add_argument('--user', default='postgres')
    parser.add_argument('--passw', default='') 

    args = parser.parse_args()
    
    if len(sys.argv) == 1:
        print("==========================================")
        print("   PHOENIX SQL IMPORTER - INTERACTIVE MODE")
        print("==========================================")
        print("Press ENTER to use default values in brackets []")
        print("")
        
        in_json = input(f"JSON File [{args.json}]: ").strip()
        if in_json: args.json = in_json
        
        in_table = input(f"Target Table [{args.table}]: ").strip()
        if in_table: args.table = in_table
        
        in_host = input(f"Host [{args.host}]: ").strip()
        if in_host: args.host = in_host
        
        in_user = input(f"User [{args.user}]: ").strip()
        if in_user: args.user = in_user
        
        in_pass = input(f"Password: ").strip()
        if in_pass: args.passw = in_pass
        
        print("\nAvailable Modes:")
        print(" 1. upsert (Update existing - Recommended)")
        print(" 2. nuke   (Drop and recreate table)")
        print(" 3. append (Insert only)")
        in_mode = input(f"Select mode [upsert]: ").strip().lower()
        if in_mode == '1' or in_mode == 'upsert': args.mode = 'upsert'
        elif in_mode == '2' or in_mode == 'nuke': args.mode = 'nuke'
        elif in_mode == '3' or in_mode == 'append': args.mode = 'append'
        
        if args.mode == 'upsert':
            in_pk = input(f"Primary Key [{args.pk}]: ").strip()
            if in_pk: args.pk = in_pk

    if not args.passw:
        import getpass
        try:
            args.passw = getpass.getpass("DB Password: ")
        except:
            args.passw = input("DB Password: ")

    try:
        engine = get_engine(args.user, args.passw, args.host, args.port, args.db)
        with engine.connect() as conn:
            pass
    except Exception as e:
        logger.exception("Connection Test Failed")
        print(f"\n[CRITICAL ERROR] Could not connect to database.")
        print(f"Detail: {e}")
        input("\nPress ENTER to exit...")
        sys.exit(1)
        
    try:
        process_data(args.json, args.table, engine, args.mode, args.pk)
    except Exception as e:
        logger.exception("Process Data Exception")
        print(f"\n[ERROR] A failure occurred during processing: {e}")
    
    if len(sys.argv) == 1:
        input("\nProcess finished. Press ENTER to close...")

if __name__ == "__main__":
    main()
