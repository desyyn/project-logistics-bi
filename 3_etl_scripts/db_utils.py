import psycopg2
import os
import argparse
import sys

def get_postgres_conn():
    """Membuat koneksi ke database PostgreSQL dari variabel lingkungan Docker."""
    conn = None
    try:
        conn = psycopg2.connect(
            database=os.environ.get("POSTGRES_DB", "airflow"), 
            user=os.environ.get("POSTGRES_USER", "airflow"),   
            password=os.environ.get("POSTGRES_PASSWORD", "airflow"), 
            host=os.environ.get("POSTGRES_HOST", "postgres"),  
            port=os.environ.get("POSTGRES_PORT", "5432")
        )
        
        conn.autocommit = True 

        with conn.cursor() as cursor:
            cursor.execute("SET search_path TO staging, dw, public;")
        
        return conn
    except Exception as e:
        print(f"Error saat membuat koneksi ke PostgreSQL: {e}")
        if conn:
            conn.close()
        sys.exit(1)

def execute_sql_file(file_path):
    """Mengeksekusi semua perintah SQL dari file DDL (memecah multi-statement)."""
    conn = None
    try:
        conn = get_postgres_conn()
        cursor = conn.cursor()
        
        print(f"Executing DDL script from: {file_path}")
        
        with open(file_path, 'r') as file:
            sql_script = file.read()
        
        for statement in sql_script.split(';'):
            if statement.strip():
                try:
                    cursor.execute(statement)
                except psycopg2.ProgrammingError as pe:
                    if "does not exist" in str(pe) or "cannot drop schema" in str(pe) or "cannot drop table" in str(pe):
                        continue
                    else:
                        raise pe

        print(f"Successfully executed all SQL statements in {file_path}")
        
    except Exception as e:
        print(f"Error executing DDL script {file_path}: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    required_vars = ['POSTGRES_HOST', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_PORT']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        print(f"Error: Variabel lingkungan berikut tidak ditemukan: {', '.join(missing_vars)}")
        sys.exit(1)
        
    parser = argparse.ArgumentParser(description='Utilitas Database untuk ETL Amazon Delivery.')
    parser.add_argument('--run_ddl', type=str, help='Path ke file SQL DDL untuk dieksekusi.')

    args = parser.parse_args()

    if args.run_ddl:
        execute_sql_file(args.run_ddl)
    else:
        try:
            conn = get_postgres_conn()
            print("Test connection successful!")
            conn.close()
        except Exception as e:
            print(f"Test connection failed: {e}")