import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from metadata_store.config.db_config import DB_CONFIG, SEED_FILE


# -----------------------------
# CONNECT HELPERS
# -----------------------------
def get_default_connection():
    return psycopg2.connect(
        dbname="postgres",
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"]
    )


def get_target_connection():
    return psycopg2.connect(**DB_CONFIG)


# -----------------------------
# 1. CREATE DATABASE
# -----------------------------
def create_database():
    conn = get_default_connection()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    db_name = DB_CONFIG["dbname"]

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(f"CREATE DATABASE {db_name}")
        print(f"[DB CREATED] {db_name}")
    else:
        print(f"[DB EXISTS] {db_name}")

    cur.close()
    conn.close()


def create_tables():
    conn = get_target_connection()
    cur = conn.cursor()

    with open("01_create_tables.sql", "r") as f:
        sql = f.read()

    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()

    print("[TABLES CREATED]")
# -----------------------------
# 2. RUN SEED.SQL
# -----------------------------
def run_seed_file():
    if not os.path.exists(SEED_FILE):
        print(f"[WARNING] Seed file not found: {SEED_FILE}")
        return

    conn = get_target_connection()
    cur = conn.cursor()

    with open(SEED_FILE, "r") as f:
        sql = f.read()

    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()

    print(f"[SEED EXECUTED] {SEED_FILE}")


# -----------------------------
# 3. CREATE LOCAL DATA FOLDERS
# -----------------------------
def create_folders():
    folders = [
        "data",
        "data/raw",
        "data/silver",
        "data/gold"
    ]

    for folder in folders:
        os.makedirs(folder, exist_ok=True)

    print("[FOLDERS READY]")

# -----------------------------
    # 4. LOAD ENV VARIABLES FROM .env
# -----------------------------

def load_env_variables():
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print("[.env LOADED]")
    else:
        print("[WARNING] .env file not found")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("Starting DI Accelerator Setup...")
    load_env_variables()
    create_database()
    create_tables()
    run_seed_file()
    create_folders()

    print("Setup Complete!")