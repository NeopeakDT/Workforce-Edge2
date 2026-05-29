"""
# Database access layer-

Database Connection
Manages Supabase (Postgres) database connections and connection pooling.

To test this file run python scripts/test_auth_jwt.py

"""
# common/db.py

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from predictable paths (systemd units use different WorkingDirectory values).
_backend_root = Path(__file__).resolve().parents[1]
_repo_root = _backend_root.parent
load_dotenv(_backend_root / ".env")
load_dotenv(_repo_root / ".env")
load_dotenv()

#Database Library Imports-
import psycopg2 # Postgres SQL Database Library
import psycopg2.extras # Postgres SQL Database Library Extensions
from psycopg2.pool import SimpleConnectionPool # Postgres SQL Database Connection Pool
from contextlib import contextmanager # Context Manager for Database Connection

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

# Lazy initialization - pool is None until first use
_pool = None


def _get_pool():
    """Get or create the connection pool (lazy initialization)"""
    global _pool
    if _pool is None:
        try:
            _pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=DATABASE_URL,
                connect_timeout=5,
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to create database connection pool. "
                f"Check your DATABASE_URL format. Error: {str(e)}"
            )
    return _pool

@contextmanager
def get_conn():
    pool = _get_pool()  # Lazy initialization
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)

@contextmanager
def get_cursor(dict_cursor: bool = True):
    with get_conn() as conn:
        cursor = conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
            if dict_cursor
            else None
        )
        try:
            yield cursor
        finally:
            cursor.close()
