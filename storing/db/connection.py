import os
import logging
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

_engine = None

def get_engine():
    """
    Return the SQLAlchemy engine. 
    """
    global _engine
    if _engine is None:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise EnvironmentError("DATABASE_URL is not set.")
        _engine = create_engine(
            db_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=1800,
            echo=False,   
        )
        log.info("SQLAlchemy engine initialised: %s", db_url.split("@")[-1])
    return _engine

@contextmanager
def get_session() -> Session:
    """
    Context manager that provides a database session with automatic
    commit on success and rollback on any exception.
    """
    SessionFactory = sessionmaker(bind=get_engine())
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def check_connection() -> bool:
    """
    Verify the database is reachable. Returns True on success.
    """
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Database connection: OK")
        return True
    except Exception as e:
        log.error("Database connection FAILED: %s", e)
        return False
