import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

db_engine = None  

def get_engine():
    """
    Returns a SQLAlchemy database engine.
    """
    global db_engine
    if db_engine is None:
        url = os.environ.get("DATABASE_URL")
        if not url:
            raise EnvironmentError(
                "DATABASE_URL is not set. "
                "Add it to your .env file: "
                "DATABASE_URL=postgresql+psycopg2://user:pass@localhost:5432/weather_db"
            )
        db_engine = create_engine(url, pool_pre_ping=True, pool_size=5)
        log.info("DB engine created: %s", url.split("@")[-1])
    return db_engine

def check_connection() -> bool:
    """Returns True if the database is reachable, False otherwise."""
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        log.error("DB connection failed: %s", e)
        return False