import os
from typing import List, Dict, Any, Optional
import asyncpg
from dotenv import load_dotenv
from src.notification import send_error_to_telegram
from src.logger import get_logger
from src.types import ScrapeStats

load_dotenv()
logger = get_logger(__name__)

# Database configuration
DB_CONFIG: Dict[str, str] = {
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", ""),
    "host": os.getenv("DB_HOST", ""),
    "port": os.getenv("DB_PORT", "5432")
}

if not all(DB_CONFIG.values()):
    raise ValueError("All DB_* environment variables must be set in .env")

async def test_connection() -> None:
    """
    Test connection to the Supabase/PostgreSQL database.
    Prints PostgreSQL version if successful.
    """
    conn: Optional[asyncpg.Connection] = None
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        version = await conn.fetchval("SELECT version();")
        logger.info(f"✅ Database connection successful! Version: {version}")
        print(f"✅ Database connection successful! Version: {version}")
    except Exception as e:
        logger.error(f"❌ Failed to connect to database: {e}")
        print(f"❌ Failed to connect to database: {e}")
    finally:
        if conn:
            await conn.close()

async def insert_log_runs_batch(logs: List[ScrapeStats]) -> None:
    """
    Insert multiple log entries into the logs_runs table asynchronously.

    Args:
        logs (List[Dict[str, Any]]): List of log records.
            Each dict must contain:
                - channel_id: Optional[str]
                - pulled: int
                - kept: int
                - success: Optional[bool]
                - error_message: Optional[str]
                - platform: Optional[str]

    Returns:
        None

    Raises:
        Exception: If database operation fails.
    """
    if not logs:
        return

    conn: Optional[asyncpg.Connection] = None
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        records = [
            (
                log.get("channel_id"),
                log["pulled"],
                log["kept"],
                log.get("platform")
            )
            for log in logs
        ]

        await conn.executemany(
              """
            INSERT INTO logs_runs (channel_id, pulled, kept, platform) VALUES ($1, $2, $3, $4)
            """,
            records
        )
        logger.info(f"✅ Successfully saved {len(logs)} log entries to logs_runs.")
    except Exception as e:
        logger.error(f"❌ Failed to insert logs into database: {e}")
        await send_error_to_telegram(f"❌ Failed to insert logs into database: {e}")
        raise
    finally:
        if conn:
            await conn.close()


async def get_all_sources() -> List[Dict[str, Any]]:
    """
    Fetch all records from the sources table asynchronously.

    Returns:
        List[Dict[str, Any]]: List of source records with keys:
            - id: int
            - channel_id: str
            - platform: str (telegram, discord, elfa)
            - name_channel: str

    Raises:
        Exception: If database query fails.
    """
    conn: Optional[asyncpg.Connection] = None
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        rows = await conn.fetch("SELECT id, channel_id, platform, channel_name FROM sources")
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"❌ Failed to fetch data from sources table: {e}")
        await send_error_to_telegram(f"❌ Failed to fetch data from sources table: {e}")
        raise
    finally:
        if conn:
            await conn.close()