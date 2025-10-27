import json
import os

import aiohttp
import pandas as pd
from dotenv import load_dotenv
from src.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

# Load Telegram bot credentials from .env
BOT_TOKEN = os.getenv("NOTIF_BOT_TOKEN")
CHAT_ID = os.getenv("NOTIF_CHAT_ID")


async def send_dataframe_to_telegram(df: pd.DataFrame, name_data: str = "data") -> bool:
    """
    Send a pandas DataFrame as a JSON file to a Telegram chat using a bot.

    Args:
        df (pd.DataFrame): The DataFrame to send.
        name_data (str): Base name for the JSON file (default: "data").

    Returns:
        bool: True if successful, False otherwise.
    """
    if BOT_TOKEN is None or CHAT_ID is None:
        logger.error("❌ Missing NOTIF_BOT_TOKEN or NOTIF_CHAT_ID in .env")
        return False

    try:
        # Convert DataFrame to JSON bytes
        data = df.to_dict(orient="records")
        json_bytes = json.dumps(data, indent=4, ensure_ascii=False, default=str).encode("utf-8")

        # Prepare API URL
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"

        # Send document via aiohttp
        async with aiohttp.ClientSession() as session:
            form = aiohttp.FormData()
            form.add_field("chat_id", CHAT_ID)
            form.add_field(
                "document",
                json_bytes,
                filename=f"{name_data}.json",
                content_type="application/json"
            )
            async with session.post(url, data=form) as response:
                if response.status == 200:
                    logger.info("✅ JSON file successfully sent to Telegram.")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Failed to send file. Telegram API response: {error_text}")
                    return False

    except Exception as e:
        logger.error(f"❌ Exception while sending file DataFrame to Telegram: {e}")
        return False


async def send_error_to_telegram(error_message: str) -> bool:
    """
    Send an error notification to a Telegram chat using a bot.

    Args:
        error_message (str): The error message to send.

    Returns:
        bool: True if successful, False otherwise.
    """
    if BOT_TOKEN is None or CHAT_ID is None:
        logger.error("❌ Missing NOTIF_BOT_TOKEN or NOTIF_CHAT_ID in .env")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": f"🚨 *ERROR NOTIFICATION*\n\n{error_message}",
        "parse_mode": "Markdown"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.info("✅ Error notification sent to Telegram.")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Failed to send error message. Telegram API response: {error_text}")
                    return False

    except Exception as e:
        logger.error(f"❌ Exception while sending error to Telegram: {e}")
        return False


async def send_notify_telegram(message: str) -> bool:
    """
    Send an error notification to a Telegram chat using a bot.

    Args:
        message (str): The error message to send.

    Returns:
        bool: True if successful, False otherwise.
    """
    if BOT_TOKEN is None or CHAT_ID is None:
        logger.error("❌ Missing NOTIF_BOT_TOKEN or NOTIF_CHAT_ID in .env")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": f"{message}",
        "parse_mode": "Markdown"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.info("✅ Message notification sent to Telegram.")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Failed to send error message. Telegram API response: {error_text}")
                    return False

    except Exception as e:
        logger.error(f"❌ Exception while sending error to Telegram: {e}")
        return False
