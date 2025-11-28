import os
from datetime import datetime, timezone

import pandas as pd
import aiohttp
from dotenv import load_dotenv

from src.logger import get_logger
from src.types import ScrapeStats
from src.utils import is_valid_endpoint_path, tweet_id_to_timestamp, get_endpoint_name
from src.notification import send_error_to_telegram
from urllib.parse import urljoin

load_dotenv()

class ElfaScraper:
    def __init__(self):
        """
        Initialize the Elfa API scraper.
        API key is loaded from .env.
        """
        self.api_key = os.getenv("ELFA_API_KEY")
        if not self.api_key:
            raise ValueError("ELFA_API_KEY not found in .env")
        self.logger = get_logger(self.__class__.__name__)

    async def fetch_endpoint(self, path_url: str) -> tuple[pd.DataFrame, ScrapeStats]:
        """
        Accepts only the path + query part, e.g.:

          "/trending-narratives?timeFrame=day&maxNarratives=20"

          "/event-summary?keywords=...&timeWindow=24h"

        Automatically prepends base URL and delegates to fetch_endpoint.
        """

        # Validate endpoint
        try:
            endpoint_type = is_valid_endpoint_path(path_url)
        except ValueError as e:
            error_msg = f"❌ Invalid Elfa endpoint: {e}"
            await send_error_to_telegram(error_msg)
            self.logger.error(error_msg)
            empty_df = pd.DataFrame(columns=["id", "text", "timestamp", "author", "platform", "links"])
            return empty_df, ScrapeStats(channel_id=path_url, platform="elfa", pulled=0, kept=0)

        title_elfa = get_endpoint_name(path_url)

        elfa_base_url = "https://api.elfa.ai/v2/data"

        full_url = urljoin(elfa_base_url.rstrip("/") + "/", path_url.lstrip("/"))

        headers = {
            "Accept": "application/json",
            "x-elfa-api-key": self.api_key
        }
        empty_df = pd.DataFrame(columns=["id", "text", "timestamp", "author", "platform", "links"])

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                        full_url,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:

                    if resp.status != 200:
                        error_text = (await resp.text())[:200]
                        error_msg = f"❌ Elfa {title_elfa}: HTTP {resp.status} – {error_text}"
                        await send_error_to_telegram(error_msg)
                        self.logger.error(error_msg)
                        return empty_df, ScrapeStats(channel_id=title_elfa,
                                                     platform="elfa",
                                                     pulled=0,
                                                     kept=0,
                                                     success=False,
                                                     error=error_text)

                    try:
                        raw_data = await resp.json()
                    except Exception as e:
                        error_msg = f"❌ Failed to parse JSON from Elfa {title_elfa}: {e}"
                        await send_error_to_telegram(error_msg)
                        self.logger.error(error_msg)
                        return empty_df, ScrapeStats(channel_id=title_elfa,
                                                     platform="elfa",
                                                     pulled=0, kept=0,
                                                     success=False,
                                                     error=str(e))

                    # Parse based on endpoint type
                    records = []
                    if endpoint_type == "event-summary":
                        if not (isinstance(raw_data, dict) and "data" in raw_data):
                            error_msg = f"⚠️ Missing 'data' field in event-summary response"
                            await send_error_to_telegram(error_msg)
                            self.logger.warning(error_msg)
                            return empty_df, ScrapeStats(channel_id=title_elfa,
                                                         platform="elfa",
                                                         pulled=0, kept=0,
                                                         success=False,
                                                         error=error_msg)

                        items = raw_data["data"]
                        total_pulled = len(items)

                        for item in items:
                            tweet_ids = item.get("tweetIds", [])
                            if not tweet_ids:
                                continue

                            tweet_id = tweet_ids[0]

                            timestamp = datetime.now(timezone.utc)

                            record = {
                                "id": tweet_id,
                                "text": str(item.get("summary", "")).strip(),
                                "timestamp": timestamp,
                                "author": f"elfa_{tweet_id}",
                                "platform": "elfa",
                                "channel_id": path_url,
                                "links": [
                                    link.strip() for link in item.get("sourceLinks", [])
                                    if isinstance(link, str) and link.strip()
                                ]
                            }
                            records.append(record)

                    elif endpoint_type == "trending-narratives":
                        if not (isinstance(raw_data, dict) and "data" in raw_data and "trending_narratives" in raw_data[
                            "data"]):
                            error_msg = f"⚠️ Missing 'trending_narratives' in response"
                            await send_error_to_telegram(error_msg)
                            self.logger.warning(error_msg)
                            return empty_df, ScrapeStats(channel_id=title_elfa,
                                                         platform="elfa",
                                                         pulled=0,
                                                         kept=0,
                                                         success=False,
                                                         error=error_msg)

                        narratives = raw_data["data"]["trending_narratives"]
                        total_pulled = len(narratives)

                        for item in narratives:
                            tweet_ids = item.get("tweet_ids", [])
                            if not tweet_ids:
                                continue

                            tweet_id = tweet_ids[0]

                            timestamp = tweet_id_to_timestamp(tweet_id)

                            record = {
                                "id": tweet_id,
                                "text": str(item.get("narrative", "")).strip(),
                                "timestamp": timestamp,
                                "author": f"elfa_{tweet_id}",
                                "platform": "elfa",
                                "channel_id": path_url,
                                "links": [
                                    link.strip() for link in item.get("source_links", [])
                                    if isinstance(link, str) and link.strip()
                                ]
                            }
                            records.append(record)

                    # Finalize
                    total_kept = len(records)
                    self.logger.info(f"📊 [Elfa] Pulled: {total_pulled} | Kept: {total_kept}")

                    df = pd.DataFrame(records) if records else empty_df
                    return df, ScrapeStats(channel_id=title_elfa,
                                           platform="elfa",
                                           pulled=total_pulled,
                                           kept=total_kept)

            except Exception as e:
                error_msg = f"💥 Elfa {title_elfa}: Unexpected error – {e}"
                await send_error_to_telegram(error_msg)
                self.logger.error(error_msg)
                return empty_df, ScrapeStats(channel_id=title_elfa,
                                                         platform="elfa",
                                                         pulled=0,
                                                         kept=0,
                                                         success=False,
                                                         error=error_msg)