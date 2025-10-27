from typing import TypedDict, NotRequired, Optional
import pandas as pd


class ScrapeStats(TypedDict):
    channel_id: str
    platform: str
    pulled: int
    kept: int
    success: NotRequired[Optional[bool]]
    error: NotRequired[Optional[str]]
