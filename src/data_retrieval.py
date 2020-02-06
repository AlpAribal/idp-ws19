import asyncio
import json
import logging
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import List, Tuple

import httpx
from tqdm import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging_format = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


ALLOWED_TYPES = (
    "contracts",
    "direct_payments",
    "grants",
    "idvs",
    "loans",
    "other_financial_assistance",
)


async def download_bulk_in_batches(
    start_date: str,
    end_date: str,
    dl_path: str,
    award_types: List[str] = ALLOWED_TYPES,
    batch_size: int = 4,
    sleep_time: int = 1,
) -> None:
    """Download awards data in batches.

    Batches are downloaded asynchronously.

    Parameters:
        start_date: str
            Beginning of date range for which bulk awards data will be downloaded.
            Its format must be 'yyyy-mm-dd'.
        end_date: str
            End of date range for which bulk awards data will be downloaded.
            Its format must be 'yyyy-mm-dd'.
        dl_path: Pathlike
            Path to the folder in which awards data batches will be downloaded.
        award_types: str (opt)
            The specific type(s) of the award to be downloaded.
        batch_size: int (opt)
            Size of each batch in number of weeks.
        sleep_time: int (opt)
            Number of seconds to wait before rechecking file download links.
    """
    # Enable logging to a log file
    log_file = "download.log"
    ch = logging.FileHandler(log_file, "w")
    ch.setFormatter(logging_format)
    logger.addHandler(ch)
    print(f"Starting bulk download. Progress will be logged in {log_file}.")

    # Assert that the inputs are of correct format
    if not isinstance(start_date, str):
        raise ValueError("`start_date` must be of type `str`.")

    if not isinstance(end_date, str):
        raise ValueError("`end_date` must be of type `str`.")

    if not isinstance(dl_path, str):
        raise ValueError("`dl_path` must be of type `str`.")

    dl_path = Path(dl_path)
    if not dl_path.is_dir():
        raise ValueError("`dl_path` must be a path to a valid folder.")

    if not isinstance(award_types, List) and not isinstance(award_types, Tuple):
        raise ValueError("`award_types` must be of type `List[str]`.")

    if len(award_types) == 0:
        raise ValueError("`award_types` cannot be empty.")

    if not isinstance(batch_size, int):
        raise ValueError("`batch_size` must be of type `int`.")

    # Check whether given award types are allowed by USAspending API
    for a_type in award_types:
        if a_type not in ALLOWED_TYPES:
            raise ValueError(
                f"Disallowed award type: {repr(a_type)}. Allowed award types are"
                f" {str(ALLOWED_TYPES)}."
            )

    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    wait_list = []
    batch_date_list = []
    batch_start = start_date
    batch_end = start_date + timedelta(weeks=batch_size)

    while batch_end < end_date:
        batch_date_list.append((batch_start, batch_end))
        batch_start = batch_end + timedelta(days=1)
        batch_end = batch_start + timedelta(weeks=batch_size)

    batch_end = end_date
    batch_date_list.append((batch_start, batch_end))

    logger.debug(f"Number of batches: {len(batch_date_list)}")

    # Initialize the progress bar
    pbar = tqdm(total=len(batch_date_list))

    for batch_start, batch_end in batch_date_list:
        batch_start = batch_start.strftime("%Y-%m-%d")
        batch_end = batch_end.strftime("%Y-%m-%d")

        req = {
            "award_levels": ["prime_awards"],
            "filters": {
                "agency": "all",
                "award_types": award_types,
                "date_range": {"start_date": batch_start, "end_date": batch_end},
                "date_type": "action_date",
            },
        }

        headers = {"Content-Type": "application/json"}
        response = httpx.post(
            "https://api.usaspending.gov/api/v2/bulk_download/awards/",
            headers=headers,
            data=json.dumps(req),
        )

        if not response:
            raise ConnectionError(
                f"USAspending API returned error: `{response.text}`.`"
            )

        response = response.json()
        file_url = response["file_url"]
        file_name = response["file_name"]
        logger.debug(f"Generated file will be at `{file_url}`.")

        wait_list.append((file_name, file_url))

    await asyncio.gather(
        *[
            download_file(file_name, file_url, dl_path, sleep_time, logger, pbar)
            for (file_name, file_url) in wait_list
        ]
    )

    logger.debug("All downloads are completed.")
    logger.removeHandler(ch)


async def download_file(
    file_name, file_url, dl_path, sleep_time=1, logger=None, pbar=None
):
    """Download single awards file asynchronously.

    Parameters:
        file_name: str
            Name of the file to download.
        file_url: str
            Url of the file.
        dl_path: Pathlike
            Path to the folder in which awards data batches will be downloaded.
        sleep_time: int (opt)
            Number of seconds to wait before rechecking file download links.
        logger: 
            Logger to use when logging download events.
        pbar: tqdm.tqdm
            tqdm object to update when the download is finished.
    """
    file_ready = False
    while not file_ready:
        await asyncio.sleep(sleep_time)
        file_ready = httpx.head(file_url)

    if logger:
        logger.debug(f"`{file_name}` is ready, starting download.")

    async with httpx.AsyncClient() as client:
        async with client.stream("GET", file_url) as r:
            with open(str(dl_path.joinpath(file_name)), "wb") as f:
                async for a in r.aiter_raw():
                    f.write(a)

    if logger:
        logger.debug(f"Download of `{file_name}` is finished.")

    if pbar:
        pbar.update(1)
