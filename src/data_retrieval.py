import requests
import json
from pathlib import Path
from time import sleep
import shutil
import logging
import sys
from datetime import timedelta, datetime
from typing import List, Tuple
from tqdm import tqdm


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
logger.addHandler(ch)


ALLOWED_TYPES = (
    "contracts",
    "direct_payments",
    "grants",
    "idvs",
    "loans",
    "other_financial_assistance",
)


def download_bulk_in_batches(
    start_date: str,
    end_date: str,
    dl_path: str,
    award_types: List[str] = ALLOWED_TYPES,
    batch_size: int = 4,
) -> None:
    """Download awards data in batches.

    Parameters:
        start_date: str
            Beginning of date range for which bulk awards data will be downloaded. Its format must be 'yyyy-mm-dd'.
        end_date: str
            End of date range for which bulk awards data will be downloaded. Its format must be 'yyyy-mm-dd'.
        dl_path: Pathlike
            Path to the folder in which awards data batches will be downloaded.
        award_types: str (opt)
            The specific type(s) of the award to be downloaded.
        batch_size: int (opt)
            Size of each batch in number of weeks.
    """

    # Assert that the inputs are of correct format
    assert isinstance(start_date, str), "`start_date` must be of type `str`."
    assert isinstance(end_date, str), "`end_date` must be of type `str`."
    assert isinstance(dl_path, str), "`dl_path` must be of type `str`."

    dl_path = Path(dl_path)
    assert dl_path.is_dir(), "`dl_path` must be a path to a valid folder."

    assert isinstance(award_types, List) or isinstance(
        award_types, Tuple
    ), "`award_types` must be of type `List[str]`."
    assert isinstance(batch_size, int), "`batch_size` must be of type `int`."

    assert len(award_types) > 0, "`award_types` cannot be empty."
    # Check whether given award types are allowed by USAspending API
    for a_type in award_types:
        assert (
            a_type in ALLOWED_TYPES
        ), f"Disallowed award type: {a_type}. Allowed award types are {ALLOWED_TYPES}."

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
        response = requests.post(
            "https://api.usaspending.gov/api/v2/bulk_download/awards/",
            headers=headers,
            data=json.dumps(req),
        )

        assert response, f"USAspending API returned error: `{response.text}`.`"

        response = response.json()
        file_url = response["file_url"]
        file_name = response["file_name"]
        logger.debug(f"Generated file will be at `{file_url}`.")

        wait_list.append((file_name, file_url))

    while wait_list:
        for file_name, file_url in wait_list:
            # Check whether the file is ready to download
            file_ready = requests.head(file_url)
            if file_ready:
                logger.debug(f"`{file_name}` is ready, starting download.")
                # Stream the file and write to file on disk
                with requests.get(file_url, stream=True) as r:
                    with open(str(dl_path.joinpath(file_name)), "wb") as f:
                        shutil.copyfileobj(r.raw, f)
                wait_list.remove((file_name, file_url))
                logger.debug("Download finished, moving to the next.")
                pbar.update(1)
        if wait_list:
            # If there are jobs left, wait for a while before re-checking the urls
            sleep(1)

    logger.debug("All downloads are completed.")
