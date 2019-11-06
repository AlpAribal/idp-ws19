import requests
import json
from pathlib import Path
from time import sleep
import shutil
import logging
import sys
from datetime import date, timedelta


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
logger.addHandler(ch)

def download_bulk(start_date: str, end_date: str, dl_path: str) -> None:
    """Download awards data in bulk.

    Parameters: 
        start_date: str
            Beginning of date range for which bulk awards data will be downloaded. Its format must be 'yyyy-mm-dd'.
        end_date: str
            End of date range for which bulk awards data will be downloaded. Its format must be 'yyyy-mm-dd'.
        dl_path: Pathlike
            Path to the folder in which bulk awards data will be downloaded.
    """

    # Assert that the inputs are of correct format
    assert isinstance(start_date, str), "`start_date` must be of type `str`."
    assert isinstance(end_date, str), "`end_date` must be of type `str`."
    assert isinstance(dl_path, str), "`dl_path` must be of type `str`."
    dl_path = Path(dl_path)
    assert dl_path.is_dir(), "`dl_path` must be a path to a valid folder."

    req = {
        "award_levels": ["prime_awards"],
        "filters": {
            "agency": "all",
            "award_types": [
                "contracts",
                "direct_payments",
                "grants",
                "idvs",
                "loans",
                "other_financial_assistance",
            ],
            "date_range": {"start_date": start_date, "end_date": end_date},
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
    file_url = response["url"]
    file_name = response["file_name"]
    logger.debug(f"Generated file will be at `{file_url}`.")

    # Check whether the file is ready to download
    file_ready = requests.head(file_url)
    while not file_ready:
        # Wait for a while if file is not ready before trying again
        sleep(10)
        file_ready = requests.head(file_url)

    logger.debug("File is ready, starting download.")

    # Stream the file and write to file on disk
    with requests.get(file_url, stream=True) as r:
        with open(str(dl_path.joinpath(file_name)), "wb") as f:
            shutil.copyfileobj(r.raw, f)


def download_batch_bulk(start_date: str, end_date: str, dl_path: str, award_type: str = 'all',
                        batch_size:int = 4) -> None:
    """Download awards data in bulk.

    Parameters:
        start_date: str
            Beginning of date range for which bulk awards data will be downloaded. Its format must be 'yyyy-mm-dd'.
        end_date: str
            End of date range for which bulk awards data will be downloaded. Its format must be 'yyyy-mm-dd'.
        dl_path: Pathlike
            Path to the folder in which bulk awards data will be downloaded.
        award_type: str (opt)
            The specific type of the award to be downloaded.
        batch_size: int (opt)
            Size of each batch in number of weeks
    """

    # Assert that the inputs are of correct format
    assert isinstance(start_date, str), "`start_date` must be of type `str`."
    assert isinstance(end_date, str), "`end_date` must be of type `str`."
    assert isinstance(dl_path, str), "`dl_path` must be of type `str`."

    dl_path = Path(dl_path)
    assert dl_path.is_dir(), "`dl_path` must be a path to a valid folder."

    assert isinstance(award_type, str), "`award_type` must be of type `str`."

    if(award_type == 'all'):
        award_types = [
                "contracts",
                "direct_payments",
                "grants",
                "idvs",
                "loans",
                "other_financial_assistance",
            ]
    else:
        award_types = [award_type,]

    start_date = date(*map(int, start_date.split('-')))
    end_date = date(*map(int, end_date.split('-')))

    wait_list = []
    batch_date_list = []
    batch_start = start_date
    batch_end = start_date + timedelta(weeks=batch_size)

    while (batch_end < end_date):
        batch_date_list.append((batch_start,batch_end))
        batch_start = batch_end + timedelta(days=1)
        batch_end = batch_start + timedelta(weeks=batch_size)

    batch_end = end_date
    batch_date_list.append((batch_start, batch_end))

    logger.debug(f"Number of batches: {len(batch_date_list)}")

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
        file_url = response["url"]
        file_name = response["file_name"]
        logger.debug(f"Generated file will be at `{file_url}`.")

        wait_list.append((file_name, file_url))

    while wait_list:
        for file_name, file_url in list(wait_list):
            # Check whether the file is ready to download
            file_ready = requests.head(file_url)
            if not file_ready:
                # Wait for a while if file is not ready before trying again
                sleep(1)
                file_ready = requests.head(file_url)
            else:
                logger.debug("File is ready, starting download.")
                # Stream the file and write to file on disk
                with requests.get(file_url, stream=True) as r:
                    with open(str(dl_path.joinpath(file_name)), "wb") as f:
                        shutil.copyfileobj(r.raw, f)
                wait_list.remove((file_name, file_url))
                logger.debug("Download finished and will move to the next download when the file is available")