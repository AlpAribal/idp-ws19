import requests
import json
from pathlib import Path
from time import sleep
import shutil
import logging


logger = logging.getLogger(__name__)


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
