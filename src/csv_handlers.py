import logging
import sys
from pathlib import Path
from typing import BinaryIO, Union
from zipfile import ZipExtFile, ZipFile

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging_format = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def _merge_df_list_to_df(df_list, df):
    """Concatenate pandas.DataFrames.
    
    Helper method to concatenate a pd.DataFrame and a list of pd.DataFrames. 
    `df_list` is emptied after concatenation.
    """
    df_list.append(df)
    df = pd.concat(df_list, sort=False).drop_duplicates()
    df_list = []
    return df


def extract_all_recipients(folder_path: str) -> None:
    """Extract recipient columns from a single .csv file.

    All files will be parsed using `get_recipient_data_from_csv` and returned 
    results will be merged together into a single .csv file in the directory 
    `folder_path`.

    Parameters:
        folder_path: str
            Path to the folder that includes downloaded data.
    """
    # Assert that the inputs are of correct format
    if not isinstance(folder_path, str):
        raise ValueError("`folder_path` must be of type `str`.")
    folder_path = Path(folder_path)
    if not folder_path.is_dir():
        raise ValueError("`folder_path` must be a path to a valid folder.")

    log_file = "extraction.log"
    ch = logging.FileHandler(log_file, "w")
    ch.setFormatter(logging_format)
    logger.addHandler(ch)
    print(f"Starting extraction. Progress will be logged in {log_file}.")

    df = pd.DataFrame()
    df_list = []
    df_list_size = 0
    fpath: Path
    for fpath in folder_path.iterdir():
        # concatenate dfs when the total size reaches a million rows
        if df_list_size >= 1000000:
            logger.debug(f"Merging df_list. It has {df_list_size} lines.")
            df = _merge_df_list_to_df(df_list, df)
            df_list_size = 0

        if fpath.is_file():
            ext = fpath.suffix.lower()
            if ext == ".zip":
                # Check for .csv files in the zip
                logger.debug(f"Found .zip file: {str(fpath)}")
                with ZipFile(fpath, "r") as myzip:
                    for file in myzip.namelist():
                        if file.split(".")[-1] == "csv":
                            logger.debug(f"Found .csv file in zip, opening: {file}")
                            new_df = get_recipient_data_from_csv(myzip.open(file))
                            logger.debug(f"File has {new_df.shape[0]} lines.")
                            df_list.append(new_df)
                            df_list_size += new_df.shape[0]

            elif ext == ".csv":
                logger.debug(f"Found .csv file, opening: {str(fpath)}")
                df = pd.concat(
                    [df, get_recipient_data_from_csv(str(fpath))], sort=False
                ).drop_duplicates()

    df_list, df = merge_df_list_to_df(df_list, df)
    df.to_csv(folder_path.joinpath("all_recipients.csv"), index=False)
    logger.removeHandler(ch)


def get_recipient_data_from_csv(fp: Union[str, BinaryIO, ZipExtFile]):
    """Extract recipient columns from a single .csv file.

    Parameters:
        file: Union[str, file-like]
            File to parse. If `file` is a str, corresponding file will be opened.
    """
    assert isinstance(fp, str) or isinstance(fp, BinaryIO) or isinstance(fp, ZipExtFile)

    logger.debug(f"Reading file: {fp}")
    data = pd.read_csv(fp, low_memory=False, usecols=lambda col: "recipient" in col)
    return data.drop_duplicates()
