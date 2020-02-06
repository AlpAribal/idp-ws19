from pathlib import Path
from typing import Union, BinaryIO
from zipfile import ZipFile, ZipExtFile
import pandas as pd
import numpy as np
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
logger.addHandler(ch)

def merge_df_list_to_df(df_list, df):
    df_list.append(df)
    df = pd.concat(df_list, sort=False).drop_duplicates()
    df_list = []
    return df_list, df


def extract_all_recipients(folder_path: str):
    # Assert that the inputs are of correct format
    assert isinstance(folder_path, str), "`folder_path` must be of type `str`."
    folder_path = Path(folder_path)
    assert folder_path.is_dir(), "`folder_path` must be a path to a valid folder."

    df = pd.DataFrame()
    df_list = []
    df_list_size = 0
    fpath: Path
    for fpath in folder_path.iterdir():
        # concat dfs when it reached the size of a million
        if df_list_size >= 1000000:
            df_list, df = merge_df_list_to_df(df_list, df)
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
                            df_list.append(new_df)
                            df_list_size += new_df.shape[0]

            elif ext == ".csv":
                logger.debug(f"Found .csv file, opening: {str(fpath)}")
                df = pd.concat([df, get_recipient_data_from_csv(str(fpath))], sort=False).drop_duplicates()

    df_list, df = merge_df_list_to_df(df_list, df)
    df.to_csv(folder_path.joinpath("all_recipients.csv"), index=False)


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
