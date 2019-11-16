import pandas as pd

def get_cleaned_series(ser: pd.Series) -> pd.Series:
    ser = ser.str.replace('[\.,]', '', regex=True)
    ser = ser.str.replace(' +', ' ', regex=True)
    ser = ser.str.replace(' $','',regex=True)
    ser = ser.str.replace('^ ','',regex=True)
    return ser

def get_better_cleaned_series(ser: pd.Series) -> pd.Series:
    # delete everything except strings and spaces
    ser = ser.str.replace('[^A-Za-z ]+', '', regex=True)
    ser = ser.str.replace(' +', ' ', regex=True)
    ser = ser.str.replace(' $','',regex=True)
    ser = ser.str.replace('^ ','',regex=True)

    return ser
