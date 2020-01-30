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

def fix_letters(ser):
    res = ser.copy()
    # remove dots with very special care
    # e.g. .COM actually should stick with the name
    # res = res.str.replace("\.(?!COM)", " ")
    # there should not be any whitespace before and after "&" if the adjacent word is less than 4 letters
    res = res.str.replace("\\b(\\w{1,3})\\s+&\\s+(\\w{1,3})\\b", "\\1&\\2")
    # there should not be any whitespace before and after "." if the adjacent word is less than 4 letters
    res = res.str.replace("\\b(\\w{1,3})\\s+\\.\\s+(\\w{1,3})\\b", "\\1.\\2")
    # replace " & " with " AND "
    res = res.str.replace("\\s&\\s", " AND ")
    # replace " . " with "  "
    res = res.str.replace("\\s\\.\\s", "  ")
    # remove notes denoted with "-[NOTE]"
    res = res.str.replace("(?:[\\b\\s]-?(?:OLD|REDH|CL A|CONSOLIDATED)\\s)+$", "")
    # not sure what to do with these: #
    res = res.str.replace("[^A-Z0-9\#'&]", " ")
    # get rid of double spaces
    res = res.str.replace("\\s+", " ")
    # merge two single letter with a space in-between
    res = res.str.replace("\\b(\\w)\\s+(?!\\w{2,})\\b", "\\1")
    # trim the whitspaces at the beginning and end
    res = res.str.strip()
    return res



abbr_table = {
    "\\bINCORP(?:ORATED)\\b": "INC",
    "\\bLIMITED LIABILITY COMPANY\\b": "LLC",
    "\\bLIMITED LIABILITY PARTNERSHIP\\b": "LLP",
    "\\bLIMITED LIABILITY LIMITED PARTNERSHIP\\b": "LLLP",
    "\\bLIMITED PARTNERSHIP\\b": "LP",
    "\\bCORPORATION\\b": "CORP",
    "\\bLIMITED\\b": "LTD",
    "\\bCOMPANY$": "CO",
    "\bCOMPANIES\\b": "COS",
    "\\bINTERNATIONAL\\b": "INTL",
    "\\bINCOME\\b": "INCM",
    "\\bRETURN\\b": "RETRN",
    "\\bACQUISITION\\b": "ACQSTN",
    "\\bINFRASTRUCTURE\\b": "INFRASTR",
    "\\bHOLDINGS\\b": "HLDGS",
    "\\bFINANCIAL\\b": "FINL",
    "\\SERVICES\\b": "SVCS",
    "\\bMANAGEMENT\\b": "MGMT",
    "\\bUNITED\\b": "UTD",
    "\\bFEDERAL\\b": "FED",
    "\\bSAVINGS\\b": "SVGS",
    "\\bBANK\\b": "BK",
    "\\bNATIONAL\\b": "NATL",
    "\\bCOMMUNITY\\b": "CMNTY",
    "\\bAMERICAN\\b": "AMER",
    "\\bAMERN\\b": "AMER",
    "\\bUNIVERSITY\\b": "UNI",
    "\\bUNIV\\b": "UNI",
    "\\bCOMMUNICATIONS\\b": "CMMNCTNS",
    "\\bFUND$": "FD",
    "\\bINDUSTRIES\\b": "INDS",
    "\\bMANUFACTURING\\b": "MFG",
    "\\bGOVERNMENT\\b": "GVT",
    "\\bGOVT\\b": "GVT",
    "\\bMUNICIPAL\\b": "MUN",
    "\\bGROUP\\b": "GRP",
    "\\bMARKET\\b": "MKT",
    "\\bEMERGING\\b": "EMG",
    "\\bEMERG\\b": "EMG",
}

def fix_words(ser):
    res = ser.copy()
    # get rid of 'the's at the very end
    res = res.str.replace("\bthe$", "")
    # abbreviate company typs
    for a in abbr_table:
        res = res.str.replace(a, abbr_table[a])
    return res

