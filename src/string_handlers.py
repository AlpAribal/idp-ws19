import pandas as pd
from postal.expand import expand_address


def fix_letters(ser: pd.Series) -> pd.Series:
    """Fix letters in a pandas.Series.

    Whitespaces before and after "&" and "." are removed if neighboring words are
    shorter than four letters. Letters except "A-Z0-9#'&" are removed. All double 
    whitespaces are removed.
    
    Does not work in place. All operations are applied to a copy of `ser`.

    Parameters:
        ser: pandas.Series
            Series to fix.
        
    Returns:
        pandas.Series
            A copy of `ser`, to which various letter-level operations were applied.
    """
    if not isinstance(ser, pd.Series):
        ser = pd.Series(ser)
    res = ser.copy()
    # there should not be any whitespace before and after "&" if the adjacent
    # word is less than 4 letters
    res = res.str.replace("\\b(\\w{1,3})\\s+&\\s+(\\w{1,3})\\b", "\\1&\\2")
    # there should not be any whitespace before and after "." if the adjacent
    # word is less than 4 letters
    res = res.str.replace("\\b(\\w{1,3})\\s+\\.\\s+(\\w{1,3})\\b", "\\1.\\2")
    # replace " & " with " AND "
    res = res.str.replace("\\s&\\s", " AND ")
    # replace " . " with "  "
    res = res.str.replace("\\s\\.\\s", "  ")
    # remove notes denoted with "-[NOTE]"
    res = res.str.replace("(?:-?\\s*(?:OLD|REDH|CL A|CONSOLIDATED)\\b\\s*)+$", "")
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


def fix_words(ser: pd.Series) -> pd.Series:
    """Fix words in a pandas.Series.

    Commonly used words are replaced with their abbreviations.
    
    Does not work in place. All operations are applied to a copy of `ser`.

    Parameters:
        ser: pandas.Series
            Series to fix.
        
    Returns:
        pandas.Series
            A copy of `ser`, wherein common words were replaced with abbreviations.
    """
    if not isinstance(ser, pd.Series):
        ser = pd.Series(ser)
    res = ser.copy()
    # get rid of 'the's at the very end
    res = res.str.replace("\bthe$", "")
    # abbreviate common words
    for a in abbr_table:
        res = res.str.replace(a, abbr_table[a])
    return res


def fix_addresses(df: pd.DataFrame, addr_cols=None) -> pd.Series:
    """Fix addresses.

    Joins all address columns in `df` and expands the joined address using `pypostal`.
    
    Does not work in place. All operations are applied to a copy of `df`.

    Parameters:
        df: pandas.DataFrame
            DataFrame to fix.
        
    Returns:
        pandas.Series
            Expanded and normalized adresses.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"`df` must be a pandas.DataFrame. Got: {repr(df)}")

    if addr_cols is None:
        addr_cols = list(df.columns)
    # replaces NAs with ""
    addr = df[addr_cols].fillna("").astype("str")
    # join all address columns into a single string column
    joined_addr = addr.apply(lambda x: " ".join(x), axis=1)
    # process only unique values, merge results later
    uniq = joined_addr.unique()

    def expand(raw_addr):
        addr = str(raw_addr) if pd.notna(raw_addr) else ""
        addr = expand_address(addr)
        return addr[0].upper() if len(addr) > 0 else ""

    res = pd.Series(map(expand, uniq))
    lookup = pd.DataFrame({"uniq": uniq, "clean": res})
    return (
        pd.DataFrame({"joined": joined_addr})
        .merge(lookup, left_on="joined", right_on="uniq", how="left")["clean"]
        .values
    )


states = {
    "AL": "ALABAMA",
    "AK": "ALASKA",
    "AZ": "ARIZONA",
    "AR": "ARKANSAS",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "GA": "GEORGIA",
    "HI": "HAWAII",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "IA": "IOWA",
    "KS": "KANSAS",
    "KY": "KENTUCKY",
    "LA": "LOUISIANA",
    "ME": "MAINE",
    "MD": "MARYLAND",
    "MA": "MASSACHUSETTS",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MS": "MISSISSIPPI",
    "MO": "MISSOURI",
    "MT": "MONTANA",
    "NE": "NEBRASKA",
    "NV": "NEVADA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NY": "NEW YORK",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UT": "UTAH",
    "VT": "VERMONT",
    "VA": "VIRGINIA",
    "WA": "WASHINGTON",
    "WV": "WEST VIRGINIA",
    "WI": "WISCONSIN",
    "WY": "WYOMING",
    "AS": "AMERICAN SAMOA",
    "DC": "DISTRICT OF COLUMBIA",
    "FM": "FEDERATED STATES OF MICRONESIA",
    "GU": "GUAM",
    "MH": "MARSHALL ISLANDS",
    "MP": "NORTHERN MARIANA ISLANDS",
    "PW": "PALAU",
    "PR": "PUERTO RICO",
    "VI": "VIRGIN ISLANDS",
}


def fix_state(state_code):
    """Convert state code to state name."""
    return states[state_code] if state_code in states else ""
