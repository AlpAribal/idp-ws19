import pandas as pd
import numpy as np


def get_unique_company_names(df: pd.DataFrame) -> np.array:
    ''' Get unique preprocessed company names from the given df.

    :param df:
    :return: array of unique company names after the preprocessing
    '''

    companies = np.unique(np.concatenate([df.clean_recipient_name.dropna().unique(),
                              df.clean_recipient_parent_name.dropna().unique(),
                              df.clean_recipient_doing_business_as_name.dropna().unique()]))

    return companies


