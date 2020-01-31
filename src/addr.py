#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 21:04:00 2020

@author: alparibal
"""
import numpy as np
import pandas as pd
from postal.expand import expand_address
from postal.parser import parse_address
import json

def expand(raw_addr):
    addr = str(raw_addr) if pd.notna(raw_addr) else ""
    addr = expand_address(addr)
    return addr[0].upper() if len(addr) > 0 else ""

states = "/home/alparibal/Desktop/idp-ws19/src/states.json"
states = json.load(open(states, "r"))
# invert the dict
states = {v:k.upper() for k,v in states.items()}


ch_path = "/home/alparibal/Desktop/IDP/company_dataset_identifier.xlsx"
ch = pd.read_excel(ch_path,
                   encoding="utf-8")
ch["state_fixed"] = ch["state"].fillna("").map(lambda x: states[x] if x in states else "")
ch["add"] = ch[["add1","add2","add3","add4"]].fillna("").apply(lambda x: " ".join(x), axis=1)
ch["add_fixed"] = ch["add"].map(expand)
ch = ch[["gvkey", "state_fixed", "add_fixed"]]
ch.to_csv("/home/alparibal/Desktop/IDP/chair_addr.csv", index=False)


us_path = "/home/alparibal/Desktop/idp-ws19/data/all_recipients.csv"
us = pd.read_csv(us_path,
                 usecols=['recipient_address_line_1', 'recipient_address_line_2',
                           'recipient_state_code'],
                 low_memory=False,
                 encoding="utf-8")
us.fillna("", inplace=True)
us.drop_duplicates(inplace=True)
us["recipient_state_fixed"] = us["recipient_state_code"].map(lambda x: states[x] if x in states else "")
us["recipient_address_line"] = us[["recipient_address_line_1", "recipient_address_line_2"]].apply(lambda x: " ".join(x), axis=1)
us["recipient_address_line_fixed"] = us["recipient_address_line"].map(lambda x: expand(x))
us = us[[col for col in us.columns if col != "recipient_address_line"]]
us.to_csv("/home/alparibal/Desktop/IDP/us_addr.csv", index=False)

len(set(ch.add_fixed).intersection(set(us.recipient_address_line_fixed))) # 2628
