# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 20:48:24 2025

@author: pbishay
"""

import pandas as pd
import ast  # safer than eval for parsing strings that look like lists/dicts

# Example: your DataFrame with JSON-like strings in a column named 'json_column'
df = pd.read_csv('hud_awards_puerto_rico.csv')


# Assuming your DataFrame is named `df` and the column with the JSON list is called 'json_column'
def expand_json_column(df, json_col):
    all_rows = []

    for idx, row in df.iterrows():
        try:
            json_list = row[json_col]
            if isinstance(json_list, str):  # if stored as a string
                json_list = ast.literal_eval(json_list)

            for item in json_list:
                if isinstance(item, dict):
                    all_rows.append(item)
        except Exception as e:
            print(f"Error in row {idx}: {e}")
            continue

    return pd.DataFrame(all_rows)

# Usage
expanded_df = expand_json_column(df, 'results')

expanded_df.to_csv("usaspending_transactions.csv", index=False)