import csv
import os
import re
import datetime
import statistics
import numpy as np
import pandas as pd
import pdb
import matplotlib.pyplot as plt

print("Running Water Quality Processing Script...")

CURR_DIR = os.path.dirname(os.path.realpath(__file__))

ORIGINAL_DATA_PATH = os.path.join(CURR_DIR, "original-estuary-dataset/jobos-bay")

directory_list = os.listdir(ORIGINAL_DATA_PATH)


def num_to_month(arg):
    switcher = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }
    return switcher.get(arg, "Invalid month number")


META_PARAMS = [
    "StationCode", "DateTimeStamp",
]

VAL_PARAMS = [
    "Temp", "SpCond", "Sal", "DO_Pct",
    "DO_mgl", "Depth", "pH", "Turb"
]

flag_params = ["F_"+val for val in VAL_PARAMS]


FLAG_EXCLUDE_CODES = ["<-5>", "<-4>", "<-3>", "<-2>", "<-1>"]

included_cols = META_PARAMS + VAL_PARAMS + flag_params

# GET csv
df = pd.read_csv(
    os.path.join(ORIGINAL_DATA_PATH, directory_list[150]),
    usecols=included_cols)

# CONVERT to datetime
df['DateTimeStamp'] = pd.to_datetime(df['DateTimeStamp'])
# df.set_index(df["DateTimeStamp"], inplace=True)

# EXCLUDE values by QA flags
# If flagged, replace value with NaN; else leave value intact
for param in VAL_PARAMS:
    df[param] = np.where(
        df['F_'+param].str.contains(
            "|".join(FLAG_EXCLUDE_CODES), case=False),
        np.NaN, df[param]
    )

# Get descriptive stats for each month
df_month_stats = df.groupby(df["DateTimeStamp"].dt.month).describe()


# df_month_stats["Param"] will return stats for 'Param' for each month
# https://stackoverflow.com/questions/45003806/python-pandas-use-slice-with-describe-versions-greater-than-0-20
# To get mean of all params
mean = df_month_stats.loc[:, (slice(None), ['mean'])]

# Change index to months...
month_list = []
for i in mean.index:
    month_list.append(num_to_month(i))

mean.index = month_list
# print(mean)

print(len(df))
# mean.plot()
# plt.xlabel("Months")
# plt.xticks(np.arange(len(month_list)), month_list)
# plt.show()
