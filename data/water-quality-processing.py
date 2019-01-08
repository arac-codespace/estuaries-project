import csv
import os
import re
import datetime
import statistics

import pandas as pd

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


with open(os.path.join(ORIGINAL_DATA_PATH, directory_list[2]), newline='') as csvfile:
    file_reader = csv.DictReader(csvfile, delimiter=',')
    print(csvfile)

    # Parameters: StationCode DateTimeStamp Temp SpCond Sal DO_Pct DO_mgl
    #             Depth cDepth Level cLevel pH Turb ChlFluor
    # Excluding the following data by flags:
    # -5  Outside high sensor range
    # -4  Outside low sensor range
    # -3  Data rejected due to QA/QC
    # -2  Missing data
    # -1  Optional parameter not collected
    # Date: mm/dd/year hh:mm:ss


    #OrderedDict([(X,Y), (A,B)])
    month_array = {}

    for row in file_reader:
        # print(row["DateTimeStamp"])
        date_timestamp = row["DateTimeStamp"]
        match = re.search(r'\d{2}/\d{2}/\d{4}', date_timestamp)
        date = datetime.datetime.strptime(match.group(), '%m/%d/%Y').date()

        current_month = num_to_month(date.month)

        # Create dictionary entry for each month row

        if current_month in month_array:
            month_array[current_month].append(row)
        elif current_month not in month_array:
            month_array[current_month] = []
            month_array[current_month].append(row)
            # print("else if")
        else:
            print("uh-uh")


    # pd.DataFrame(month_array)
    print(month_array)
    # print(month_array["December"][0]["Temp"])












    # average_per_month = {}
    # # Take averages?
    # for month in month_array:

    #     # average = {row["Temp"] for row in month_array[month]}
    #     average_per_month[month] = [float(row["Temp"]) if row["Temp"] else None for row in month_array[month]]

    # print(statistics.mean(average_per_month["January"]))
    # print(average_per_month["January"])
        # if month in average_per_month:
        #     todo
        # elif month not in average_per_month:
        #     todo
        # else:
        #     print("uh-uh")

        # average_per_month[month] = []
        # average_per_month[month].append(average)
