import os
import pandas as pd
import pdb
from sqlalchemy import create_engine
import fnmatch
import d6tstack
from configparser import ConfigParser


print("Running Create Database with Pandas script...")


class Table:

    def __init__(self, name, val_params):
        self.name = name
        self.val_params = val_params

    @property
    def name(self):
        return self.__name

    @property
    def val_params(self):
        return self.__val_params

    @val_params.setter
    def val_params(self, val_params):
        if not isinstance(val_params, list):
            raise TypeError("val_params not a list...")
        elif not all(isinstance(item, str) for item in val_params):
            raise TypeError("items in the val_params list must be a string")
        else:
            self.__val_params = val_params

    @name.setter
    def name(self, name):
        if not isinstance(name, str):
            raise TypeError("name not a string...")
        elif not len(name) > 0:
            print("name string must have one or more characters")
        else:
            self.__name = name

    # MaxWSpdT(time) lacks a flag...
    def flag_params(self):
        flag_params = [
            "F_"+val for val in self.val_params if not val == "MaxWSpdT"
        ]
        return flag_params

    meta_params = ["StationCode", "DateTimeStamp"]

    def included_cols(self):
        if not self.name == "stations":
            included_cols = self.meta_params + self.val_params + self.flag_params()
            return included_cols
        else:
           # Exclude datetimestamp
            self.meta_params = self.meta_params[:1]
            included_cols = self.meta_params + self.val_params
            return included_cols


def filter_filenames(filenames):
    water_quality_files = []
    water_nutrient_files = []
    meteorology_files = []
    station_files = []
    for file in filenames:
        if fnmatch.fnmatch(file, '*nut*') and fnmatch.fnmatch(file, '*.csv'):
            water_nutrient_files.append(file)
        elif fnmatch.fnmatch(file, '*wq*') and fnmatch.fnmatch(file, '*.csv'):
            water_quality_files.append(file)
        elif fnmatch.fnmatch(file, '*met*') and fnmatch.fnmatch(file, '*.csv'):
            meteorology_files.append(file)
        elif fnmatch.fnmatch(file, '*sites*') and fnmatch.fnmatch(file, '*.csv'):
            station_files.append(file)

    if not len(water_quality_files) > 0 and not len(water_nutrient_files) > 0 and not len(meteorology_files) > 0 and not len(station_files) > 0:
        raise ValueError("There are no valid data files in the specified directory")

    filtered_filenames = {
        "water_quality": water_quality_files,
        "water_nutrient": water_nutrient_files,
        "meteorology": meteorology_files,
        "stations": station_files
    }
    return filtered_filenames


def create_cfg_uri():
    parser = ConfigParser()
    # read config file
    parser.read('database.ini')

    cfg_uri_psql = ""
    print("Setting uri params...")
    if parser.has_section('postgresql'):
        database = parser['postgresql']['database']
        pw = parser['postgresql']['password']
        user = parser['postgresql']['user']
        host = parser['postgresql']['host']

        cfg_uri_psql = f'postgresql+psycopg2://{user}:{pw}@{host}/{database}'
        return cfg_uri_psql
    else:
        raise ValueError("Valid database.ini file not found.")


def create_database(directory="."):
    print("Reading files in directory...")
    directory = "C:/pySites/estuary-project/src/data/original-estuary-dataset/jobos-bay"
    filenames = os.listdir(directory)
    filtered_filenames = filter_filenames(filenames)
    cfg_uri_psql = create_cfg_uri()

    engine = create_engine(cfg_uri_psql)

    settings = {
        "directory": directory,
        "filenames": filtered_filenames,
        "cfg_uri_psql": cfg_uri_psql,
        "engine": engine
    }

    valid_input = False

    while valid_input is False:
        print("Choose which table to populate...")
        print("1) Water Quality, 2) Water Nutrients, 3) Meteorology, 4) Stations, 5) Exit")
        user_input = input()
        user_input = int(user_input)

        if user_input == 1 or user_input == 2 or user_input == 3 or user_input == 4:
            table_object = create_table_object(user_input)
            create_table(table_object, settings)
            valid_input = True
        elif user_input == 5:
            print("Exiting...")
            break
        else:
            print("Please input a valid option...")
            valid_input = False

    print("Exciting script...")


def create_table_object(user_input):

    quality_params = [
        "Temp", "SpCond", "Sal", "DO_Pct",
        "DO_mgl", "Depth", "pH", "Turb"
    ]

    nutrient_params = [
        "PO4F", "NH4F", "NO2F", "NO3F",
        "NO23F", "CHLA_N",
    ]

    # MaxWSpdT(time) doesn't have a flag param.
    # Timestamp in met files is spelled DatetimeStamp != DateTimeStamp
    meteorological_params = [
        "ATemp", "RH", "BP", "WSpd", "MaxWSpd", "MaxWSpdT", "Wdir",
        "SDWDir", "TotPAR", "TotPrcp", "TotSoRad"
    ]

    station_params = [
        "NERRSiteID", "StationCode", "StationName", "Latitude", "Longitude",
        "Status", "ActiveDates", "State", "ReserveName"
    ]

    switcher = {
        1: {"name": "water_quality", "params": quality_params},
        2: {"name": "water_nutrient", "params": nutrient_params},
        3: {"name": "meteorology", "params": meteorological_params},
        4: {"name": "stations", "params": station_params}
    }

    table_props = switcher.get(user_input)

    # Table(table_name, [table_parameters])
    table = Table(table_props.get("name"), table_props.get("params"))

    return table


def create_table(table_object, settings):

    table_name = table_object.name
    included_cols = table_object.included_cols()
    lowered_cols = [x.lower() for x in included_cols]

    directory = settings["directory"]
    filenames = settings["filenames"][table_name]
    cfg_uri_psql = settings["cfg_uri_psql"]
    engine = settings["engine"]

    # GET csv.
    # usecols callable used because of inconsistencies between met files and wq/nut files
    print("Agregating data and importing to dataframe")
    df = pd.concat((pd.read_csv(
        os.path.join(directory, f), usecols=(lambda x, l_cols=lowered_cols: x.lower() in l_cols)) for f in filenames
    ), ignore_index=True)


    # Correcting for letter-case in met files...
    # Hmm, I can probably make the correction in the class itself...
    if "DatetimeStamp" in df.columns:
        print("Renaming...")
        df.rename(columns={"DatetimeStamp":"DateTimeStamp"}, inplace=True)

    # CONVERT string to datetime object
    # Stations has no datetimestamp...
    if "DateTimeStamp" in df.columns:
        print("Converting datetimestamp to datetime panda object... ")
        df['DateTimeStamp'] = pd.to_datetime(df['DateTimeStamp'], infer_datetime_format=True)

    print(df)
    print("Creating or appending table...")

    d6tstack.utils.pd_to_psql(df, cfg_uri_psql, table_name)

    print("Table created or updated...")
    print("Adding primary key...")
    with engine.connect() as con:
        con.execute(f"ALTER TABLE {table_name} ADD id SERIAL PRIMARY KEY")
    print("Done.")
