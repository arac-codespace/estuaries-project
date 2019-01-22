import os
import pandas as pd
<<<<<<< HEAD
import pdb
from sqlalchemy import create_engine
import fnmatch
import d6tstack
from configparser import ConfigParser
=======
# import pdb
import fnmatch
from configparser import ConfigParser
from sqlalchemy import create_engine
>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP

print("Running Create Database with sqlalchemy script...")

<<<<<<< HEAD
=======

>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973
Base = declarative_base()


class Station(Base):
    __tablename__ = 'station'

    id = Column(Integer, primary_key=True)

    nerrsite_id = Column(String)
<<<<<<< HEAD
    stationcode = Column(String)
=======
    stationcode = Column(String, unique=True)
>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973
    station_name = Column(String)
    latitude = Column(DOUBLE_PRECISION)
    longitude = Column(DOUBLE_PRECISION)
    status = Column(String)
    active_dates = Column(String)
    state = Column(String)
    reserve_name = Column(String)

    all_water_quality = relationship(
        "WaterQuality", back_populates='station',
        cascade="all, delete, delete-orphan"
    )

    all_water_nutrient = relationship(
        "WaterNutrient", back_populates='station',
        cascade="all, delete, delete-orphan"
    )

    all_meteorology = relationship(
        "Meteorology", back_populates='station',
        cascade="all, delete, delete-orphan"
    )


class WaterQuality(Base):
    __tablename__ = 'water_quality'

    id = Column(Integer, primary_key=True)
    stationcode = Column(String)
    datetimestamp = Column(TIMESTAMP)

    station_id = Column(Integer, ForeignKey('station.id'))
    station = relationship("Station", back_populates="all_water_quality")

    temp = Column(DOUBLE_PRECISION)
    f_temp = Column(String)
    spcond = Column(DOUBLE_PRECISION)
    f_spcond = Column(String)
    sal = Column(DOUBLE_PRECISION)
    f_sal = Column(String)
    do_pct = Column(DOUBLE_PRECISION)
    f_do_pct = Column(String)
    do_mgl = Column(DOUBLE_PRECISION)
    f_do_mgl = Column(String)
    depth = Column(DOUBLE_PRECISION)
    f_depth = Column(String)
    ph = Column(DOUBLE_PRECISION)
    f_ph = Column(String)
    turb = Column(DOUBLE_PRECISION)
    f_turb = Column(String)


class WaterNutrient(Base):
    __tablename__ = 'water_nutrient'

    id = Column(Integer, primary_key=True)
    stationcode = Column(String)
    datetimestamp = Column(TIMESTAMP)

    station_id = Column(Integer, ForeignKey('station.id'))
    station = relationship("Station", back_populates="all_water_nutrient")

    po4f = Column(DOUBLE_PRECISION)
    f_po4f = Column(String)
    nh4f = Column(DOUBLE_PRECISION)
    f_nh4f = Column(String)
    no2f = Column(DOUBLE_PRECISION)
    f_no2f = Column(String)
    no3f = Column(DOUBLE_PRECISION)
    f_no3f = Column(String)
    no23f = Column(DOUBLE_PRECISION)
    f_no23f = Column(String)
    chla_n = Column(DOUBLE_PRECISION)
    f_chla_n = Column(String)


class Meteorology(Base):
    __tablename__ = "meteorology"

    id = Column(Integer, primary_key=True)
    stationcode = Column(String)
    datetimestamp = Column(TIMESTAMP)

    station_id = Column(Integer, ForeignKey('station.id'))
    station = relationship("Station", back_populates="all_meteorology")

<<<<<<< HEAD
    a_temp = Column(DOUBLE_PRECISION)
    f_a_temp = Column(String)
=======
    atemp = Column(DOUBLE_PRECISION)
    f_atemp = Column(String)
>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973
    rh = Column(DOUBLE_PRECISION)
    f_rh = Column(String)
    bp = Column(DOUBLE_PRECISION)
    f_bp = Column(String)
    wspd = Column(DOUBLE_PRECISION)
    f_wspd = Column(String)
<<<<<<< HEAD
    max_wspd = Column(DOUBLE_PRECISION)
    f_max_wspd = Column(String)
    max_wspdt = Column(String)
=======
    maxwspd = Column(DOUBLE_PRECISION)
    f_maxwspd = Column(String)
    maxwspdt = Column(String)
>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973
    wdir = Column(DOUBLE_PRECISION)
    f_wdir = Column(String)
    sdwdir = Column(DOUBLE_PRECISION)
    f_sdwdir = Column(String)
    totpar = Column(DOUBLE_PRECISION)
    f_totpar = Column(String)
    totprcp = Column(DOUBLE_PRECISION)
    f_totprcp = Column(String)
    totsorad = Column(DOUBLE_PRECISION)
    f_totsorad = Column(String)


def create_cfg_uri():
<<<<<<< HEAD
=======
    print("Creating engine uri...")
>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973
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


def create_schema():
    print("Creating Schema...")
    cfg_uri_psql = create_cfg_uri()
    engine = create_engine(cfg_uri_psql)

    metadata = MetaData(bind=engine)

    Base.metadata.create_all(engine)
    print("Done...")


def drop_tables():
    print("Dropping tables...")
    cfg_uri_psql = create_cfg_uri()
    engine = create_engine(cfg_uri_psql)

    metadata = MetaData(bind=engine)

    Base.metadata.drop_all(engine)
    print("Done...")


<<<<<<< HEAD
def populate_stations():
    print("Populating stations...")
    directory = "C:/pySites/estuary-project/src/data/original-estuary-dataset/jobos-bay"
    filenames = os.listdir(directory)
    # filtered_filenames = filter_filenames(filenames)

    df = pd.read_csv(os.path.join(directory, "jobos_sites.csv"))
    # pdb.set_trace()
=======
def populate_stations(directory="."):
    print("Populating stations table...")
    filenames = os.listdir(directory)

    df = pd.read_csv(os.path.join(directory, "jobos_sites.csv"))
>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973

    cfg_uri_psql = create_cfg_uri()
    engine = create_engine(cfg_uri_psql)

    Session = sessionmaker(bind=engine)
    session = Session()

    print(df)

    session_insert = []
    print("Inserting data...")
    for index, row in df.iterrows():
        session_insert.append(
            Station(
                nerrsite_id=row[0],
                stationcode=row[1],
                station_name=row[2],
                latitude=row[3],
                longitude=row[4],
                status=row[5],
                active_dates=row[6],
                state=row[7],
                reserve_name=row[8]
            )
        )

    session.add_all(session_insert)
    print("Committing data...")
    session.commit()
<<<<<<< HEAD
    print("Stations table populated...")


def populate_water_quality():
    print("Populating stations...")
    directory = "C:/pySites/estuary-project/src/data/original-estuary-dataset/jobos-bay"
    filenames = os.listdir(directory)
    filtered_filenames = []
    # filtered_filenames = filter_filenames(filenames)

    for file in filenames:
        if fnmatch.fnmatch(file, '*wq*') and fnmatch.fnmatch(file, '*.csv'):
            filtered_filenames.append(file)

    if not len(filtered_filenames) > 0:
        print("No water quality files found...")
        return
=======
    print("Stations table populated.")


def filter_filenames(filenames):
    print("Filtering filenames...")
    water_quality_files = []
    water_nutrient_files = []
    meteorology_files = []
    for file in filenames:
        if fnmatch.fnmatch(file, '*nut*') and fnmatch.fnmatch(file, '*.csv'):
            water_nutrient_files.append(file)
        elif fnmatch.fnmatch(file, '*wq*') and fnmatch.fnmatch(file, '*.csv'):
            water_quality_files.append(file)
        elif fnmatch.fnmatch(file, '*met*') and fnmatch.fnmatch(file, '*.csv'):
            meteorology_files.append(file)

    if not (
        len(water_quality_files) and
        len(water_nutrient_files) and
        len(meteorology_files)
    ):
        raise ValueError(
            "There are no valid data files in the specified directory"
        )

    filtered_filenames = {
        "water_quality": water_quality_files,
        "water_nutrient": water_nutrient_files,
        "meteorology": meteorology_files,
    }
    return filtered_filenames


def create_table_object(user_input, directory):
    print("Creating table object...")
    filenames = os.listdir(directory)
    filtered_filenames = filter_filenames(filenames)

    table_object = {
        1: {
            "name": "water_quality",
            "filenames": filtered_filenames["water_quality"],
            "directory": directory
        },
        2: {
            "name": "water_nutrient",
            "filenames": filtered_filenames["water_nutrient"],
            "directory": directory
        },
        3: {
            "name": "meteorology",
            "filenames": filtered_filenames["meteorology"],
            "directory": directory
        }
    }

    return table_object.get(user_input)


def tables_to_populate():
    directory = "./original-estuary-dataset/jobos-bay"
    cfg_uri_psql = create_cfg_uri()
    engine = create_engine(cfg_uri_psql)

    valid_input = False
    while valid_input is False:
        print("Choose which table to populate...")
        print("1) Water Quality, 2) Water Nutrients, 3) Meteorology, 4) Stations, 5) Exit")
        try:
            user_input = input()
            user_input = int(user_input)

            if 1 <= user_input <= 3:
                # print("Input:" + str(user_input))
                table_object = create_table_object(user_input, directory)
                panda_to_csv(table_object, engine)
            elif user_input == 4:
                # print("Input:" + str(user_input))
                populate_stations(directory)
            elif user_input == 5:
                print("Exiting...")
                break
        except ValueError:
            print("Input must be an integer.")
            valid_input = False


# Populate wq, nut, met, stations or all...
def panda_to_csv(table_object, engine):
    print("Converting data to csv...")
    directory = table_object["directory"]
    table = table_object
>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973

    skipcols = [
        "isSWMP", "Historical", "ProvisionalPlus",
        "CollMethd", "REP", "F_Record",
        "cDepth", "F_cDepth", "Level", "F_Level",
<<<<<<< HEAD
        "cLevel", "F_cLevel", "ChlFluor", "F_ChlFluor"

=======
        "cLevel", "F_cLevel", "ChlFluor", "F_ChlFluor",
        "Frequency"
>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973
    ]
    # Lower to prevent issues with letter case...
    skipcols = [x.lower() for x in skipcols]

    print("Agregating data and importing to dataframe")

<<<<<<< HEAD
    print("Agregating data and importing to dataframe")

    df = pd.concat((pd.read_csv(
        os.path.join(directory, f),  usecols=(lambda x, l_cols=skipcols: x.lower() not in l_cols)) for f in filtered_filenames
        ), ignore_index=True, sort=False)

    # df = pd.read_csv(os.path.join(directory, filtered_filenames[0]),  usecols=(lambda x, l_cols=skipcols: x.lower() not in l_cols))

    cfg_uri_psql = create_cfg_uri()
    engine = create_engine(cfg_uri_psql)

    Session = sessionmaker(bind=engine)
    session = Session()

    # Removes columns with no values (NA)
    df.dropna(axis=1, how='all', inplace=True)
    df.columns = [x.lower() for x in df.columns]

    def get_station_id(stationcode):
        query = session.query(Station).filter(Station.stationcode.like(stationcode))
        return query.first().id

    # get rid of whitespace with trim!
    print("Trimming")
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    # print("Creating and setting station_id")
    # df["station_id"] = df.apply(lambda row: get_station_id(row.stationcode), axis=1)
    # df["id"] = df.index
    print(df)

    print("Exporting to csv...")
    df.to_csv(os.path.join(directory, "all_watqt_datapoints.csv"), index=False, encoding='utf-8')

    # df.to_sql('water_quality', con=engine, if_exists='append', chunksize=1000, index=False)
    # d6tstack.utils.pd_to_psql(df, cfg_uri_psql, 'water_quality', if_exists="append")

    # print("Adding data to session...")
    # session.bulk_insert_mappings(WaterQuality, df.to_dict(orient="records"))
    # # # # session.add_all(session_insert)
    # print("Committing data...")
    # session.commit()
    # print("WaterQuality table populated...")

def csv_to_psql():
    directory = "C:/pySites/estuary-project/src/data/original-estuary-dataset/jobos-bay"
    file = "all_watqt_datapoints.csv" 

    cfg_uri_psql = create_cfg_uri()
    engine = create_engine(cfg_uri_psql)
    session = sessionmaker(bind=engine)

    with open(os.path.join(directory, file), 'r') as f:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        columns = """
            stationcode,datetimestamp,temp,f_temp,spcond,
            f_spcond,sal,f_sal,do_pct,f_do_pct,do_mgl,
            f_do_mgl,depth,f_depth,ph,f_ph,turb,f_turb
        """
        sql = f"COPY water_quality({columns}) FROM STDIN WITH CSV HEADER DELIMITER ','"
        cursor.copy_expert(sql, f)
        sql = """
            UPDATE water_quality
            SET station_id = station.id
            FROM station
            WHERE water_quality.stationcode = station.stationcode
        """
        cursor.execute(sql)
        connection.commit()
=======
    df = pd.concat(
        (
            pd.read_csv(os.path.join(directory, f),
                        usecols=(lambda x, y=skipcols: x.lower() not in y)
                        ) for f in table["filenames"]
        ),
        ignore_index=True, sort=False)

    print("Removing empty columns and trimming whitespace...")
    # Removes columns with no values (NA)
    df.dropna(axis=1, how='all', inplace=True)
    df.columns = [x.lower() for x in df.columns]
    # get rid of whitespace with trim!
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    print(df)

    table_name = table["name"]
    print("Exporting to csv...")
    out_path = "./data/processed-dataset/jobos_bay"
    filename = f"all_{table_name}_datapoints.csv"
    output = os.path.join(out_path, filename)
    df.to_csv(output, index=False, encoding='utf-8')

    csv_to_psql(table_name, output, df.columns.values, engine)


# check if station is populated...
def csv_to_psql(table_name, file, columns, engine):
    print("CSV to psql...")
    columns = ",".join(columns)
    print(f"Columns for {table_name}: {columns}")

    with open(file, 'r') as f:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        print("Copying csv to psql...")
        sql = f"COPY {table_name}({columns}) FROM STDIN WITH CSV HEADER DELIMITER ','"
        cursor.copy_expert(sql, f)

        # Check if stations is populated, if not populate...
        print("Checking if station is populated...")
        sql = f"SELECT True FROM station LIMIT 1"
        try:
            station_is_populated = engine.execute(sql).fetchone()[0]
        except TypeError as type_error:
            print("Station not populated. Populating...")
            print(type_error)
            populate_stations()
        finally:
            # Set foreign key...
            print("Setting foreign key...")
            sql = f"""
                UPDATE {table_name}
                SET station_id = station.id
                FROM station
                WHERE {table_name}.stationcode = station.stationcode
            """
            cursor.execute(sql)
            print("Transacition done.")
            connection.commit()
>>>>>>> 8c898489a6c27fa21130b6e805fcd7aea5ebe973
