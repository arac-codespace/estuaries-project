from create_database_with_sqlalchemy import create_cfg_uri
import os
import pandas as pd
import numpy as np
from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP
import pdb
import matplotlib.pyplot as plt


def table_params(table_name):

    quality_params = [
        "temp", "spcond", "sal", "do_pct",
        "do_mgl", "depth", "ph", "turb"
    ]

    nutrient_params = [
        "po4f", "nh4f", "no2f", "no3f",
        "no23f", "chla_n",
    ]
    '''
        Excluding bc not "aggregatable"
        MaxWSpdT, Wdir, SDWDir
    '''
    meteorological_params = [
        "atemp", "rh", "bp", "wspd", "maxwspd",
        "totpar", "totprcp", "totsorad"
    ]

    switcher = {
        "water_quality": {"params": quality_params},
        "water_nutrient": {"params": nutrient_params},
        "meteorology": {"params": meteorological_params},
    }

    table_params = switcher.get(table_name)

    if not table_params:
        raise TypeError(f'{table_name} not a valid table name.')

    return table_params


def create_math_sql(fx, params):
    fx_statements = []
    for param in params['params']:
        '''
          Had to include this condition bc I found invalid values (-99)
          for atemp in meteorological data...
          Apparently, NERRS replaced -99999 to -99, so I'll check for -99
          for every param...
        '''
        sql = f"{fx}(case when (f_{param} not like '%%-%%') and ({param} != -99) and ({param} != -99999) then {param} end) as {fx}_{param}, count({param}) as {param}_count"

        fx_statements.append(sql)
    # Create expression string
    expression = ",".join(fx_statements)
    return expression


def get_aggregate_by_date(table_name, date="month"):

    params = table_params(table_name)

    column_sql = create_math_sql("avg", params)

    sql = f"""
        SELECT
            date_trunc('{date}',datetimestamp),
            stationcode,
            {column_sql}
        FROM {table_name}
        GROUP BY date_trunc('{date}',datetimestamp), stationcode
        ORDER BY date_trunc
    """
    uri = create_cfg_uri()
    engine = create_engine(uri)
    df = pd.read_sql(sql, con=engine)

    return df


def plot_line():
    df = get_aggregate_by_date('water_nutrient')
    # Need to convert datetime to something other than
    # datetime64 bc matplotlib doesn't seem to support it.
    df.index = df['date_trunc'].astype('O')
    del df['date_trunc']
    lines = df.plot.line()
    plt.show()
    plt.close()

    # df["date_trunc"] = df["date_trunc"].astype("O")
    # df.groupby(df["date_trunc"].dt.month).count()
    # df.hist(grid=True)
    # ts = pd.Series(np.random.randn(1000), index=pd.date_range('1/1/2000', periods=1000))
    # ts = ts.cumsum()
    # df.plot()
    # plt.show()
