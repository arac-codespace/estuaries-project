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


def create_math_sql(fx, table):
    fx_statements = []
    for param in table['params']:
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

    table = table_params(table_name)

    column_sql = create_math_sql("avg", table)

    sql = f"""
        SELECT
            date_trunc('{date}',datetimestamp) as date,
            stationcode,
            {column_sql}
        FROM {table_name}
        GROUP BY date, stationcode
        ORDER BY date
    """
    uri = create_cfg_uri()
    engine = create_engine(uri)
    df = pd.read_sql(sql, con=engine)

    return df


def plot_line():
    df = get_aggregate_by_date('water_nutrient')
    table = table_params('water_nutrient')

    # for use in legend and restructuring df
    labels = table['params']

    # Need to convert datetime to something other than
    # datetime64 bc matplotlib doesn't seem to support it.
    df.index = df['date'].astype('O')
    del df['date']

    # Aggregate dict
    f = {}
    for param in table['params']:
        f[f'avg_{param}'] = ['mean']
        f[f'{param}_count'] = ['sum']

    # Multi-index/grouped df.
    gdf = df.groupby([('stationcode'), (df.index.year.rename('year')), (df.index.month).rename('month')]).agg(f)

    # Creating tuples to restructure df
    tuples = []
    for x in gdf.columns.values:
        for y in labels:
            # Checks if plain param is in avg_param/count_param
            if y in x[0]:
                # (plain param, sum or mean)
                tuples.append((y, x[1]))

    gdf.columns = pd.MultiIndex.from_tuples(tuples)
    fig, axes = plt.subplots(nrows=2, ncols=1)

    fig.suptitle("Number of observations in station x")
    gdf.loc['job20nut', (slice(None), 'sum')].groupby('year').sum().plot(kind='bar', ax=axes[0], legend=False)
    gdf.loc['job20nut', (slice(None), 'sum')].groupby('month').sum().plot(kind='bar', ax=axes[1], legend=False)

    axes[0].set_xlabel("Year")
    axes[0].set_ylabel("Count")
    axes[1].set_xlabel("Month")
    axes[1].set_ylabel("Count")
    plt.tight_layout(pad=3, h_pad=2)

    handles = axes[0].get_legend_handles_labels()
    # # pdb.set_trace()
    fig.legend(labels=labels, loc='upper right')    
    # # plt.figlegend(('po4f', "nh4f", "no2f", "no3f", "no23f", "chla_n"))
    plt.show()
