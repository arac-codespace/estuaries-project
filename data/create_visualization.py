from create_database_with_sqlalchemy import create_cfg_uri
import os
import pandas as pd
from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP


def create_math_sql(fx, params):
    fx_statements = []
    for param in params:
        fx_statements.append(
            f"{fx}(case when f_{param} not like '%%-%%' then {param} end) as {fx}_{param}"
        )
    # Create expression string
    expression = ",".join(fx_statements)
    print(expression)
    return expression


def get_water_nutrients():

    nutrient_params = [
        "PO4F", "NH4F", "NO2F", "NO3F",
        "NO23F", "CHLA_N",
    ]

    column_sql = create_math_sql("avg", nutrient_params)

    sql = f"""
        SELECT
            date_trunc('month',datetimestamp),
            stationcode,
            {column_sql}
        FROM water_nutrient
        GROUP BY date_trunc('month',datetimestamp), stationcode
        ORDER BY date_trunc
    """
    uri = create_cfg_uri()
    engine = create_engine(uri)
    # sql = "SELECT * FROM water_nutrient"

    df = pd.read_sql(sql, con=engine)

    return df
