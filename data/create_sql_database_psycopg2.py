#!/usr/bin/python
import psycopg2
from configparser import ConfigParser


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename)
        )

    return db


def create_table(table_name=False, table_atts=False):
    """ Connect to the PostgreSQL database server """
    conn = None

    if table_name is False or table_atts is False:
        print("Invalid parameters.")
        return

    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # create column_name
        print('Creating -{0}- column...'.format(table_name))

        # table_attributes = """
        #     id integer PRIMARY KEY,
        #     name varchar,
        #     station_code varchar
        # """

        cur.execute("""
            CREATE TABLE {0}(
                {1}
        )""".format(table_name, table_attributes))

        conn.commit()

        print("The -{0}- column has been created.".format(table_name))

        # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')



# if __name__ == '__main__':
#     create_table(column_name, sql)
