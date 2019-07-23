import configparser
import datetime

import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def print_executing_query_in_one_line(query):
    """
    Print time and a slice of the query.

    :param query: the query string.
    :return: None
    """
    one_liner = " ".join(query.splitlines())
    print('{}: Executing query:\n{:.100}..'.format(datetime.datetime.now(), one_liner))


def load_staging_tables(cur, conn):
    """
    Load data from S3 using COPY SQL statement, for each query listed on copy_table_queries.

    :param cur: Database Cursor
    :param conn: Database Connection
    :return: None
    """
    for query in copy_table_queries:
        print_executing_query_in_one_line(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data from staging tables into dimensional model tables.

    :param cur: Database Cursor
    :param conn: Database Connection
    :return: None
    """
    for query in insert_table_queries:
        print_executing_query_in_one_line(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
