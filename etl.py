import configparser
import datetime

import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def print_executing_query_in_one_line(query):
    one_liner = " ".join(query.splitlines())
    print('{}: Executing query:\n{:.100}..'.format(datetime.datetime.now(), one_liner))


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print_executing_query_in_one_line(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
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
