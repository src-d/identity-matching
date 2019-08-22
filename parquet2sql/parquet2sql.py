import argparse
import logging

import pandas as pd
from sqlalchemy import create_engine


def get_parser():
    parser = argparse.ArgumentParser(
        description="Util to write a Parquet file to an SQL database.")
    parser.add_argument("--parquet", required=True, help="Parquet file to write to SQL database.")
    parser.add_argument(
        "--db", default="postgresql://superset:superset@localhost:5432/superset",
        help="the URL that indicates database dialect and connection arguments. The string form "
             "of the URL is ``dialect[+driver]://user:password@host/dbname[?key=value..]``, "
             "where ``dialect`` is a database name such as ``mysql``, ``oracle``, ``postgresql``, "
             "etc., and ``driver`` the name of a DBAPI, such as ``psycopg2``, ``pyodbc``, "
             "``cx_oracle``, etc.")
    parser.add_argument("--table", required=True, help="Name of the table in the database.")
    return parser


def parquet2sql(args):
    log = logging.getLogger("parquet2sql")
    df = pd.read_parquet(args.parquet)
    engine = create_engine(args.database)
    df.to_sql(args.table, engine)
    log.info("File %s saved to table %s at %s" % (args.parquet, args.table, args.database))
    return 0


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    exit(parquet2sql(args))
