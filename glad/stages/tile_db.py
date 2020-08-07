import pandas as pd
import sqlite3
import logging
import glob
from datetime import datetime


def delete_current_years(**kwargs):
    """
    Delete all entries for current years from DB
    :param db:
    :param kwargs:
    :return:
    """
    years = kwargs["years"]
    db_path = kwargs["db"]["db_path"]
    db_table = kwargs["db"]["db_table"]

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for year in years:

        logging.info("Delete entries for year {}".format(year))
        cursor.execute(
            "DELETE FROM {0} "
            "WHERE alert_date >= '{1}-01-01' "
            "AND alert_date <= '{1}-12-31';".format(db_table, year)
        )

    logging.info("Delete any future entries, if they exist")
    today = datetime.now().strftime("%Y-%m-%d")
    cursor.execute(
        "DELETE FROM {0} "
        "WHERE alert_date > '{1}';".format(db_table, today)
    )

    conn.commit()
    conn.close()


def get_xyz_csv(**kwargs):
    root = kwargs["root"]

    logging.info("Get XYZ csv files.")
    for csv in glob.iglob(root + "/db/*/csv/*/*.csv"):
        yield csv


def make_main_table(csvs):

    df = pd.DataFrame()
    logging.info("Merge files into one data frame")
    for csv in csvs:
        df = df.append(pd.read_csv(csv), ignore_index=True)

    return df


def group_df_by_xyz(df):

    logging.info("Group main_table by X/Y/Z date and conf")

    groupby_df = (
        df.groupby(["x", "y", "z", "alert_date", "confidence"]).sum().reset_index()
    )
    return groupby_df


def insert_data(df, **kwargs):

    db_path = kwargs["db"]["db_path"]
    db_table = kwargs["db"]["db_table"]

    conn = sqlite3.connect(db_path)
    logging.info("insert into database " + db_path)
    df.to_sql(db_table, con=conn, if_exists="append", index=False)
    conn.commit()
    conn.close()


def reindex(**kwargs):

    db_path = kwargs["db"]["db_path"]
    db_table = kwargs["db"]["db_table"]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    logging.info("Rebuild index for table " + db_table)
    cur.execute("REINDEX {};".format(db_table))
    conn.commit()
    conn.close()


def update_latest(**kwargs):

    db_path = kwargs["db"]["db_path"]
    db_table = kwargs["db"]["db_table"]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    logging.info("Update latest date")
    cur.execute(
        "UPDATE latest "
        "SET alert_date = (SELECT max(alert_date) FROM {});".format(db_table)
    )
    conn.commit()
    conn.close()


def vacuum(**kwargs):

    db_path = kwargs["db"]["db_path"]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    logging.info("Vacuum database")
    cur.execute("VACUUM;")
    conn.commit()
    conn.close()
