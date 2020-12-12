import configparser
import datetime as dt
import logging

import sqlalchemy

from sqlalchemy import create_engine
from sqlalchemy.orm import Session


def create_partition_table(now):
    """
    create partition tables for redditor_subreddit_submission and redditor_subreddit_comment
    with the current ds.
    """
    ds = (
        dt.datetime(now.year, now.month, now.day, now.hour) + dt.timedelta(hours=-1)
    ).strftime("%Y-%m-%d")
    logging.info(f"Creating parition tables for ds({ds}) started")
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    db = create_engine(config["DB"].get("db_url"))
    session = Session(db)
    try:
        redditor_subreddit_submission_string = f"""
            CREATE TABLE IF NOT EXISTS redditor_subreddit_submission_{ds.replace("-", "_")}
            PARTITION OF redditor_subreddit_submission FOR VALUES IN ('{ds}');
        """
        redditor_subreddit_comment_string = f"""
            CREATE TABLE IF NOT EXISTS redditor_subreddit_comment_{ds.replace("-", "_")}
            PARTITION OF redditor_subreddit_comment FOR VALUES IN ('{ds}');
        """
        session.execute(redditor_subreddit_submission_string)
        session.execute(redditor_subreddit_comment_string)
        session.commit()
        logging.info(f"Successfully created partition table for ds({ds})")
    except:
        logging.error(f"Failed to create partition table for ds({ds})")
        session.rollback()
    finally:
        session.close()

    logging.info(f"Creating parition tables for ds({ds}) ended")


def create_table(path):
    """
    create tables with using sql file.
    """
    logging.info(f"Creating full tables with {path} started")
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    db = create_engine(config["DB"].get("db_url"))
    sql_file = open(path, "r")
    session = Session(db)
    try:
        session.execute(sqlalchemy.text(sql_file.read()))
        session.commit()
        logging.info(f"Successfully created full tables with {path}")
    except:
        logging.error(f"Failed to create full tables with {path}")
        session.rollback()
    finally:
        session.close()
    logging.info(f"Creating full tables with {path} ended")


if __name__ == "__main__":
    create_partition_table("2020-12-04")