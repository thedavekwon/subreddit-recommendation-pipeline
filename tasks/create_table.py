import configparser
import datetime as dt
import logging

import sqlalchemy

from sqlalchemy import create_engine


def create_partition_table(now):
    ds = (
        dt.datetime(now.year, now.month, now.day, now.hour) + dt.timedelta(hours=-1)
    ).strftime("%Y-%m-%d")
    logging.critical(f"Creating parition tables for ds({ds}) started")
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    db = create_engine(config["DB"].get("db_url"))
    redditor_subreddit_submission_string = f"""
        CREATE TABLE IF NOT EXISTS redditor_subreddit_submission_{ds.replace("-", "_")}
        PARTITION OF redditor_subreddit_submission FOR VALUES IN ('{ds}');
    """
    redditor_subreddit_comment_string = f"""
        CREATE TABLE IF NOT EXISTS redditor_subreddit_comment_{ds.replace("-", "_")}
        PARTITION OF redditor_subreddit_comment FOR VALUES IN ('{ds}');
    """
    db.execute(redditor_subreddit_submission_string)
    db.execute(redditor_subreddit_comment_string)
    logging.critical(f"Creating parition tables for ds({ds}) ended")


def create_table(path):
    logging.critical(f"Creating full tables with {path} started")
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    db = create_engine(config["DB"].get("db_url"))
    sql_file = open(path, "r")
    db.execute(sqlalchemy.text(sql_file.read()))
    logging.critical(f"Creating full tables with {path} ended")


if __name__ == "__main__":
    create_partition_table("2020-12-04")