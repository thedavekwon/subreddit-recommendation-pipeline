import configparser

import sqlalchemy

from sqlalchemy import create_engine


def create_partition_table(db, ds):
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


def create_table(db, path):
    sql_file = open(path, "r")
    db.execute(sqlalchemy.text(sql_file.read()))


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    db = create_engine(config["DB"].get("db_url"))
    create_partition_table(db, "2020-01-01")