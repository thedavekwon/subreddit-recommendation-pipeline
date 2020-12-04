import configparser

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine

config = configparser.ConfigParser()
config.read("config/config.ini")

Base = automap_base()
engine = create_engine(config["DB"].get("db_url"))
Base.prepare(engine, reflect=True)

Redditor = Base.classes.redditor
Subreddit = Base.classes.subreddit
Submission = Base.classes.submission
Comment = Base.classes.comment
Subreddit = Base.classes.subreddit
Redditor_Subreddit_Submission = Base.classes.redditor_subreddit_submission
Redditor_Subreddit_Comment = Base.classes.redditor_subreddit_comment

if __name__ == "__main__":
    pass