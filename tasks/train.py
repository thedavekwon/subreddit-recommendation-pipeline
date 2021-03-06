import configparser
import implicit
import logging
import scipy
import redis
import numpy as np
import pandas as pd

# import models as m

from . import models as m
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import func
from implicit.nearest_neighbours import bm25_weight


def train_implicit():
    logging.info("Training for implicit recommendation model started")

    config = configparser.ConfigParser()
    config.read("config/config.ini")

    # Create SQLAlchemy Session
    engine = create_engine(config["DB"].get("db_url"))
    session = Session(engine)

    # Get users who commented on more than two different subreddits
    s = (
        session.query(m.Redditor_Subreddit_Comment.redditor_id)
        .group_by(m.Redditor_Subreddit_Comment.redditor_id)
        .having(func.count(m.Redditor_Subreddit_Comment.subreddit_id.distinct()) > 2)
        .subquery()
    )
    redditor_subreddit_comment = np.array(
        session.query(
            m.Redditor_Subreddit_Comment.redditor_id, m.Subreddit.name, m.Redditor.name
        )
        .distinct(m.Redditor_Subreddit_Comment.redditor_id, m.Subreddit.name)
        .filter(m.Redditor_Subreddit_Comment.redditor_id.in_(s))
        .join(m.Subreddit, m.Redditor_Subreddit_Comment.subreddit_id == m.Subreddit.id)
        .join(m.Redditor, m.Redditor_Subreddit_Comment.redditor_id == m.Redditor.id)
        .all()
    ).T
    df = pd.DataFrame(
        {
            "user_id": redditor_subreddit_comment[0],
            "subreddit_id": redditor_subreddit_comment[1],
            "rating": np.ones_like(redditor_subreddit_comment[0]),
        }
    )
    df["rating"] = pd.to_numeric(df["rating"])
    df = df.pivot_table(
        index="user_id",
        columns="subreddit_id",
        values=["rating"],
        aggfunc="mean",
        fill_value=0,
    )
    # Remove multi-level index
    df.columns = df.columns.levels[1]

    # Create Redis Client
    r = redis.Redis(
        host=config["REDIS"].get("host"),
        port=config["REDIS"].get("port"),
        db=config["REDIS"].get("db"),
    )

    model = implicit.als.AlternatingLeastSquares(factors=100, iterations=30)
    user_item = scipy.sparse.csr_matrix(df.values)
    ratings = scipy.sparse.csr_matrix(df.values.T)

    # bm25 weights
    ratings = (bm25_weight(ratings)).tocsr()
    model.fit(ratings)
    logging.info("Training for implicit recommendation model ended")
    logging.info("Loading Reddit Recommendation to Redis started")
    for user_id in range(len(df.index)):
        recommends = model.recommend(user_id, user_item, recalculate_user=False, N=5)
        user_item_recommend = [df.columns[r] for r, s in recommends]
        # Delete only when we are updating
        r.delete(df.index[user_id])
        for subreddit in user_item_recommend:
            r.rpush(df.index[user_id], subreddit)
    logging.info("Loading Reddit Recommendation to Redis ended")
    return df.shape


if __name__ == "__main__":
    # train_nmf(df)
    # train_apriori(df)
    train_implicit()
