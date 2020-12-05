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
    logging.critical("Training for implicit recommendation model started")
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    engine = create_engine(config["DB"].get("db_url"))
    session = Session(engine)

    s = (
        session.query(m.Redditor_Subreddit_Comment.redditor_id)
        .group_by(m.Redditor_Subreddit_Comment.redditor_id)
        .having(func.count(m.Redditor_Subreddit_Comment.subreddit_id.distinct()) > 2)
        .subquery()
    )
    redditor_subreddit_comment = np.array(
        session.query(m.Redditor_Subreddit_Comment.redditor_id, m.Subreddit.name)
        .filter(m.Redditor_Subreddit_Comment.redditor_id.in_(s))
        .join(m.Subreddit, m.Redditor_Subreddit_Comment.subreddit_id == m.Subreddit.id)
        .all()
    ).T
    df = pd.DataFrame(
        {
            "userID": redditor_subreddit_comment[0],
            "subredditID": redditor_subreddit_comment[1],
            "rating": np.ones_like(redditor_subreddit_comment[0]),
        }
    )
    df["rating"] = pd.to_numeric(df["rating"])
    df = df.pivot_table(
        index="userID",
        columns="subredditID",
        values=["rating"],
        aggfunc="mean",
        fill_value=0,
    )
    # Remove multi-level index
    df.columns = df.columns.levels[1]
    r = redis.Redis(
        host=config["REDIS"].get("host"),
        port=config["REDIS"].get("port"),
        db=config["REDIS"].get("db"),
    )

    model = implicit.als.AlternatingLeastSquares(factors=100)
    user_item = scipy.sparse.csr_matrix(df.values)
    ratings = scipy.sparse.csr_matrix(df.values.T)

    # bm25 weights
    ratings = (bm25_weight(ratings, B=0.9) * 5).tocsr()
    model.fit(ratings)
    logging.critical("Training for implicit recommendation model ended")
    logging.critical("Loading Reddit Recommendation to Redis started")
    for user_id in range(len(df.index)):
        recommends = model.recommend(user_id, user_item, recalculate_user=False, N=5)
        user_item_recommend = [df.columns[r] for r, s in recommends]
        # Delete only when we are updating
        r.delete(df.index[user_id])
        for subreddit in user_item_recommend:
            r.rpush(df.index[user_id], subreddit)
    logging.critical("Loading Reddit Recommendation to Redis ended")
    return df.shape


if __name__ == "__main__":
    # train_nmf(df)
    # train_apriori(df)
    train_implicit()
