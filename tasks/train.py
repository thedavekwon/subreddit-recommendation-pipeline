import configparser
import implicit
import scipy
import numpy as np
import pandas as pd

# import models as m
from . import models as m
from collections import defaultdict
from mlxtend.frequent_patterns import apriori
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import func
from implicit.nearest_neighbours import bm25_weight


def train_apriori(df):
    # Only get users with interactions with more than one subreddit
    # df = df[df.sum(axis=1) > 1]
    print(df.shape)
    apriori_df = apriori(df, min_support=0.004, use_colnames=True)
    apriori_df["length"] = apriori_df["itemsets"].apply(lambda x: len(x))
    apriori_df = apriori_df[apriori_df["length"] >= 2]
    print(apriori_df)


def train_implicit(df):
    model = implicit.als.AlternatingLeastSquares(factors=100)
    user_item = scipy.sparse.csr_matrix(df.values)
    ratings = scipy.sparse.csr_matrix(df.values.T)

    # bm25 weights
    ratings = (bm25_weight(ratings, B=0.9) * 5).tocsr()
    model.fit(ratings)
    user_id = 40
    print(ratings.shape)
    print(df.index[user_id], ratings.getcol(user_id).T)
    recommends = model.recommend(user_id, user_item, recalculate_user=False, N=5)
    user_item_recommend = [(df.columns[r], s) for r, s in recommends]
    # models.explain
    print(user_item_recommend)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    engine = create_engine(config["DB"].get("db_url"))
    session = Session(engine)

    s = (
        session.query(m.Redditor_Subreddit_Comment.redditor_id)
        .group_by(m.Redditor_Subreddit_Comment.redditor_id)
        .having(func.count(m.Redditor_Subreddit_Comment.subreddit_id.distinct()) > 4)
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
    # train_nmf(df)
    # train_apriori(df)
    train_implicit(df)
