import concurrent.futures
import configparser
import praw
import logging

import datetime as dt
import numpy as np
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from . import models as m

# import models as m
from typing import Dict
from praw.models import Redditor, Submission, Comment
from psaw import PushshiftAPI


def parse_redditor(redditor: Redditor) -> Dict[str, any]:
    if not redditor or not hasattr(redditor, "id") or not redditor.id:
        raise Exception("Reddit account deleted")
    return {"id": redditor.id, "name": redditor.name}


class RedditSubmission:
    def __init__(self, submission: Submission):
        self.submission = submission
        # Get submission author
        self.author = submission.author
        # Get submission subreddit
        self.subreddit = submission.subreddit
        # Get submission comments
        self.comments = submission.comments
        # Flatten comments
        self.comments.replace_more(limit=None)
        # Filter deleted comments
        self.comments = list(filter(lambda x: x.author, self.comments))


class RedditScraper:
    def __init__(self, subreddit_list, ds, after, before, config):
        self.reddit: praw.Reddit = praw.Reddit(
            client_id=config["REDDIT"].get("client_id"),
            client_secret=config["REDDIT"].get("client_secret"),
            user_agent=config["REDDIT"].get("user_agent"),
        )
        self.api = PushshiftAPI(self.reddit)
        self.subreddit_list = subreddit_list
        self.ds = ds
        self.after = after
        self.before = before
        self.submissions = None
        self.engine = create_engine(config["DB"].get("db_url"))
        self.session = Session(self.engine)

    def get_submissions(self):
        self.submissions = map(
            RedditSubmission,
            self.api.search_submissions(
                after=self.after,
                before=self.before,
                subreddit=",".join(self.subreddit_list),
            ),
        )

    def upload(self, fetch_past):
        redditors = {}
        submissions = {}
        subreddits = {}
        comments = {}
        redditor_subreddit_comments = {}
        redditor_subreddit_submissions = {}
        for s in self.submissions:
            try:
                if (
                    not s
                    or not s.submission
                    or not hasattr(s.submission, "id")
                    or not s.submission.id
                    or not s.author
                    or not hasattr(s.author, "id")
                    or not s.author.id
                ):
                    continue
                if not fetch_past:
                    redditors[s.author.id] = parse_redditor(s.author)
                    submissions[s.submission.id] = {
                        "id": s.submission.id,
                        "score": s.submission.score,
                    }
                    subreddits[s.subreddit.id] = {
                        "id": s.subreddit.id,
                        "name": s.subreddit.display_name,
                    }
                    redditor_subreddit_submissions[s.submission.id] = {
                        "redditor_id": s.author.id,
                        "subreddit_id": s.subreddit.id,
                        "submission_id": s.submission.id,
                        "ds": self.ds,
                    }
                for c in s.comments:
                    if (
                        not c
                        or not hasattr(c, "id")
                        or not c.id
                        or not c.author
                        or not hasattr(c.author, "id")
                    ):
                        continue
                    comments[c.id] = {
                        "id": c.id,
                        "submission_id": s.submission.id,
                        "score": c.score,
                    }
                    subreddits[s.subreddit.id] = {
                        "id": s.subreddit.id,
                        "name": s.subreddit.display_name,
                    }
                    redditors[c.author.id] = parse_redditor(c.author)
                    redditor_subreddit_comments[c.id] = {
                        "redditor_id": c.author.id,
                        "subreddit_id": s.subreddit.id,
                        "comment_id": c.id,
                        "ds": self.ds,
                    }
            except Exception as e:
                logging.warning(f"An error has occured while parsing: {e}")
                continue

        if not redditor_subreddit_comments and not redditor_subreddit_submissions:
            return len(redditor_subreddit_submissions), len(redditor_subreddit_comments)

        # PostgreSQL upsert
        # https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert
        try:
            self.session.execute(
                insert(m.Redditor)
                .values(list(redditors.values()))
                .on_conflict_do_nothing()
            )
            if not fetch_past:
                self.session.execute(
                    insert(m.Submission)
                    .values(list(submissions.values()))
                    .on_conflict_do_nothing()
                )
                self.session.execute(
                    insert(m.Redditor_Subreddit_Submission)
                    .values(list(redditor_subreddit_submissions.values()))
                    .on_conflict_do_nothing()
                )
            self.session.execute(
                insert(m.Subreddit)
                .values(list(subreddits.values()))
                .on_conflict_do_nothing()
            )
            self.session.execute(
                insert(m.Comment)
                .values(list(comments.values()))
                .on_conflict_do_nothing()
            )
            self.session.execute(
                insert(m.Redditor_Subreddit_Comment)
                .values(list(redditor_subreddit_comments.values()))
                .on_conflict_do_nothing()
            )
            self.session.commit()
        except Exception as e:
            logging.warning(f"An error occured during insertion: {e}")
            self.session.rollback()
        finally:
            self.session.close()
        return len(redditor_subreddit_submissions), len(redditor_subreddit_comments)


def scrape(fetch_past, subreddit_list, ds, after, before, config):
    temp_str = "all" if not fetch_past else "comment"
    logging.info(f"Scraping {temp_str} for between {after} and {before} started")
    rs = RedditScraper(
        subreddit_list, ds, int(after.timestamp()), int(before.timestamp()), config
    )
    rs.get_submissions()
    len_submissions, len_comments = rs.upload(fetch_past)
    logging.info(
        f"Scraping {temp_str} for between {after} and {before} fetched {len_submissions} submissions and {len_comments} comments"
    )


def fetch_reddit(fetch_past, now):
    cur = dt.datetime.now()
    logging.info("Fetching reddit information started")
    delta = dt.timedelta(hours=-1) if not fetch_past else dt.timedelta(days=-3)
    before = dt.datetime(now.year, now.month, now.day, now.hour) + delta
    after = before + dt.timedelta(hours=-1)
    ds = now.strftime("%Y-%m-%d")

    config = configparser.ConfigParser()
    config.read("config/config.ini")
    num_threads = int(config["REDDIT"].get("num_threads"))
    subreddit_list_path = config["REDDIT"].get("subreddit_list_path")
    with open(subreddit_list_path) as f:
        subreddit_list = np.array(f.readline().split(","))
    np.random.shuffle(subreddit_list)
    if not len(subreddit_list):
        raise Exception("Empty subreddit list")
    subreddit_lists = np.array_split(subreddit_list, num_threads)

    # Multi-Threading pool for Network I/O bound
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for subreddit_list in subreddit_lists:
            executor.submit(
                scrape, fetch_past, subreddit_list, ds, after, before, config
            )
    logging.info(
        f"Fetching reddit information ended in {round((dt.datetime.now()-cur).total_seconds(), 2)}s"
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    # fetch_reddit(False, dt.datetime(2020, i, j, k))
