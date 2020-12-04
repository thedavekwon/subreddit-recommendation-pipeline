CREATE TABLE IF NOT EXISTS redditor
(
    id   VARCHAR(255) PRIMARY KEY NOT NULL,
    name VARCHAR(255)
);


CREATE TABLE IF NOT EXISTS subreddit
(
    id   VARCHAR(255) PRIMARY KEY NOT NULL,
    name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS submission
(
    id    VARCHAR(255) PRIMARY KEY NOT NULL,
    score FLOAT
);

CREATE TABLE IF NOT EXISTS comment
(
    id            VARCHAR(255) PRIMARY KEY NOT NULL,
    submission_id VARCHAR(255),
    score         FLOAT,

    FOREIGN KEY (submission_id) REFERENCES submission (id)
);

CREATE TABLE IF NOT EXISTS redditor_subreddit_submission
(
    redditor_id   VARCHAR(255) NOT NULL,
    subreddit_id  VARCHAR(255) NOT NULL,
    submission_id VARCHAR(255) NOT NULL,
    ds            DATE         NOT NULL,

    PRIMARY KEY (redditor_id, subreddit_id, submission_id, ds),
    FOREIGN KEY (redditor_id) REFERENCES redditor (id),
    FOREIGN KEY (subreddit_id) REFERENCES subreddit (id),
    FOREIGN KEY (submission_id) REFERENCES submission (id)
) PARTITION BY LIST (ds);

CREATE TABLE IF NOT EXISTS redditor_subreddit_comment
(
    redditor_id  VARCHAR(255) NOT NULL,
    subreddit_id VARCHAR(255) NOT NULL,
    comment_id   VARCHAR(255) NOT NULL,
    ds           DATE         NOT NULL,


    PRIMARY KEY (redditor_id, subreddit_id, comment_id, ds),
    FOREIGN KEY (redditor_id) REFERENCES redditor (id),
    FOREIGN KEY (subreddit_id) REFERENCES subreddit (id),
    FOREIGN KEY (comment_id) REFERENCES comment (id)
) PARTITION BY LIST (ds);

CREATE INDEX ON redditor_subreddit_submission (ds);
CREATE INDEX ON redditor_subreddit_comment (ds);

/* CREATE TABLE IF NOT EXISTS redditor_subreddit_comment_2020_01_01 PARTITION OF redditor_subreddit_comment
    FOR VALUES IN ('2020-01-01');

CREATE TABLE IF NOT EXISTS redditor_subreddit_submission_2020_01_01 PARTITION OF redditor_subreddit_submission
    FOR VALUES IN ('2020-01-01');

drop schema public cascade;
create schema public; */