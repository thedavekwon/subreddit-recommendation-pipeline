# Subreddit Recommendation ETL Pipeline
Final Project for ECE-464 Database for Cooper Union.

This project consists of a generalized ETL pipeline with a DAG scheduler to create a subreddit recommendation data pipeline. It stages data into PostgreSQL and stores recommendation in Redis. The recommendations are generated using a collaborative filtering method for implicit feedbacks. 

## Requirements
* PostgreSQL
* Redis
* Reddit developer account
* Use subreddit list in data/subreddits.csv or use python tasks/fetch_reddit_list.py to fetch new list.
* Or manually put subreddits that you are interested in data/subreddits.csv
* Fill in appropriate config to config/config.ini following the format of config/sample-config.ini
## Installation
```
git clone https://github.com/thedavekwon/subreddit-recommendation-pipeline.git
cd subreddit-recommendation-pipeline
pip install -r requirements.txt
```

## Usage
```
# Test DAG
pytest test.py

# Fetch subreddit list from redditlist.com
python tasks/fetch_reddit_list.py

# Run ETL pipeline
# look at comments in subreddit_recommendation_pipeline to visualize DAG, run once, or run the scheduled pipeline.
python tasks/subreddit_recommendation_pipeline.py

```