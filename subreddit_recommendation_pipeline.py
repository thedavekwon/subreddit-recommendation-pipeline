from lib.dag import DAG, Task
from tasks import fetch_reddit as fr, create_table as ct, train
import logging

logging.getLogger().setLevel(logging.ERROR)


if __name__ == "__main__":
    dag = DAG('subreddit_recommendation_pipeline', 1)
    
    # Create full table if not exist
    create_table = Task('create_table', ct.create_table, ["sql/subreddit_stage.sql"], dag)
    
    # Create daily table if not exist
    create_partition_table = Task('create_partition_table', ct.create_partition_table, None, dag, True)
    
    # Extract from Reddit
    fetch_reddit = Task('fetch_reddit', fr.fetch_reddit, None, dag, True)
        
    # Train recommendation implicit model  
    train_model_implicit = Task('train_model_implicit', train.train_implicit, None, dag)

    create_table.add_edge(create_partition_table)
    create_partition_table.add_edge(fetch_reddit)
    fetch_reddit.add_edge(train_model_implicit)
    
    # draw dag
    # dag.draw()

    # scheduler
    # dag.run_scheduler()
    dag.run()
    