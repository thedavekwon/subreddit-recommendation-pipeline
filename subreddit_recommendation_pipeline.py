from lib.dag import DAG, Task
from tasks import fetch_reddit as fr, create_table as ct, train

if __name__ == "__main__":
    dag = DAG('subreddit_recommendation_pipeline')
    
    # Create full table if not exist
    create_full_table = Task('create_full_table', ct.create_table, dag)
    
    # Create daily table if not exist
    create_daily_table = Task('create_daily_table', ct.create_table, dag)
    
    # Extract from Reddit
    fetch_reddit = Task('fetch_reddit', fr.fetch_reddit, dag)
    
    # Train recommendation apriori model  
    train_model_apriori = Task('train_model_apriori', train.train_apriori, dag)
    
    # Train recommendation implicit model  
    train_model_implicit = Task('train_model_implicit', train.train_implicit, dag)
    
    create_full_table.add_edge(create_daily_table)
    create_daily_table.add_edge(fetch_reddit)
    fetch_reddit.add_edges([train_model_apriori, train_model_implicit])
    
    # draw dag
    dag.draw()

    # scheduler
    
    