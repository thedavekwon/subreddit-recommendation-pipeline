from lib.dag import DAG, Task
from tasks import fetch_reddit as fr, create_table as ct, train

if __name__ == "__main__":
    dag = DAG(id="subreddit_recommendation_pipeline", hour=1)

    # Create full table if not exist
    create_table = Task(
        "create_table", ct.create_table, ["sql/subreddit_stage.sql"], dag
    )

    # Create daily table if not exist
    create_partition_table = Task(
        "create_partition_table", ct.create_partition_table, None, dag, True
    )

    # Extract from Reddit
    fetch_reddit_now = Task("fetch_reddit_now", fr.fetch_reddit, [False], dag, True)

    fetch_reddit_past = Task("fetch_reddit_past", fr.fetch_reddit, [True], dag, True)

    # Train recommendation implicit model
    train_model_implicit = Task("train_model_implicit", train.train_implicit, None, dag)

    create_table.add_edge(create_partition_table)
    create_partition_table.add_edges([fetch_reddit_now, fetch_reddit_past])
    fetch_reddit_now.add_edge(train_model_implicit)
    fetch_reddit_past.add_edge(train_model_implicit)

    # Draw dag
    # dag.draw()

    # Run once
    # if not dag.check_cycle():
    #     dag.run()

    # Run scheduler if cycle does not exist in DAG
    if not dag.check_cycle():
        dag.run_scheduler()
