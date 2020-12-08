import datetime as dt
import time
import concurrent.futures
import logging
import copy

import schedule
import networkx as nx
import matplotlib.pyplot as plt

logging.getLogger().setLevel(logging.CRITICAL)
LOG_FORMAT = "%(asctime)-15s %(message)s"
logging.basicConfig(format=LOG_FORMAT)
fh = logging.FileHandler("log/log.log")
fh.setLevel(logging.CRITICAL)
fh.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(fh)


class Task:
    def __init__(self, id, task, params, dag, need_dt=False):
        if not isinstance(dag, DAG):
            raise Exception(f"Invalid DAG: {dag}")
        if not id:
            raise Exception(f"Invalid format of id: {id}")
        if id in dag.tasks:
            raise Exception(f"Duplicate id in DAG: {id}")
        self.id = id
        self.dag = dag
        self.task = task
        self.params = params
        if not self.params:
            self.params = []
        self.need_dt = need_dt
        dag.tasks[id] = self

    def add_edge(self, task):
        if not isinstance(task, Task):
            raise Exception(f"Invalid Task: {task}")
        if self.dag != task.dag:
            raise Exception(f"Task should be in the same DAG")
        if task.id == self.id:
            raise Exception(f"Cannot add edge to itself: {self.id}")

        self.dag.add_edge(self, task)

    def add_edges(self, tasks):
        for task in tasks:
            self.add_edge(task)


class DAG:
    def __init__(self, id, hour):
        self.id = id
        self.hour = hour
        self.adj_list = {}
        self.inverted_adj_list = {}
        self.inverted_adj_list_temp = {}
        self.tasks = {}

        # For visualization
        self.nx_graph = nx.DiGraph()

    def add_edge(self, task_a, task_b):
        self.adj_list[task_a.id] = self.adj_list.get(task_a.id, []) + [task_b.id]
        self.inverted_adj_list[task_b.id] = self.inverted_adj_list.get(
            task_b.id, []
        ) + [task_a.id]
        self.nx_graph.add_edge(task_a.id, task_b.id)

    # https://www.geeksforgeeks.org/detect-cycle-in-a-graph/
    def dfs(self, v, visited, curstack):
        visited[v] = 1
        curstack[v] = 1

        for n in self.adj_list.get(v, []):
            if not visited[n]:
                if self.dfs(n, visited, curstack) == True:
                    return True
            elif curstack[n]:
                return True
        curstack[v] = 0
        return False

    def check_cycle(self):
        visited = dict.fromkeys(self.tasks.keys(), 0)
        curstack = dict.fromkeys(self.tasks.keys(), 0)
        for v in self.tasks.keys():
            if not visited[v]:
                if self.dfs(v, visited, curstack) == True:
                    return True
        return False

    def draw(self):
        plt.plot()
        nx.draw(self.nx_graph, with_labels=True, font_weight="bold")
        plt.show()

    def get_initial_tasks(self):
        return [
            task
            for task in self.tasks.keys()
            if task not in self.inverted_adj_list_temp
        ]

    def update_task(self, task):
        for t in self.inverted_adj_list_temp.keys():
            if task in self.inverted_adj_list_temp[t]:
                self.inverted_adj_list_temp[t].remove(task)

    def get_ready_task(self, waiting_tasks, future_to_task):
        return [
            task
            for task in self.inverted_adj_list_temp.keys()
            if len(self.inverted_adj_list_temp[task]) == 0
            and task in waiting_tasks
            and task not in future_to_task.values()
        ]

    def insert_tasks(self, executor, future_to_task, ready_tasks, now):
        for task in ready_tasks:
            task_node = self.tasks[task]
            if task_node.need_dt:
                future_to_task[
                    executor.submit(
                        task_node.task,
                        *task_node.params,
                        now,
                    )
                ] = task
            else:
                future_to_task[
                    executor.submit(
                        task_node.task,
                        *task_node.params,
                    )
                ] = task

    def run(self):
        logging.critical("DAG started")
        
        # Get current timestamp
        now = dt.datetime.now()

        executor = concurrent.futures.ProcessPoolExecutor()

        # Reset
        self.inverted_adj_list_temp = copy.deepcopy(self.inverted_adj_list)

        # All tasks
        tasks = list(self.tasks.keys())
        future_to_task = {}
        ready_tasks = self.get_initial_tasks()

        self.insert_tasks(executor, future_to_task, ready_tasks, now)
        while tasks:
            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                if task not in tasks:
                    time.sleep(1)
                    continue
                tasks.remove(task)
                self.update_task(task)
                ready_tasks = self.get_ready_task(tasks, future_to_task)
                self.insert_tasks(executor, future_to_task, ready_tasks, now)
        executor.shutdown()
        logging.critical("DAG ended")

    def run_scheduler(self):
        logging.critical("Scheduler Started")
        schedule.every(self.hour).hour.at(":00").do(self.run)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    d = DAG("test", 1)
    t1 = Task("1", None, None, d)
    t2 = Task("2", None, None, d)
    t3 = Task("3", None, None, d)
    t4 = Task("4", None, None, d)
    # t1.add_edges([t2, t3])
    # t2.add_edge(t4)
    # t3.add_edge(t4)
    t1.add_edge(t2)
    t1.add_edge(t3)
    t2.add_edge(t4)
    t3.add_edge(t4)
    print(d.check_cycle())
