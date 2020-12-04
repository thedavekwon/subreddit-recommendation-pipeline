import time

import schedule
import networkx as nx
import matplotlib.pyplot as plt


class Task:
    def __init__(self, id: str, task, dag):
        if not isinstance(dag, DAG):
            raise Exception(f"Invalid DAG: {dag}")
        if not id:
            raise Exception(f"Invalid format of id: {id}")
        if id in dag.task_id_list:
            raise Exception(f"Duplicate id in DAG: {id}")
        self.id = id
        self.dag = dag
        self.task = task
        dag.task_id_list[id] = 0

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
    def __init__(self, id: str):
        self.id = id
        self.adj_list = {}
        self.inverted_adj_list = {}
        self.task_id_list = {}

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
        visited = self.task_id_list.copy()
        curstack = self.task_id_list.copy()
        for v in self.task_id_list.keys():
            if not visited[v]:
                if self.dfs(v, visited, curstack) == True:
                    return True
        return False

    def draw(self):
        plt.plot()
        nx.draw(self.nx_graph, with_labels=True, font_weight="bold")
        plt.show()
        
    def run(self):
        pass


if __name__ == "__main__":
    d = DAG("test")
    t1 = Task("1", None, d)
    t2 = Task("2", None, d)
    t3 = Task("3", None, d)
    t4 = Task("4", None, d)
    # t1.add_edges([t2, t3])
    # t2.add_edge(t4)
    # t3.add_edge(t4)
    t1.add_edge(t2)
    t1.add_edge(t3)
    t2.add_edge(t4)
    t3.add_edge(t4)
    print(d.check_cycle())
