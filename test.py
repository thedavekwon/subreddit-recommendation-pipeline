from lib.dag import Task, DAG

def test_dag_cyclic_true():
    d = DAG("test", 1)
    t1 = Task("1", None, None, d)
    t2 = Task("2", None, None, d)
    t3 = Task("3", None, None, d)
    t4 = Task("4", None, None, d)
    t1.add_edge(t2)
    t2.add_edge(t3)
    t3.add_edge(t4)
    t4.add_edge(t1)
    assert d.check_cycle()
    
def test_dag_cyclic_false():
    d = DAG("test", 1)
    t1 = Task("1", None, None, d)
    t2 = Task("2", None, None, d)
    t3 = Task("3", None, None, d)
    t4 = Task("4", None, None, d)
    t1.add_edge(t2)
    t1.add_edge(t3)
    t2.add_edge(t4)
    t3.add_edge(t4)
    assert not d.check_cycle()