
from __future__ import annotations

from datetime import datetime
import os
from glob import glob
import yaml
import networkx as nx

from airflow.models.dag import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash import BashOperator

class RunTransform(BaseOperator):
    """A custom operator that adds one to the input."""

    def __init__(self, doc, sources, **kwargs):
        super().__init__(**kwargs)
        self.doc = doc
        self.sources = sources

    def execute(self, context):

        out = {}
        for i in self.doc.get("steps", []):
            if "commandLine" in i:
                print("Run", i["commandLine"])
            for k, v in i.get("outputs", {}).items():
                out[k] = v

        return {"outputs" : out}



def build(project_dir, dag):
    
    plans = {}

    for i in glob(os.path.join(project_dir, "transforms/*/BUILD.yaml")):
        print(f"Loading {i}")
        with open(i, "rt", encoding="ascii") as handle:
            doc = yaml.load(handle, Loader=yaml.SafeLoader)
            #print(doc)
            doc["_dir"] = os.path.dirname(i)
            plans[ doc['name'] ] = doc

    taskTree = nx.DiGraph()

    for k, d in plans.items():
        taskTree.add_node(k, doc=d)

    outputs = {}
    for k, d in plans.items():
        for step in d.get('steps',[]):
            for out in step.get("outputs", []):
                oname = f"{k}.{out}"
                outputs[oname] = k

    for k, d in plans.items():
        for step in d.get('steps',[]):
            for i in step.get("inputs", []):
                if i in outputs:
                    if k != outputs[i]: # ignore links within the same task
                        #n, v = i.split(".")
                        print(f"Linking {outputs[i]} => {k}")
                        taskTree.add_edge(outputs[i], k)
                else:
                    print(f"Missing input {i}")


    tasks = {}
    for i in nx.topological_sort(taskTree):
        sources = []
        for a, b in taskTree.in_edges(i):
            sources.append(tasks[a])

        t = RunTransform(doc=taskTree.nodes[i]['doc'], sources=sources, task_id=i)
        for s in sources:
            s >> t
        tasks[i] = t


