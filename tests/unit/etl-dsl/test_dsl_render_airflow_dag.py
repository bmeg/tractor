from pprint import pprint

from airflow.operators.bash import BashOperator

from tractor.etl_dsl.generators.dag_generator import AggregateLoadDagGenerator
from tractor.etl_dsl.model import ETLProject


def test_load_multi_source(multi_source_etl_project):
    """Test to render DAGs from etl_project"""

    etl_project = ETLProject(**multi_source_etl_project)

    etl_project.apply_defaults()  # expand defaults
    print('project:')
    pprint(etl_project)

    generator = AggregateLoadDagGenerator(project=etl_project)
    dags = generator.mkdags()
    print('dags:')
    pprint(dags)
    assert len(dags) == 3
    transform_dags = [_ for _ in dags if _.dag_id.endswith("transformer")]
    assert len(transform_dags) == 2
    for transform_dag in transform_dags:
        transform_task = transform_dag.tasks[0]
        assert len(transform_task.inlets) == 1
        assert len(transform_task.outlets) == 1
        assert transform_task.pre_execute, "missing pre_execute"
        assert transform_task.on_success_callback, "missing on_success_callback"
        assert transform_task.on_failure_callback, "missing on_failure_callback"

    load_dag = next(iter([_ for _ in dags if _.dag_id.endswith("loader")]), None)
    assert load_dag, "Did not fine aggregate loader"
    assert load_dag.dag_id == f"{multi_source_etl_project['id']}-loader"
    load_task = load_dag.tasks[0]
    pprint(load_task)
    assert load_task.task_id == f"{multi_source_etl_project['id']}-loader"
    assert load_task.operator_class == BashOperator
    assert len(load_task.inlets) == 2
    assert len(load_task.outlets) == 1
    assert sorted([_.uri for _ in load_task.inlets]) == ['source_1-processed', 'source_2-processed']
    print('load_task.outlets:')
    pprint(load_task.outlets)
    assert len(load_task.outlets) == 1
    assert sorted([_.uri for _ in load_task.outlets]) == ['minimal_project-loaded']
