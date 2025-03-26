import pytest
from airflow import DAG
from tractor.etl_dsl.generators.fhir_aggregator import (
    fhir_aggregator_config,
    create_project,
    fhir_aggregator_dag,
)


def test_fhir_aggregator_dag_creation():
    config = fhir_aggregator_config()
    project = create_project(config)
    dags = fhir_aggregator_dag(project)

    assert isinstance(dags, list)
    assert all(isinstance(dag, DAG) for dag in dags)
    assert len(dags) > 0

    assert project.defaults
    assert project.defaults.bucket_prefix is None
    assert project.defaults.transformer
    assert project.defaults.transformer.command is not None

    inlets = []
    for dag in dags:
        for task in dag.tasks:
            inlets.append([task.task_id, task.inlets])
    print("inlets:")
    print(inlets)

    for dag in dags:
        assert dag.dag_id is not None
        expected_task_count = 1
        assert len(dag.tasks) == expected_task_count, dag.dag_id
        for task in dag.tasks:
            assert task.task_id is not None
            assert task.operator_class is not None
            assert task.inlets is not None
            assert task.outlets is not None
            source_id = task.task_id.replace(config["id"] + "-", "").split("-")[0]
            if "transformer" in task.dag_id:
                assert len(task.inlets) == 1
                assert "raw" in task.inlets[0].uri
                assert source_id in task.inlets[0].uri

                assert len(task.outlets) == 1
                assert "processed" in task.outlets[0].uri
                assert source_id in task.outlets[0].uri

    dag_ids = [dag.dag_id for dag in dags]
    expected_transformer_dag_ids = [
        f"{project.id}-{source.id}-transformer" for source in project.sources
    ]

    assert set(expected_transformer_dag_ids).issubset(set(dag_ids))


# Test for invalid configuration
def test_invalid_configuration():
    invalid_config = {"invalid_key": "invalid_value"}
    with pytest.raises(AssertionError):
        create_project(invalid_config)


# Test for empty project
def test_empty_project():
    config = {}
    with pytest.raises(AssertionError):
        project = create_project(config)
        dags = fhir_aggregator_dag(project)
        assert isinstance(dags, list)
        assert len(dags) == 0


# Test for missing required fields
def test_missing_required_fields():
    config = {"name": "test_project"}
    with pytest.raises(AssertionError):
        create_project(config)
