# import os
# import sys
# import pytest
# from airflow.models import DagBag

# pytestmark = pytest.mark.skipif(
#     sys.platform.startswith("win"),
#     reason="Airflow DAG parsing not supported on Windows"
# )

# DAGS_PATH = "/usr/local/airflow/dags"

# def test_dag_imports():
#     """Ensure all DAGs can be parsed without import errors."""
#     dag_bag = DagBag(dag_folder=DAGS_PATH, include_examples=False)
#     assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

# def test_each_dag_has_tasks():
#     """Ensure every DAG has at least one task."""
#     dag_bag = DagBag(dag_folder=DAGS_PATH, include_examples=False)
#     for dag_id, dag in dag_bag.dags.items():
#         assert len(dag.tasks) > 0, f"DAG '{dag_id}' has no tasks"
