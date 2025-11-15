# import sys
# import pytest
# from airflow.models import DagBag

# pytestmark = pytest.mark.skipif(
#     sys.platform.startswith("win"),
#     reason="Airflow DAG parsing not supported on Windows"
# )

# DAGS_PATH = "/usr/local/airflow/dags"

# def test_dag_default_args():
#     """Validate default args for all DAGs."""
#     dag_bag = DagBag(dag_folder=DAGS_PATH, include_examples=False)
#     for dag_id, dag in dag_bag.dags.items():
#         default_args = dag.default_args or {}

#         # You can customize these expectations
#         assert "owner" in default_args, f"DAG '{dag_id}' missing 'owner'"
#         assert default_args.get("retries", 0) >= 1, f"DAG '{dag_id}' should have at least 1 retry"
#         assert "email" in default_args, f"DAG '{dag_id}' missing 'email'"
#         assert "start_date" in default_args, f"DAG '{dag_id}' missing 'start_date'"

#         # Check consistency on tasks
#         for task in dag.tasks:
#             assert task.retries == default_args["retries"], f"Task {task.task_id} has incorrect retry setting"
