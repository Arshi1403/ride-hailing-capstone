import os
import sys

import pytest

pytest.importorskip("airflow")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from airflow.models import DagBag  # noqa: E402

import ride_hailing_daily as dag_module  # noqa: E402

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..")


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
    assert not bag.import_errors
    return bag.get_dag("ride_hailing_daily")


def test_required_tasks_exist(dag):
    required = {
        "sense_historical_dump", "load_history_to_raw", "run_curated_spark", "merge_fact",
        "merge_dim_scd2", "refresh_mart_views", "retrain_bqml", "dq_checks",
        "branch_on_dq", "alert", "notify_success",
    }
    assert required <= set(dag.task_ids)


def test_reliability_settings(dag):
    assert dag.default_args["retries"] == 2
    assert dag.default_args["retry_exponential_backoff"] is True
    assert dag.default_args["sla"].total_seconds() == 4 * 3600


def test_sql_templates_render(dag):
    import jinja2

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(dag_module.SQL_DIR), undefined=jinja2.StrictUndefined)
    for name in ("merge_fact", "merge_dim_scd2", "mart_views", "bqml", "ddl"):
        sql = env.get_template(f"{name}.sql").render(
            params={"project": "p", "bucket": "b"}, ds="2026-01-01"
        )
        assert "{{" not in sql


def test_dq_evaluation():
    ok = {"ds_rows": 10, "null_ratio": 0.0, "negative_fares": 0, "duplicate_rides": 0}
    assert dag_module.evaluate_dq(ok) == []
    bad = {"ds_rows": 0, "null_ratio": 0.5, "negative_fares": 2, "duplicate_rides": 1}
    assert len(dag_module.evaluate_dq(bad)) == 4
