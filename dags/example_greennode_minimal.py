"""Ví dụ DAG tối thiểu sử dụng GreenNodeOperator (style giống iomete)."""

import pendulum
from airflow import DAG

from greennode_airflow_plugin import GreenNodeOperator

args = {
    "owner": "airflow",
    "email": ["airflow@example.com"],
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 1, 1, tz="UTC"),
}

dag = DAG(dag_id="greennode-task", default_args=args, schedule=None)

task = GreenNodeOperator(
    task_id="greennode-spark-job-task",
    workspace_id="{{ var.value.greennode_workspace_id }}",
    job_id="{{ var.value.greennode_job_id }}",
    dag=dag,
)
