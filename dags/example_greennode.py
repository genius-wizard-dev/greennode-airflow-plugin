from datetime import datetime

from airflow import DAG

from greennode_airflow_plugin import GreenNodeOperator

with DAG(
    dag_id="example_greennode_spark_job",
    description="Trigger Spark Job trên VNG Data Platform",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["greennode", "spark", "example"],
) as dag:

    run_spark_job = GreenNodeOperator(
        task_id="run_spark_job",
        workspace_id="{{ var.value.greennode_workspace_id }}",
        job_id="{{ var.value.greennode_job_id }}",
        application_args=["--date", "{{ ds }}", "--mode", "prod"],
        vng_conn_id="vng_cloud_default",
        polling_period_seconds=15,
        do_xcom_push=True,
    )
