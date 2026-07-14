from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from datetime import datetime

def slack_alert(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('dag').dag_id
    
    alert = SimpleHttpOperator(
        task_id='slack_notification',
        http_conn_id='slack_connection',
        endpoint='',
        method='POST',
        data=json.dumps({"text": f"🔴 Alert: Task {task_id} failed in DAG {dag_id}"}),
        headers={"Content-Type": "application/json"},
    )
    return alert.execute(context=context)

with DAG(
    dag_id="azure_1_group_4_dag_instructor_dashboard",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 5 * * *",
    max_active_runs=1,
    catchup=False,
    on_failure_callback=slack_alert
) as dag:

    check_silver_freshness = DatabricksRunNowOperator(
        task_id="check_silver_freshness",
        databricks_conn_id="azure_1_group_4",
        job_id=1098596179264260
    )

    run_job = DatabricksRunNowOperator(
        task_id="run_dashboard_job",
        databricks_conn_id="azure_1_group_4",
        job_id=500037762057976
    )

    check_silver_freshness >> run_job
