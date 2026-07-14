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
    dag_id="azure_1_group_4_dag_bronze_to_silver",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    on_failure_callback=slack_alert
) as dag:

    check_freshness = DatabricksRunNowOperator(
        task_id="check_bronze_freshness",
        databricks_conn_id="azure_1_group_4",
        job_id=1098596179264260
    )

    run_job = DatabricksRunNowOperator(
        task_id="run_ingestion_job",
        databricks_conn_id="azure_1_group_4",
        job_id=3823830430696
    )

    check_freshness >> run_job
