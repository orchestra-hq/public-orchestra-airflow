from airflow import DAG
from airflow.contrib.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.microsoft.teams.operators.teams_webhook import TeamsWebhookOperator
from datetime import datetime, timedelta
import requests

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 24),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A DAG to trigger Fivetran, dbt Cloud, and Power BI refresh',
    schedule_interval=timedelta(days=1),
)

# Define Slack and Teams webhook URLs
slack_webhook_url = 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
teams_webhook_url = 'https://outlook.office.com/webhook/YOUR/TEAMS/WEBHOOK'

# Define the functions to be executed by the operators

def send_slack_notification(message):
    slack_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack_connection',
        message=message,
        webhook_token=slack_webhook_url,
        dag=dag,
    )
    slack_alert.execute()

def send_teams_notification(message):
    teams_alert = TeamsWebhookOperator(
        task_id='teams_alert',
        message=message,
        webhook_conn_id='teams_connection',
        dag=dag,
    )
    teams_alert.execute()

def send_email_notification(subject, html_content):
    email_alert = EmailOperator(
        task_id='email_alert',
        to='your@email.com',
        subject=subject,
        html_content=html_content,
        dag=dag,
    )
    email_alert.execute()

def trigger_fivetran_run():
    # Assuming Fivetran API call to trigger a run
    fivetran_run_response = requests.post('https://api.fivetran.com/v1/runs', headers={'Authorization': 'Bearer YOUR_API_KEY'})
    print("Fivetran run triggered")

def trigger_dbt_cloud_run():
    # Assuming dbt Cloud API call to trigger a run
    dbt_cloud_run_response = requests.post('https://cloud.getdbt.com/api/v2/run/', headers={'Authorization': 'Bearer YOUR_API_KEY'})
    print("dbt Cloud run triggered")

def trigger_power_bi_refresh():
    # Assuming Power BI API call to trigger a refresh
    power_bi_refresh_response = requests.post('https://api.powerbi.com/v1.0/myorg/groups/GROUP_ID/datasets/DATASET_ID/refreshes', headers={'Authorization': 'Bearer YOUR_API_KEY'})
    print("Power BI refresh triggered")

# Define the operators to execute the functions

fivetran_operator = PythonOperator(
    task_id='trigger_fivetran_run',
    python_callable=trigger_fivetran_run,
    dag=dag,
)

fivetran_sensor = HttpSensor(
    task_id='fivetran_sensor',
    http_conn_id='fivetran_connection',
    endpoint='https://api.fivetran.com/v1/runs/RUN_ID',
    method='GET',
    headers={'Authorization': 'Bearer YOUR_API_KEY'},
    response_check=lambda response: response.json()['data']['status'] in ['success', 'failure'],
    poke_interval=30,  # Poll every 30 seconds
    timeout=600,  # Timeout after 10 minutes
    soft_fail=True,
    mode='reschedule',  # Reschedule the task if it fails
    dag=dag,
)

dbt_cloud_operator = PythonOperator(
    task_id='trigger_dbt_cloud_run',
    python_callable=trigger_dbt_cloud_run,
    dag=dag,
)

dbt_cloud_sensor = HttpSensor(
    task_id='dbt_cloud_sensor',
    http_conn_id='dbt_cloud_connection',
    endpoint='https://cloud.getdbt.com/api/v2/runs/RUN_ID',
    method='GET',
    headers={'Authorization': 'Bearer YOUR_API_KEY'},
    response_check=lambda response: response.json()['status'] in ['success', 'failed'],
    poke_interval=30,  # Poll every 30 seconds
    timeout=600,  # Timeout after 10 minutes
    soft_fail=True,
    mode='reschedule',  # Reschedule the task if it fails
    dag=dag,
)

power_bi_operator = PythonOperator(
    task_id='trigger_power_bi_refresh',
    python_callable=trigger_power_bi_refresh,
    dag=dag,
)

power_bi_sensor = HttpSensor(
    task_id='power_bi_sensor',
    http_conn_id='power_bi_connection',
    endpoint='https://api.powerbi.com/v1.0/myorg/groups/GROUP_ID/datasets/DATASET_ID/refreshes/REFRESH_ID',
    method='GET',
    headers={'Authorization': 'Bearer YOUR_API_KEY'},
    response_check=lambda response: response.json()['status'] in ['Completed', 'Failed'],
    poke_interval=30,  # Poll every 30 seconds
    timeout=600,  # Timeout after 10 minutes
    soft_fail=True,
    mode='reschedule',  # Reschedule the task if it fails
    dag=dag,
)

# Define the notification tasks

send_slack_notification_task = PythonOperator(
    task_id='send_slack_notification',
    python_callable=send_slack_notification,
    op_kwargs={'message': 'Fivetran run failed'},
    dag=dag,
)

send_teams_notification_task = PythonOperator(
    task_id='send_teams_notification',
    python_callable=send_teams_notification,
    op_kwargs={'message': 'Fivetran run failed'},
    dag=dag,
)

send_email_notification_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=send_email_notification,
    op_kwargs={'subject': 'Fivetran run failed', 'html_content': 'Fivetran run failed'},
    dag=dag,
)

# Define the execution order of the tasks

fivetran_operator >> fivetran_sensor >> dbt_cloud_operator >> dbt_cloud_sensor >> power_bi_operator >> power_bi_sensor
power_bi_sensor >> [send_slack_notification_task, send_teams_notification_task, send_email_notification_task]
