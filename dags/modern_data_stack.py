from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.microsoft.teams.operators.teams_operator import TeamsWebhookOperator
from datetime import datetime, timedelta
import requests
import time

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

def trigger_fivetran_run(**kwargs):
    try:
        # Assuming Fivetran API call to trigger a run
        fivetran_run_response = requests.post('https://api.fivetran.com/v1/runs', headers={'Authorization': 'Bearer YOUR_API_KEY'})
        print("Fivetran run triggered")

        # Polling for Fivetran run completion
        while True:
            # Assuming Fivetran API call to get run status
            fivetran_status_response = requests.get('https://api.fivetran.com/v1/runs/RUN_ID', headers={'Authorization': 'Bearer YOUR_API_KEY'})
            status = fivetran_status_response.json()['data']['status']
            if status == 'success':
                print("Fivetran run completed successfully")
                break
            elif status == 'failure':
                print("Fivetran run failed")
                raise Exception("Fivetran run failed")
            time.sleep(30)  # Poll every 30 seconds
    except Exception as e:
        # Send Slack notification on failure
        slack_alert = SlackWebhookOperator(
            task_id='slack_alert',
            http_conn_id='slack_connection',
            message=f"Fivetran run failed: {str(e)}",
            webhook_token=slack_webhook_url,
            dag=dag,
        )
        slack_alert.execute(context=kwargs)

        # Send Teams notification on failure
        teams_alert = TeamsWebhookOperator(
            task_id='teams_alert',
            message=f"Fivetran run failed: {str(e)}",
            webhook_conn_id='teams_connection',
            dag=dag,
        )
        teams_alert.execute(context=kwargs)

        # Send email notification on failure
        email_alert = EmailOperator(
            task_id='email_alert',
            to='your@email.com',
            subject='Fivetran run failed',
            html_content=f"Fivetran run failed: {str(e)}",
            dag=dag,
        )
        email_alert.execute(context=kwargs)

def trigger_dbt_cloud_run(**kwargs):
    try:
        # Assuming dbt Cloud API call to trigger a run
        dbt_cloud_run_response = requests.post('https://cloud.getdbt.com/api/v2/run/', headers={'Authorization': 'Bearer YOUR_API_KEY'})
        print("dbt Cloud run triggered")

        # Polling for dbt Cloud run completion
        while True:
            # Assuming dbt Cloud API call to get run status
            dbt_cloud_status_response = requests.get('https://cloud.getdbt.com/api/v2/runs/RUN_ID', headers={'Authorization': 'Bearer YOUR_API_KEY'})
            status = dbt_cloud_status_response.json()['status']
            if status == 'success':
                print("dbt Cloud run completed successfully")
                break
            elif status == 'failed':
                print("dbt Cloud run failed")
                raise Exception("dbt Cloud run failed")
            time.sleep(30)  # Poll every 30 seconds
    except Exception as e:
        # Send Slack notification on failure
        slack_alert = SlackWebhookOperator(
            task_id='slack_alert',
            http_conn_id='slack_connection',
            message=f"dbt Cloud run failed: {str(e)}",
            webhook_token=slack_webhook_url,
            dag=dag,
        )
        slack_alert.execute(context=kwargs)

        # Send Teams notification on failure
        teams_alert = TeamsWebhookOperator(
            task_id='teams_alert',
            message=f"dbt Cloud run failed: {str(e)}",
            webhook_conn_id='teams_connection',
            dag=dag,
        )
        teams_alert.execute(context=kwargs)

        # Send email notification on failure
        email_alert = EmailOperator(
            task_id='email_alert',
            to='your@email.com',
            subject='dbt Cloud run failed',
            html_content=f"dbt Cloud run failed: {str(e)}",
            dag=dag,
        )
        email_alert.execute(context=kwargs)

def trigger_power_bi_refresh(**kwargs):
    try:
        # Assuming Power BI API call to trigger a refresh
        power_bi_refresh_response = requests.post('https://api.powerbi.com/v1.0/myorg/groups/GROUP_ID/datasets/DATASET_ID/refreshes', headers={'Authorization': 'Bearer YOUR_API_KEY'})
        print("Power BI refresh triggered")

        # Polling for Power BI refresh completion
        while True:
            # Assuming Power BI API call to get refresh status
            power_bi_refresh_status_response = requests.get('https://api.powerbi.com/v1.0/myorg/groups/GROUP_ID/datasets/DATASET_ID/refreshes/REFRESH_ID', headers={'Authorization': 'Bearer YOUR_API_KEY'})
            status = power_bi_refresh_status_response.json()['status']
            if status == 'Completed':
                print("Power BI refresh completed successfully")
                break
            elif status == 'Failed':
                print("Power BI refresh failed")
                raise Exception("Power BI refresh failed")
            time.sleep(30)  # Poll every 30 seconds
    except Exception as e:
        # Send Slack notification on failure
        slack_alert = SlackWebhookOperator(
            task_id='slack_alert',
            http_conn_id='slack_connection',
            message=f"Power BI refresh failed: {str(e)}",
            webhook_token=slack_webhook_url,
            dag=dag,
        )
        slack_alert.execute(context=kwargs)

        # Send Teams notification on failure
        teams_alert = TeamsWebhookOperator(
            task_id='teams_alert',
            message=f"Power BI refresh failed: {str(e)}",
            webhook_conn_id='teams_connection',
            dag=dag,
        )
        teams_alert.execute(context=kwargs)

        # Send email notification on failure
        email_alert = EmailOperator(
            task_id='email_alert',
            to='your@email.com',
            subject='Power BI refresh failed',
            html_content=f"Power BI refresh failed: {str(e)}",
            dag=dag,
        )
        email_alert.execute(context=kwargs)

# Define the operators to execute the functions

trigger_fivetran_operator = PythonOperator(
    task_id='trigger_fivetran_run',
    python_callable=trigger_fivetran_run,
    provide_context=True,
    dag=dag,
)

trigger_dbt_cloud_operator = PythonOperator(
    task_id='trigger_dbt_cloud_run',
    python_callable=trigger_dbt_cloud_run,
    provide_context=True,
    dag=dag,
)

trigger_power_bi_operator = PythonOperator(
    task_id='trigger_power_bi_refresh',
    python_callable=trigger_power_bi_refresh,
    provide_context=True,
    dag=dag,
)

# Define the execution order of the tasks
trigger_fivetran_operator >> trigger_dbt_cloud_operator >> trigger_power_bi_operator
