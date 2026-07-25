from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weekly_discord_stats_summary',
    default_args=default_args,
    description='A DAG to extract weekly metrics and send a summary to Discord',
    schedule_interval='0 12 * * 0', # Run at 12:00 PM every Sunday
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['twitch', 'discord', 'reporting'],
)

def extract_metrics(**kwargs):
    """
    Extracts metrics. In a production environment, this would query a 
    Prometheus PromQL endpoint (e.g., http://prometheus:9090/api/v1/query) 
    or the YouTube API. For this pipeline, we query the local bot's metrics endpoint.
    """
    # Assuming the bot is running on a container named 'twitch-recorder' inside the same network
    # or localhost if running locally.
    metrics_url = os.environ.get('METRICS_URL', 'http://localhost:8000/metrics')
    
    try:
        response = requests.get(metrics_url, timeout=10)
        response.raise_for_status()
        metrics_data = response.text
        
        # Simple text parsing for the Prometheus metrics
        vods_recorded = 0
        errors = 0
        
        for line in metrics_data.split('\n'):
            if line.startswith('vods_recorded_total_total'):
                vods_recorded = float(line.split(' ')[1])
            elif line.startswith('recording_errors_total_total'):
                errors = float(line.split(' ')[1])
                
        # Push the extracted data to XCom so the next task can use it
        kwargs['ti'].xcom_push(key='weekly_stats', value={
            'vods_recorded': int(vods_recorded),
            'errors': int(errors)
        })
        print("Successfully extracted metrics.")
        
    except Exception as e:
        print(f"Failed to extract metrics: {e}")
        # Push default values so the pipeline doesn't completely fail
        kwargs['ti'].xcom_push(key='weekly_stats', value={
            'vods_recorded': 0,
            'errors': 0,
            'note': f'Extraction failed: {e}'
        })

def transform_and_load_discord(**kwargs):
    """
    Transforms the raw metrics into a readable summary and loads it to Discord.
    """
    stats = kwargs['ti'].xcom_pull(task_ids='extract_metrics', key='weekly_stats')
    discord_webhook = os.environ.get('DISCORD_WEBHOOK_URL')
    
    if not discord_webhook:
        print("No DISCORD_WEBHOOK_URL provided. Skipping Discord alert.")
        return
        
    # Transform
    vods = stats.get('vods_recorded', 0)
    errors = stats.get('errors', 0)
    note = stats.get('note', '')
    
    message = (
        "📊 **Weekly Twitch Recorder Summary** 📊\n"
        f"✅ **VODs Successfully Recorded:** `{vods}`\n"
        f"❌ **Recording Errors:** `{errors}`\n"
    )
    
    if note:
        message += f"\n⚠️ *Note: {note}*"
        
    # Load (Send to Discord)
    try:
        response = requests.post(discord_webhook, json={"content": message})
        response.raise_for_status()
        print("Successfully sent weekly summary to Discord.")
    except Exception as e:
        print(f"Failed to send Discord message: {e}")

extract_task = PythonOperator(
    task_id='extract_metrics',
    python_callable=extract_metrics,
    dag=dag,
)

load_discord_task = PythonOperator(
    task_id='transform_and_load_discord',
    python_callable=transform_and_load_discord,
    dag=dag,
)

# Define the pipeline flow
extract_task >> load_discord_task
