from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import glob

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'upload_twitch_vods_to_youtube',
    default_args=default_args,
    description='A DAG to upload completed Twitch VODs to YouTube triggered by recorder',
    schedule_interval=None, # Triggered on-demand via API
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=10, # Allow multiple concurrent uploads
    tags=['twitch', 'youtube'],
)

# This assumes the youtubeuploader binary is accessible in the Airflow environment
# and configured properly with credentials
upload_task = BashOperator(
    task_id='upload_to_youtube',
    bash_command='''
        VOD_PATH="{{ dag_run.conf.get('vod_path', '') }}"
        if [ -n "$VOD_PATH" ]; then
            echo "Uploading $VOD_PATH to YouTube..."
            # Replace /usr/local/bin/youtubeuploader with actual path
            youtubeuploader -filename "$VOD_PATH" -title "Twitch VOD" -privacy private
        else
            echo "Skipping upload, no file path provided in dag_run.conf."
            exit 1
        fi
    ''',
    dag=dag,
)

cleanup_task = BashOperator(
    task_id='cleanup_vod',
    bash_command='''
        VOD_PATH="{{ dag_run.conf.get('vod_path', '') }}"
        if [ -n "$VOD_PATH" ]; then
            if [ "${DELETE_AFTER_UPLOAD:-false}" = "true" ]; then
                echo "DELETE_AFTER_UPLOAD is true. Deleting $VOD_PATH..."
                rm -f "$VOD_PATH"
            else
                echo "DELETE_AFTER_UPLOAD is false. Keeping $VOD_PATH."
            fi
        else
            echo "Skipping cleanup, no file path provided."
        fi
    ''',
    dag=dag,
)

upload_task >> cleanup_task
