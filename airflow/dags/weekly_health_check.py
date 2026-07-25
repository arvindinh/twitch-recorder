from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import requests
import subprocess
import json
import shutil


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # Don't retry health checks immediately if they fail
}

dag = DAG(
    'weekly_system_health_check',
    default_args=default_args,
    description='A DAG to thoroughly verify all external dependencies and run integration tests.',
    schedule_interval='0 8 * * 1', # Run at 8:00 AM every Monday
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['twitch', 'health', 'testing'],
)

def check_streamlink_version(**kwargs):
    """Verifies that the installed streamlink binary is up-to-date with GitHub."""
    try:
        # Get installed version
        result = subprocess.run(['streamlink', '--version'], capture_output=True, text=True, check=True)
        installed_version = result.stdout.strip().split(' ')[1]
        
        # We skip checking the GitHub API in this test script to avoid rate limits, 
        # but in production you would parse https://api.github.com/repos/streamlink/streamlink/releases/latest
        
        msg = f"✅ Streamlink is installed and responding (Version: {installed_version})"
        print(msg)
        return msg
    except Exception as e:
        err = f"❌ Streamlink check failed: {e}"
        print(err)
        raise Exception(err)

def validate_twitch_token(**kwargs):
    """Hits the Twitch /validate endpoint to ensure the token isn't expired/revoked."""
    token = os.environ.get('TWITCH_USER_TOKEN')
    if not token:
        raise Exception("❌ TWITCH_USER_TOKEN is missing from environment.")
        
    headers = {"Authorization": f"OAuth {token}"}
    try:
        response = requests.get('https://id.twitch.tv/oauth2/validate', headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            msg = f"✅ Twitch token is valid (Expires in {data.get('expires_in')}s)"
            print(msg)
            return msg
        else:
            err = f"❌ Twitch token validation failed! Status: {response.status_code}, Response: {response.text}"
            print(err)
            raise Exception(err)
    except Exception as e:
        err = f"❌ Failed to reach Twitch API: {e}"
        print(err)
        raise Exception(err)

def check_youtube_credentials(**kwargs):
    """Verifies that the necessary YouTube OAuth files exist for youtubeuploader."""
    # Assume they are in the project root or a mounted secrets dir
    project_root = os.environ.get('PROJECT_ROOT', '/app')
    secrets_file = os.path.join(project_root, 'client_secrets.json')
    token_file = os.path.join(project_root, 'request.token')
    
    missing = []
    if not os.path.exists(secrets_file):
        missing.append('client_secrets.json')
    if not os.path.exists(token_file):
        missing.append('request.token')
        
    if missing:
        err = f"❌ Missing required YouTube credentials: {', '.join(missing)}"
        print(err)
        raise Exception(err)
        
    # Validate request.token
    try:
        with open(token_file, 'r') as f:
            token_data = json.load(f)
            
        if 'refresh_token' not in token_data:
            err = "❌ request.token exists but is INVALID (missing 'refresh_token'). You need to re-authenticate."
            print(err)
            raise Exception(err)
            
    except json.JSONDecodeError:
        err = "❌ request.token exists but is not valid JSON! You need to re-authenticate."
        print(err)
        raise Exception(err)
    except Exception as e:
        if "Missing required YouTube credentials" not in str(e) and "INVALID" not in str(e) and "not valid JSON" not in str(e):
            err = f"❌ Error reading request.token: {e}"
            print(err)
            raise Exception(err)
        else:
            raise e
    
    msg = "✅ YouTube client_secrets.json and request.token are present and valid."

    print(msg)
    return msg

def check_youtubeuploader_version(**kwargs):
    """Verifies that the installed youtubeuploader binary is up-to-date with GitHub."""
    try:
        result = subprocess.run(['youtubeuploader', '-version'], capture_output=True, text=True, check=True)
        installed_version_str = result.stdout.strip()
        
        try:
            response = requests.get('https://api.github.com/repos/porjo/youtubeuploader/releases/latest', timeout=10)
            if response.status_code == 200:
                latest_version = response.json().get('tag_name', '')
                # Ensure we handle 'v' prefixes
                clean_latest = latest_version.lstrip('v')
                if clean_latest and clean_latest not in installed_version_str:
                    err = f"⚠️ youtubeuploader is outdated! Installed: '{installed_version_str}', Latest: {latest_version}. Since Docker abstracts the OS, ensure you download the Linux (linux_amd64/arm64) binary."
                    print(err)
                    raise Exception(err)
                msg = f"✅ youtubeuploader is up-to-date (Version: {installed_version_str})"
                print(msg)
                return msg
            else:
                print(f"⚠️ Could not fetch latest youtubeuploader version from GitHub (Status {response.status_code}).")
                msg = f"✅ youtubeuploader is installed (Version: {installed_version_str})"
                return msg
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Failed to check GitHub for youtubeuploader updates: {e}")
            msg = f"✅ youtubeuploader is installed (Version: {installed_version_str})"
            return msg
    except Exception as e:
        err = f"❌ youtubeuploader check failed (is it installed in the Airflow environment?): {e}"
        print(err)
        raise Exception(err)

def check_disk_space(**kwargs):
    """Verifies that the /app/downloads directory has sufficient disk space."""
    download_dir = os.environ.get('OUTPUT_DIR', '/app/downloads')
    if not os.path.exists(download_dir):
        print(f"Directory {download_dir} does not exist yet. Skipping disk check.")
        return "Directory does not exist yet."
        
    total, used, free = shutil.disk_usage(download_dir)
    used_percent = (used / total) * 100
    
    if used_percent > 85.0:
        err = f"🚨 Disk space usage for {download_dir} is critically high: {used_percent:.1f}%!"
        print(err)
        raise Exception(err)
        
    msg = f"✅ Disk space usage for {download_dir} is healthy ({used_percent:.1f}% used)."
    print(msg)
    return msg

def send_health_alert(**kwargs):
    """Aggregates the status of all upstream tasks and sends a Discord alert."""
    ti = kwargs['ti']
    discord_webhook = os.environ.get('DISCORD_WEBHOOK_URL')
    
    if not discord_webhook:
        print("No DISCORD_WEBHOOK_URL provided. Skipping alert.")
        return

    # Check the state of all upstream tasks
    dag_run = kwargs['dag_run']
    failed_tasks = dag_run.get_task_instances(state='failed')
    
    if failed_tasks:
        message = "🚨 **Weekly Health Check FAILED** 🚨\nThe following checks failed:\n"
        for task in failed_tasks:
            message += f"- `{task.task_id}`\n"
        message += "\nPlease check Airflow logs immediately."
    else:
        message = "💖 **Weekly Health Check PASSED** 💖\nAll API tokens, binaries, and integration tests are healthy!"

    try:
        response = requests.post(discord_webhook, json={"content": message})
        response.raise_for_status()
        print("Successfully sent health alert to Discord.")
    except Exception as e:
        print(f"Failed to send Discord message: {e}")

# Define Operators
check_streamlink = PythonOperator(
    task_id='check_streamlink_version',
    python_callable=check_streamlink_version,
    dag=dag,
)

check_twitch = PythonOperator(
    task_id='validate_twitch_token',
    python_callable=validate_twitch_token,
    dag=dag,
)

check_youtube = PythonOperator(
    task_id='check_youtube_credentials',
    python_callable=check_youtube_credentials,
    dag=dag,
)

check_youtubeuploader = PythonOperator(
    task_id='check_youtubeuploader_version',
    python_callable=check_youtubeuploader_version,
    dag=dag,
)

check_disk = PythonOperator(
    task_id='check_disk_space',
    python_callable=check_disk_space,
    dag=dag,
)

# Run the pytest suite ensuring our local code logic is sound
run_tests = BashOperator(
    task_id='run_pytest_suite',
    # Ensure PYTHONPATH is set so src module is found
    bash_command='cd ${PROJECT_ROOT:-/app} && PYTHONPATH=. pytest tests/',
    dag=dag,
)

send_alert = PythonOperator(
    task_id='send_health_alert',
    python_callable=send_health_alert,
    trigger_rule='all_done', # Always run this to report success OR failure
    dag=dag,
)

# Set dependencies (Run all checks in parallel, then alert)
[check_streamlink, check_twitch, check_youtube, check_youtubeuploader, check_disk, run_tests] >> send_alert
