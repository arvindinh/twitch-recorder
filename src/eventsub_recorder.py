#!/usr/bin/env python3
import asyncio
import aiohttp
import websockets
import json
import os
import sys
import datetime
import signal
from dotenv import load_dotenv
from src.observability import logger, start_metrics_server, VODS_RECORDED_TOTAL, RECORDING_ERRORS_TOTAL, TOKEN_REFRESH_TOTAL, ACTIVE_DOWNLOADS

# Load from .env file or system environment
load_dotenv()

CLIENT_ID = os.environ.get('TWITCH_CLIENT_ID')
CLIENT_SECRET = os.environ.get('TWITCH_CLIENT_SECRET')
OAUTH_TOKEN = os.environ.get('TWITCH_USER_TOKEN')
REFRESH_TOKEN = os.environ.get('TWITCH_REFRESH_TOKEN')

TARGET_USERNAME = os.environ.get('TARGET_USERNAME')
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', '.')
QUALITY = os.environ.get('QUALITY', 'best')

DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')
YOUTUBE_UPLOADER_BIN = os.environ.get('YOUTUBE_UPLOADER_BIN')
YOUTUBE_PRIVACY = os.environ.get('YOUTUBE_PRIVACY', 'private')

EVENTSUB_WSS_URL = "wss://eventsub.wss.twitch.tv/ws"
TWITCH_API_BASE = "https://api.twitch.tv/helix"

def update_env_file(key, value):
    """Update the .env file permanently with new values."""
    if not os.path.exists('.env'):
        return
    with open('.env', 'r') as f:
        lines = f.readlines()
    
    key_found = False
    with open('.env', 'w') as f:
        for line in lines:
            if line.startswith(f"{key}="):
                f.write(f"{key}={value}\n")
                key_found = True
            else:
                f.write(line)
        if not key_found:
            f.write(f"\n{key}={value}\n")

async def send_discord_alert(session, message):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        await session.post(DISCORD_WEBHOOK_URL, json={"content": message})
    except Exception as e:
        logger.warning(f"⚠️ Failed to send Discord alert: {e}")

async def refresh_user_access_token(session):
    """Refresh the User Auth Token using the Refresh Token."""
    global OAUTH_TOKEN, REFRESH_TOKEN
    url = "https://id.twitch.tv/oauth2/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN
    }
    
    async with session.post(url, data=data) as response:
        resp_data = await response.json()
        if response.status == 200:
            logger.info("🔑 Successfully Auto-Refreshed the Twitch User Access Token!")
            TOKEN_REFRESH_TOTAL.inc()
            OAUTH_TOKEN = resp_data['access_token']
            REFRESH_TOKEN = resp_data['refresh_token']
            update_env_file('TWITCH_USER_TOKEN', OAUTH_TOKEN)
            update_env_file('TWITCH_REFRESH_TOKEN', REFRESH_TOKEN)
            return True
        else:
            logger.error(f"❌ Critical Auth Error: Failed to refresh user token: {resp_data}")
            return False

async def get_broadcaster_id(session, username):
    """Resolve the Twitch username to a Twitch Broadcaster ID via the Helix API."""
    url = f"{TWITCH_API_BASE}/users"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {OAUTH_TOKEN}"
    }
    params = {"login": username}
    
    async with session.get(url, headers=headers, params=params) as response:
        if response.status == 401:
            return None # Triggers refresh flow dynamically
        if response.status == 200:
            data = await response.json()
            if data['data']:
                return data['data'][0]['id']
        raise Exception(f"Could not get user ID for {username}: {await response.text()}")

async def subscribe_to_event(session, session_id, broadcaster_id):
    """Subscribe to the stream.online event using User Token."""
    url = f"{TWITCH_API_BASE}/eventsub/subscriptions"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {OAUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "type": "stream.online",
        "version": "1",
        "condition": {
            "broadcaster_user_id": broadcaster_id
        },
        "transport": {
            "method": "websocket",
            "session_id": session_id
        }
    }
    
    async with session.post(url, headers=headers, json=payload) as response:
        data = await response.json()
        if response.status == 401:
            return False # Token logic expired mid connection
        if response.status in (200, 202):
            logger.info(f"✅ Successfully subscribed to stream.online for: {TARGET_USERNAME} (ID: {broadcaster_id})")
            return True
        else:
            logger.error(f"❌ Failed to subscribe (Token Error or Invalid Scope): {data}")
            return False

def sanitize_filename(text):
    if not text:
        return "stream"
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        text = text.replace(char, '-')
    return text.replace(' ', '_')

async def trigger_airflow_upload(session, filepath):
    airflow_url = os.environ.get('AIRFLOW_API_URL', 'http://localhost:8080/api/v1/dags/upload_twitch_vods_to_youtube/dagRuns')
    airflow_user = os.environ.get('AIRFLOW_USERNAME', 'airflow')
    airflow_pass = os.environ.get('AIRFLOW_PASSWORD', 'airflow')
    
    title_var = os.path.basename(filepath)
    msg = f"☁️ **Triggering Airflow YouTube upload** for `{title_var}`..."
    logger.info(msg.replace("**", "").replace("`", ""))
    await send_discord_alert(session, msg)
    
    payload = {
        "conf": {
            "vod_path": filepath
        }
    }
    
    auth = aiohttp.BasicAuth(airflow_user, airflow_pass)
    
    try:
        async with session.post(airflow_url, json=payload, auth=auth) as response:
            if response.status in (200, 201):
                finish_msg = f"✅ **Successfully triggered Airflow Upload DAG** for `{title_var}`!"
                logger.info(finish_msg.replace("**", "").replace("`", ""))
                await send_discord_alert(session, finish_msg)
            else:
                resp_text = await response.text()
                err_msg = f"❌ **Failed to trigger Airflow Upload** for `{title_var}`. Status: {response.status}, Response: {resp_text}"
                logger.error(err_msg.replace("**", "").replace("`", ""))
                await send_discord_alert(session, err_msg)
    except Exception as e:
        error_msg = f"❌ **Fatal Error triggering Airflow DAG:** {e}"
        logger.error(error_msg.replace("**", "").replace("`", ""))
        await send_discord_alert(session, error_msg)


async def trigger_download(session):
    stream_start_timestamp = datetime.datetime.now().strftime("%d_%m_%y-%H_%M")
    part_num = 1
    # Max duration per part: 11 hours 59 minutes = 43140 seconds (configurable via MAX_PART_DURATION_SECONDS env var)
    max_part_duration = int(os.environ.get('MAX_PART_DURATION_SECONDS', 11 * 3600 + 59 * 60))

    session_msg = f"🎬 **Starting download session for {TARGET_USERNAME}**\nStart time: `{stream_start_timestamp}`"
    logger.info(session_msg.replace("**", "").replace("`", ""))
    await send_discord_alert(session, session_msg)

    while True:
        filename = f"{TARGET_USERNAME}_{stream_start_timestamp}_part{part_num}.mp4"
        output_path = os.path.join(OUTPUT_DIR, filename)

        part_msg = f"📹 **Starting recording part {part_num} for {TARGET_USERNAME}**\n📂 File: `{filename}`"
        logger.info(part_msg.replace("**", "").replace("`", ""))
        await send_discord_alert(session, part_msg)

        cmd = [
            sys.executable,
            "-m",
            "streamlink",
            f"https://www.twitch.tv/{TARGET_USERNAME}",
            QUALITY,
            "--output",
            output_path
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT
            )
            ACTIVE_DOWNLOADS.inc()
            logger.info(f"📡 Recording part {part_num} started. PID: {process.pid}")

            part_start_time = asyncio.get_event_loop().time()
            max_duration_reached = False

            while True:
                elapsed = asyncio.get_event_loop().time() - part_start_time
                remaining_time = max_part_duration - elapsed

                if remaining_time <= 0:
                    hours = max_part_duration // 3600
                    minutes = (max_part_duration % 3600) // 60
                    logger.info(f"⏰ Part {part_num} reached duration limit ({hours}h {minutes}m). Stopping early to split file...")
                    max_duration_reached = True
                    try:
                        if sys.platform != 'win32':
                            process.send_signal(signal.SIGINT)
                        else:
                            process.terminate()
                    except Exception:
                        try:
                            process.terminate()
                        except Exception:
                            pass
                    break

                try:
                    line = await asyncio.wait_for(process.stdout.readline(), timeout=min(remaining_time, 5.0))
                    if not line:
                        break
                    line_str = line.decode('utf-8', errors='replace').strip()
                    if line_str:
                        logger.info(f"[streamlink - {TARGET_USERNAME} (part {part_num})] {line_str}")
                except asyncio.TimeoutError:
                    continue

            try:
                await asyncio.wait_for(process.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ Process PID {process.pid} didn't exit gracefully, killing...")
                try:
                    process.kill()
                except Exception:
                    pass
                await process.wait()

            if max_duration_reached:
                finish_msg = f"⏳ **Part {part_num} reached maximum duration limit** for {TARGET_USERNAME}. Saved `{filename}`."
            else:
                finish_msg = f"🏁 **Recording part {part_num} finished** for {TARGET_USERNAME}. (Exit code {process.returncode})"

            logger.info(finish_msg.replace("**", "").replace("`", ""))
            await send_discord_alert(session, finish_msg)

            ACTIVE_DOWNLOADS.dec()
            VODS_RECORDED_TOTAL.inc()
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                asyncio.create_task(trigger_airflow_upload(session, output_path))
            else:
                await send_discord_alert(session, f"⚠️ Part {part_num} file was empty or missing. Skipping YouTube upload.")

            if max_duration_reached:
                part_num += 1
                await send_discord_alert(session, f"🔄 **Splitting stream into part {part_num}...** Starting new part.")
                await asyncio.sleep(2)
                continue
            else:
                break

        except Exception as e:
            error_msg = f"❌ **Error during download subprocess (part {part_num}):** {e}"
            logger.info(error_msg.replace("**", ""))
            await send_discord_alert(session, error_msg)
            break

async def listen_eventsub():
    global OAUTH_TOKEN, REFRESH_TOKEN
    
    if not CLIENT_ID or not CLIENT_SECRET or not OAUTH_TOKEN or not TARGET_USERNAME:
        logger.error("❌ Missing required environment variables.")
        logger.info("Required: TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, TWITCH_USER_TOKEN, TWITCH_REFRESH_TOKEN, TARGET_USERNAME.")
        sys.exit(1)

    if not os.path.exists(OUTPUT_DIR):
        logger.info(f"Creating output directory: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    broadcaster_id = None

    async with aiohttp.ClientSession() as session:
        await send_discord_alert(session, f"🚀 **Twitch Auto-Recorder Started**\nTracking user: `{TARGET_USERNAME}`")
        while True:
            try:
                # 1. Broadcaster ID Resolve (Validates Token Health)
                broadcaster_id = await get_broadcaster_id(session, TARGET_USERNAME)
                if not broadcaster_id:
                    logger.warning("⚠️ User Access Token is definitively expired. Attempting token refresh...")
                    success = await refresh_user_access_token(session)
                    if not success:
                        logger.info("🛑 Shutting down because token refresh fundamentally failed. Get a new token pair and restart.")
                        sys.exit(1)
                    continue # Re-loop to resolve broadcaster_id again with new token
                
                logger.info(f"🔍 Resolved {TARGET_USERNAME} to broadcaster ID: {broadcaster_id}")

                logger.info("🔌 Connecting to Twitch EventSub WebSocket...")
                async with websockets.connect(EVENTSUB_WSS_URL) as websocket:
                    while True:
                        msg_str = await websocket.recv()
                        msg = json.loads(msg_str)
                        msg_type = msg.get("metadata", {}).get("message_type")

                        if msg_type == "session_welcome":
                            session_id = msg['payload']['session']['id']
                            logger.info(f"👋 Received session_welcome, initializing session_id: {session_id}")
                            success = await subscribe_to_event(session, session_id, broadcaster_id)
                            if not success:
                                logger.warning("⚠️ Token might be expired during subscription. Refreshing...")
                                # Force a refresh loop
                                success_refresh = await refresh_user_access_token(session)
                                break 
                            else:
                                await send_discord_alert(session, "🟢 **Successfully connected to Twitch EventSub WebSocket**")

                        elif msg_type == "session_keepalive":
                            pass 
                        
                        elif msg_type == "notification":
                            sub_type = msg.get("metadata", {}).get("subscription_type")
                            if sub_type == "stream.online":
                                logger.info(f"🔔 {TARGET_USERNAME} IS ONLINE! Triggering background download...")
                                await send_discord_alert(session, f"🔔 **{TARGET_USERNAME.upper()} IS ONLINE!**")
                                asyncio.create_task(trigger_download(session))
                            
                        elif msg_type == "session_reconnect":
                            reconnect_url = msg['payload']['session']['reconnect_url']
                            logger.info(f"🔄 Twitch requested reconnect to {reconnect_url}. Resetting connection...")
                            await send_discord_alert(session, "🔄 **Twitch requested WebSocket reconnect**. Resetting...")
                            break

            except websockets.ConnectionClosed as e:
                logger.warning(f"⚠️ WebSocket closed ({e.code}: {e.reason}). Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.warning(f"⚠️ Unexpected error in websocket loop: {e}. Reconnecting in 5 seconds...")
                await send_discord_alert(session, f"⚠️ **Unexpected WebSocket Error:** {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        logger.info("🚀 Starting Twitch EventSub Recorder Wrapper (User Auto-Refresh flow)...")
        start_metrics_server(8000)
        asyncio.run(listen_eventsub())
    except KeyboardInterrupt:
        logger.info("🛑 Exiting gracefully due to Keyboard Interrupt...")
