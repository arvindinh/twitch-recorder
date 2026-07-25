import re

with open('src/eventsub_recorder.py', 'r') as f:
    code = f.read()

if 'from src.observability import' not in code:
    code = code.replace('from dotenv import load_dotenv\n', 'from dotenv import load_dotenv\nfrom src.observability import logger, start_metrics_server, VODS_RECORDED_TOTAL, RECORDING_ERRORS_TOTAL, TOKEN_REFRESH_TOTAL, ACTIVE_DOWNLOADS\n')

if 'start_metrics_server(' not in code:
    code = code.replace('asyncio.run(listen_eventsub())', 'start_metrics_server(8000)\n        asyncio.run(listen_eventsub())')

code = re.sub(r'print\((.*?)\)', r'logger.info(\1)', code)

code = code.replace('logger.info(f"❌', 'logger.error(f"❌')
code = code.replace('logger.info("❌', 'logger.error("❌')
code = code.replace('logger.info(f"⚠️', 'logger.warning(f"⚠️')
code = code.replace('logger.info("⚠️', 'logger.warning("⚠️')

# Add Prometheus increments
code = code.replace('OAUTH_TOKEN = resp_data[\'access_token\']', 'TOKEN_REFRESH_TOTAL.inc()\n            OAUTH_TOKEN = resp_data[\'access_token\']')
code = code.replace('logger.info(f"📡 Recording part {part_num} started', 'ACTIVE_DOWNLOADS.inc()\n            logger.info(f"📡 Recording part {part_num} started')
code = code.replace('if os.path.exists(output_path)', 'ACTIVE_DOWNLOADS.dec()\n            VODS_RECORDED_TOTAL.inc()\n            if os.path.exists(output_path)')

with open('src/eventsub_recorder.py', 'w') as f:
    f.write(code)
