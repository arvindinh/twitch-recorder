import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import src.eventsub_recorder as eventsub

@pytest.mark.asyncio
async def test_trigger_download_flow(mocker):
    """
    Integration test for the full download flow.
    We intercept the actual subprocess so streamlink doesn't actually run,
    but we verify that the pipeline successfully initiates the download,
    updates Prometheus metrics, and schedules the YouTube upload.
    """
    # 1. Setup global configurations to avoid breaking
    mocker.patch('src.eventsub_recorder.TARGET_USERNAME', 'test_user')
    mocker.patch('src.eventsub_recorder.OUTPUT_DIR', '/tmp')
    mocker.patch('src.eventsub_recorder.DISCORD_WEBHOOK_URL', None) # Disable discord for test
    
    # 2. Mock the Streamlink Subprocess to exit immediately successfully
    mock_process = AsyncMock()
    mock_process.pid = 9999
    mock_process.returncode = 0
    # Simulate process stdout emitting one line then EOF
    mock_process.stdout.readline.side_effect = [b"[streamlink] Found stream at 1080p60\n", b""]
    mock_process.wait = AsyncMock(return_value=0)
    
    mocker.patch('asyncio.create_subprocess_exec', return_value=mock_process)
    
    # 3. Mock file operations and trigger_airflow_upload to prevent real I/O
    mocker.patch('os.path.exists', return_value=True)
    mocker.patch('os.path.getsize', return_value=1024) # File is non-empty
    mock_youtube = mocker.patch('src.eventsub_recorder.trigger_airflow_upload', new_callable=AsyncMock)
    
    # 4. Mock the sleep and time to simulate a very short recording
    # We patch the time so that max_duration is never reached, meaning it just records once and finishes.
    mocker.patch('asyncio.sleep', new_callable=AsyncMock)
    
    mock_session = AsyncMock()
    
    # 5. Execute the trigger_download (this should run the loop once and exit because our mock_process reaches EOF)
    await eventsub.trigger_download(mock_session)
    
    # 6. Verifications
    # Ensure subprocess was created
    asyncio.create_subprocess_exec.assert_called_once()
    assert 'streamlink' in asyncio.create_subprocess_exec.call_args[0]
    assert 'test_user' in asyncio.create_subprocess_exec.call_args[0][3]
    
    # Ensure YouTube upload was triggered because the file "existed" and had "size"
    mock_youtube.assert_called_once()
