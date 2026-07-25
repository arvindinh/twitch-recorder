import pytest
import aiohttp
from src.eventsub_recorder import sanitize_filename, get_broadcaster_id, send_discord_alert
from unittest.mock import AsyncMock

def test_sanitize_filename():
    """Test that the filename sanitizer correctly removes illegal characters."""
    assert sanitize_filename("shroud playing valo <3") == "shroud_playing_valo_-3"
    assert sanitize_filename('test ?\\/|*:"<>') == "test_---------"
    assert sanitize_filename("normal_filename") == "normal_filename"

@pytest.mark.asyncio
async def test_get_broadcaster_id(mocker):
    """Test resolving a Twitch username to an ID by mocking the API response."""
    # Mock aiohttp session and response
    mock_session = AsyncMock(spec=aiohttp.ClientSession)
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json.return_value = {
        'data': [{'id': '12345', 'login': 'shroud', 'display_name': 'shroud'}]
    }
    
    # We must configure the context manager logic for async with session.get()
    mock_get_ctx = AsyncMock()
    mock_get_ctx.__aenter__.return_value = mock_response
    mock_session.get.return_value = mock_get_ctx

    # Patch OAUTH_TOKEN and CLIENT_ID as they are global
    mocker.patch('src.eventsub_recorder.OAUTH_TOKEN', 'fake_token')
    mocker.patch('src.eventsub_recorder.CLIENT_ID', 'fake_client_id')
    
    broadcaster_id = await get_broadcaster_id(mock_session, 'shroud')
    
    assert broadcaster_id == '12345'
    mock_session.get.assert_called_once()
    
    # Check unauthorized handling
    mock_response.status = 401
    broadcaster_id_unauth = await get_broadcaster_id(mock_session, 'shroud')
    assert broadcaster_id_unauth is None

@pytest.mark.asyncio
async def test_send_discord_alert(mocker):
    """Test that Discord alerts attempt to post to the webhook URL."""
    # Ensure DISCORD_WEBHOOK_URL is set
    mocker.patch('src.eventsub_recorder.DISCORD_WEBHOOK_URL', 'http://fake-webhook.com')
    
    mock_session = AsyncMock(spec=aiohttp.ClientSession)
    mock_post_ctx = AsyncMock()
    mock_session.post.return_value = mock_post_ctx

    await send_discord_alert(mock_session, "Test message")
    
    mock_session.post.assert_called_once_with(
        'http://fake-webhook.com', 
        json={'content': 'Test message'}
    )
