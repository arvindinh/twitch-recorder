import logging
from pythonjsonlogger import jsonlogger
from prometheus_client import start_http_server, Counter, Gauge

# Prometheus Metrics
VODS_RECORDED_TOTAL = Counter('vods_recorded_total', 'Total number of VOD parts fully recorded')
RECORDING_ERRORS_TOTAL = Counter('recording_errors_total', 'Total number of recording errors/crashes')
TOKEN_REFRESH_TOTAL = Counter('token_refresh_total', 'Total number of Twitch OAuth token refreshes')
ACTIVE_DOWNLOADS = Gauge('active_downloads', 'Number of active stream downloads currently in progress')

def setup_logger(name="twitch_recorder", level=logging.INFO):
    """Sets up a JSON structured logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid adding multiple handlers if setup is called multiple times
    if not logger.handlers:
        logHandler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%SZ'
        )
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
        
    return logger

def start_metrics_server(port=8000):
    """Starts the Prometheus metrics server."""
    logger = setup_logger()
    try:
        start_http_server(port)
        logger.info(f"Prometheus metrics server started", extra={"port": port})
    except Exception as e:
        logger.error(f"Failed to start Prometheus server", extra={"error": str(e)})

# Default global logger
logger = setup_logger()
