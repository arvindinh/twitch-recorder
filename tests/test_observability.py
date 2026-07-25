import json
import logging
from io import StringIO
from src.observability import setup_logger, VODS_RECORDED_TOTAL, RECORDING_ERRORS_TOTAL

def test_json_logging():
    # Capture log output
    stream = StringIO()
    logHandler = logging.StreamHandler(stream)
    
    # We create a new logger for testing to avoid conflict with global one
    test_logger = setup_logger(name="test_logger")
    # Swap handler for capturing
    test_logger.handlers = []
    
    from pythonjsonlogger import jsonlogger
    formatter = jsonlogger.JsonFormatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    logHandler.setFormatter(formatter)
    test_logger.addHandler(logHandler)
    
    test_logger.info("Test message", extra={"user": "shroud"})
    
    log_output = stream.getvalue()
    assert log_output != ""
    
    # Verify it parses as JSON
    parsed = json.loads(log_output)
    assert parsed["message"] == "Test message"
    assert parsed["user"] == "shroud"
    assert parsed["levelname"] == "INFO"

def test_prometheus_counters():
    # Increment counters
    initial_vods = VODS_RECORDED_TOTAL._value.get()
    initial_errors = RECORDING_ERRORS_TOTAL._value.get()
    
    VODS_RECORDED_TOTAL.inc()
    RECORDING_ERRORS_TOTAL.inc(2)
    
    assert VODS_RECORDED_TOTAL._value.get() == initial_vods + 1
    assert RECORDING_ERRORS_TOTAL._value.get() == initial_errors + 2
