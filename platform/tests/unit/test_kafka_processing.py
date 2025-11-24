# import pytest
# import json

# import sys, os
# PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..'))
# sys.path.insert(0, os.path.join(PROJECT_ROOT, "kafka","python_kafka"))

# from kafka_processing import safe_json_deserializer

# @pytest.mark.unit
# def test_json_deserialize_good():
#     msg = b'{"event": "ok"}'
#     assert safe_json_deserializer(msg) == {"event": "ok"}


# @pytest.mark.unit
# def test_json_deserialize_bad(caplog):
#     msg = b"not-json"
#     result = safe_json_deserializer(msg)
#     assert result is None
