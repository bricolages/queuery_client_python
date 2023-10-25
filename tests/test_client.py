import json
from typing import Any, Dict
from unittest import mock

from queuery_client import QueueryClient


class MockResponse:
    def __init__(self, content: bytes, status_code: int) -> None:
        self.content = content
        self.status_code = status_code

    def json(self) -> Dict[str, Any]:
        data = json.loads(self.content)
        assert isinstance(data, dict)
        return data

    def raise_for_status(self) -> None:
        pass

    def __enter__(self) -> "MockResponse":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


def test_client() -> None:
    client = QueueryClient(endpoint="https://queuery.queuery.example.com")
    response = MockResponse(
        b"""
        {
            "id": 1,
            "data_file_urls": [],
            "error": "",
            "status": 201,
            "unknown_property": 1
        }
        """,
        201,
    )

    with mock.patch("requests.Session.post", return_value=response):
        _ = client._client.execute_query("select 1 from target.tabele")


def test_client_with_type_cast() -> None:
    client = QueueryClient(endpoint="https://queuery.example.com", enable_cast=True)
    response = MockResponse(
        b"""
        {
            "id": 1,
            "data_file_urls": [],
            "error": "",
            "status": 201,
            "manifest_file_url": "https://queuery.example.com/manifest"
        }
        """,
        201,
    )

    with mock.patch("requests.Session.post", return_value=response):
        queuery_response = client._client.execute_query("select 1 from target.table")
        assert queuery_response._enable_cast
        assert queuery_response._response.manifest_file_url == "https://queuery.example.com/manifest"
