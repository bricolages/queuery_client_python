import gzip
import json
from typing import Any, Dict
from unittest import mock

from queuery_client.response import Response, ResponseBody


class MockResponse:
    def __init__(self, content: bytes, status_code: int) -> None:
        self.content = content
        self.status_code = status_code

    def json(self) -> Dict[str, Any]:
        data = json.loads(self.content)
        assert isinstance(data, dict)
        return data


def test_response() -> None:
    response_body = ResponseBody(
        id=1,
        data_file_urls=["https://queuery.example.com"],
        error=None,
        status="success",
    )
    response = Response(response_body)

    mock_response = MockResponse(gzip.compress(b'"1","test_recipe1"\n"2","test_recipe2"'), 200)
    with mock.patch("requests.Session.get", return_value=mock_response):
        data = response.read()
        assert data == [["1", "test_recipe1"], ["2", "test_recipe2"]]


def test_response_with_type_cast() -> None:
    response_body = ResponseBody(
        id=1,
        data_file_urls=["https://queuery.example.com/data"],
        error=None,
        status="success",
        manifest_file_url="https://queuery.example.com/manifest",
    )
    response = Response(response_body, enable_cast=True)

    manifest_response = MockResponse(
        b"""
        {
            "schema": {
                "elements": [
                    {"name": "id", "type": {"base": "integer"}},
                    {"name": "title", "type": {"base": "character varying"}}
                ]
            },
            "meta": {"record_count": 2}
        }
        """,
        200,
    )
    with mock.patch("requests.Session.get", return_value=manifest_response):
        response.fetch_manifest()

    data_response = MockResponse(gzip.compress(b'"1","test_recipe1"\n"2","test_recipe2"'), 200)
    with mock.patch("requests.Session.get", return_value=data_response):
        data = response.read()
        assert data == [[1, "test_recipe1"], [2, "test_recipe2"]]
