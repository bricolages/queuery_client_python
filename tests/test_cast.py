from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from queuery_client.cast import _cast_type, cast_row


@pytest.mark.parametrize(
    "value, typename, desired",
    [
        ("2", "smallint", 2),
        ("123", "integer", 123),
        ("12345", "bigint", 12345),
        ("3.14", "numeric", 3.14),
        ("2.71828", "double precision", 2.71828),
        ("abc", "character", "abc"),
        ("abcde", "character varying", "abcde"),
        (
            "2021-01-01 12:34:56",
            "timestamp without time zone",
            datetime(2021, 1, 1, 12, 34, 56),
        ),
        (
            "2021-01-01 12:34:56.123456+09",
            "timestamp with time zone",
            datetime(
                2021,
                1,
                1,
                12,
                34,
                56,
                123456,
                tzinfo=timezone(timedelta(hours=9)),
            ),
        ),
        ("true", "boolean", True),
        ("false", "boolean", False),
        ("0", "boolean", False),
        ("1", "boolean", True),
    ],
)
def test_cast_type(value: str, typename: str, desired: Any) -> None:
    assert _cast_type(value, typename) == desired


def test_cast_row() -> None:
    inputs = ["1", "1", "1"]
    manifest = {
        "schema": {
            "elements": [
                {"type": {"base": "integer"}},
                {"type": {"base": "character"}},
                {"type": {"base": "boolean"}},
            ]
        }
    }
    desired = [1, "1", True]

    output = cast_row(inputs, manifest)
    assert output == desired


def test_cast_row_with_mismatched_manifest() -> None:
    row = ["1", "abc", "0", "2021-01-01 01:23:45"]
    manifest = {
        "schema": {
            "elements": [
                {"type": {"base": "integer"}},
                {"type": {"base": "character"}},
                {"type": {"base": "boolean"}},
            ]
        }
    }

    with pytest.raises(ValueError) as e:
        _ = cast_row(row, manifest)

    assert str(e.value).startswith("len(row) is not equal to the size of schema.elements in manifest:")
