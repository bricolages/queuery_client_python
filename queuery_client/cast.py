import datetime
from typing import Any, Dict, List

from dateutil.parser import isoparse

FALSE_VALUES = frozenset(["0", "f", "F", "false", "FALSE", "off", "OFF"])


def cast_row(row: List[str], manifest: Dict[str, Any]) -> List[Any]:
    column_types = [e["type"]["base"] for e in manifest["schema"]["elements"]]
    if len(row) != len(column_types):
        raise ValueError(
            f"len(row) is not equal to the size of schema.elements in manifest: "
            f"row={row}, schema.elements={manifest['schema']['elements']}"
        )
    return [_cast_type(x, t) for x, t in zip(row, column_types)]


def _cast_type(value: str, typename: str) -> Any:
    if not value:
        return None
    if typename in ("smallint", "integer", "bigint"):
        return int(value)
    if typename in ("numeric", "double precision"):
        return float(value)
    if typename in ("character", "character varying"):
        return value
    if typename in (
        "timestamp without time zone",
        "timestamp with time zone",
    ):
        return isoparse(value)
    if typename in ("date"):
        return datetime.date.fromisoformat(value)
    if typename in ("boolean"):
        return value not in FALSE_VALUES

    raise ValueError(f"Not supported data type: {typename}")
