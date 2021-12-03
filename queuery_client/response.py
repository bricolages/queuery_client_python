import csv
import dataclasses
import gzip
from io import StringIO
from typing import Any, Dict, Iterator, List, Literal, Optional, Union, overload

import requests

from queuery_client.cast import cast_row

try:
    import pandas
except ModuleNotFoundError:
    pandas = None


@dataclasses.dataclass
class ResponseBody:
    id: int
    data_file_urls: List[str]
    error: Optional[str]
    status: str
    manifest_file_url: Optional[str] = None

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "ResponseBody":
        properties = set(field.name for field in dataclasses.fields(cls))
        params = {key: value for key, value in params.items() if key in properties}
        return cls(**params)


class Response:
    def __init__(
        self,
        response: ResponseBody,
        enable_cast: bool = False,
    ):
        self._response = response
        self._data_file_urls = response.data_file_urls
        self._cursor = 0
        self._parser = csv.reader
        self._session = requests.Session()
        self._enable_cast = enable_cast
        self._manifest: Optional[Dict[str, Any]] = None

    def __iter__(self) -> Iterator[List[Any]]:
        for url in self._data_file_urls:
            for row in self._open(url):
                if self._enable_cast:
                    yield cast_row(row, self.fetch_manifest())
                else:
                    yield row

    def _open(self, url: str) -> List[List[str]]:
        data = self._session.get(url).content
        response = gzip.decompress(data).decode()
        reader = csv.reader(StringIO(response), escapechar="\\")

        self._cursor += 1
        return list(reader)

    def fetch_manifest(self, force: bool = False) -> Dict[str, Any]:
        if self._manifest is None or force:
            if not self._response.manifest_file_url:
                raise RuntimeError("Response does not contain manifest_file_url.")

            manifest = self._session.get(self._response.manifest_file_url).json()
            assert isinstance(manifest, dict)
            self._manifest = manifest
        return self._manifest

    @overload
    def read(self) -> List[List[Any]]:
        ...

    @overload
    def read(self, use_pandas: Literal[True]) -> "pandas.DataFrame":
        ...

    @overload
    def read(self, use_pandas: Literal[False]) -> List[List[Any]]:
        ...

    def read(
        self,
        use_pandas: bool = False,
    ) -> Union[List[List[Any]], "pandas.DataFrame"]:
        elems = list(self)

        if use_pandas:
            if pandas is None:
                raise ModuleNotFoundError(
                    "pandas is not availabe. Please make sure that "
                    "pandas is successfully installed to use use_pandas option."
                )
            return pandas.DataFrame(elems)

        return elems
