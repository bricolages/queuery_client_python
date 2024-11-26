import logging
import os
import time
from typing import Optional, Tuple

import requests
from requests import Session

from queuery_client.response import Response, ResponseBody

logger = logging.getLogger(__name__)


class Client(object):
    api_version = "v1"
    max_polling_interval = 30

    def __init__(
        self,
        endpoint: Optional[str] = None,
        token: Optional[str] = None,
        token_secret: Optional[str] = None,
        timeout: int = 300,
        enable_cast: bool = False,
        session: Optional[Session] = None,
        use_manifest: Optional[bool] = None,
    ) -> None:
        endpoint = endpoint or os.getenv("QUEUERY_ENDPOINT")
        if endpoint is None:
            raise ValueError("Queuery endpoint is not specified.")
        self._endpoint = endpoint
        self._token = token or os.getenv("QUEUERY_TOKEN")
        self._token_secret = token_secret or os.getenv("QUEUERY_TOKEN_SECRET")
        self._timeout = timeout
        self._enable_cast = enable_cast
        self._session = session or Session()
        self._use_manifest = use_manifest

    @property
    def _auth(self) -> Optional[Tuple[str, str]]:
        if self._token is self._token_secret is None:
            return None
        if self._token is None or self._token_secret is None:
            raise ValueError("Both token and token_secret should be specified.")
        return (self._token, self._token_secret)

    @property
    def _enable_metadata(self) -> bool:
        return bool(self._enable_cast or self._use_manifest)

    def execute_query(self, select_stmt: str) -> Response:
        logger.info("Sending select statement to queuery...")
        request_url = os.path.join(self._endpoint, Client.api_version, "queries")
        payload = {"q": select_stmt}
        if self._enable_metadata:
            payload["enable_metadata"] = "true"

        with self._session.post(url=request_url, auth=self._auth, data=payload) as resp:
            resp.raise_for_status()
            if resp.status_code == requests.codes.created:
                body = ResponseBody.from_dict(resp.json())
                return Response(response=body, enable_cast=self._enable_cast)
            else:
                raise RuntimeError(f"Unexpected status code was returned: {resp.status_code}")

    def get_body(self, qid: int) -> Response:
        request_url = os.path.join(self._endpoint, Client.api_version, "queries", str(qid))

        fields = ["__default__"]
        if self._enable_metadata:
            fields.append("manifest_file_url")

        data = {"fields": ",".join(fields)}

        with self._session.get(url=request_url, auth=self._auth, data=data) as resp:
            if resp.status_code != requests.codes.ok:
                resp.raise_for_status()
            body = ResponseBody.from_dict(resp.json())
            return Response(
                response=body,
                enable_cast=self._enable_cast,
                session=self._session,
                use_manifest=self._use_manifest,
            )

    def wait_for(self, qid: int) -> Response:
        timeout = self._timeout
        if timeout < 0:
            raise ValueError("Parameter `timeout` should be positive integer")

        n = 1
        st = time.time()
        logger.info("Waiting for the query (%i) to complete." % (qid))
        while True:
            body = self.get_body(qid)

            if body._response.status in ("success", "failed"):
                logger.info("Query (%i) completed" % (qid))
                return body

            if (time.time() - st) >= timeout:
                raise TimeoutError("Query time exceeded (change `timeout` parameter if you want to wait longer.)")

            logger.debug(".")
            time.sleep(min(3 * n, self.max_polling_interval))
            n += 1

    def query_and_wait(self, select_stmt: str) -> Response:
        body = self.execute_query(select_stmt)
        return self.wait_for(body._response.id)
