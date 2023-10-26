from typing import Optional

from requests import Session

from queuery_client.client import Client
from queuery_client.exceptions import QueueryError
from queuery_client.response import Response


class QueueryClient:
    def __init__(
        self,
        endpoint: Optional[str] = None,
        timeout: int = 300,
        enable_cast: bool = False,
        session: Optional[Session] = None,
        use_manifest: Optional[bool] = None,
    ) -> None:
        self._client = Client(
            endpoint=endpoint,
            timeout=timeout,
            enable_cast=enable_cast,
            session=session,
            use_manifest=use_manifest,
        )

    def run(self, sql: str) -> Response:
        response = self._client.query_and_wait(sql)
        body = response._response
        if body.status == "failed":
            raise QueueryError(
                status=body.status,
                message=body.error,
            )
        return response
