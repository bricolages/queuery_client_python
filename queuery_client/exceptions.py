from typing import Optional


class QueueryError(Exception):
    def __init__(
        self,
        status: str,
        message: Optional[str] = None,
    ) -> None:
        self.status = status
        self.message = message

    def __str__(self) -> str:
        return f"<QueueryError {self.status}: {self.message}>"

    def __repr__(self) -> str:
        return f"<QueueryError {self.status}: {self.message}>"
