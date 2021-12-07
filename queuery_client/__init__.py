from importlib_metadata import version

from queuery_client.queuery_client import QueueryClient  # NOQA: F401

__version__ = version("queuery_client")  # type: ignore[no-untyped-call]
