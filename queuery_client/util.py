from typing import Generic, Iterator, TypeVar

T = TypeVar("T")


class SizedIterator(Generic[T]):
    """
    A wrapper for an iterator that knows its size.

    Args:
        iterator: The iterator.
        size: The size of the iterator.
    """

    def __init__(self, iterator: Iterator[T], size: int):
        self.iterator = iterator
        self.size = size

    def __iter__(self) -> Iterator[T]:
        return self.iterator

    def __next__(self) -> T:
        return next(self.iterator)

    def __len__(self) -> int:
        return self.size
