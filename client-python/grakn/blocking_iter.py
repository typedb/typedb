from multiprocessing import Queue
from typing import Generic, Optional, Iterator, TypeVar

T = TypeVar('T')


class BlockingIter(Generic[T]):
    """An iterator that blocks until an element is available"""

    def __init__(self) -> None:
        self._queue: Queue[Optional[T]] = Queue()

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        elem = self._queue.get(block=True)
        if elem is None:
            raise StopIteration()
        else:
            return elem

    def add(self, elem: T) -> None:
        """Add an element to the iterator"""
        if elem is None:
            raise ValueError()
        else:
            self._queue.put(elem)

    def close(self) -> None:
        """Close the iterator, so that __next__ will throw StopIteration"""
        self._queue.put(None)
