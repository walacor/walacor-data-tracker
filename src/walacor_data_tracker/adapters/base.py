import abc

from walatrack.core.tracker import Tracker


class BaseAdapter(abc.ABC):
    """All adapters must expose only *start* / *stop*."""

    def __init__(self) -> None:
        self._active = False

    @abc.abstractmethod
    def _patch(self, tracker: Tracker) -> None: ...

    @abc.abstractmethod
    def _unpatch(self) -> None: ...

    def start(self, tracker: Tracker) -> "BaseAdapter":
        if not self._active:
            self._patch(tracker)
            self._active = True
        return self

    def stop(self) -> None:
        if self._active:
            self._unpatch()
            self._active = False
