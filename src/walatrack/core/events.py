import threading
from types import TracebackType
from typing import Callable, MutableMapping


class EventBus:
    """Lightweight, thread-safe event emitter.

    • Synchronous: `publish()` blocks until all callbacks return.
    • Exceptions raised by listeners **propagate** to the caller—writers
      should catch their own errors.
    """

    _REGISTRY: MutableMapping[str, list[Callable[..., None]]] = {}
    _LOCK = threading.RLock()

    def subscribe(self, event: str, callback: Callable[..., None]) -> Callable[[], None]:
        """Register *callback* for *event*.

        Returns a zero-argument function that **unsubscribes** this callback.
        """
        with self._LOCK:
            self._REGISTRY.setdefault(event, []).append(callback)

        def _unsubscribe() -> None:
            with self._LOCK:
                self._REGISTRY.get(event, []).remove(callback)

        return _unsubscribe

    def unsubscribe(self, callback: Callable[..., None], event: str | None = None) -> None:
        """Remove *callback* from one or all events."""
        with self._LOCK:
            if event is not None:
                if event in self._REGISTRY and callback in self._REGISTRY[event]:
                    self._REGISTRY[event].remove(callback)
            else:
                for listeners in self._REGISTRY.values():
                    if callback in listeners:
                        listeners.remove(callback)

    def publish(self, event: str, **payload) -> None:
        """Fire *event*, forwarding all keyword arguments to each listener."""
        with self._LOCK:
            listeners = list(self._REGISTRY.get(event, ()))
        for fn in listeners:
            fn(**payload)

    def __enter__(self) -> "EventBus":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        self.reset()
        return False

    def reset(self) -> None:
        """Remove **all** listeners (used by unit tests)."""
        with self._LOCK:
            self._REGISTRY.clear()


global_bus: EventBus = EventBus()
