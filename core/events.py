from collections import defaultdict
from typing import Any, Callable


class EventBus:
    def __init__(self):
        self._subscribers: dict[str,list[Callable[[Any],None]]] = defaultdict(list)
    
    def subscribe(self, event_type:str, handler: Callable[[Any],None])->None:
        """Register handler to listen for an event_type"""
        if handler not in self._subscribers[event_type]:
            self._subscribers[event_type].append(handler)

    def publish(self, event_type: str, payload: any)->None:
        """Send an event instantly to all subscribers"""
        for handler in self._subscribers.get(event_type, []):
            handler(payload)
        
    def unsubscribe(self, event_type: str, handler: Callable[[Any],None])->None:
        """Unregister handler from an event_type"""
        if handler in self._subscribers[event_type]:
            self._subscribers[event_type].remove(handler)
    
