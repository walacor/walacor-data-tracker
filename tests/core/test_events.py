
import pytest
from core.events import EventBus


@pytest.fixture
def bus() -> EventBus:
    """Provides a fresh EventBus for each test."""
    return EventBus()

def test_bus_subscribe_and_publish_event(bus):
    dummy_log  = []

    def dummy_handler(**kwargs):
        dummy_log.append(kwargs.get("message"))

    bus.subscribe("dummy_event", dummy_handler)
    bus.publish("dummy_event", message="test_message")

    assert dummy_log == ["test_message"]


def test_bus_unsubscribe_specific_handler(bus):
    dummy_log= []

    def dummy_handler(**kwargs):
        dummy_log.append(kwargs.get("message"))
    
    unsubscribe = bus.subscribe("dummy_event", dummy_handler)
    unsubscribe()

    bus.publish("dummy_event", message="should_not_be_logged")
    assert dummy_log == []


def test_bus_unsubscribe_handler_from_all_events(bus):
    dummy_log = []

    def dummy_handler(**kwargs):
        dummy_log.append(kwargs.get("message"))
    
    bus.subscribe("event_one", dummy_handler)
    bus.subscribe("event_two", dummy_handler)

    bus.unsubscribe(dummy_handler)

    bus.publish("event_one",message= "no_log")
    bus.publish("event_two",message= "no_log")

    assert dummy_log == []

def test_bus_publish_no_subscribe(bus):

    bus.publish("unregistered_event", data="ignored")


def test_bus_handler_exception_propagates(bus):

    def faulty_handler(**kwargs):
        raise RuntimeError("handler failed")
    
    bus.subscribe("faulty_event", faulty_handler)

    with pytest.raises(RuntimeError, match="handler failed"):
        bus.publish("faulty_event", dummy="data")


def test_bus_context_manager_resets_automatically():
    dummy_log = []

    with EventBus() as bus:
        def dummy_handler(**kwargs):
            dummy_log.append(kwargs.get("message"))
    
        bus.subscribe("dummy_event", dummy_handler)
        bus.publish("dummy_event", message="inside_context")
        assert dummy_log == ["inside_context"]
    
    bus.publish("dummy_event", message="inside_context")
    assert dummy_log == ["inside_context"] 

def test_bus_manual_reset_removes_all_listeners(bus):
    dummy_log= []

    def dummy_handler(**kwargs):
            dummy_log.append(kwargs.get("message"))

    bus.subscribe("dummy_event", dummy_handler)
    bus.reset()
    bus.publish("dummy_event",message="after_reset")    
    
    assert dummy_log == []


def test_bus_multiple_handlers_receive_event(bus):
    dummy_log = []
    dummy_second_log = []

    def dummy_handler(**kwargs):
        dummy_log.append(kwargs.get("message"))
        
    def dummy_second_handler(**kwargs):
        dummy_second_log.append(kwargs.get("message"))

    bus.subscribe("shared_event", dummy_handler)
    bus.subscribe("shared_event", dummy_second_handler)

    bus.publish("shared_event", message="multi_listener_test")
    assert dummy_log == ["multi_listener_test"]
    assert dummy_second_log == ["multi_listener_test"]