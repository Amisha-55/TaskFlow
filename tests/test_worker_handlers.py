
import pytest

from backend.app.worker.handlers import (
    HandlerRegistry,
    create_default_registry,
)


def test_echo_handler_returns_payload():
    registry = create_default_registry()
    payload = {"message": "hello", "count": 3}

    result = registry.execute("echo", payload)

    assert result == {"echo": payload}


def test_add_handler_returns_sum():
    registry = create_default_registry()

    assert registry.execute("add", {"a": 10, "b": 15}) == {
        "result": 25
    }


def test_add_handler_supports_fractional_values():
    registry = create_default_registry()

    assert registry.execute("add", {"a": 1.5, "b": 2.25}) == {
        "result": 3.75
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"a": 1},
        {"a": 1, "b": "2"},
        {"a": True, "b": 2},
        {"a": 1, "b": False},
    ],
)
def test_add_handler_rejects_invalid_payload(payload):
    registry = create_default_registry()

    with pytest.raises(ValueError, match="numeric"):
        registry.execute("add", payload)


def test_unknown_task_type_raises_error():
    registry = create_default_registry()

    with pytest.raises(ValueError, match="No handler registered"):
        registry.execute("does_not_exist", {})


def test_registry_rejects_duplicate_registration():
    registry = HandlerRegistry()
    registry.register("example", lambda payload: payload)

    with pytest.raises(ValueError, match="already registered"):
        registry.register("example", lambda payload: payload)


def test_registry_rejects_blank_task_type():
    registry = HandlerRegistry()

    with pytest.raises(ValueError, match="must not be blank"):
        registry.register("   ", lambda payload: payload)
