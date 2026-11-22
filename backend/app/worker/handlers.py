
from collections.abc import Callable
from typing import Any


TaskHandler = Callable[[dict[str, Any]], Any]


def echo_handler(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the supplied payload unchanged."""
    return {"echo": payload}


def add_handler(payload: dict[str, Any]) -> dict[str, int | float]:
    """Add two numeric values supplied in the payload."""
    a = payload.get("a")
    b = payload.get("b")

    # bool is a subclass of int in Python, so reject it explicitly.
    if (
        isinstance(a, bool)
        or not isinstance(a, (int, float))
        or isinstance(b, bool)
        or not isinstance(b, (int, float))
    ):
        raise ValueError(
            "The 'add' task requires numeric 'a' and 'b' values."
        )

    result = a + b

    if isinstance(result, float) and not result.is_integer():
        return {"result": result}

    return {"result": int(result)}


class HandlerRegistry:
    """Maps supported task types to their handler functions."""

    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(
        self,
        task_type: str,
        handler: TaskHandler,
    ) -> None:
        task_type = task_type.strip()

        if not task_type:
            raise ValueError("task_type must not be blank")

        if not callable(handler):
            raise TypeError("handler must be callable")

        if task_type in self._handlers:
            raise ValueError(
                f"A handler is already registered for '{task_type}'."
            )

        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler:
        try:
            return self._handlers[task_type]
        except KeyError:
            raise ValueError(
                f"No handler registered for task type '{task_type}'."
            ) from None

    def execute(
        self,
        task_type: str,
        payload: dict[str, Any],
    ) -> Any:
        handler = self.get(task_type)
        return handler(payload)


def create_default_registry() -> HandlerRegistry:
    """Create a registry populated with built-in task handlers."""
    registry = HandlerRegistry()
    registry.register("echo", echo_handler)
    registry.register("add", add_handler)
    return registry
