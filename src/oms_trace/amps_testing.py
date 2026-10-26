"""Test doubles for the AMPS client (no broker, no ``amps-python-client`` needed).

Shared by the pure-python and the embedded-engine tests, and usable from a console to play the
broker against a running source: ``FakeMessage`` exposes the three getters
:class:`oms_trace.amps_sow.TopicMirror` reads; ``StubClient`` records the handler of every
``execute_async`` so the caller can deliver messages per topic; ``StubCommand`` is what a
``command_factory`` returns.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class FakeMessage:
    def __init__(self, command: str, data: Optional[str] = None, sow_key: str = "") -> None:
        self._command, self._data, self._sow_key = command, data, sow_key

    def get_command(self) -> str:
        return self._command

    def get_sow_key(self) -> str:
        return self._sow_key

    def get_data(self) -> str:
        return "" if self._data is None else self._data


class StubCommand:
    """What ``command_factory`` returns: the stub client reads the topic off it."""

    def __init__(self, topic: str, amps: Any) -> None:
        self.topic, self.amps = topic, amps


class StubClient:
    def __init__(self) -> None:
        self.handlers: Dict[str, Any] = {}
        self.listeners: List[Any] = []
        self.closed = False
        self.unsubscribed: List[Any] = []

    def add_connection_state_listener(self, fn: Any) -> None:
        self.listeners.append(fn)

    def connect_and_logon(self) -> None:
        pass

    def execute_async(self, command: StubCommand, handler: Any) -> str:
        self.handlers[command.topic] = handler
        return f"sub-{len(self.handlers)}"

    def unsubscribe(self, sub_id: Any) -> None:
        self.unsubscribed.append(sub_id)

    def close(self) -> None:
        self.closed = True

    def connection(self, up: bool) -> None:
        """Play the HA client's connection-state listener (True: connected, False: disconnected)."""
        for fn in self.listeners:
            fn(up)
