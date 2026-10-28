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
    def __init__(self, command: str, data: Optional[str] = None, sow_key: str = "", bookmark: str = "",
                 sub_id: str = "") -> None:
        self._command, self._data, self._sow_key, self._bookmark, self._sub_id = command, data, sow_key, bookmark, sub_id

    def get_command(self) -> str:
        return self._command

    def get_sow_key(self) -> str:
        return self._sow_key

    def get_data(self) -> str:
        return "" if self._data is None else self._data

    def get_bookmark(self) -> str:
        return self._bookmark

    def get_sub_id(self) -> str:
        return self._sub_id


class FakeBookmarkStore:
    """The two calls the source makes: the most recent bookmark per sub id, and discards."""

    def __init__(self) -> None:
        self.recent: Dict[str, str] = {}
        self.discarded: List[Any] = []

    def get_most_recent(self, sub_id: str) -> str:
        return self.recent.get(sub_id, "0")

    def discard_message(self, message: Any) -> None:
        self.discarded.append(message)
        self.recent[message.get_sub_id()] = message.get_bookmark()


class StubCommand:
    """What ``command_factory`` returns: the stub client reads the topic off it (and the bookmark start)."""

    def __init__(self, topic: str, amps: Any, start: Optional[str] = None) -> None:
        self.topic, self.amps, self.start = topic, amps, start
        self.kind = "subscribe" if amps.mode == "bookmark" else "sow_and_subscribe"
        self.sub_id = amps.sub_id(topic) if amps.mode == "bookmark" else ""


class StubClient:
    def __init__(self) -> None:
        self.handlers: Dict[str, Any] = {}
        self.listeners: List[Any] = []
        self.closed = False
        self.unsubscribed: List[Any] = []
        self.commands: Dict[str, StubCommand] = {}
        self.store: Any = None

    def add_connection_state_listener(self, fn: Any) -> None:
        self.listeners.append(fn)

    def set_bookmark_store(self, store: Any) -> None:
        self.store = store

    def discard(self, message: Any) -> None:
        """The real client discards through its bookmark store."""
        self.store.discard_message(message)

    def connect_and_logon(self) -> None:
        pass

    def execute_async(self, command: StubCommand, handler: Any) -> str:
        self.handlers[command.topic] = handler
        self.commands[command.topic] = command
        return f"sub-{len(self.handlers)}"

    def unsubscribe(self, sub_id: Any) -> None:
        self.unsubscribed.append(sub_id)

    def close(self) -> None:
        self.closed = True

    def connection(self, up: bool) -> None:
        """Play the HA client's connection-state listener (True: connected, False: disconnected)."""
        for fn in self.listeners:
            fn(up)
