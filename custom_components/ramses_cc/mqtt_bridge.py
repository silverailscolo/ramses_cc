"""The Bridge between Home Assistant MQTT and ramses_rf."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback

if TYPE_CHECKING:
    from homeassistant.components.mqtt import PublishPayloadType

_LOGGER = logging.getLogger(__name__)


class MqttTransport(asyncio.Transport):
    """A virtual transport that sends data via HA MQTT."""

    def __init__(
        self,
        bridge: RamsesMqttBridge,
        protocol: asyncio.Protocol,
        extra: dict[Any, Any] | None,
        disable_sending: bool = False,
    ) -> None:
        """Initialize the transport."""
        super().__init__()
        self._bridge = bridge
        self._protocol = protocol
        self._extra = extra or {}
        self._disable_sending = disable_sending
        self._closing = False
        _LOGGER.debug("MqttTransport: Initialized with extra=%s", self._extra)

    def _dt_now(self) -> Any:
        """Return the current datetime.

        Required by ramses_rf to determine packet expiration and timestamps.
        Must return a naive datetime to match ramses_tx defaults (dt.now()).
        """
        return datetime.now()

    def get_extra_info(self, name: Any, default: Any = None) -> Any:
        """Get extra information about the transport."""
        val = self._extra.get(name, default)
        return val

    def write(self, data: bytes) -> None:
        """Write data to the transport (publish to MQTT)."""
        if self._closing or self._disable_sending:
            return

        # ramses_rf typically uses write_frame, but we handle raw write for safety.
        # We assume the bytes are a utf-8 command string.
        try:
            payload = data.decode("utf-8")

            # Wrap in JSON as confirmed by testing
            json_payload = json.dumps({"msg": payload})

            _LOGGER.debug("MqttTransport: TX (raw) -> %s", json_payload)
            self._bridge.publish(json_payload)
        except UnicodeDecodeError:
            _LOGGER.warning("Attempted to publish non-utf8 data to MQTT: %s", data)
        except Exception as err:
            _LOGGER.error("MqttTransport: Failed to publish raw data: %s", err)

    async def write_frame(self, frame: str) -> None:
        """Write a frame to the transport.

        Required by ramses_tx.protocol which awaits this method.
        """
        if self._closing or self._disable_sending:
            return

        # Wrap frame in JSON to match ramses_esp expectations.
        # Confirmed by test: Device responds to {"msg": "RQ ..."}
        try:
            json_payload = json.dumps({"msg": frame})
            _LOGGER.debug("MqttTransport: TX (frame) -> %s", json_payload)
            self._bridge.publish(json_payload)
        except TypeError as err:
            _LOGGER.error("MqttTransport: Failed to JSON encode frame: %s", err)

    def close(self) -> None:
        """Close the transport."""
        _LOGGER.debug("MqttTransport: Closing")
        self._closing = True
        self._protocol.connection_lost(None)

    def abort(self) -> None:
        """Abort the transport."""
        self.close()

    def is_closing(self) -> bool:
        """Return True if the transport is closing or closed."""
        return self._closing

    def pause_reading(self) -> None:
        """Pause the receiving end."""
        _LOGGER.debug("MqttTransport: pause_reading (No-op)")

    def resume_reading(self) -> None:
        """Resume the receiving end."""
        _LOGGER.debug("MqttTransport: resume_reading (No-op)")


class RamsesMqttBridge:
    """Isolates all MQTT translation logic."""

    def __init__(self, hass: HomeAssistant, topic_root: str) -> None:
        """Initialize the bridge."""
        self._hass = hass
        self._topic_root = topic_root.rstrip("/")
        self._protocol: asyncio.Protocol | None = None
        self._transport: MqttTransport | None = None
        self._unsubscribe: Callable[[], None] | None = None
        self._unsubscribe_status: Callable[[], None] | None = None

    async def async_transport_factory(
        self,
        protocol: asyncio.Protocol,
        disable_sending: bool = False,
        extra: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> MqttTransport:
        """The factory method passed to ramses_rf.Gateway."""
        _LOGGER.debug(
            "MqttBridge: async_transport_factory called for protocol %s", type(protocol)
        )
        self._protocol = protocol
        self._transport = MqttTransport(self, protocol, extra, disable_sending)

        # Bind immediately to satisfy ramses_rf 1.0s timeout.
        _LOGGER.debug("MqttBridge: Calling protocol.connection_made(ramses=True)")

        try:
            # ramses_rf.PortProtocol requires 'ramses=True' to accept connection
            protocol.connection_made(self._transport, ramses=True)  # type: ignore[call-arg]
        except TypeError:
            # Fallback for standard protocols (e.g. testing)
            protocol.connection_made(self._transport)

        # Perform subscription in the background
        self._hass.async_create_task(self._async_attach())

        return self._transport

    async def _async_attach(self) -> None:
        """Start listening to MQTT."""
        topic = f"{self._topic_root}/#"
        _LOGGER.debug("MqttBridge: Starting subscription to %s", topic)

        try:
            self._unsubscribe = await mqtt.async_subscribe(
                self._hass, topic, self._handle_mqtt_message, qos=0
            )
            _LOGGER.info("MqttBridge: Successfully subscribed to %s", topic)

            self._unsubscribe_status = mqtt.async_subscribe_connection_status(
                self._hass, self._handle_connection_status
            )
        except Exception as err:
            _LOGGER.error("MqttBridge: Failed to subscribe to MQTT: %s", err)

    @callback
    def _handle_mqtt_message(self, msg: Any) -> None:
        """Process incoming MQTT messages and inject into ramses_rf."""
        if self._protocol is None:
            return

        payload = msg.payload
        # Debug logging reduced to avoid spamming logic
        # _LOGGER.debug("MqttBridge: RX <- %s", payload)

        try:
            # Handle bytes payload
            if isinstance(payload, bytes):
                payload_str = payload.decode("utf-8")
            else:
                payload_str = str(payload)

            # Unwrap JSON if present (standard ramses_esp format)
            try:
                data = json.loads(payload_str)
                if isinstance(data, dict) and "msg" in data:
                    payload_str = data["msg"]
            except json.JSONDecodeError:
                pass  # Treat as raw packet string if not JSON

            # ramses_rf expects a serial stream ending in \r\n
            if not payload_str.endswith("\r\n"):
                payload_str += "\r\n"

            self._protocol.data_received(payload_str.encode("utf-8"))

        except Exception as err:
            _LOGGER.error("Error processing MQTT message: %s", err)

    @callback
    def _handle_connection_status(self, status: str) -> None:
        """Handle MQTT broker connection/disconnection."""
        _LOGGER.debug("MqttBridge: Connection status changed to %s", status)
        if status == "online":
            _LOGGER.info("MQTT Broker connected. Resuming ramses_rf.")
        elif status == "offline":
            _LOGGER.warning("MQTT Broker disconnected. Pausing ramses_rf.")

    def publish(self, payload: PublishPayloadType) -> None:
        """Publish a packet to MQTT."""
        topic = f"{self._topic_root}/tx"
        self._hass.async_create_task(mqtt.async_publish(self._hass, topic, payload))

    def close(self) -> None:
        """Cleanup subscriptions."""
        _LOGGER.debug("MqttBridge: Cleanup called")
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None

        if self._unsubscribe_status:
            self._unsubscribe_status()
            self._unsubscribe_status = None
