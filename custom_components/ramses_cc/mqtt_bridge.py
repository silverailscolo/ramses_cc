"""The Bridge between Home Assistant MQTT and ramses_rf."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
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
    ) -> None:
        """Initialize the transport."""
        super().__init__(extra)
        self._bridge = bridge
        self._protocol = protocol
        self._closing = False

    def write(self, data: bytes) -> None:
        """Write data to the transport (publish to MQTT)."""
        if self._closing:
            return
        # The library sends bytes; we assume it's a string payload for MQTT
        try:
            payload = data.decode("utf-8")
            self._bridge.publish(payload)
        except UnicodeDecodeError:
            _LOGGER.warning("Attempted to publish non-utf8 data to MQTT: %s", data)

    def close(self) -> None:
        """Close the transport."""
        self._closing = True
        self._protocol.connection_lost(None)

    def abort(self) -> None:
        """Abort the transport."""
        self.close()


class RamsesMqttBridge:
    """Isolates all MQTT translation logic."""

    def __init__(self, hass: HomeAssistant, topic_root: str) -> None:
        """Initialize the bridge."""
        self._hass = hass
        self._topic_root = topic_root.rstrip("/")
        self._protocol: asyncio.Protocol | None = None
        self._transport: MqttTransport | None = None
        self._unsubscribe: Callable[[], None] | None = None

    async def async_transport_factory(
        self, protocol: asyncio.Protocol
    ) -> MqttTransport:
        """The factory method passed to ramses_rf.Gateway."""
        self._protocol = protocol
        self._transport = MqttTransport(self, protocol, None)

        # Start listening to MQTT when the transport is created
        await self._async_attach()

        # Signal that the connection is made
        protocol.connection_made(self._transport)
        return self._transport

    async def _async_attach(self) -> None:
        """Start listening to MQTT."""
        # Subscribe to the command topics or raw packet topics
        # Assuming ramses_rf expects raw packets on {topic_root}/#
        # Adjust topic filter based on actual library expectation
        topic = f"{self._topic_root}/#"

        _LOGGER.debug("Subscribing to HA MQTT topic: %s", topic)

        self._unsubscribe = await mqtt.async_subscribe(
            self._hass, topic, self._handle_mqtt_message, qos=0
        )

        # Monitor connection status (Circuit Breaker)
        await mqtt.async_subscribe_connection_status(
            self._hass, self._handle_connection_status
        )

    @callback
    def _handle_mqtt_message(self, msg: Any) -> None:
        """Process incoming MQTT messages and inject into ramses_rf."""
        if self._protocol is None:
            return

        payload = msg.payload

        # logic to strip timezone if necessary (as per report)
        if isinstance(payload, str):
            # Example timestamp stripping if the library requires it
            # This logic depends on the exact format ramses_rf fails on
            pass

        # Pass data to the library's protocol
        try:
            if isinstance(payload, str):
                self._protocol.data_received(payload.encode("utf-8"))
            else:
                self._protocol.data_received(payload)
        except Exception as err:
            _LOGGER.error("Error processing MQTT message: %s", err)

    @callback
    def _handle_connection_status(self, status: str) -> None:
        """Handle MQTT broker connection/disconnection."""
        if status == "online":
            _LOGGER.info("MQTT Broker connected. Resuming ramses_rf.")
            # Optionally trigger a re-connection logic in protocol if supported
        elif status == "offline":
            _LOGGER.warning("MQTT Broker disconnected. Pausing ramses_rf.")
            # If the transport supports pausing, call it here

    def publish(self, payload: PublishPayloadType) -> None:
        """Publish a packet to MQTT."""
        # Construct the topic. ramses_rf usually writes raw packets.
        # You might need a specific subtopic like /tx or similar depending on setup.
        topic = f"{self._topic_root}/tx"

        self._hass.async_create_task(mqtt.async_publish(self._hass, topic, payload))

    def close(self) -> None:
        """Cleanup subscriptions."""
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None
