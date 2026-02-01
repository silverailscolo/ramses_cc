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
        _LOGGER.debug(
            "MqttTransport: Initialized (disable_sending=%s, extra=%s)",
            self._disable_sending,
            self._extra,
        )

    def _dt_now(self) -> Any:
        """Return the current datetime.

        Required by ramses_rf to determine packet expiration and timestamps.
        Must return a naive datetime to match ramses_tx defaults (dt.now()).
        """
        return datetime.now()

    def get_extra_info(self, name: Any, default: Any = None) -> Any:
        """Get extra information about the transport."""
        val = self._extra.get(name, default)
        if val is not None:
            return val
        if name == "serial":
            return self._bridge.device_id
        return None

    def write(self, data: bytes) -> None:
        """Write data to the transport (publish to MQTT)."""
        if self._closing:
            _LOGGER.debug("MqttTransport: TX BLOCKED (Transport closing) -> %s", data)
            return
        if self._disable_sending:
            _LOGGER.debug("MqttTransport: TX BLOCKED (Disable sending) -> %s", data)
            return

        # ramses_rf typically uses write_frame, but we handle raw write for safety.
        # We assume the bytes are a utf-8 command string.
        try:
            payload = data.decode("utf-8")
            # The firmware separates "Commands" (!V, !C) from "Radio Packets".
            # If the data starts with '!', send to cmd topic. Otherwise, tx topic.
            if payload.strip().startswith("!"):
                _LOGGER.debug("MqttTransport: Sending Command -> %s", payload)
                self._bridge.publish_command(payload)
            else:
                # Wrap in JSON for the /tx topic
                json_payload = json.dumps({"msg": payload})
                _LOGGER.debug("MqttTransport: TX (raw) -> %s", json_payload)
                self._bridge.publish_tx(json_payload)

        except UnicodeDecodeError:
            _LOGGER.warning("Attempted to publish non-utf8 data to MQTT: %s", data)
        except Exception as err:
            _LOGGER.error("MqttTransport: Failed to publish raw data: %s", err)

    async def write_frame(self, frame: str) -> None:
        """Write a frame to the transport.

        Required by ramses_tx.protocol which awaits this method.
        """
        if self._closing:
            _LOGGER.debug("MqttTransport: TX Frame BLOCKED (Closing) -> %s", frame)
            return
        if self._disable_sending:
            _LOGGER.debug(
                "MqttTransport: TX Frame BLOCKED (Disable sending) -> %s", frame
            )
            return

        # Wrap frame in JSON to match ramses_esp expectations.
        # Confirmed by test: Device responds to {"msg": "RQ ..."}
        try:
            json_payload = json.dumps({"msg": frame})
            _LOGGER.debug("MqttTransport: TX (frame) -> %s", json_payload)
            self._bridge.publish_tx(json_payload)

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

    def __init__(self, hass: HomeAssistant, topic_prefix: str, device_id: str) -> None:
        """Initialize the bridge."""
        self._hass = hass
        self._topic_prefix = topic_prefix.rstrip("/")
        self._device_id = device_id
        self._protocol: asyncio.Protocol | None = None
        self._transport: MqttTransport | None = None
        self._unsubscribe: Callable[[], None] | None = None
        self._unsubscribe_status: Callable[[], None] | None = None

        # Subscriptions
        self._sub_rx: Callable[[], None] | None = None
        self._sub_cmd: Callable[[], None] | None = None
        self._sub_status: Callable[[], None] | None = None

    @property
    def device_id(self) -> str:
        """Return the configured device ID."""
        return self._device_id

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

        # 1. Bind Protocol
        try:
            # ramses_rf.PortProtocol requires 'ramses=True' to accept connection
            protocol.connection_made(self._transport, ramses=True)  # type: ignore[call-arg]
        except TypeError:
            # Fallback for standard protocols (e.g. testing)
            protocol.connection_made(self._transport)

        # 2. Start Subscription - Perform subscription in the background
        self._hass.async_create_task(self._async_attach())
        # Ensure subscription is active before sending command
        await asyncio.sleep(1)

        # 3. Real Handshake: Request version from the device
        # This sends "!V" to .../cmd/cmd.
        # The response will come back on .../cmd/result and trigger the FSM.
        _LOGGER.info("MqttBridge: Requesting device version (!V) [Initial]...")
        self.publish_command("!V")

        return self._transport

    async def _async_attach(self) -> None:
        """Start listening to MQTT."""
        # Topic 1: Radio Packets (RAMSES/GATEWAY/ID/rx)
        topic_rx = f"{self._topic_prefix}/{self._device_id}/rx"
        _LOGGER.debug("MqttBridge: Starting subscription to %s", topic_rx)

        # Topic 2: Command Results (RAMSES/GATEWAY/ID/cmd/result)
        # Matches main.cpp: snprintf(cmd_result_topic, ..., "%s/cmd/result", base_topic);
        topic_cmd = f"{self._topic_prefix}/{self._device_id}/cmd/result"
        _LOGGER.debug("MqttBridge: Starting subscription to %s", topic_cmd)

        try:
            # Subscribe to RX
            self._sub_rx = await mqtt.async_subscribe(
                self._hass, topic_rx, self._handle_rx_message, qos=0
            )
            _LOGGER.info("MqttBridge: Successfully subscribed to %s", topic_rx)
            # Subscribe to Command Results
            self._sub_cmd = await mqtt.async_subscribe(
                self._hass, topic_cmd, self._handle_cmd_message, qos=0
            )
            _LOGGER.info("MqttBridge: Successfully subscribed to %s", topic_cmd)

            self._sub_status = mqtt.async_subscribe_connection_status(
                self._hass, self._handle_connection_status
            )
            _LOGGER.info("MqttBridge: Successfully subscribed to connection status")

        except Exception as err:
            _LOGGER.error("MqttBridge: Failed to subscribe to MQTT: %s", err)

    @callback
    def _handle_rx_message(self, msg: Any) -> None:
        """Process incoming radio packets."""
        if self._protocol is None:
            _LOGGER.warning("MqttBridge RX: Protocol is None, dropping message")
            return

        payload_str = self._extract_payload(msg)
        if not payload_str:
            return

        # ramses_esp wraps RX in JSON: {"msg": "..."}
        try:
            data = json.loads(payload_str)
            if isinstance(data, dict) and "msg" in data:
                raw_line = data["msg"]
                # PACKET STRUCTURE RULE (from Packet Structure Wiki):
                # The Verb field is strictly 2 characters wide.
                # - "RQ", "RP", " W" (space W), " I" (space I).
                # - We must preserve internal whitespace (e.g. "059  I") to maintain this alignment.
                # - However, we MUST strip leading/trailing garbage (newlines, nulls) to avoid parser errors.
                # ramses_rf expects a serial stream ending in exactly \r\n
                frame = raw_line.strip() + "\r\n"
                # Log exact repr() to reveal hidden characters or malformed line endings
                _LOGGER.debug("MqttBridge: RX <- %s", repr(frame))
                self._protocol.data_received(frame.encode("utf-8"))
        except json.JSONDecodeError as err:
            _LOGGER.debug("MqttBridge RX: Failed to decode JSON payload: %s", err)
        except UnicodeEncodeError as err:
            _LOGGER.error("MqttBridge RX: Encoding error in frame: %s", err)
        except Exception as err:
            _LOGGER.exception(
                "MqttBridge RX: Unexpected error processing MQTT message: %s", err
            )

    @callback
    def _handle_cmd_message(self, msg: Any) -> None:
        """Process incoming MQTT messages and inject into ramses_rf."""
        if self._protocol is None:
            _LOGGER.warning("MqttBridge CMD: Protocol is None, dropping message")
            return

        payload_str = self._extract_payload(msg)
        if not payload_str:
            return

        try:
            # Unwrap JSON if present (standard ramses_esp format)
            data = json.loads(payload_str)
            if isinstance(data, dict) and "return" in data:
                result_str = data["return"]

                # Compatibility: ramses_rf requires 'evofw3' to transition FSM
                if "ramses_esp_eth" in result_str:
                    result_str = result_str.replace("ramses_esp_eth", "evofw3")

                # Re-add the hash if missing, because ramses_rf expects "# evofw3..."
                if not result_str.strip().startswith("#"):
                    result_str = f"# {result_str}"

                # Ensure CRLF
                result_str = result_str.strip() + "\r\n"

                _LOGGER.info("MqttBridge: CMD Response <- %s", repr(result_str))

                # Feed this directly to protocol.
                # This makes ramses_rf think the serial device just answered "!V"
                self._protocol.data_received(result_str.encode("utf-8"))

        except json.JSONDecodeError as err:
            _LOGGER.debug("MqttBridge CMD: Failed to decode JSON payload: %s", err)
        except UnicodeEncodeError as err:
            _LOGGER.error("MqttBridge CMD: Encoding error in frame: %s", err)
        except Exception as err:
            _LOGGER.exception(
                "MqttBridge CMD: Unexpected error processing MQTT message: %s", err
            )

    def _extract_payload(self, msg: Any) -> str:
        """Helper to decode bytes to string."""
        if isinstance(msg.payload, bytes):
            return msg.payload.decode("utf-8", errors="ignore")
        return str(msg.payload)

    def publish_tx(self, payload: PublishPayloadType) -> None:
        """Publish a radio packet to the /tx topic."""
        # Publish to TX topic: {prefix}/{device_id}/tx
        topic = f"{self._topic_prefix}/{self._device_id}/tx"
        self._hass.async_create_task(mqtt.async_publish(self._hass, topic, payload))
        _LOGGER.debug("MqttBridge: TX -> %s, on topic: %s", payload, topic)

    def publish_command(self, payload: PublishPayloadType) -> None:
        """Publish a command to the /cmd/cmd topic."""
        # Publish to CMD topic: {prefix}/{device_id}/cmd/cmd
        topic = f"{self._topic_prefix}/{self._device_id}/cmd/cmd"
        self._hass.async_create_task(mqtt.async_publish(self._hass, topic, payload))
        _LOGGER.debug("MqttBridge: CMD -> %s, on topic: %s", payload, topic)

    @callback
    def _handle_connection_status(self, status: str) -> None:
        """Handle MQTT broker connection/disconnection."""
        _LOGGER.debug("MqttBridge: Connection status changed to %s", status)
        if status == "online":
            _LOGGER.info("MQTT Broker connected. Resuming ramses_rf.")
            # Send handshake immediately when MQTT comes online
            self.publish_command("!V")
        elif status == "offline":
            _LOGGER.warning("MQTT Broker disconnected. Pausing ramses_rf.")

    def close(self) -> None:
        """Cleanup subscriptions."""
        _LOGGER.debug("MqttBridge: Cleanup called")
        if self._sub_rx:
            self._sub_rx()
        if self._sub_cmd:
            self._sub_cmd()
        if self._sub_status:
            self._sub_status()
