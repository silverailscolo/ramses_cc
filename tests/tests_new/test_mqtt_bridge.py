"""Tests for the RamsesMqttBridge and MqttTransport."""

import asyncio
import logging
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant

from custom_components.ramses_cc.mqtt_bridge import MqttTransport, RamsesMqttBridge


@pytest.fixture
def mock_protocol() -> MagicMock:
    """Mock an asyncio.Protocol."""
    return MagicMock(spec=asyncio.Protocol)


@pytest.fixture
def mock_mqtt(hass: HomeAssistant) -> Generator[dict[str, AsyncMock]]:
    """Mock the HA MQTT integration methods."""
    with (
        patch(
            "homeassistant.components.mqtt.async_subscribe", new_callable=AsyncMock
        ) as mock_sub,
        patch(
            "homeassistant.components.mqtt.async_subscribe_connection_status",
            new_callable=AsyncMock,
        ) as mock_conn_status,
        patch(
            "homeassistant.components.mqtt.async_publish", new_callable=AsyncMock
        ) as mock_pub,
    ):
        # Mock subscribe to return a simple unsubscribe callback (not async)
        mock_sub.return_value = MagicMock()
        mock_conn_status.return_value = MagicMock()
        yield {
            "subscribe": mock_sub,
            "connection_status": mock_conn_status,
            "publish": mock_pub,
        }


async def test_mqtt_transport_write_valid(hass: HomeAssistant) -> None:
    """Test MqttTransport writes valid utf-8 data to the bridge."""
    bridge = MagicMock(spec=RamsesMqttBridge)
    protocol = MagicMock(spec=asyncio.Protocol)
    transport = MqttTransport(bridge, protocol, None)

    # Valid string as bytes
    data = b"Hello MQTT"
    transport.write(data)

    bridge.publish.assert_called_once_with("Hello MQTT")
    assert not transport._closing


async def test_mqtt_transport_write_invalid_utf8(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Test MqttTransport handles invalid utf-8 data gracefully."""
    bridge = MagicMock(spec=RamsesMqttBridge)
    protocol = MagicMock(spec=asyncio.Protocol)
    transport = MqttTransport(bridge, protocol, None)

    # Invalid utf-8 sequence (0xFF)
    data = b"\xff\xff"

    with caplog.at_level(logging.WARNING):
        transport.write(data)

    bridge.publish.assert_not_called()
    assert "Attempted to publish non-utf8 data" in caplog.text


async def test_mqtt_transport_close_and_abort(hass: HomeAssistant) -> None:
    """Test MqttTransport close and abort methods."""
    bridge = MagicMock(spec=RamsesMqttBridge)
    protocol = MagicMock(spec=asyncio.Protocol)
    transport = MqttTransport(bridge, protocol, None)

    # Test close
    transport.close()
    assert transport._closing
    protocol.connection_lost.assert_called_once_with(None)

    # Reset
    transport._closing = False
    protocol.connection_lost.reset_mock()

    # Test abort (should call close)
    transport.abort()
    assert transport._closing
    protocol.connection_lost.assert_called_once_with(None)

    # Verify write does nothing when closing
    bridge.publish.reset_mock()
    transport.write(b"data")
    bridge.publish.assert_not_called()


async def test_bridge_factory_and_attach(
    hass: HomeAssistant, mock_mqtt: dict[str, AsyncMock], mock_protocol: MagicMock
) -> None:
    """Test RamsesMqttBridge transport factory and mqtt attachment."""
    bridge = RamsesMqttBridge(hass, "ramses_cc")

    # Run the factory
    transport = await bridge.async_transport_factory(mock_protocol)

    assert isinstance(transport, MqttTransport)
    assert bridge._protocol == mock_protocol

    # Check MQTT subscriptions
    mock_mqtt["subscribe"].assert_called_once()
    assert mock_mqtt["subscribe"].call_args[0][1] == "ramses_cc/#"

    mock_mqtt["connection_status"].assert_called_once()

    # Check Protocol connection made
    mock_protocol.connection_made.assert_called_once_with(transport)


async def test_bridge_handle_mqtt_message(
    hass: HomeAssistant, mock_mqtt: dict[str, AsyncMock], mock_protocol: MagicMock
) -> None:
    """Test handling incoming MQTT messages."""
    bridge = RamsesMqttBridge(hass, "ramses_cc")
    # Manually set protocol as if factory was called
    bridge._protocol = mock_protocol

    # Retrieve the message callback from the subscribe call
    await bridge._async_attach()
    msg_callback = mock_mqtt["subscribe"].call_args[0][2]

    # Test 1: String Payload
    msg_str = MagicMock()
    msg_str.payload = "some command"
    msg_callback(msg_str)
    mock_protocol.data_received.assert_called_with(b"some command")

    # Test 2: Bytes Payload
    msg_bytes = MagicMock()
    msg_bytes.payload = b"raw bytes"
    msg_callback(msg_bytes)
    mock_protocol.data_received.assert_called_with(b"raw bytes")

    # Test 3: Exception in protocol (should be caught and logged)
    mock_protocol.data_received.side_effect = Exception("Boom")
    # Should not raise
    msg_callback(msg_str)


async def test_bridge_handle_mqtt_message_no_protocol(
    hass: HomeAssistant, mock_mqtt: dict[str, AsyncMock]
) -> None:
    """Test MQTT message ignored if protocol not ready."""
    bridge = RamsesMqttBridge(hass, "ramses_cc")
    bridge._protocol = None  # Ensure None

    # We need to simulate the callback execution logic without calling _async_attach
    # or just call the method directly since it is what we are testing.
    msg = MagicMock()
    msg.payload = "test"

    # Should safely return without error
    bridge._handle_mqtt_message(msg)


async def test_bridge_connection_status_logging(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Test connection status changes are logged."""
    bridge = RamsesMqttBridge(hass, "ramses_cc")

    # We need to get the callback ref. Using a mock setup for _async_attach
    with (
        patch("homeassistant.components.mqtt.async_subscribe", new_callable=AsyncMock),
        patch(
            "homeassistant.components.mqtt.async_subscribe_connection_status",
            new_callable=AsyncMock,
        ) as mock_status_sub,
    ):
        await bridge._async_attach()
        status_callback = mock_status_sub.call_args[0][1]

        with caplog.at_level(logging.INFO):
            status_callback("online")
            assert "MQTT Broker connected" in caplog.text

        with caplog.at_level(logging.WARNING):
            status_callback("offline")
            assert "MQTT Broker disconnected" in caplog.text


async def test_bridge_publish(
    hass: HomeAssistant, mock_mqtt: dict[str, AsyncMock]
) -> None:
    """Test publishing logic."""
    bridge = RamsesMqttBridge(hass, "ramses_cc")

    bridge.publish("test_payload")

    # Process tasks to ensure async_create_task runs
    await hass.async_block_till_done()

    mock_mqtt["publish"].assert_called_once()
    args = mock_mqtt["publish"].call_args[0]
    # args: (hass, topic, payload)
    assert args[1] == "ramses_cc/tx"
    assert args[2] == "test_payload"


async def test_bridge_close(
    hass: HomeAssistant, mock_mqtt: dict[str, AsyncMock]
) -> None:
    """Test bridge cleanup."""
    bridge = RamsesMqttBridge(hass, "ramses_cc")

    # Attach to set up unsubscribe
    await bridge._async_attach()
    mock_unsub = mock_mqtt["subscribe"].return_value

    assert bridge._unsubscribe is not None

    bridge.close()

    mock_unsub.assert_called_once()
    assert bridge._unsubscribe is None

    # Calling close again should be safe
    bridge.close()
