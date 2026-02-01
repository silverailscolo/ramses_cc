"""Tests for the RamsesMqttBridge."""

import asyncio
import json
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import (  # type: ignore[import-untyped]
    MockConfigEntry,
)

from custom_components.ramses_cc.config_flow import SZ_PORT_NAME, SZ_SERIAL_PORT
from custom_components.ramses_cc.const import (
    CONF_MQTT_HGI_ID,
    CONF_MQTT_USE_HA,
    CONF_RAMSES_RF,
    DOMAIN,
)
from custom_components.ramses_cc.mqtt_bridge import RamsesMqttBridge

TEST_DEVICE_ID = "18:123456"


@pytest.fixture
def mock_protocol() -> MagicMock:
    """Mock an asyncio.Protocol."""
    return MagicMock(spec=asyncio.Protocol)


@pytest.fixture
def mock_mqtt(hass: HomeAssistant) -> Iterator[dict[str, Any]]:
    """Mock the HA MQTT integration methods."""
    with (
        patch(
            "homeassistant.components.mqtt.async_subscribe", new_callable=AsyncMock
        ) as mock_sub,
        patch(
            "homeassistant.components.mqtt.async_subscribe_connection_status",
            new_callable=MagicMock,
        ) as mock_conn_status,
        patch(
            "homeassistant.components.mqtt.async_publish", new_callable=AsyncMock
        ) as mock_pub,
    ):
        # FIX: Ensure async_subscribe returns a synchronous mock (the unsubscribe callback)
        # This prevents "coroutine never awaited" warnings when close() calls it.
        mock_sub.return_value = MagicMock()

        yield {
            "subscribe": mock_sub,
            "connection_status": mock_conn_status,
            "publish": mock_pub,
        }


async def test_bridge_init(hass: HomeAssistant) -> None:
    """Test the bridge initialization."""
    bridge = RamsesMqttBridge(hass, "RAMSES/GATEWAY", TEST_DEVICE_ID)
    assert bridge.device_id == TEST_DEVICE_ID


async def test_bridge_flow(
    hass: HomeAssistant, mock_mqtt: dict[str, Any], mock_protocol: MagicMock
) -> None:
    """Test the full flow: Factory -> Subscribe -> Rx -> Tx."""

    # 1. Setup Mock MQTT Config Entry
    mqtt_entry = MockConfigEntry(domain="mqtt", data={"broker": "mock_broker"})
    mqtt_entry.add_to_hass(hass)

    # 2. Setup Ramses Config Entry
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="central_controller",
        data={},
        options={
            CONF_RAMSES_RF: {
                "enforce_known_list": True,
            },
            SZ_SERIAL_PORT: {
                SZ_PORT_NAME: "mqtt://mqtt_host:1883",
            },
            CONF_MQTT_USE_HA: True,
            CONF_MQTT_HGI_ID: TEST_DEVICE_ID,
        },
    )
    entry.add_to_hass(hass)

    # 3. Mock classes
    # CRITICAL: We patch where the classes are IMPORTED/USED in mqtt_bridge.py.
    # Since mqtt_bridge.py does 'from ramses_tx.transport import CallbackTransport',
    # we must patch 'custom_components.ramses_cc.mqtt_bridge.CallbackTransport'.
    with (
        patch(
            "custom_components.ramses_cc.coordinator.MqttGateway"
        ) as mock_gateway_cls,
        patch(
            "custom_components.ramses_cc.mqtt_bridge.CallbackTransport"
        ) as mock_transport_cls,
    ):
        # 4. Setup the Gateway Mock
        mock_gateway = mock_gateway_cls.return_value
        mock_gateway.start = AsyncMock()
        mock_gateway.get_state.return_value = ({}, {})

        # Setup the Transport Mock
        mock_transport = mock_transport_cls.return_value

        # 5. Initialize the Integration
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        # 6. Get the active coordinator and bridge
        coordinator = hass.data[DOMAIN][entry.entry_id]
        bridge = coordinator.mqtt_bridge
        assert bridge is not None

        # 7. Simulate the wiring
        transport = await bridge.async_transport_factory(mock_protocol)

        # Verify the transport was created
        assert transport == mock_transport
        mock_transport_cls.assert_called_once()

        # 8. Verify Subscriptions
        mock_mqtt["subscribe"].assert_any_call(
            hass,
            f"RAMSES/GATEWAY/{TEST_DEVICE_ID}/rx",
            bridge._handle_rx_message,
            qos=0,
        )
        mock_mqtt["subscribe"].assert_any_call(
            hass,
            f"RAMSES/GATEWAY/{TEST_DEVICE_ID}/cmd/result",
            bridge._handle_cmd_message,
            qos=0,
        )

        # 9. Test INBOUND (MQTT -> Transport)
        rx_call = next(
            call
            for call in mock_mqtt["subscribe"].call_args_list
            if call[0][1].endswith("/rx")
        )
        rx_callback = rx_call[0][2]

        # Simulate an incoming MQTT message
        msg = MagicMock()
        msg.payload = json.dumps(
            {"msg": "RQ --- 18:123456 01:000000 --:------ 0005 002 0000"}
        )

        rx_callback(msg)

        # Verify it was unwrapped and passed to the transport
        expected_frame = "RQ --- 18:123456 01:000000 --:------ 0005 002 0000\r\n"
        mock_transport.receive_frame.assert_called_with(expected_frame)

        # 10. Test OUTBOUND (Transport Writer -> MQTT)
        call_kwargs = mock_transport_cls.call_args[1]
        io_writer = call_kwargs["io_writer"]

        # A. Test TX Packet
        tx_frame = "RP --- 01:000000 18:123456 --:------ 0005 002 0000"
        await io_writer(tx_frame)

        expected_topic_tx = f"RAMSES/GATEWAY/{TEST_DEVICE_ID}/tx"
        expected_payload_tx = json.dumps({"msg": tx_frame})
        mock_mqtt["publish"].assert_called_with(
            hass, expected_topic_tx, expected_payload_tx
        )

        # B. Test Command
        cmd_frame = "!V"
        await io_writer(cmd_frame)

        expected_topic_cmd = f"RAMSES/GATEWAY/{TEST_DEVICE_ID}/cmd/cmd"
        mock_mqtt["publish"].assert_called_with(hass, expected_topic_cmd, cmd_frame)
