"""Tests for the extraction and sanitisation of the known_list."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock, patch

from homeassistant.core import HomeAssistant

from custom_components.ramses_cc.const import (
    CONF_COMMANDS,
    CONF_MQTT_HGI_ID,
    CONF_MQTT_USE_HA,
    CONF_RAMSES_RF,
    SZ_KNOWN_LIST,
    SZ_PORT_NAME,
    SZ_SERIAL_PORT,
)
from custom_components.ramses_cc.coordinator import RamsesCoordinator
from ramses_rf.gateway import GatewayConfig


@patch("custom_components.ramses_cc.coordinator.Gateway")
async def test_known_list_sanitised_for_serial(
    mock_gateway_class: MagicMock, hass: HomeAssistant
) -> None:
    """Test commands are stripped before passing to ramses_rf via serial.

    :param mock_gateway_class: Mock for the ramses_rf Gateway class.
    :type mock_gateway_class: MagicMock
    :param hass: The Home Assistant instance fixture.
    :type hass: HomeAssistant
    """
    # Arrange
    mock_entry = MagicMock()
    mock_entry.options = {
        CONF_RAMSES_RF: {},
        SZ_SERIAL_PORT: {SZ_PORT_NAME: "/dev/ttyUSB0"},
        SZ_KNOWN_LIST: {
            "01:123456": {"class": "controller", CONF_COMMANDS: {"ping": "A"}},
            "04:654321": {"class": "TRV", "faked": True},
        },
    }

    coordinator = RamsesCoordinator(hass, mock_entry)

    # Act
    coordinator._create_client({})

    # Assert
    mock_gateway_class.assert_called_once()
    call_kwargs = cast(dict[str, Any], mock_gateway_class.call_args.kwargs)
    gwy_config = cast(GatewayConfig, call_kwargs["config"])

    passed_known_list = gwy_config.known_list

    # Ensure the commands are stripped to prevent schema validation errors
    assert "01:123456" in passed_known_list
    assert CONF_COMMANDS not in passed_known_list["01:123456"]
    assert passed_known_list["01:123456"]["class"] == "CTL"

    # Ensure other valid traits remain intact
    assert "04:654321" in passed_known_list
    assert passed_known_list["04:654321"]["faked"] is True


@patch("custom_components.ramses_cc.coordinator.Gateway")
async def test_known_list_mqtt_hgi_injection(
    mock_gateway_class: MagicMock, hass: HomeAssistant
) -> None:
    """Test HGI details are injected into known_list when using MQTT.

    :param mock_gateway_class: Mock for the ramses_rf Gateway class.
    :type mock_gateway_class: MagicMock
    :param hass: The Home Assistant instance fixture.
    :type hass: HomeAssistant
    """
    # Arrange
    mock_entry = MagicMock()
    mock_entry.options = {
        CONF_RAMSES_RF: {},
        SZ_SERIAL_PORT: {SZ_PORT_NAME: "mqtt_ha"},
        CONF_MQTT_USE_HA: True,
        CONF_MQTT_HGI_ID: "18:111111",
        SZ_KNOWN_LIST: {
            "01:123456": {"class": "controller"},
        },
    }

    with patch.object(
        hass.config_entries, "async_entries", return_value=["mock_mqtt_entry"]
    ):
        coordinator = RamsesCoordinator(hass, mock_entry)

        # Act
        coordinator._create_client({})

    # Assert
    mock_gateway_class.assert_called_once()
    call_kwargs = cast(dict[str, Any], mock_gateway_class.call_args.kwargs)
    gwy_config = cast(GatewayConfig, call_kwargs["config"])

    passed_known_list = gwy_config.known_list

    # Verify original devices exist
    assert "01:123456" in passed_known_list

    # Verify the HGI has been correctly injected for MQTT operations
    assert "18:111111" in passed_known_list
    hgi_traits = passed_known_list["18:111111"]
    assert hgi_traits["class"] == "HGI"
    assert hgi_traits["alias"] == "ramses_esp"
