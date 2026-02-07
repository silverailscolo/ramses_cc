"""Tests for the RamsesEntity base class."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from homeassistant.const import ATTR_ID
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo

from custom_components.ramses_cc.const import DOMAIN, SIGNAL_UPDATE
from custom_components.ramses_cc.entity import RamsesEntity, RamsesEntityDescription
from ramses_rf.entity_base import Entity as RamsesRFEntity

# Constants
DEVICE_ID = "32:123456"


@pytest.fixture
def mock_coordinator(hass: HomeAssistant) -> MagicMock:
    """Return a mock coordinator."""
    coordinator = MagicMock()
    coordinator.hass = hass
    coordinator._entities = {}
    return coordinator


@pytest.fixture
def mock_device() -> MagicMock:
    """Return a mock RAMSES RF entity."""
    device = MagicMock(spec=RamsesRFEntity)
    device.id = DEVICE_ID
    return device


def test_init(mock_coordinator: MagicMock, mock_device: MagicMock) -> None:
    """Test entity initialization and default attributes."""
    description = RamsesEntityDescription(key="test_key")
    entity = RamsesEntity(mock_coordinator, mock_device, description)

    assert entity.unique_id == DEVICE_ID
    assert entity.device_info == DeviceInfo(identifiers={(DOMAIN, DEVICE_ID)})
    assert entity.should_poll is False
    assert entity.has_entity_name is True


def test_extra_state_attributes_basic(
    mock_coordinator: MagicMock, mock_device: MagicMock
) -> None:
    """Test extra_state_attributes returns the device ID by default."""
    description = RamsesEntityDescription(key="test_key")
    entity = RamsesEntity(mock_coordinator, mock_device, description)

    attrs = entity.extra_state_attributes
    assert attrs == {ATTR_ID: DEVICE_ID}


def test_extra_state_attributes_with_extras(
    mock_coordinator: MagicMock, mock_device: MagicMock
) -> None:
    """Test extra_state_attributes includes mapped attributes from the device."""
    # Setup device with specific attributes
    mock_device.attribute_a = "value_a"
    mock_device.attribute_b = "value_b"

    description = RamsesEntityDescription(
        key="test_key",
        ramses_cc_extra_attributes={
            "output_a": "attribute_a",
            "output_b": "attribute_b",
            "output_missing": "attribute_missing",  # Should be ignored
        },
    )
    entity = RamsesEntity(mock_coordinator, mock_device, description)

    attrs = entity.extra_state_attributes
    assert attrs[ATTR_ID] == DEVICE_ID
    assert attrs["output_a"] == "value_a"
    assert attrs["output_b"] == "value_b"
    assert "output_missing" not in attrs


async def test_async_added_to_hass(
    hass: HomeAssistant, mock_coordinator: MagicMock, mock_device: MagicMock
) -> None:
    """Test lifecycle hook when entity is added to Home Assistant."""
    description = RamsesEntityDescription(key="test_key")
    entity = RamsesEntity(mock_coordinator, mock_device, description)
    entity.hass = hass

    # Mock the dispatcher connect function to verify subscription
    with (
        patch(
            "custom_components.ramses_cc.entity.async_dispatcher_connect"
        ) as mock_connect,
        patch.object(entity, "async_on_remove") as mock_on_remove,
    ):
        await entity.async_added_to_hass()

        # 1. Verify entity is registered in the coordinator
        assert mock_coordinator._entities[DEVICE_ID] == entity

        # 2. Verify signal listener is attached
        expected_signal = f"{SIGNAL_UPDATE}_{DEVICE_ID}"
        mock_connect.assert_called_once_with(
            hass, expected_signal, entity.async_write_ha_state
        )

        # 3. Verify the signal listener cleanup is registered
        # CoordinatorEntity also calls async_on_remove, so we check using assert_any_call
        mock_on_remove.assert_any_call(mock_connect.return_value)
