"""Tests for ramses_cc helper utilities.

This module targets 100% coverage for helpers.py by testing device ID
mappings between Home Assistant and RAMSES RF hardware IDs.
"""

from datetime import datetime

from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (  # type: ignore[import-untyped]
    MockConfigEntry,
)

from custom_components.ramses_cc.const import DOMAIN
from custom_components.ramses_cc.helpers import (
    as_iso,
    fields_to_aware,
    ha_device_id_to_ramses_device_id,
    ramses_device_id_to_ha_device_id,
)

# Constants
RAMSES_ID = "32:153289"


def test_ha_to_ramses_id_mapping(hass: HomeAssistant) -> None:
    """Test mapping from HA registry ID to RAMSES hardware ID.

    This targets ha_device_id_to_ramses_device_id in helpers.py.

    :param hass: The Home Assistant instance.
    """
    # 1. Handle empty input
    assert ha_device_id_to_ramses_device_id(hass, "") is None

    # 2. Handle non-existent device
    assert ha_device_id_to_ramses_device_id(hass, "missing") is None

    # 3. Create a valid ConfigEntry to satisfy the DeviceRegistry requirement
    config_entry = MockConfigEntry(domain=DOMAIN, entry_id="test_config")
    config_entry.add_to_hass(hass)

    # 4. Handle valid device mapping
    dev_reg = dr.async_get(hass)
    dev_reg.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={(DOMAIN, RAMSES_ID)},
    )

    # Retrieve the HA ID created by the registry
    device = dev_reg.async_get_device(identifiers={(DOMAIN, RAMSES_ID)})
    assert device is not None

    result = ha_device_id_to_ramses_device_id(hass, device.id)
    assert result == RAMSES_ID


def test_ramses_to_ha_id_mapping(hass: HomeAssistant) -> None:
    """Test mapping from RAMSES hardware ID to HA registry ID.

    This targets ramses_device_id_to_ha_device_id in helpers.py.

    :param hass: The Home Assistant instance.
    """
    # 1. Handle empty input
    assert ramses_device_id_to_ha_device_id(hass, "") is None

    # 2. Handle non-existent hardware
    assert ramses_device_id_to_ha_device_id(hass, "99:999999") is None

    # 3. Create a valid ConfigEntry
    config_entry = MockConfigEntry(domain=DOMAIN, entry_id="test_config_2")
    config_entry.add_to_hass(hass)

    # 4. Handle valid mapping
    dev_reg = dr.async_get(hass)
    device = dev_reg.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={(DOMAIN, RAMSES_ID)},
    )

    result = ramses_device_id_to_ha_device_id(hass, RAMSES_ID)
    assert result == device.id


def test_ha_to_ramses_id_wrong_domain(hass: HomeAssistant) -> None:
    """Test mapping when the device registry entry belongs to another domain."""

    config_entry = MockConfigEntry(domain="not_ramses", entry_id="other_entry")
    config_entry.add_to_hass(hass)

    dev_reg = dr.async_get(hass)
    device = dev_reg.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={("not_ramses", "some_id")},
    )

    assert ha_device_id_to_ramses_device_id(hass, device.id) is None


def test_fields_to_aware_none() -> None:
    """Test fields_to_aware with None input (Line 67)."""
    assert fields_to_aware(None) is None


def test_fields_to_aware_parsing() -> None:
    """Test fields_to_aware with strings and invalid inputs (Lines 73-76)."""
    # Test valid ISO string
    iso_str = "2024-01-20T12:00:00"
    result = fields_to_aware(iso_str)
    assert isinstance(result, datetime)
    assert result.year == 2024

    # Test invalid string that fails parsing (Line 76)
    assert fields_to_aware("not-a-date") is None


def test_fields_to_aware_logic() -> None:
    """Test fields_to_aware logic for aware and naive datetimes (Lines 80-84)."""
    # Test already aware datetime (Line 80)
    aware_dt = dt_util.now()
    assert fields_to_aware(aware_dt) == aware_dt

    # Test naive datetime conversion (Line 84)
    naive_dt = datetime(2024, 1, 20, 12, 0, 0)
    result = fields_to_aware(naive_dt)
    assert result is not None
    assert result.tzinfo is not None
    # dt_util.as_local makes it aware based on HA's configured timezone
    assert result.hour == 12


def test_as_iso_conversion() -> None:
    """Test as_iso helper for both datetime and other types (Line 93-95)."""
    # Test datetime branch (Line 94)
    # We use a specific date to ensure the string output is predictable
    test_dt = datetime(2024, 1, 20, 12, 0, 0)
    assert as_iso(test_dt) == "2024-01-20T12:00:00"

    # Test non-datetime branch (Line 95)
    assert as_iso("already_a_string") == "already_a_string"
    assert as_iso(123) == "123"
