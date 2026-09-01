"""Unit tests for custom_components.ramses_cc.helpers."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime as dt
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (  # type: ignore[import-untyped]
    MockConfigEntry,
)

from custom_components.ramses_cc.const import DOMAIN
from custom_components.ramses_cc.helpers import (
    as_iso,
    clear_async_attr_cache,
    extract_demand,
    fields_to_aware,
    ha_device_id_to_ramses_device_id,
    parse_packet_string,
    ramses_device_id_to_ha_device_id,
    resolve_async_attr,
    resolve_demand_attr,
)

RAMSES_ID = "01:145038"


def test_ha_to_ramses_id_mapping(hass: HomeAssistant) -> None:
    """Test mapping from HA registry ID to RAMSES hardware ID."""
    # 1. Handle empty input
    assert ha_device_id_to_ramses_device_id(hass, "") is None

    # 2. Handle non-existent device
    assert ha_device_id_to_ramses_device_id(hass, "non_existent_id") is None

    # 3. Create a valid ConfigEntry
    config_entry = MockConfigEntry(domain=DOMAIN, entry_id="test_config_1")
    config_entry.add_to_hass(hass)

    # 4. Create device in registry
    dev_reg = dr.async_get(hass)
    device = dev_reg.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={(DOMAIN, RAMSES_ID)},
    )

    # 5. Verify successful mapping
    result = ha_device_id_to_ramses_device_id(hass, device.id)
    assert result == RAMSES_ID
    hass.config_entries._entries.pop(config_entry.entry_id, None)


def test_ramses_to_ha_id_mapping(hass: HomeAssistant) -> None:
    """Test mapping from RAMSES hardware ID to HA registry ID."""
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

    # 5. Verify successful mapping - deprecated  # TODO: remove Q2 2027
    result = ramses_device_id_to_ha_device_id(hass, RAMSES_ID)
    assert result == device.id

    # 6. Verify successful mapping - current
    result = ramses_device_id_to_ha_device_id(
        hass, RAMSES_ID, entry_id="test_config_2"
    )
    assert result == device.id
    hass.config_entries._entries.pop(config_entry.entry_id, None)


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
    hass.config_entries._entries.pop(config_entry.entry_id, None)


def test_fields_to_aware_none() -> None:
    """Test fields_to_aware with None input."""
    assert fields_to_aware(None) is None


def test_fields_to_aware_parsing() -> None:
    """Test fields_to_aware with strings and invalid inputs."""
    # Test valid ISO string
    iso_str = "2024-01-20T12:00:00"
    result = fields_to_aware(iso_str)
    assert isinstance(result, dt)
    assert result.year == 2024

    # Test invalid string that fails parsing
    assert fields_to_aware("not-a-date") is None


def test_fields_to_aware_logic() -> None:
    """Test fields_to_aware logic for aware and naive datetimes."""
    # Test already aware datetime
    aware_dt = dt_util.now()
    assert fields_to_aware(aware_dt) == aware_dt

    # Test naive datetime conversion
    naive_dt = dt(2024, 1, 20, 12, 0, 0)
    result = fields_to_aware(naive_dt)
    assert result is not None
    assert result.tzinfo is not None
    # dt_util.as_local makes it aware based on HA's configured timezone
    assert result.year == 2024


def test_as_iso_conversion() -> None:
    """Test as_iso helper for both datetime and string inputs."""
    # Test datetime input
    current_dt = dt_util.now()
    # as_iso strips tzinfo, so we compare to a naive version of the expected string
    assert as_iso(current_dt) == current_dt.replace(tzinfo=None).isoformat()

    # Test string input (pass-through)
    iso_str = "2024-01-01T10:00:00"
    assert as_iso(iso_str) == iso_str

    # Test None - implementation converts None to "None"
    assert as_iso(None) == "None"


def test_extract_demand_variants() -> None:
    """Test extract_demand with various object structures."""
    assert extract_demand(None) is None
    assert extract_demand(0.75) == 0.75
    assert extract_demand(1) == 1.0

    # Object with heat_demand
    obj_heat = type("HeatObj", (), {"heat_demand": 0.65})()
    assert extract_demand(obj_heat) == 0.65

    # Object with demand
    obj_demand = type("DemandObj", (), {"demand": 0.42})()
    assert extract_demand(obj_demand) == 0.42

    # Object with invalid demand
    obj_invalid = type("InvalidObj", (), {"other": "value"})()
    assert extract_demand(obj_invalid) is None


def test_resolve_demand_attr_fallback() -> None:
    """Test resolve_demand_attr with primary and fallback attributes."""
    entity = MagicMock()
    dev = MagicMock()
    dev.thermal_demand = None
    dev.heat_demand = 0.88

    res = resolve_demand_attr(entity, dev, "thermal_demand", "heat_demand")
    assert res == 0.88


def test_clear_async_attr_cache_empty_state() -> None:
    """Test clear_async_attr_cache handles entity without state_map."""
    entity = MagicMock(spec=[])
    clear_async_attr_cache(entity)  # Must not raise


def test_clear_async_attr_cache_cancels_tasks() -> None:
    """Test clear_async_attr_cache cancels in-flight resolving tasks."""
    mock_task = MagicMock()
    mock_task.done.return_value = False

    state = MagicMock()
    state.resolving_task = mock_task

    entity = MagicMock()
    entity._async_attr_state = {(123, "prop"): state}

    clear_async_attr_cache(entity)
    mock_task.cancel.assert_called_once()


def test_parse_packet_string_raw_frame() -> None:
    """Test parse_packet_string on raw RF packet frame."""
    raw_packet = "045  I --- 01:145038 --:------ 01:145038 1F09 003 0005C8"
    cmd = parse_packet_string(raw_packet)
    assert cmd is not None
    assert cmd.verb.strip() == "I"
    assert cmd.code == "1F09"

    # Invalid frame returns None
    assert parse_packet_string("invalid raw frame") is None


@pytest.mark.asyncio
async def test_resolve_async_attr_sync_and_async(hass: HomeAssistant) -> None:
    """Test resolve_async_attr helper with sync and async targets."""
    # 1. Sync value test
    entity = SimpleNamespace(hass=hass, entity_id="sensor.test")
    obj = SimpleNamespace(sync_prop="sync_val")

    val = resolve_async_attr(entity, obj, "sync_prop")
    assert val == "sync_val"
    assert not hasattr(entity, "_async_attr_state")

    # 2. Async target resolution test
    async def _async_getter() -> str:
        await asyncio.sleep(0.01)
        return "async_resolved"

    obj.async_prop = _async_getter
    res = resolve_async_attr(entity, obj, "async_prop", default="default_val")
    assert res == "default_val"
    assert hasattr(entity, "_async_attr_state")

    # Wait for background task to resolve
    state_map = entity._async_attr_state
    state = list(state_map.values())[0]
    if state.resolving_task:
        await state.resolving_task

    # Next call returns cached value
    res_cached = resolve_async_attr(entity, obj, "async_prop")
    assert res_cached == "async_resolved"


def test_fields_to_aware_and_as_iso_edge_cases() -> None:
    """Test fields_to_aware and as_iso date conversion edge cases."""
    aware_dt = dt(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    assert fields_to_aware(aware_dt) == aware_dt
    assert as_iso("2026-01-01T12:00:00") == "2026-01-01T12:00:00"
    assert as_iso(aware_dt) == "2026-01-01T12:00:00"
