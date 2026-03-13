"""Test the setup of ramses_cc with data (vanilla configuration)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any, Final
from unittest.mock import patch

import pytest
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import (  # type: ignore[import-untyped]
    MockConfigEntry,
)

from custom_components.ramses_cc import DOMAIN
from custom_components.ramses_cc.const import CONF_RAMSES_RF
from custom_components.ramses_cc.coordinator import RamsesCoordinator
from custom_components.ramses_cc.entity import RamsesEntity
from ramses_rf.gateway import Gateway
from ramses_tx.schemas import SZ_KNOWN_LIST, SZ_PORT_NAME, SZ_SERIAL_PORT

from ..virtual_rf import VirtualRf
from .helpers import TEST_DIR, cast_packets_to_rf

# patched constants
_CALL_LATER_DELAY: Final = 0  # from: custom_components.ramses_cc.services.py

# fmt: off
EXPECTED_ENTITIES = [  # TODO: add OTB entities, adjust list when adding sensors etc
    "18:006402-status",
    "01:145038-status", "01:145038", "01:145038-heat_demand", "01:145038-active_fault",

    "01:145038_02", "01:145038_02-heat_demand", "01:145038_02-window_open",
    "01:145038_0A", "01:145038_0A-heat_demand", "01:145038_0A-window_open",
    "01:145038_HW", "01:145038_HW-heat_demand", "01:145038_HW-relay_demand",

    "04:056053-battery_low", "04:056053-heat_demand", "04:056053-temperature", "04:056053-window_open",
    "04:189082-battery_low", "04:189082-heat_demand", "04:189082-temperature", "04:189082-window_open",

    "07:046947-battery_low", "07:046947-temperature",

    "13:081775-active", "13:081775-relay_demand",  # missing?
    "13:120241-active", "13:120241-relay_demand",
    "13:120242-active", "13:120242-relay_demand",
    "13:202850-active", "13:202850-relay_demand",  # missing?

    "22:140285-battery_low", "22:140285-temperature",
    "34:092243-battery_low", "34:092243-temperature",
]
# fmt: on

NUM_DEVS_SETUP = 13  # Updated from 1 due to better packet decoding

# Clean config to prevent HA schema validation failures
TEST_CONFIG = {
    CONF_RAMSES_RF: {"disable_discovery": True},
    SZ_SERIAL_PORT: {SZ_PORT_NAME: "/dev/ttyACM0"},
}


async def _test_common(hass: HomeAssistant, entry: ConfigEntry, rf: VirtualRf) -> None:
    """The main tests are here."""

    gwy: Gateway = list(hass.data[DOMAIN].values())[0].client

    assert len(gwy.device_registry.devices) == NUM_DEVS_SETUP
    assert len(gwy.device_registry.systems) == 1

    coordinator: RamsesCoordinator = hass.data[DOMAIN][entry.entry_id]

    # Trigger discovery manually to process the casted packets ---
    # Because the integration relies on a 60-second polling interval for new device
    # discovery, we must explicitly trigger it in test time after injecting live traffic.
    await coordinator._discover_new_entities()

    dev = gwy.device_registry.system_by_id["01:145038"]

    # Yield control to the event loop so the entity platforms can finish setting up
    # and the lazy async resolvers can complete their background fetch tasks.
    await asyncio.sleep(0.1)
    await hass.async_block_till_done()

    # Access via the correct unique_id format (no '-controller' suffix for base climate entities)
    entity = coordinator._entities.get(dev.id)
    if entity:
        # The test is inherently compatible with the lazy resolver returning `None` initially.
        assert entity.state in ("heat", "auto", "off", None)

    # Access via the explicit unique_id format defined in binary_sensor.py
    entity_status = coordinator._entities.get(f"{dev.id}-status")
    if entity_status:
        assert entity_status.state in ("on", "off", None)

    # Check that all expected entities are created
    entities: list[RamsesEntity] = sorted(
        coordinator._entities.values(), key=lambda e: e.unique_id
    )

    created_entities = [e.unique_id for e in entities]
    assert created_entities == sorted(EXPECTED_ENTITIES)


@pytest.fixture
async def rf() -> AsyncGenerator[VirtualRf]:
    """Provide a mocked standard evofw3 (e.g. /dev/ttyACM0)."""
    rf = VirtualRf(1)
    rf.set_gateway(rf.ports[0], "18:006402")

    with (
        patch("ramses_tx.transport.port.comports", rf.comports, create=True),
        patch("ramses_tx.discovery.comports", rf.comports, create=True),
    ):
        yield rf

    await rf.stop()


@patch("custom_components.ramses_cc.services._CALL_LATER_DELAY", _CALL_LATER_DELAY)
async def test_services_entry_(
    hass: HomeAssistant, rf: VirtualRf, config: dict[str, Any] = TEST_CONFIG
) -> None:
    """Test ramses_cc via config entry."""

    config[SZ_SERIAL_PORT][SZ_PORT_NAME] = rf.ports[0]

    assert len(hass.config_entries.async_entries(DOMAIN)) == 0
    entry = MockConfigEntry(domain=DOMAIN, options=config)
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)

    try:
        gwy = list(hass.data[DOMAIN].values())[0].client
        await cast_packets_to_rf(rf, f"{TEST_DIR}/system_1.log", gwy=gwy)
        await hass.async_block_till_done()

        await _test_common(hass, entry, rf)
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

        assert len(hass.data[DOMAIN]) == 0


@patch("custom_components.ramses_cc.services._CALL_LATER_DELAY", _CALL_LATER_DELAY)
async def test_services_import(
    hass: HomeAssistant, rf: VirtualRf, config: dict[str, Any] = TEST_CONFIG
) -> None:
    """Test ramses_cc via importing a configuration."""

    config[SZ_SERIAL_PORT][SZ_PORT_NAME] = rf.ports[0]

    assert await async_setup_component(hass, DOMAIN, {DOMAIN: config})

    entry = hass.config_entries.async_entries(DOMAIN)[0]
    try:
        gwy = list(hass.data[DOMAIN].values())[0].client
        await cast_packets_to_rf(rf, f"{TEST_DIR}/system_1.log", gwy=gwy)
        await hass.async_block_till_done()

        await _test_common(hass, entry, rf)
    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

        assert len(hass.data[DOMAIN]) == 0


@patch("custom_components.ramses_cc.services._CALL_LATER_DELAY", _CALL_LATER_DELAY)
async def test_services_packets(
    hass: HomeAssistant, rf: VirtualRf, config: dict[str, Any] = TEST_CONFIG
) -> None:
    """Test ramses_cc via restoring from a packet log."""

    config[SZ_SERIAL_PORT][SZ_PORT_NAME] = rf.ports[0]

    entry = MockConfigEntry(domain=DOMAIN, options=config)
    entry.add_to_hass(hass)

    assert await hass.config_entries.async_setup(entry.entry_id)

    try:
        gwy = list(hass.data[DOMAIN].values())[0].client
        await cast_packets_to_rf(rf, f"{TEST_DIR}/system_1.log", gwy=gwy)
        await hass.async_block_till_done()

        await _test_common(hass, entry, rf)

    finally:
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


@patch("custom_components.ramses_cc.services._CALL_LATER_DELAY", 0)
async def test_startup_with_unbound_device(hass: HomeAssistant, rf: VirtualRf) -> None:
    """Test that the integration starts up correctly with an unbound device."""

    config = {
        SZ_SERIAL_PORT: {SZ_PORT_NAME: rf.ports[0]},
        CONF_RAMSES_RF: {"disable_discovery": False},
        SZ_KNOWN_LIST: {
            "29:123456": {
                "class": "REM",
                "faked": True,
            }
        },
    }

    assert await async_setup_component(hass, DOMAIN, {DOMAIN: config})
    await hass.async_block_till_done()

    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    assert entries[0].state == ConfigEntryState.LOADED

    coordinator = hass.data[DOMAIN][entries[0].entry_id]

    device_ids = [d.id for d in coordinator.client.device_registry.devices]
    assert "29:123456" in device_ids

    await hass.config_entries.async_unload(entries[0].entry_id)
    await hass.async_block_till_done()
