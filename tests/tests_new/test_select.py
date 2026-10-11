"""Tests for the ramses_cc select platform (system mode, issue 1323)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError

from custom_components.ramses_cc.const import SystemMode
from custom_components.ramses_cc.select import (
    SELECT_DESCRIPTIONS,
    RamsesSystemModeSelect,
    async_setup_entry,
)
from ramses_rf.const import SZ_SYSTEM_MODE
from ramses_rf.systems.tcs import Evohome
from ramses_rf.systems.zones import Zone
from ramses_tx.exceptions import ProtocolSendFailed

TCS_ID = "01:123456"


@pytest.fixture
def mock_coordinator() -> MagicMock:
    """Return a mock RamsesCoordinator."""
    coordinator = MagicMock()
    coordinator.async_register_platform = MagicMock()
    coordinator.devices = []
    return coordinator


@pytest.fixture
def mock_tcs() -> MagicMock:
    """Return a mock Evohome (TCS) device."""
    device = MagicMock(spec=Evohome)
    device.id = TCS_ID
    device.set_mode = AsyncMock()
    return device


def _make_select(
    coordinator: MagicMock, device: MagicMock
) -> RamsesSystemModeSelect:
    """Create the select entity via the single SELECT_DESCRIPTIONS entry."""
    description = SELECT_DESCRIPTIONS[0]
    return RamsesSystemModeSelect(coordinator, device, description)


async def test_async_setup_entry_registers_callback(
    hass: HomeAssistant, mock_coordinator: MagicMock
) -> None:
    """The platform setup must register the add_devices callback."""
    entry = MagicMock()
    entry.runtime_data = mock_coordinator
    async_add_entities = MagicMock()

    with patch(
        "custom_components.ramses_cc.select.async_get_current_platform"
    ) as mock_plat:
        mock_plat.return_value = MagicMock()
        await async_setup_entry(hass, entry, async_add_entities)

    mock_coordinator.async_register_platform.assert_called_once()


async def test_add_devices_creates_select_for_evohome_only(
    hass: HomeAssistant, mock_coordinator: MagicMock, mock_tcs: MagicMock
) -> None:
    """Only Evohome-type systems get a system mode select."""
    entry = MagicMock()
    entry.runtime_data = mock_coordinator
    async_add_entities = MagicMock()

    with patch(
        "custom_components.ramses_cc.select.async_get_current_platform"
    ) as mock_plat:
        mock_plat.return_value = MagicMock()
        await async_setup_entry(hass, entry, async_add_entities)

    callback_func = mock_coordinator.async_register_platform.call_args[0][1]

    dev_zone = MagicMock(spec=Zone)
    dev_zone.id = "04:111"

    callback_func([mock_tcs, dev_zone])
    async_add_entities.assert_called_once()
    entities = async_add_entities.call_args[0][0]
    assert len(entities) == 1
    assert isinstance(entities[0], RamsesSystemModeSelect)
    assert entities[0].unique_id == f"{TCS_ID}-system_mode"


def test_current_option(
    mock_coordinator: MagicMock, mock_tcs: MagicMock
) -> None:
    """current_option must reflect the hydrated system mode."""
    select = _make_select(mock_coordinator, mock_tcs)
    select.async_write_ha_state = MagicMock()

    mock_tcs.system_mode = MagicMock(
        return_value={SZ_SYSTEM_MODE: SystemMode.AWAY}
    )
    assert select.current_option == SystemMode.AWAY

    mock_tcs.system_mode = MagicMock(return_value=None)
    assert select.current_option is None


def test_options_cover_all_system_modes(
    mock_coordinator: MagicMock, mock_tcs: MagicMock
) -> None:
    """The select must expose all evohome system modes, including those
    not mapped to climate presets (eco_boost, day_off_eco, auto_with_reset).
    """
    select = _make_select(mock_coordinator, mock_tcs)
    assert select.options == [str(mode) for mode in SystemMode]
    assert SystemMode.DAY_OFF_ECO in select.options
    assert SystemMode.RESET in select.options


async def test_select_option_sets_mode(
    mock_coordinator: MagicMock, mock_tcs: MagicMock
) -> None:
    """Selecting an option must dispatch set_mode on the TCS."""
    select = _make_select(mock_coordinator, mock_tcs)
    select.async_write_ha_state = MagicMock()

    await select.async_select_option(SystemMode.ECO_BOOST)
    mock_tcs.set_mode.assert_awaited_once_with(
        SystemMode.ECO_BOOST, until=None
    )


async def test_select_option_invalid_raises(
    mock_coordinator: MagicMock, mock_tcs: MagicMock
) -> None:
    """An invalid option must raise ServiceValidationError."""
    select = _make_select(mock_coordinator, mock_tcs)
    select.async_write_ha_state = MagicMock()

    with pytest.raises(ServiceValidationError):
        await select.async_select_option("not_a_mode")
    mock_tcs.set_mode.assert_not_awaited()


async def test_select_option_send_failure_raises(
    mock_coordinator: MagicMock, mock_tcs: MagicMock
) -> None:
    """A failed RF send must surface as HomeAssistantError."""
    mock_tcs.set_mode = AsyncMock(
        side_effect=ProtocolSendFailed("send failed")
    )
    select = _make_select(mock_coordinator, mock_tcs)
    select.async_write_ha_state = MagicMock()

    with pytest.raises(HomeAssistantError):
        await select.async_select_option(SystemMode.AWAY)
