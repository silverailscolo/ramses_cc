"""Tests for the storage aspect of RamsesCoordinator (Persistence)."""

import asyncio
from datetime import datetime as dt, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from homeassistant.core import HomeAssistant

from custom_components.ramses_cc.const import (
    CONF_RAMSES_RF,
    CONF_SCHEMA,
    SZ_CLIENT_STATE,
    SZ_KNOWN_LIST,
    SZ_PACKETS,
)
from custom_components.ramses_cc.coordinator import RamsesCoordinator
from ramses_rf.gateway import Gateway

REM_ID = "32:111111"


@pytest.fixture
def mock_hass(event_loop: asyncio.AbstractEventLoop) -> MagicMock:
    """Return a mock Home Assistant instance."""
    hass = MagicMock()
    hass.loop = event_loop  # Use the actual test event loop

    # Use an AsyncMock for async_create_task so it's awaited correctly by HA
    hass.async_create_task = MagicMock(
        side_effect=lambda coro: event_loop.create_task(coro)
    )

    return hass


@pytest.fixture
def mock_entry() -> MagicMock:
    """Return a mock ConfigEntry."""
    entry = MagicMock()
    entry.entry_id = "test_entry"
    entry.options = {
        SZ_KNOWN_LIST: {},
        CONF_SCHEMA: {},
        CONF_RAMSES_RF: {},
        "serial_port": "/dev/ttyUSB0",
    }
    entry.async_on_unload = MagicMock()
    return entry


@pytest.fixture
def mock_coordinator(hass: HomeAssistant, mock_entry: MagicMock) -> RamsesCoordinator:
    """Return a mock coordinator for storage tests."""
    # We use the real RamsesCoordinator but mock its internal store/client
    coordinator = RamsesCoordinator(hass, mock_entry)

    # Mock the store for persistence tests
    coordinator.store = MagicMock()
    coordinator.store.async_load = AsyncMock()
    coordinator.store.async_save = AsyncMock()

    # Pre-set the client as a MagicMock with Gateway spec
    coordinator.client = MagicMock(spec=Gateway)
    coordinator.client.start = AsyncMock()
    return coordinator


async def test_setup_with_corrupted_storage_dates(
    hass: HomeAssistant, mock_entry: MagicMock
) -> None:
    """Test that startup survives invalid date strings in storage."""
    # 1. Setup Coordinator
    coordinator = RamsesCoordinator(hass, mock_entry)

    # 2. Mock Storage with corrupted date
    # Valid date: 2023-01-01T12:00:00
    # Invalid date: "INVALID-DATE-STRING"
    mock_storage_data = {
        SZ_CLIENT_STATE: {
            SZ_PACKETS: {
                dt.now().isoformat(): "00 ... valid packet ...",
                "INVALID-DATE-STRING": "00 ... corrupted packet ...",
            }
        }
    }

    coordinator.store.async_load = AsyncMock(return_value=mock_storage_data)

    # Ensure _create_client returns the mock that we check later
    mock_client = MagicMock(spec=Gateway)
    mock_client.start = AsyncMock()
    coordinator._create_client = MagicMock(return_value=mock_client)

    # 3. Run async_setup
    # This should NOT raise ValueError
    await coordinator.async_setup()

    # 4. Yield to the event loop to allow AsyncMock
    # internal coroutines to complete before the test ends.
    await asyncio.sleep(0)

    # 5. Verify client started
    assert mock_client.start.called

    # 6. Verify only valid packet was passed to start
    call_args = mock_client.start.call_args
    cached_packets = call_args.kwargs.get("cached_packets", {})

    assert len(cached_packets) == 1
    assert "INVALID-DATE-STRING" not in cached_packets
    await asyncio.sleep(0)


async def test_save_client_state_remotes(mock_coordinator: RamsesCoordinator) -> None:
    """Test saving remote commands to persistent storage."""
    mock_coordinator.client.get_state.return_value = ({}, {})
    mock_coordinator._remotes = {REM_ID: {"boost": "packet_data"}}

    # Reset mocks to clear any setup calls
    mock_coordinator.store.async_save.reset_mock()

    await mock_coordinator.async_save_client_state()

    # Verify remotes were included in the save payload
    assert mock_coordinator.store.async_save.called
    args = mock_coordinator.store.async_save.call_args[0]
    saved_remotes = args[2]

    assert saved_remotes == mock_coordinator._remotes


async def test_setup_packet_filtering(
    hass: HomeAssistant, mock_entry: MagicMock
) -> None:
    """Test logic for filtering cached packets based on age and known list."""
    coordinator = RamsesCoordinator(hass, mock_entry)

    # Wire up mock_client to be returned by _create_client
    mock_client = MagicMock(spec=Gateway)
    mock_client.start = AsyncMock()
    coordinator._create_client = MagicMock(return_value=mock_client)

    now = dt.now()
    old_date = (now - timedelta(days=2)).isoformat()
    recent_date = (now - timedelta(hours=1)).isoformat()

    # Known list contains a device 01:123456
    coordinator.options[SZ_KNOWN_LIST] = {"01:123456": {}}
    coordinator.options[CONF_RAMSES_RF] = {"enforce_known_list": True}

    # Helper to construct a packet where ID matches [11:20]
    # Indices 0-10 (11 chars) are padding.
    # [11:20] is 9 chars -> "01:123456"
    padding = " " * 11
    valid_packet = f"{padding}01:123456" + (" " * 20)
    unknown_packet = f"{padding}99:999999" + (" " * 20)

    mock_storage_data = {
        SZ_CLIENT_STATE: {
            SZ_PACKETS: {
                old_date: valid_packet,  # Too old
                recent_date: valid_packet,  # Good
                (now - timedelta(minutes=1)).isoformat(): unknown_packet,  # Unknown
            }
        }
    }
    coordinator.store.async_load = AsyncMock(return_value=mock_storage_data)

    await coordinator.async_setup()

    # Check which packets survived
    mock_client.start.assert_called_once()
    packets = mock_client.start.call_args.kwargs["cached_packets"]

    # Verify recent known packet is present
    assert recent_date in packets
    # Verify old packet is gone
    assert old_date not in packets
    # Verify unknown device packet is gone
    # Note: unknown_packet timestamp key was dynamically generated, so we check count
    assert len(packets) == 1

    # Ensure the event loop has processed all mock callbacks
    await asyncio.sleep(0)
