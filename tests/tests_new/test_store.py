"""Tests for the storage aspect of RamsesCoordinator (Persistence)."""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from custom_components.ramses_cc.const import (
    CONF_RAMSES_RF,
    CONF_SCHEMA,
    SZ_CLIENT_STATE,
    SZ_KNOWN_LIST,
    SZ_PACKETS,
    SZ_REMOTES,
    SZ_SCHEMA,
)
from custom_components.ramses_cc.coordinator import RamsesCoordinator
from custom_components.ramses_cc.store import RamsesStore
from ramses_rf.gateway import Gateway

REM_ID = "32:111111"


# -- Part 1: Unit Tests for RamsesStore (Fixes Coverage) --


async def test_store_init(hass: HomeAssistant) -> None:
    """Test the initialization of the store."""
    with patch("custom_components.ramses_cc.store.Store") as mock_store_cls:
        store = RamsesStore(hass)
        mock_store_cls.assert_called_once()
        assert store._store is not None


async def test_store_async_load(hass: HomeAssistant) -> None:
    """Test loading data from the store."""
    store = RamsesStore(hass)
    # Mock the internal HA Store instance
    store._store = AsyncMock()

    # Case 1: Data exists
    mock_data = {"some_key": "some_value"}
    store._store.async_load.return_value = mock_data
    assert await store.async_load() == mock_data

    # Case 2: No data (None) -> Should return empty dict (Line 32 coverage)
    store._store.async_load.return_value = None
    assert await store.async_load() == {}


async def test_store_async_save(hass: HomeAssistant) -> None:
    """Test saving data to the store."""
    store = RamsesStore(hass)
    store._store = AsyncMock()

    schema = {"device_id": "123"}
    packets = {"date": "packet_data"}
    remotes = {"remote_id": "command"}

    # Execute save (Line 43-47 coverage)
    await store.async_save(schema, packets, remotes)

    expected_data = {
        SZ_CLIENT_STATE: {SZ_SCHEMA: schema, SZ_PACKETS: packets},
        SZ_REMOTES: remotes,
    }
    store._store.async_save.assert_called_once_with(expected_data)


# -- Part 2: Integration Tests for Coordinator Persistence (Existing Tests) --


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
    now: datetime = dt_util.now()
    timestamp: str = now.isoformat()
    mock_storage_data = {
        SZ_CLIENT_STATE: {
            SZ_PACKETS: {
                timestamp: "00 ... valid packet ...",
                "INVALID-DATE-STRING": "00 ... corrupted packet ...",
            }
        }
    }

    coordinator.store = MagicMock()  # Ensure store is mocked
    coordinator.store.async_load = AsyncMock(return_value=mock_storage_data)

    # Ensure _create_client returns the mock that we check later
    mock_client = MagicMock(spec=Gateway)
    mock_client.start = AsyncMock()
    coordinator._create_client = MagicMock(return_value=mock_client)

    # 3. Run async_setup
    await coordinator.async_setup()

    # 4. Yield to the event loop
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

    mock_client = MagicMock(spec=Gateway)
    mock_client.start = AsyncMock()
    coordinator._create_client = MagicMock(return_value=mock_client)

    now: datetime = dt_util.now()
    old_date = (now - timedelta(days=2)).isoformat()
    recent_date = (now - timedelta(hours=1)).isoformat()

    coordinator.options[SZ_KNOWN_LIST] = {"01:123456": {}}
    coordinator.options[CONF_RAMSES_RF] = {"enforce_known_list": True}

    padding = " " * 11
    valid_packet = f"{padding}01:123456" + (" " * 20)
    unknown_packet = f"{padding}99:999999" + (" " * 20)

    mock_storage_data = {
        SZ_CLIENT_STATE: {
            SZ_PACKETS: {
                old_date: valid_packet,
                recent_date: valid_packet,
                (now - timedelta(minutes=1)).isoformat(): unknown_packet,
            }
        }
    }
    coordinator.store = MagicMock()
    coordinator.store.async_load = AsyncMock(return_value=mock_storage_data)

    await coordinator.async_setup()

    mock_client.start.assert_called_once()
    packets = mock_client.start.call_args.kwargs["cached_packets"]

    assert recent_date in packets
    assert old_date not in packets
    assert len(packets) == 1

    await asyncio.sleep(0)
