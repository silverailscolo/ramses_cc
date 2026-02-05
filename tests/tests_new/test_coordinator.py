"""Tests for the coordinator aspect of RamsesCoordinator (Lifecycle, Config, Updates)."""

import asyncio
from datetime import datetime
from typing import Any
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

import pytest
import voluptuous as vol  # type: ignore[import-untyped, unused-ignore]
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_SCAN_INTERVAL, Platform
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.util import dt as dt_util

from custom_components.ramses_cc.const import (
    CONF_MQTT_USE_HA,
    CONF_RAMSES_RF,
    CONF_SCHEMA,
    DEFAULT_HGI_ID,
    DEFAULT_MQTT_TOPIC,
    DOMAIN,
    SIGNAL_NEW_DEVICES,
)
from custom_components.ramses_cc.coordinator import (
    SZ_CLIENT_STATE,
    SZ_PACKETS,
    SZ_SCHEMA,
    MqttGateway,
    RamsesCoordinator,
)
from ramses_rf import Gateway
from ramses_rf.system import Evohome
from ramses_tx.schemas import SZ_KNOWN_LIST, SZ_PORT_NAME, SZ_SERIAL_PORT

# Constants
FAN_ID = "30:111222"
REM_ID = "32:111111"


@pytest.fixture
def mock_hass() -> MagicMock:
    """Return a mock Home Assistant instance."""
    hass = MagicMock()
    hass.loop = MagicMock()
    hass.config_entries = MagicMock()
    hass.config_entries.async_get_entry = MagicMock(return_value=None)

    # Ensure these methods are AsyncMocks
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    hass.config_entries.async_forward_entry_unload = AsyncMock(return_value=True)

    # async_create_task must return an awaitable (Future).
    # CRITICAL: It must also 'close' the coro passed to it to prevent RuntimeWarnings.
    def _create_task(coro: Any) -> asyncio.Future[Any]:
        coro.close()  # Prevent "coroutine '...' was never awaited" warning
        f: asyncio.Future[Any] = asyncio.Future()
        f.set_result(None)
        return f

    hass.async_create_task = MagicMock(side_effect=_create_task)
    return hass


@pytest.fixture
def mock_entry(mock_hass: MagicMock) -> MagicMock:
    """Return a mock ConfigEntry."""
    entry = MagicMock()
    entry.entry_id = "test_entry"
    entry.options = {
        SZ_KNOWN_LIST: {},
        CONF_SCHEMA: {},
        CONF_RAMSES_RF: {},
        SZ_SERIAL_PORT: {SZ_PORT_NAME: "/dev/ttyUSB0"},
        CONF_SCAN_INTERVAL: 60,
    }
    entry.async_on_unload = MagicMock()

    # Register this entry with the mock hass instance
    mock_hass.config_entries.async_get_entry.side_effect = lambda eid: (
        entry if eid == entry.entry_id else None
    )

    return entry


@pytest.fixture
def mock_coordinator(mock_hass: MagicMock, mock_entry: MagicMock) -> RamsesCoordinator:
    """Return a mock coordinator with an entry attached."""
    coordinator = RamsesCoordinator(mock_hass, mock_entry)
    coordinator.client = MagicMock()
    coordinator.client.async_send_cmd = AsyncMock()
    coordinator._device_info = {}
    coordinator.platforms = {}

    mock_hass.data[DOMAIN] = {mock_entry.entry_id: coordinator}
    return coordinator


async def test_setup_fails_gracefully_on_bad_config(
    mock_hass: MagicMock, mock_entry: MagicMock
) -> None:
    """Test that startup catches client creation errors and logs them."""
    coordinator = RamsesCoordinator(mock_hass, mock_entry)
    coordinator.store.async_load = AsyncMock(return_value={})

    # Force _create_client to raise vol.Invalid (simulation of bad schema)
    coordinator._create_client = MagicMock(side_effect=vol.Invalid("Invalid config"))

    # Verify it raises a clean ValueError with helpful message
    with pytest.raises(ValueError, match="Failed to initialise RAMSES client"):
        await coordinator.async_setup()


async def test_device_registry_update_slugs(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test registry update logic for different device slugs."""
    mock_device = MagicMock()
    mock_device.id = FAN_ID
    mock_device._SLUG = "FAN"
    # Ensure name is None so coordinator falls back to slug-based logic
    mock_device.name = None
    mock_device._msg_value_code.return_value = None  # No 10E0 info

    with patch("homeassistant.helpers.device_registry.async_get") as mock_dr_get:
        mock_reg = mock_dr_get.return_value

        mock_coordinator._update_device(mock_device)

        # Verify the name and model were derived from the SLUG
        call_kwargs = mock_reg.async_get_or_create.call_args[1]
        assert call_kwargs["name"] == f"FAN {FAN_ID}"
        assert call_kwargs["model"] == "FAN"


async def test_setup_schema_merge_failure(
    mock_hass: MagicMock, mock_entry: MagicMock
) -> None:
    """Test async_setup handling of schema merge failures."""
    coordinator = RamsesCoordinator(mock_hass, mock_entry)

    # Provide a non-empty schema so the code enters the "merge" block
    coordinator.store.async_load = AsyncMock(
        return_value={SZ_CLIENT_STATE: {SZ_SCHEMA: {"existing": "data"}}}
    )

    mock_client = MagicMock()
    mock_client.start = AsyncMock()

    # Mock _create_client to fail on first call (merged schema) but succeed on second (config schema)
    coordinator._create_client = MagicMock(
        side_effect=[LookupError("Merge failed"), mock_client]
    )

    with patch(
        "custom_components.ramses_cc.coordinator.merge_schemas",
        return_value={"mock": "schema"},
    ):
        await coordinator.async_setup()

    assert coordinator._create_client.call_count == 2
    # First call with merged schema, second with config schema


async def test_update_device_relationships(mock_coordinator: RamsesCoordinator) -> None:
    """Test _update_device logic for parent/child and TCS relationships."""

    # Define dummy class with required attributes for spec matching
    class DummyZone:
        tcs: Any | None = None
        name: str | None = None
        _SLUG: str | None = None

        def _msg_value_code(self, code: Any) -> Any:
            pass

    # We patch the class in the BROKER module so it checks against our dummy
    with patch("custom_components.ramses_cc.coordinator.Zone", DummyZone):
        # 1. Test Zone with TCS (hits via_device logic for Zones)
        mock_zone = MagicMock(spec=DummyZone)
        mock_zone.id = "04:123456"
        mock_zone.tcs = MagicMock()
        mock_zone.tcs.id = "01:999999"
        mock_zone._msg_value_code.return_value = {"description": "Zone Name"}
        mock_zone.name = "Custom Zone"

        with patch("homeassistant.helpers.device_registry.async_get") as mock_dr_get:
            mock_reg = mock_dr_get.return_value
            mock_coordinator._update_device(mock_zone)

            # Verify via_device was set to TCS ID
            call_kwargs = mock_reg.async_get_or_create.call_args[1]
            assert call_kwargs["via_device"] == (DOMAIN, "01:999999")


async def test_update_device_child_parent(mock_coordinator: RamsesCoordinator) -> None:
    """Test _update_device logic for Child devices (actuators/sensors)."""
    # Test logic around lines 535-548
    from ramses_rf.entity_base import Child

    mock_child = MagicMock(spec=Child)
    mock_child.id = "13:123456"
    mock_child._parent = MagicMock()
    mock_child._parent.id = "04:123456"
    mock_child._msg_value_code.return_value = None
    mock_child._SLUG = "BDR"
    mock_child.name = None

    with patch("homeassistant.helpers.device_registry.async_get") as mock_dr_get:
        mock_reg = mock_dr_get.return_value
        mock_coordinator._update_device(mock_child)

        call_kwargs = mock_reg.async_get_or_create.call_args[1]
        assert call_kwargs["via_device"] == (DOMAIN, "04:123456")


async def test_async_start(mock_coordinator: RamsesCoordinator) -> None:
    """Test async_start sets up updates and saving."""

    # MOCK CHANGE: DataUpdateCoordinator.async_start calls async_config_entry_first_refresh
    # We patch it to avoid actual execution logic during this specific lifecycle test
    mock_coordinator.async_config_entry_first_refresh = AsyncMock()
    mock_coordinator.async_save_client_state = AsyncMock()
    mock_coordinator.client.start = AsyncMock()

    with patch(
        "custom_components.ramses_cc.coordinator.async_track_time_interval"
    ) as mock_track:
        await mock_coordinator.async_start()

        # Check that the first refresh was triggered
        assert mock_coordinator.async_config_entry_first_refresh.called

        # Should setup 2 timers:
        # 1. Discovery Loop (_async_discovery_task)
        # 2. Save Client State (async_save_client_state)
        assert mock_track.call_count == 2


async def test_platform_lifecycle(mock_coordinator: RamsesCoordinator) -> None:
    """Test registering, setting up, and unloading platforms."""
    # 1. Register Platform
    mock_platform = MagicMock()
    mock_platform.domain = "climate"
    mock_callback = MagicMock()

    mock_coordinator.async_register_platform(mock_platform, mock_callback)
    assert "climate" in mock_coordinator.platforms
    # Test duplicate registration
    mock_coordinator.async_register_platform(mock_platform, mock_callback)
    assert len(mock_coordinator.platforms["climate"]) == 2

    # 2. Setup Platform
    # Since mock_coordinator.hass is a MagicMock, we verify the call directly on the mock
    await mock_coordinator._async_setup_platform("climate")
    assert mock_coordinator.hass.config_entries.async_forward_entry_setups.called

    # Already set up path
    mock_coordinator.hass.config_entries.async_forward_entry_setups.reset_mock()
    await mock_coordinator._async_setup_platform("climate")
    assert not mock_coordinator.hass.config_entries.async_forward_entry_setups.called

    # 3. Unload Platforms
    assert await mock_coordinator.async_unload_platforms()
    assert mock_coordinator.hass.config_entries.async_forward_entry_unload.called


async def test_create_client_real(mock_coordinator: RamsesCoordinator) -> None:
    """Test the _create_client method execution (port extraction)."""
    # Setup options to contain the expected dict structure for the serial port
    mock_coordinator.options[SZ_SERIAL_PORT] = {SZ_PORT_NAME: "/dev/ttyUSB0"}

    with patch("custom_components.ramses_cc.coordinator.Gateway") as mock_gateway_cls:
        # Pass empty dict as config_schema
        mock_coordinator._create_client({})

        assert mock_gateway_cls.called
        _, kwargs = mock_gateway_cls.call_args
        # Verify the port config was extracted and passed
        assert "port_name" in kwargs
        assert kwargs["port_name"] == "/dev/ttyUSB0"


async def test_async_update_discovery(mock_coordinator: RamsesCoordinator) -> None:
    """Test async_update discovering and adding new entities."""
    # Setup mock entities in client
    mock_system = MagicMock(spec=Evohome)
    mock_system.id = "01:123456"
    mock_system.dhw = MagicMock()  # Has DHW
    mock_system.zones = [MagicMock()]  # Has Zone

    mock_device = MagicMock()
    mock_device.id = "04:123456"  # Device

    mock_coordinator.client.systems = [mock_system]
    mock_coordinator.client.devices = [mock_device]

    mock_coordinator.client.get_state.return_value = ({}, {})

    # Mock device registry to allow lookup AND Mock dispatcher to verify signals
    with (
        patch("homeassistant.helpers.device_registry.async_get"),
        patch(
            "custom_components.ramses_cc.coordinator.async_dispatcher_send"
        ) as mock_dispatch,
    ):
        # Call _discover_new_entities directly (was _async_update_data)
        await mock_coordinator._discover_new_entities()

        # Verify signal sent for new devices
        assert mock_dispatch.call_count >= 1
        calls = [c[0][1] for c in mock_dispatch.call_args_list]
        assert SIGNAL_NEW_DEVICES.format(Platform.CLIMATE) in calls
        assert SIGNAL_NEW_DEVICES.format(Platform.WATER_HEATER) in calls


async def test_async_update_setup_failure(mock_coordinator: RamsesCoordinator) -> None:
    """Test platform setup failure handling."""
    # Create a future that raises an exception when awaited
    f: asyncio.Future[Any] = asyncio.Future()
    f.set_exception(Exception("Setup failed"))

    # The side effect needs to close the coro argument to prevent warning
    def _fail_task(coro: Any) -> asyncio.Future[Any]:
        if asyncio.iscoroutine(coro):
            coro.close()
        return f

    # Mock create_task to return this failing future
    mock_coordinator.hass.async_create_task.side_effect = _fail_task

    # async_forward_entry_setups needs to fail if it were awaited directly
    mock_coordinator.hass.config_entries.async_forward_entry_setups.side_effect = (
        Exception("Setup failed")
    )

    result = await mock_coordinator._async_setup_platform("climate")
    assert result is False


async def test_setup_ignores_invalid_cached_packet_timestamps(
    mock_hass: MagicMock, mock_entry: MagicMock
) -> None:
    """Test that async_setup ignores packets with invalid timestamps."""

    coordinator = RamsesCoordinator(mock_hass, mock_entry)

    # Use a fresh timestamp for the valid packet so it isn't filtered out by the 24h check
    now: datetime = dt_util.now()
    valid_dtm: str = now.isoformat()
    invalid_dtm = "invalid-iso-format"

    coordinator.store.async_load = AsyncMock(
        return_value={
            SZ_CLIENT_STATE: {
                SZ_PACKETS: {
                    valid_dtm: "valid_packet_data",
                    invalid_dtm: "broken_packet_data",
                }
            }
        }
    )

    # Mock client creation
    coordinator._create_client = MagicMock()
    mock_client = MagicMock()
    mock_client.start = AsyncMock()
    coordinator._create_client.return_value = mock_client

    # Run setup
    await coordinator.async_setup()

    # Verify client.start was called with only the valid packet
    args, kwargs = mock_client.start.call_args
    cached = kwargs.get("cached_packets", {})

    assert valid_dtm in cached
    assert invalid_dtm not in cached


async def test_update_device_system_naming(mock_coordinator: RamsesCoordinator) -> None:
    """Test _update_device naming logic for System devices."""

    # Define dummy class to patch System for isinstance check
    class DummySystem:
        def _msg_value_code(self, *args: Any, **kwargs: Any) -> Any:
            return None

    # Patch the System class in the coordinator module
    with patch("custom_components.ramses_cc.coordinator.System", DummySystem):
        mock_system = MagicMock(spec=DummySystem)
        mock_system.id = "01:123456"
        mock_system.name = None
        mock_system._SLUG = None

        # Ensure the method returns None as expected
        mock_system._msg_value_code.return_value = None

        with patch("homeassistant.helpers.device_registry.async_get") as mock_dr_get:
            mock_reg = mock_dr_get.return_value

            mock_coordinator._update_device(mock_system)

            # Verify the name format "Controller {id}"
            call_kwargs = mock_reg.async_get_or_create.call_args[1]
            assert call_kwargs["name"] == "Controller 01:123456"


async def test_async_update_adds_systems_and_guards(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test async_update handles new systems and guards empty lists."""

    # Define a dummy class with all required attributes for coordinator.py logic
    class DummyEvohome:
        id: str = "01:111111"
        dhw: Any = None
        zones: list[Any] = []
        name: str | None = None
        _SLUG: str = "EVO"

        def _msg_value_code(self, *args: Any, **kwargs: Any) -> Any:
            return None

    # Patch Evohome in the coordinator with our dummy CLASS (using new=...)
    with patch("custom_components.ramses_cc.coordinator.Evohome", new=DummyEvohome):
        # Create a system that is an instance of our dummy class
        mock_system = DummyEvohome()
        mock_system.zones = []

        # Setup client to return this system
        mock_coordinator.client.systems = [mock_system]
        mock_coordinator.client.devices = []

        mock_coordinator.client.get_state.return_value = ({}, {})

        # Capture the calls to dispatcher to verify system was added
        with (
            patch(
                "custom_components.ramses_cc.coordinator.async_dispatcher_send"
            ) as mock_dispatch,
            patch("homeassistant.helpers.device_registry.async_get"),
        ):
            # Call _discover_new_entities directly (was _async_update_data)
            await mock_coordinator._discover_new_entities()

            # Use assert_any_call for robust verification
            expected_signal = SIGNAL_NEW_DEVICES.format(Platform.CLIMATE)
            mock_dispatch.assert_any_call(
                mock_coordinator.hass, expected_signal, [mock_system]
            )


async def test_setup_uses_merged_schema_on_success(
    mock_hass: MagicMock, mock_entry: MagicMock
) -> None:
    """Test that async_setup successfully uses the merged schema."""
    coordinator = RamsesCoordinator(mock_hass, mock_entry)

    # Setup mock data
    cached_schema = {"cached_key": "cached_val"}
    config_schema = {"config_key": "config_val"}
    merged_result = {"merged_key": "merged_val"}

    coordinator.store.async_load = AsyncMock(
        return_value={SZ_CLIENT_STATE: {SZ_SCHEMA: cached_schema}}
    )

    # 2. Setup a mock config schema in options
    coordinator.options[CONF_SCHEMA] = config_schema

    # 3. Mock _create_client to return a valid client object (Success case)
    mock_client = MagicMock()
    mock_client.start = AsyncMock()
    coordinator._create_client = MagicMock(return_value=mock_client)

    # 4. Patch merge_schemas to return a known merged dictionary

    # Patch mock schema_is_minimal to prevent TypeError on our dummy config_schema
    with (
        patch(
            "custom_components.ramses_cc.coordinator.merge_schemas",
            return_value=merged_result,
        ) as mock_merge,
        patch(
            "custom_components.ramses_cc.coordinator.schema_is_minimal",
            return_value=True,
        ),
    ):
        # 5. Execute async_setup
        await coordinator.async_setup()

        # VERIFICATION

        # Ensure merge_schemas was called correctly
        mock_merge.assert_called_once_with(config_schema, cached_schema)

        # CRITICAL: Verify _create_client was called exactly ONCE with the MERGED schema.
        coordinator._create_client.assert_called_once_with(merged_result)

        # Ensure the coordinator's client attribute was set to our mock
        assert coordinator.client is mock_client


async def test_setup_logs_warning_on_non_minimal_schema(
    mock_hass: MagicMock, mock_entry: MagicMock
) -> None:
    """Test that a warning is logged when the schema is not minimal (Line 155)."""
    coordinator = RamsesCoordinator(mock_hass, mock_entry)
    coordinator.store.async_load = AsyncMock(return_value={})

    # Mock success path for client creation so setup completes
    mock_client = MagicMock()
    mock_client.start = AsyncMock()
    coordinator._create_client = MagicMock(return_value=mock_client)

    # Patch schema_is_minimal to return False -> triggers the warning
    with (
        patch(
            "custom_components.ramses_cc.coordinator.schema_is_minimal",
            return_value=False,
        ),
        patch("custom_components.ramses_cc.coordinator._LOGGER") as mock_logger,
    ):
        await coordinator.async_setup()

        mock_logger.warning.assert_any_call(
            "The config schema is not minimal (consider minimising it)"
        )


async def test_update_device_name_fallback_to_id(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test _update_device falls back to device.id when no name/slug/system (Line 626)."""

    # 1. Create a generic mock device
    # MagicMock is not an instance of ramses_rf.system.System, so that check fails automatically.
    mock_device = MagicMock()
    mock_device.id = "99:888777"

    # 2. Ensure preceding checks fail
    mock_device.name = None  # Fails 'if device.name'
    mock_device._SLUG = None  # Fails 'elif device._SLUG'

    # Stub helper method to return None (affects 'model' variable, not 'name')
    mock_device._msg_value_code.return_value = None

    # 3. Patch the device registry to verify the result
    with patch("homeassistant.helpers.device_registry.async_get") as mock_dr_get:
        mock_reg = mock_dr_get.return_value

        # 4. Call the method under test
        mock_coordinator._update_device(mock_device)

        # 5. Verify device_registry was called with name == device.id
        call_kwargs = mock_reg.async_get_or_create.call_args[1]
        assert call_kwargs["name"] == "99:888777"


async def test_coordinator_save_client_state_no_client(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test async_save_client_state returns early when client is None (Lines 232-233)."""
    # Force client to None
    mock_coordinator.client = None
    # Mock the store to verify it is NOT called
    mock_coordinator.store.async_save = AsyncMock()

    await mock_coordinator.async_save_client_state()

    mock_coordinator.store.async_save.assert_not_called()


async def test_coordinator_update_data_no_client(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test _async_update_data returns early when client is None (Line 353)."""
    mock_coordinator.client = None

    # Patch _discover_new_entities to ensure it is NOT called
    with patch.object(mock_coordinator, "_discover_new_entities") as mock_discover:
        await mock_coordinator._async_update_data()
        mock_discover.assert_not_called()


async def test_coordinator_run_fan_param_sequence(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test _async_run_fan_param_sequence delegates to service_handler (Line 452)."""
    call_data = {"test": "data"}
    # Mock the handler method on the service_handler
    mock_coordinator.service_handler._async_run_fan_param_sequence = AsyncMock()

    await mock_coordinator._async_run_fan_param_sequence(call_data)

    mock_coordinator.service_handler._async_run_fan_param_sequence.assert_awaited_once_with(
        call_data
    )


async def test_discovery_task_calls_discovery(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test that _async_discovery_task calls discovery when client is present."""
    # Ensure client exists
    mock_coordinator.client = MagicMock()

    # Patch the discovery method to verify it gets called
    with patch.object(mock_coordinator, "_discover_new_entities") as mock_discover:
        await mock_coordinator._async_discovery_task()

        mock_discover.assert_called_once()


async def test_save_client_state_hybrid_compatibility(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test that state saving works with both Sync and Async client methods."""

    # Mock the store and internal state needed for the save method
    mock_coordinator.store.async_save = AsyncMock()
    mock_coordinator._remotes = {}
    mock_coordinator._entities = {}

    # --- SCENARIO 1: New Async Client ---
    # get_state returns an Awaitable (Coroutine) that resolves to the tuple
    mock_coordinator.client.get_state = AsyncMock(return_value=({"type": "async"}, {}))

    await mock_coordinator.async_save_client_state()

    # Verify the awaitable was awaited and data passed to store
    mock_coordinator.store.async_save.assert_awaited_with({"type": "async"}, {}, {})

    # --- SCENARIO 2: Old Sync Client ---
    # get_state returns the tuple directly (MagicMock is not awaitable by default)
    mock_coordinator.client.get_state = MagicMock(return_value=({"type": "sync"}, {}))

    await mock_coordinator.async_save_client_state()

    # Verify the synchronous result was handled correctly
    mock_coordinator.store.async_save.assert_awaited_with({"type": "sync"}, {}, {})


async def test_create_client_mqtt_not_ready(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test _create_client raises ConfigEntryNotReady if MQTT integration is missing."""

    # Enable MQTT in options
    mock_coordinator.options[CONF_MQTT_USE_HA] = True

    # Mock HA to report NO MQTT entries
    mock_coordinator.hass.config_entries.async_entries.return_value = []

    with pytest.raises(
        ConfigEntryNotReady, match="Home Assistant MQTT integration is not set up"
    ):
        # Pass an empty schema as it's required by the signature
        mock_coordinator._create_client({})


async def test_create_client_mqtt_success(mock_coordinator: RamsesCoordinator) -> None:
    """Test _create_client sets up the MQTT bridge correctly."""

    # Enable MQTT in options
    mock_coordinator.options[CONF_MQTT_USE_HA] = True

    # Mock HA to report MQTT entries exist
    mock_coordinator.hass.config_entries.async_entries.return_value = ["mqtt_entry"]

    with (
        patch(
            "custom_components.ramses_cc.coordinator.MqttGateway"
        ) as mock_mqtt_gateway_cls,
        patch(
            "custom_components.ramses_cc.coordinator.RamsesMqttBridge"
        ) as mock_bridge_cls,
    ):
        # Setup the mock bridge instance
        mock_bridge_instance = mock_bridge_cls.return_value
        mock_bridge_instance.async_transport_factory = MagicMock()

        # Call the method under test
        mock_coordinator._create_client({})

        # 1. Verify Bridge Initialization
        # It should use the default topic and ID from const
        mock_bridge_cls.assert_called_once_with(
            mock_coordinator.hass, DEFAULT_MQTT_TOPIC, DEFAULT_HGI_ID
        )
        assert mock_coordinator.mqtt_bridge is mock_bridge_instance

        # 2. Verify MqttGateway Initialization arguments
        assert mock_mqtt_gateway_cls.called
        _, kwargs = mock_mqtt_gateway_cls.call_args

        # Check specific MQTT-related arguments were passed to Gateway
        assert (
            kwargs.get("transport_constructor")
            == mock_bridge_instance.async_transport_factory
        )
        assert kwargs.get("port_name") == "/dev/ttyUSB0"
        assert "hgi_id" in kwargs


@pytest.mark.asyncio
async def test_discover_new_entities_registration_order(
    mock_hass: MagicMock,
) -> None:
    """Test that parent devices are registered before child devices.

    This ensures Systems/DHW/Zones are processed before generic Devices to
    maintain Device Registry integrity.
    """
    # 1. Setup Mock Gateway
    mock_gateway = MagicMock(spec=Gateway)

    # Fix the ValueError: mock the return value for get_state()
    # It expects a tuple: (schema_dict, packets_dict)
    mock_gateway.get_state.return_value = ({}, {})

    # Setup Device Hierarchy
    mock_system = MagicMock(spec=Evohome)
    mock_system.id = "01:123456"
    mock_system.dhw = None
    mock_system.zones = []
    mock_gateway.systems = [mock_system]

    mock_device = MagicMock()
    mock_device.id = "07:048080"
    mock_gateway.devices = [mock_device]

    # 2. Setup Mock Config Entry
    mock_entry = MagicMock(spec=ConfigEntry)
    mock_entry.options = {"scan_interval": 60}
    mock_entry.entry_id = "test_entry_id"
    # Fix the AttributeError: provide a domain for the mock entry
    mock_entry.domain = "ramses_cc"

    # 3. Initialize Coordinator and Inject Mock Client
    with (
        patch(
            "custom_components.ramses_cc.coordinator.RamsesCoordinator._update_device"
        ) as mock_update_device,
        patch(
            "custom_components.ramses_cc.coordinator.RamsesFanHandler.async_setup_fan_device",
            new_callable=AsyncMock,
        ),
        # We patch async_setup_platform to avoid real HA platform loading during this unit test
        patch(
            "custom_components.ramses_cc.coordinator.RamsesCoordinator._async_setup_platform",
            return_value=True,
        ),
    ):
        coordinator = RamsesCoordinator(mock_hass, mock_entry)
        coordinator.client = mock_gateway

        # Manually trigger discovery
        await coordinator._discover_new_entities()

        # 4. Assertions
        expected_calls = [
            call(mock_system),  # Parent first
            call(mock_device),  # Child second
        ]

        mock_update_device.assert_has_calls(expected_calls, any_order=False)
        assert mock_update_device.call_count == 2


async def test_setup_with_corrupted_storage_dates(
    mock_hass: MagicMock, mock_entry: MagicMock
) -> None:
    """Test that startup survives invalid date strings in storage.

    From test_coordinator_startup.py.
    """
    # 1. Setup Coordinator
    coordinator = RamsesCoordinator(mock_hass, mock_entry)

    # 2. Mock Storage with corrupted date
    # Valid date: 2023-01-01T12:00:00
    # Invalid date: "INVALID-DATE-STRING"
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

    coordinator.store.async_load = AsyncMock(return_value=mock_storage_data)
    coordinator._create_client = MagicMock()
    coordinator.client = MagicMock()
    coordinator.client.start = AsyncMock()

    # 3. Run async_setup
    # This should NOT raise ValueError
    await coordinator.async_setup()

    # 4. Verify client started
    assert coordinator.client.start.called

    # 5. Verify only valid packet was passed to start
    call_args = coordinator.client.start.call_args
    cached_packets = call_args.kwargs.get("cached_packets", {})

    assert len(cached_packets) == 1
    assert "INVALID-DATE-STRING" not in cached_packets


async def test_save_client_state_remotes(mock_coordinator: RamsesCoordinator) -> None:
    """Test saving remote commands to persistent storage.

    From test_coordinator_services.py.
    """
    mock_coordinator.client.get_state.return_value = ({}, {})
    mock_coordinator._remotes = {REM_ID: {"boost": "packet_data"}}
    mock_coordinator.store = MagicMock(spec=mock_coordinator.store)
    mock_coordinator.store.async_save = AsyncMock()

    await mock_coordinator.async_save_client_state()

    # Verify remotes were included in the save payload
    args = mock_coordinator.store.async_save.call_args[0]
    saved_remotes = args[2]

    assert saved_remotes[REM_ID]["boost"] == "packet_data"


async def test_mqtt_gateway_logic() -> None:
    """Test MqttGateway extracts and reinjects arguments.

    Covers lines 84-88, 94-104.
    """
    custom_factory = MagicMock()
    custom_extra = {"test": "extra"}

    # Mock the parent class behavior
    with (
        patch(
            "custom_components.ramses_cc.coordinator.Gateway.__init__",
            return_value=None,
        ) as mock_super_init,
        patch(
            "custom_components.ramses_cc.coordinator.Gateway.start",
            new_callable=AsyncMock,
        ) as mock_super_start,
    ):
        # 1. Initialize MqttGateway
        # This calls __init__, which should pop our custom args
        gw = MqttGateway(
            transport_constructor=custom_factory, extra=custom_extra, other_arg="value"
        )

        # Simulate what parent Gateway does (store kwargs) because we mocked __init__
        gw._kwargs = {"other_arg": "value"}

        # Check that parent __init__ was called without the custom args
        mock_super_init.assert_called_once_with(other_arg="value")
        assert gw._custom_factory == custom_factory
        assert gw._custom_extra == custom_extra

        # 2. Start MqttGateway
        await gw.start()

        # Check that arguments were re-injected into _kwargs
        assert gw._kwargs["transport_constructor"] == custom_factory
        assert gw._kwargs["extra"] == custom_extra

        # Check that parent start was called
        mock_super_start.assert_awaited_once()


async def test_setup_handles_naive_timestamps(
    mock_hass: MagicMock, mock_entry: MagicMock
) -> None:
    """Test that async_setup adds timezone info to naive timestamps.

    Covers line 170.
    """
    coordinator = RamsesCoordinator(mock_hass, mock_entry)

    # Create a naive timestamp string (no offset)
    naive_dt = "2023-01-01T12:00:00"

    coordinator.store.async_load = AsyncMock(
        return_value={
            SZ_CLIENT_STATE: {SZ_PACKETS: {naive_dt: "naive_packet"}},
            SZ_KNOWN_LIST: {},
        }
    )
    coordinator._create_client = MagicMock()
    coordinator.client = MagicMock()
    coordinator.client.start = AsyncMock()

    # Patch dt_util.now() to ensure the packet isn't discarded as too old
    # Packet is 2023-01-01, so we pretend "now" is 2023-01-01 13:00
    fake_now = datetime(2023, 1, 1, 13, 0, 0, tzinfo=dt_util.DEFAULT_TIME_ZONE)

    with patch("homeassistant.util.dt.now", return_value=fake_now):
        await coordinator.async_setup()

    # Verify the packet was accepted (tzinfo added implies it didn't fail parsing)
    call_args = coordinator.client.start.call_args
    cached = call_args.kwargs.get("cached_packets", {})
    assert naive_dt in cached


async def test_get_device_lookup(mock_coordinator: RamsesCoordinator) -> None:
    """Test _get_device lookups via internal list and client fallback.

    Covers lines 373-377.
    """
    # 1. Test finding in self._devices
    dev1 = MagicMock()
    dev1.id = "01:111111"
    mock_coordinator._devices = [dev1]

    assert mock_coordinator._get_device("01:111111") == dev1

    # 2. Test fallback to client.device_by_id
    dev2 = MagicMock()
    dev2.id = "02:222222"
    mock_coordinator.client.device_by_id = {"02:222222": dev2}

    assert mock_coordinator._get_device("02:222222") == dev2

    # 3. Test not found (client exists)
    assert mock_coordinator._get_device("99:999999") is None

    # 4. Test not found (no client) -> Hits the final return None
    mock_coordinator.client = None
    mock_coordinator._devices = []  # Clear devices to ensure fall-through
    assert mock_coordinator._get_device("01:111111") is None


async def test_update_device_skips_redundant_update(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test _update_device returns early if info hasn't changed.

    Covers line 470.
    """
    mock_device = MagicMock()
    mock_device.id = "01:000000"
    mock_device.name = "Test Device"
    mock_device._msg_value_code.return_value = None
    mock_device._SLUG = "TST"

    with patch("homeassistant.helpers.device_registry.async_get") as mock_dr_get:
        mock_reg = mock_dr_get.return_value

        # First call: Should create
        mock_coordinator._update_device(mock_device)
        assert mock_reg.async_get_or_create.call_count == 1

        # Second call: Should return early
        mock_coordinator._update_device(mock_device)
        assert mock_reg.async_get_or_create.call_count == 1


async def test_discovery_task_handles_exception(
    mock_coordinator: RamsesCoordinator,
) -> None:
    """Test _async_discovery_task catches and logs exceptions.

    Covers lines 496-497.
    """
    with (
        patch.object(
            mock_coordinator,
            "_discover_new_entities",
            side_effect=Exception("Boom"),
        ),
        patch("custom_components.ramses_cc.coordinator._LOGGER") as mock_logger,
    ):
        await mock_coordinator._async_discovery_task()

        mock_logger.error.assert_called_with("Discovery error: %s", ANY)


async def test_service_delegates(mock_coordinator: RamsesCoordinator) -> None:
    """Test simple service delegates pass calls to handler.

    Covers lines 582, 586, 590, 594, 611.
    """
    call_obj = MagicMock()
    handler = mock_coordinator.service_handler
    handler.async_bind_device = AsyncMock()
    handler.async_send_packet = AsyncMock()
    handler.async_get_fan_param = AsyncMock()
    handler.async_set_fan_param = AsyncMock()
    mock_coordinator.async_refresh = AsyncMock()

    # 1. bind_device
    await mock_coordinator.async_bind_device(call_obj)
    handler.async_bind_device.assert_awaited_once_with(call_obj)

    # 2. force_update
    await mock_coordinator.async_force_update(call_obj)
    mock_coordinator.async_refresh.assert_awaited_once()

    # 3. send_packet
    await mock_coordinator.async_send_packet(call_obj)
    handler.async_send_packet.assert_awaited_once_with(call_obj)

    # 4. get_fan_param
    await mock_coordinator.async_get_fan_param(call_obj)
    handler.async_get_fan_param.assert_awaited_once_with(call_obj)

    # 5. set_fan_param
    await mock_coordinator.async_set_fan_param(call_obj)
    handler.async_set_fan_param.assert_awaited_once_with(call_obj)


async def test_get_all_fan_params_delegate(mock_coordinator: RamsesCoordinator) -> None:
    """Test get_all_fan_params creates a task.

    Covers lines 605.
    """
    call_obj = MagicMock()
    handler = mock_coordinator.service_handler
    handler._async_run_fan_param_sequence = AsyncMock()

    # This method is not async, it uses hass.async_create_task
    mock_coordinator.get_all_fan_params(call_obj)

    # Verify task creation was called
    mock_coordinator.hass.async_create_task.assert_called_once()
    # Note: verifying the exact coroutine passed to create_task is complex with mocks,
    # but line coverage is satisfied by calling the method.


async def test_async_update_data_success(mock_coordinator: RamsesCoordinator) -> None:
    """Test _async_update_data runs to completion when client exists.

    Covers line 490.
    """
    # Ensure client exists so we don't return early
    mock_coordinator.client = MagicMock()

    # Call the method
    result = await mock_coordinator._async_update_data()

    # Verify it reached the end
    assert result is None
