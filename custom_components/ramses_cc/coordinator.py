"""Coordinator for RAMSES integration."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Coroutine
from copy import deepcopy
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Final

import voluptuous as vol  # type: ignore[import-untyped, unused-ignore]
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.dispatcher import (
    async_dispatcher_connect,
    async_dispatcher_send,
)
from homeassistant.helpers.entity_platform import EntityPlatform
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from ramses_rf.device import Device
from ramses_rf.device.hvac import HvacRemoteBase, HvacVentilator
from ramses_rf.entity_base import Child, Entity as RamsesRFEntity
from ramses_rf.gateway import Gateway
from ramses_rf.system import Evohome, System, Zone
from ramses_tx.const import Code
from ramses_tx.schemas import extract_serial_port

from .const import (
    CONF_COMMANDS,
    CONF_MQTT_USE_HA,
    CONF_RAMSES_RF,
    CONF_SCAN_INTERVAL,
    CONF_SCHEMA,
    DOMAIN,
    SIGNAL_NEW_DEVICES,
    SZ_CLIENT_STATE,
    SZ_ENFORCE_KNOWN_LIST,
    SZ_KNOWN_LIST,
    SZ_PACKET_LOG,
    SZ_PACKETS,
    SZ_REMOTES,
    SZ_SCHEMA,
    SZ_SERIAL_PORT,
)
from .fan_handler import RamsesFanHandler
from .mqtt_bridge import RamsesMqttBridge
from .schemas import merge_schemas, schema_is_minimal
from .services import RamsesServiceHandler
from .store import RamsesStore

if TYPE_CHECKING:
    from .entity import RamsesEntity

_LOGGER = logging.getLogger(__name__)

SAVE_STATE_INTERVAL: Final[timedelta] = timedelta(minutes=5)


class RamsesCoordinator(DataUpdateCoordinator):
    """Central coordinator for the RAMSES integration."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Initialize the RAMSES coordinator and its data structures."""
        self.hass = hass
        self.entry = entry
        self.options = deepcopy(dict(entry.options))
        self.store = RamsesStore(hass)

        # Initialize handlers
        self.fan_handler = RamsesFanHandler(self)
        self.service_handler = RamsesServiceHandler(self)
        self.mqtt_bridge: RamsesMqttBridge | None = None

        _LOGGER.debug("Config = %s", entry.options)

        self.client: Gateway | None = None
        self._remotes: dict[str, dict[str, str]] = {}

        self._platform_setup_tasks: dict[str, asyncio.Task[bool]] = {}
        self._entities: dict[str, RamsesEntity] = {}  # domain entities
        self._device_info: dict[str, DeviceInfo] = {}

        # Discovered client objects...
        self._devices: list[Device] = []
        self._systems: list[System] = []
        self._zones: list[Zone] = []
        self._dhws: list[Zone] = []
        self._parameter_entities_created: set[str] = set()

        # Initialize platforms dictionary to store platform references
        self.platforms: dict[str, Any] = {}
        self.learn_device_id: str | None = None

        # Load scan interval from options, default to 60s if missing
        scan_interval = entry.options.get(CONF_SCAN_INTERVAL, 60)

        # Initialize the DataUpdateCoordinator
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=scan_interval),
        )

    def _get_saved_packets(self, client_state: dict[str, Any]) -> dict[str, str]:
        """Filter cached packets to remove expired or unwanted entries."""
        msg_code_filter = ["313F"]
        known_list = self.options.get(SZ_KNOWN_LIST, {})
        enforce_known_list = self.options[CONF_RAMSES_RF].get(SZ_ENFORCE_KNOWN_LIST)

        packets = {}
        now = dt_util.now()

        # Iterate over packets from storage
        for dtm, pkt in client_state.get(SZ_PACKETS, {}).items():
            try:
                dt_obj = datetime.fromisoformat(dtm)
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
            except ValueError:
                _LOGGER.warning(
                    "Ignoring cached packet with invalid timestamp: %s", dtm
                )
                continue

            # Check age (keep last 24 hours) and known list enforcement
            if (
                dt_obj > now - timedelta(days=1)
                and pkt[41:45] not in msg_code_filter
                and (
                    not enforce_known_list
                    or pkt[11:20] in known_list
                    or pkt[21:30] in known_list
                )
            ):
                packets[dtm] = pkt

        return packets

    async def async_setup(self) -> None:
        """Set up the RAMSES client and load configuration."""
        storage = await self.store.async_load()
        _LOGGER.debug("Storage = %s", storage)

        # 1. Load Remotes
        remote_commands = {
            k: v[CONF_COMMANDS]
            for k, v in self.options.get(SZ_KNOWN_LIST, {}).items()
            if v.get(CONF_COMMANDS)
        }
        self._remotes = storage.get(SZ_REMOTES, {}) | remote_commands

        client_state: dict[str, Any] = storage.get(SZ_CLIENT_STATE, {})

        # 2. Schema Handling
        config_schema = self.options.get(CONF_SCHEMA, {})
        _LOGGER.debug("CONFIG_SCHEMA: %s", config_schema)
        if not schema_is_minimal(config_schema):
            _LOGGER.warning("The config schema is not minimal (consider minimising it)")

        cached_schema = client_state.get(SZ_SCHEMA, {})
        _LOGGER.debug("CACHED_SCHEMA: %s", cached_schema)

        # Try merging schemas
        if cached_schema and (
            merged_schema := merge_schemas(config_schema, cached_schema)
        ):
            try:
                self.client = self._create_client(merged_schema)
            except (LookupError, vol.MultipleInvalid) as err:
                _LOGGER.warning("Failed to initialise with merged schema: %s", err)

        # Fallback to config schema
        if not self.client:
            try:
                self.client = self._create_client(config_schema)
            except (ValueError, vol.Invalid) as err:
                _LOGGER.error(
                    "Critical error: Failed to initialise client with config schema: %s",
                    err,
                )
                raise ValueError(f"Failed to initialise RAMSES client: {err}") from err

        # 3. Packet Handling (Refactored)
        cached_packets = self._get_saved_packets(client_state)
        _LOGGER.info("Starting with %s cached packets", len(cached_packets))

        await self.client.start(cached_packets=cached_packets)
        self.entry.async_on_unload(self.client.stop)

    async def async_start(self) -> None:
        """Start the coordinator and initiate the first refresh."""
        # Note: self.client.start() should have been called in async_setup

        # Trigger the first update immediately (calls _async_update_data)
        # This will raise ConfigEntryNotReady if it fails, which is handled by HA
        await self.async_config_entry_first_refresh()

        # Keep the dedicated interval for saving client state to disk
        self.entry.async_on_unload(
            async_track_time_interval(
                self.hass, self.async_save_client_state, SAVE_STATE_INTERVAL
            )
        )
        self.entry.async_on_unload(self.async_save_client_state)

    def _create_client(self, schema: dict[str, Any]) -> Gateway:
        """Create and configure a new RAMSES client instance."""
        kwargs = {
            "packet_log": self.options.get(SZ_PACKET_LOG, {}),
            "known_list": self.options.get(SZ_KNOWN_LIST, {}),
            "config": self.options.get(CONF_RAMSES_RF, {}),
            **schema,
        }

        # Check for HA MQTT Strategy
        if self.options.get(CONF_MQTT_USE_HA):
            if not self.hass.config_entries.async_entries("mqtt"):
                raise ConfigEntryNotReady(
                    "Home Assistant MQTT integration is not set up"
                )

            # Default topic if not specified
            topic = "ramses_cc"
            self.mqtt_bridge = RamsesMqttBridge(self.hass, topic)

            # Inject the transport factory
            kwargs["transport_factory"] = self.mqtt_bridge.async_transport_factory
            port_name = None  # No physical port
            port_config = {}

        else:
            # Standard Serial/USB setup
            port_name, port_config = extract_serial_port(self.options[SZ_SERIAL_PORT])
            kwargs["port_config"] = port_config

        return Gateway(
            port_name=port_name,
            loop=self.hass.loop,
            **kwargs,
        )

    async def async_save_client_state(self, _: datetime | None = None) -> None:
        """Save the current state of the RAMSES client to persistent storage."""

        if not self.client:
            _LOGGER.debug("Cannot save state: Client not initialized")
            return

        # Support both async (new) and sync (old) client.get_state()
        result = self.client.get_state()

        if inspect.isawaitable(result):
            schema, packets = await result
        else:
            schema, packets = result

        _LOGGER.info("Saving the client state cache (packets, schema)")

        remotes = self._remotes | {
            k: v._commands for k, v in self._entities.items() if hasattr(v, "_commands")
        }

        await self.store.async_save(schema, packets, remotes)

    def _get_device(self, device_id: str) -> Any | None:
        """Get a device by ID."""
        if dev := next((d for d in self._devices if d.id == device_id), None):
            return dev
        if self.client and hasattr(self.client, "device_by_id"):
            return self.client.device_by_id.get(device_id)
        return None

    def async_register_platform(
        self,
        platform: EntityPlatform,
        add_new_devices: Callable[[RamsesRFEntity], None],
    ) -> None:
        """Register a platform that has entities with the coordinator."""
        platform_str = platform.domain if hasattr(platform, "domain") else platform
        _LOGGER.debug("Registering platform %s", platform_str)

        if platform_str not in self.platforms:
            self.platforms[platform_str] = []
        self.platforms[platform_str].append(platform)

        _LOGGER.debug(
            "Connecting signal for platform %s: %s",
            platform_str,
            SIGNAL_NEW_DEVICES.format(platform_str),
        )

        self.entry.async_on_unload(
            async_dispatcher_connect(
                self.hass, SIGNAL_NEW_DEVICES.format(platform_str), add_new_devices
            )
        )

    async def _async_setup_platform(self, platform: str) -> bool:
        """Set up a platform and return True if successful."""
        if platform not in self._platform_setup_tasks:
            self._platform_setup_tasks[platform] = self.hass.async_create_task(
                self.hass.config_entries.async_forward_entry_setups(
                    self.entry, [platform]
                )
            )
        try:
            await self._platform_setup_tasks[platform]
            _LOGGER.debug("Platform setup completed for %s", platform)
            return True
        except Exception as err:
            _LOGGER.error(
                "Error setting up %s platform: %s", platform, str(err), exc_info=True
            )
            return False

    async def async_unload_platforms(self) -> bool:
        """Unload all platforms associated with this integration."""
        tasks: list[Coroutine[Any, Any, bool]] = [
            self.hass.config_entries.async_forward_entry_unload(self.entry, platform)
            for platform, task in self._platform_setup_tasks.items()
            if not task.cancel()
        ]
        result = all(await asyncio.gather(*tasks))
        _LOGGER.debug("Platform unload completed with result: %s", result)
        return result

    def _update_device(self, device: RamsesRFEntity) -> None:
        """Update device information in the device registry."""
        if hasattr(device, "name") and device.name:
            name = device.name
        elif isinstance(device, System):
            name = f"Controller {device.id}"
        elif device._SLUG:
            name = f"{device._SLUG} {device.id}"
        else:
            name = device.id

        if info := device._msg_value_code(Code._10E0):
            model = info.get("description")
        else:
            model = device._SLUG

        device_registry = dr.async_get(self.hass)

        if isinstance(device, Zone) and device.tcs:
            _LOGGER.info(f"ZONE {model} via_device SET to {device.tcs}")
            via_device = (DOMAIN, device.tcs.id)
        elif isinstance(device, Child) and device._parent:
            _LOGGER.info(f"CHILD {model} via_device SET to {device._parent}")
            via_device = (DOMAIN, device._parent.id)
        else:
            via_device = None

        device_info = DeviceInfo(
            identifiers={(DOMAIN, device.id)},
            name=name,
            manufacturer=None,
            model=model,
            via_device=via_device,
            serial_number=device.id,
        )

        if self._device_info.get(device.id) == device_info:
            return
        self._device_info[device.id] = device_info

        device_registry.async_get_or_create(
            config_entry_id=self.entry.entry_id, **device_info
        )

    async def _async_update_data(self) -> None:
        """Fetch data from the RAMSES RF client and discover new entities."""
        if not self.client:
            return

        # We don't await self.client.update() here because ramses_rf
        # runs in a background task, but if we needed to poll, we'd do it here.
        # If your client has a specific poll method, call it.
        # Otherwise, we just proceed to discovery.

        # Run discovery
        await self._discover_new_entities()

    async def _discover_new_entities(self) -> None:
        """Discover new devices in the client and register them with HA."""
        gwy: Gateway = self.client

        # --- DIAGNOSTIC LOGGING ---
        # This will reveal if ramses_rf has actually found any devices.
        _LOGGER.info(
            "Discovery: Devices=%s, Systems=%s", len(gwy.devices), len(gwy.systems)
        )
        if len(gwy.devices) > 0:
            _LOGGER.debug("Discovered Devices: %s", [d.id for d in gwy.devices])

        async def async_add_entities(
            platform: str, devices: list[RamsesRFEntity]
        ) -> None:
            if not devices:
                return
            await self._async_setup_platform(platform)
            async_dispatcher_send(
                self.hass, SIGNAL_NEW_DEVICES.format(platform), devices
            )

        def find_new_entities(
            known: list[RamsesRFEntity], current: list[RamsesRFEntity]
        ) -> tuple[list[RamsesRFEntity], list[RamsesRFEntity]]:
            new = [x for x in current if x not in known]
            return known + new, new

        # Identify new items compared to what we already know
        self._systems, new_systems = find_new_entities(
            self._systems,
            [s for s in gwy.systems if isinstance(s, Evohome)],
        )
        self._zones, new_zones = find_new_entities(
            self._zones,
            [z for s in gwy.systems for z in s.zones if isinstance(s, Evohome)],
        )
        self._dhws, new_dhws = find_new_entities(
            self._dhws,
            [s.dhw for s in gwy.systems if s.dhw if isinstance(s, Evohome)],
        )
        self._devices, new_devices = find_new_entities(self._devices, gwy.devices)

        # Process new devices for fan logic
        # Systems/DHWs must be processed before Devices to ensure via_device parents exist
        for device in new_systems + new_dhws + new_zones + new_devices:
            await self.fan_handler.async_setup_fan_device(device)
            # Register device in registry once upon discovery
            self._update_device(device)

        new_entities = new_systems + new_dhws + new_zones + new_devices

        if not new_entities:
            return

        # Register new entities with platforms
        await async_add_entities(Platform.BINARY_SENSOR, new_entities)
        await async_add_entities(Platform.SENSOR, new_entities)

        await async_add_entities(
            Platform.CLIMATE, [d for d in new_devices if isinstance(d, HvacVentilator)]
        )
        await async_add_entities(
            Platform.REMOTE, [d for d in new_devices if isinstance(d, HvacRemoteBase)]
        )
        await async_add_entities(Platform.CLIMATE, new_systems)
        await async_add_entities(Platform.CLIMATE, new_zones)
        await async_add_entities(Platform.WATER_HEATER, new_dhws)
        await async_add_entities(Platform.NUMBER, new_entities)

        # Trigger a save if we found something new
        await self.async_save_client_state()

    # Delegate service calls to the Service Handler
    async def async_bind_device(self, call: ServiceCall) -> None:
        """Delegate to Service Handler."""
        await self.service_handler.async_bind_device(call)

    async def async_force_update(self, _: ServiceCall) -> None:
        """Force an immediate update of all device states."""
        await self.async_refresh()

    async def async_send_packet(self, call: ServiceCall) -> None:
        """Delegate to Service Handler."""
        await self.service_handler.async_send_packet(call)

    async def async_get_fan_param(self, call: dict[str, Any] | ServiceCall) -> None:
        """Delegate to Service Handler."""
        await self.service_handler.async_get_fan_param(call)

    async def _async_run_fan_param_sequence(
        self, call: dict[str, Any] | ServiceCall
    ) -> None:
        """Delegate to Service Handler to run the fan parameter sequence."""
        await self.service_handler._async_run_fan_param_sequence(call)

    def get_all_fan_params(self, call: dict[str, Any] | ServiceCall) -> None:
        """Delegate to Service Handler."""
        # Note: get_all_fan_params is not async, it wraps the async call in a task
        self.hass.async_create_task(
            self.service_handler._async_run_fan_param_sequence(call)
        )

    async def async_set_fan_param(self, call: dict[str, Any] | ServiceCall) -> None:
        """Delegate to Service Handler."""
        await self.service_handler.async_set_fan_param(call)
