"""Coordinator for RAMSES integration."""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import logging
import re
from collections.abc import Callable, Coroutine, Sequence
from contextlib import suppress
from copy import deepcopy
from datetime import datetime as dt, timedelta as td
from threading import Semaphore
from typing import TYPE_CHECKING, Any, Final, TypeVar, cast

import serial  # type: ignore[import-untyped]
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
from homeassistant.helpers.event import async_call_later, async_track_time_interval
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from ramses_rf.devices import Device, HvacRemoteBase, HvacVentilator
from ramses_rf.entity import Entity as RamsesRFEntity
from ramses_rf.gateway import Gateway, GatewayConfig
from ramses_rf.schemas import (
    SZ_ACTUATORS,
    SZ_APPLIANCE_CONTROL,
    SZ_DHW_SYSTEM,
    SZ_DHW_VALVE,
    SZ_HTG_VALVE,
    SZ_MAIN_TCS,
    SZ_ORPHANS,
    SZ_ORPHANS_HEAT,
    SZ_ORPHANS_HVAC,
    SZ_REMOTES,
    SZ_SENSOR,
    SZ_SENSORS,
    SZ_SYSTEM,
    SZ_UFH_SYSTEM,
    SZ_ZONES,
)
from ramses_rf.systems import Evohome, System, Zone
from ramses_rf.topology import Child
from ramses_tx import exceptions as exc
from ramses_tx.config import EngineConfig
from ramses_tx.const import SZ_ACTIVE_HGI, Code
from ramses_tx.schemas import extract_serial_port

from .const import (
    CONF_ADVANCED_FEATURES,
    CONF_AUTO_NOTIFY,
    CONF_COMMANDS,
    CONF_GATEWAY_TIMEOUT,
    CONF_LOST_THRESHOLD,
    CONF_MQTT_HGI_ID,
    CONF_MQTT_TOPIC,
    CONF_MQTT_USE_HA,
    CONF_PASSIVE_SCAN,
    CONF_RAMSES_RF,
    CONF_SCAN_INTERVAL,
    CONF_SCHEMA,
    DEFAULT_HGI_ID,
    DEFAULT_MQTT_TOPIC,
    DOMAIN,
    SIGNAL_NEW_DEVICES,
    SZ_CLIENT_STATE,
    SZ_DEVICE_COMMENTS,
    SZ_ENFORCE_KNOWN_LIST,
    SZ_KNOWN_LIST,
    SZ_PACKET_LOG,
    SZ_PACKETS,
    SZ_PORT_NAME,
    SZ_SCHEMA,
    SZ_SERIAL_PORT,
    SZ_TR_ALIAS,
    SZ_TR_CLASS,
    SZ_TR_DISABLED,
    SZ_TR_NAME,
    SZ_TR_SKIPPED,
)
from .discovery import DiscoveryManager
from .fan_handler import RamsesFanHandler
from .mqtt_bridge import RamsesMqttBridge
from .schemas import merge_schemas, schema_is_minimal, sync_learned_topology
from .services import RamsesServiceHandler
from .store import RamsesStore

if TYPE_CHECKING:
    from .entity import RamsesEntity
    from .number import RamsesNumberParam

_LOGGER = logging.getLogger(__name__)

SAVE_STATE_INTERVAL: Final[td] = td(minutes=5)
_DEVICE_ID_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9A-F]{2}:[0-9A-F]{6}$", re.I)
_EXTRACT_DEVICE_ID_RE: Final[re.Pattern[str]] = re.compile(
    r"[0-9A-F]{2}:[0-9A-F]{6}", re.I
)

# Generic Type for Entity Discovery to satisfy Pylance covariance
_T_Entity = TypeVar("_T_Entity", bound=RamsesRFEntity)


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
        self.discovery_manager: DiscoveryManager | None = None
        self._cached_discovery_state: dict[str, Any] | None = None
        self._suppress_reload: bool = False
        self._skip_topology_sync: bool = False

        # Redact port details for safe exchange of logs
        print_options = deepcopy(dict(self.options))  # need an extra copy
        if print_options.get("serial_port", None) is not None:
            ser_port = print_options.get("serial_port", "")
            if isinstance(ser_port, dict):
                if ser_port.get("port_name", "").startswith("mqtt://"):
                    print_options["serial_port"]["port_name"] = (
                        "mqtt://usr:pwd(at)url:1883"
                    )
        _LOGGER.debug("Config = %s", print_options)

        self.client: Gateway | None = None
        self._remotes: dict[str, dict[str, str]] = {}

        self._platform_setup_tasks: dict[str, asyncio.Task[Any]] = {}
        self._entities: dict[str, RamsesEntity] = {}  # domain entities
        self._device_info: dict[str, DeviceInfo] = {}

        # Discovered client objects...
        self._devices: list[Device] = []
        self._systems: list[System] = []
        self._zones: list[Zone] = []
        self._dhws: list[Zone] = []
        self._parameter_entities_pending: set[str] = set()
        self._parameter_entities_loaded: set[str] = set()
        self._parameter_entities_created: dict[str, RamsesNumberParam] = {}

        self._sem = Semaphore(value=1)

        # Initialize platforms dictionary to store platform references
        self.platforms: dict[str, Any] = {}
        self.learn_device_id: str | None = None

        # Load scan interval from options, default to 60s if missing
        scan_interval = entry.options.get(CONF_SCAN_INTERVAL, 60)
        _LOGGER.debug(
            "Coordinator initialized with scan_interval: %s seconds", scan_interval
        )

        # Initialize the DataUpdateCoordinator
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=td(seconds=scan_interval),
        )

    def _get_saved_packets(
        self, client_state: dict[str, Any]
    ) -> dict[str, dict[str, Any] | str]:
        """Filter cached packets to remove expired or unwanted entries.

        Extracts device IDs dynamically to enforce the known list, ensuring
        compatibility with varying packet string formats and JSON DTOs.
        """
        msg_code_filter = ["313F"]
        known_list = self.options.get(SZ_KNOWN_LIST, {})
        enforce_known_list = self.options[CONF_RAMSES_RF].get(SZ_ENFORCE_KNOWN_LIST)

        packets: dict[str, dict[str, Any] | str] = {}
        now = dt_util.now()

        # Iterate over packets from storage
        for dtm, pkt in client_state.get(SZ_PACKETS, {}).items():
            try:
                dt_obj = dt.fromisoformat(dtm)
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
            except ValueError:
                _LOGGER.warning(
                    "Ignoring cached packet with invalid timestamp: %s", dtm
                )
                continue

            # 1. Check age (keep last 24 hours)
            if dt_obj <= now - td(days=1):
                continue

            # Handle new PacketDTO dictionary format natively
            if isinstance(pkt, dict):
                # 2. Filter out unwanted message codes
                if pkt.get("code") in msg_code_filter:
                    continue

                # 3. Enforce known list dynamically
                if enforce_known_list:
                    found_devices = []
                    # Check raw L3 addresses (addr1/2/3) first — these are
                    # the legacy PacketDTO keys that ramses_rf's get_state()
                    # provides for known_list enforcement (PR 782).
                    # Fall back to logical src/dst for ramses_rf versions
                    # that only have PR 780 (no addr1/2/3 keys yet).
                    for key in ("addr1", "addr2", "addr3", "src", "dst"):
                        addr = pkt.get(key)
                        if not addr:
                            continue
                        if (
                            isinstance(addr, dict)
                            and addr.get("device_type") is not None
                            and addr.get("device_id") is not None
                        ):
                            # Reconstruct address string safely
                            found_devices.append(
                                f"{addr['device_type']:02d}:{addr['device_id']:06d}"
                            )
                        else:  # simple string passed in PacketDTO
                            found_devices.append(addr)

                    # If the packet contains no devices from our known_list, discard it
                    if not any(dev in known_list for dev in found_devices):
                        continue

            # Fallback for users migrating from legacy string-based caches
            else:
                # 2. Filter out unwanted message codes
                # Using string containment is safer against format changes than pkt[41:45]
                if any(f" {code} " in pkt for code in msg_code_filter):
                    continue

                # 3. Enforce known list dynamically
                if enforce_known_list:
                    # Extract all potential device IDs from the string
                    found_devices = _EXTRACT_DEVICE_ID_RE.findall(pkt)

                    # If the packet contains no devices from our known_list, discard it
                    if not any(dev in known_list for dev in found_devices):
                        continue

            packets[dtm] = pkt

        return packets

    async def async_setup(self) -> None:
        """Set up the RAMSES client and load configuration.

        Loads storage, restores remote commands, and initializes the Gateway client.
        """
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

        # 1b. Migration: when passive scan is enabled, check if known_list
        # has devices not in schema and migrate them.  For legacy setups
        # (passive scan off), the derivation logic already handles
        # known_list-only devices, so no migration is needed.
        config_schema = self.options.get(CONF_SCHEMA, {})
        advanced = self.entry.options.get(CONF_ADVANCED_FEATURES, {})
        if advanced.get(CONF_PASSIVE_SCAN, False):
            user_known_list = self.options.get(SZ_KNOWN_LIST, {})
            schema_device_ids = self._extract_schema_device_ids(config_schema)
            known_list_only = set(user_known_list.keys()) - schema_device_ids
            # Filter out HGI devices (gateways, handled by transport config)
            known_list_only = {d for d in known_list_only if not d.startswith("18:")}

            if known_list_only:
                _LOGGER.warning(
                    "Migration: %d known_list devices not in schema: %s. "
                    "Backing up and migrating to schema as orphans.",
                    len(known_list_only),
                    sorted(known_list_only),
                )
                # Backup before migration
                await self.store.async_save_backup(config_schema, user_known_list)

                # Migrate: add missing devices to schema as heat orphans
                migrated_schema = dict(config_schema)
                existing_orphans = list(migrated_schema.get(SZ_ORPHANS_HEAT, []))
                for device_id in sorted(known_list_only):
                    if device_id not in existing_orphans:
                        existing_orphans.append(device_id)
                if existing_orphans != list(config_schema.get(SZ_ORPHANS_HEAT, [])):
                    migrated_schema[SZ_ORPHANS_HEAT] = existing_orphans
                    self.options[CONF_SCHEMA] = migrated_schema
                    config_schema = migrated_schema
                    _LOGGER.info(
                        "Migration complete: schema now has %d orphan devices",
                        len(existing_orphans),
                    )

        # 2. Schema Handling
        _LOGGER.debug("CONFIG_SCHEMA: %s", config_schema)
        if not schema_is_minimal(config_schema):
            _LOGGER.warning("The config schema is not minimal (consider minimising it)")

        # Sanitise main_tcs: must point to a key that exists in the schema
        # and looks like a CTL (01:).  A stale/corrupt main_tcs (e.g. a TRV
        # ID from a bad sync_learned_topology cycle) will crash ramses_rf.
        main_tcs = config_schema.get(SZ_MAIN_TCS)
        if main_tcs and (
            main_tcs not in config_schema
            or not isinstance(config_schema.get(main_tcs), dict)
            or not str(main_tcs).startswith("01:")
        ):
            _LOGGER.warning(
                "Sanitising invalid main_tcs=%r (not a valid CTL ID in schema), "
                "clearing it",
                main_tcs,
            )
            config_schema = dict(config_schema)
            config_schema.pop(SZ_MAIN_TCS, None)
            self.options[CONF_SCHEMA] = config_schema

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

        start_kwargs: dict[str, Any] = {"cached_packets": cached_packets}

        await self.client.start(**start_kwargs)
        self.entry.async_on_unload(self._async_stop_client)

    async def async_start(self) -> None:
        """Start the coordinator and initiate the first refresh.

        Starts discovery loops, saves initial state, and triggers the first data update.
        """
        # Note: self.client.start() should have been called in async_setup

        # 1. Trigger the first discovery immediately
        #    We call this directly because we want entities found BEFORE we finish setup
        _LOGGER.debug("Coordinator: Starting initial discovery...")
        await self._discover_new_entities()

        # 2. Schedule the Discovery Loop
        #    This runs independently of the DataUpdateCoordinator's internal timer.
        self.entry.async_on_unload(
            async_track_time_interval(
                self.hass,
                self._async_discovery_task,
                td(seconds=self.entry.options.get(CONF_SCAN_INTERVAL, 60)),
            )
        )

        # 3. Start passive device scan if enabled
        advanced = self.entry.options.get(CONF_ADVANCED_FEATURES, {})
        if advanced.get(CONF_PASSIVE_SCAN, False) and self.client:
            await self._async_start_discovery_scan()

        # Trigger the first update immediately (calls _async_update_data)
        # This will raise ConfigEntryNotReady if it fails, which is handled by HA
        await self.async_config_entry_first_refresh()

        # Keep the dedicated interval for saving client state to disk
        self.entry.async_on_unload(
            async_track_time_interval(
                self.hass, self.async_save_client_state, SAVE_STATE_INTERVAL
            )
        )
        # On unload, save state but skip topology sync — the learned topology
        # from the dying coordinator should NOT overwrite a fresh-start schema
        # that the user (or simulator) has just cleared.
        self.entry.async_on_unload(self._async_save_on_unload)

    async def _async_start_discovery_scan(self) -> None:
        """Start the passive device scan engine and discovery manager."""
        from ramses_rf.discovery_scan import DiscoveryScan

        if not self.client:
            _LOGGER.warning("Cannot start discovery scan: client not initialized")
            return

        advanced = self.entry.options.get(CONF_ADVANCED_FEATURES, {})
        scan = DiscoveryScan(self.client)
        self.discovery_manager = DiscoveryManager(
            self.hass,
            scan,
            auto_notify=advanced.get(CONF_AUTO_NOTIFY, True),
            lost_threshold_days=advanced.get(CONF_LOST_THRESHOLD, 7),
        )

        # Restore persisted state
        stored = await self.store.async_load()
        from .discovery import SZ_DISCOVERY

        if stored.get(SZ_DISCOVERY):
            self.discovery_manager.restore_state(stored[SZ_DISCOVERY])

        # Schedule periodic checkpoint + check for new/lost devices.
        # Use 5 min interval for now — TODO: replace with a real-time
        # callback from ramses_rf's DiscoveryScan (see notepad.txt).
        self.entry.async_on_unload(
            async_track_time_interval(
                self.hass,
                self._async_discovery_checkpoint,
                td(minutes=5),
            )
        )
        # Run an immediate check after 10 seconds so new devices from
        # cached packets are detected quickly.
        unsub = async_call_later(self.hass, 10, self._async_discovery_checkpoint)
        self.entry.async_on_unload(unsub)
        self.entry.async_on_unload(self._async_stop_discovery_scan)
        _LOGGER.info("Passive device scan started")

    async def _async_discovery_checkpoint(self, _: dt | None = None) -> None:
        """Periodic checkpoint: check for new/lost devices and save state."""
        if not self.discovery_manager:
            return
        self.discovery_manager.check_for_new_devices()
        self.discovery_manager.check_for_lost_devices()
        await self.async_save_client_state()

    async def _async_stop_discovery_scan(self) -> None:
        """Stop the discovery scan engine.

        Saves discovery state before stopping so it can be restored on reload.
        """
        if self.discovery_manager:
            # Export and cache state before stopping, so async_save_client_state
            # (which runs later in the unload chain) still has it available
            self._cached_discovery_state = self.discovery_manager.export_state()
            _LOGGER.info(
                "Stopping discovery scan: caching %d metadata entries for save",
                len(self._cached_discovery_state.get("devices", {})),
            )
            await self.async_save_client_state()
            self.discovery_manager.stop()
            self.discovery_manager = None

    # ── Schema-as-single-source-of-truth ──────────────────────────────

    # Keys that ramses_cc adds to the schema dict but ramses_rf doesn't
    # understand.  They are stripped before passing the schema to the Gateway.
    _SCHEMA_EXTENSION_KEYS: Final[frozenset[str]] = frozenset({SZ_DEVICE_COMMENTS})

    @staticmethod
    def _extract_schema_device_ids(schema: dict[str, Any]) -> set[str]:
        """Extract all device IDs from a schema dict (for migration checks).

        Delegates to the same logic as ``_derive_known_list_from_schema``
        but returns only the device ID set.
        """
        # Reuse the derivation logic, just take the keys
        derived = RamsesCoordinator._derive_known_list_from_schema(schema)
        return set(derived.keys())

    @staticmethod
    def _strip_schema_extensions(schema: dict[str, Any]) -> dict[str, Any]:
        """Return a copy of *schema* with ramses_cc-only keys removed.

        ramses_rf's ``SCH_GLOBAL_SCHEMAS`` validator would reject our
        extension keys (``device_comments``) and ``_`` prefixed user traits
        (``_disabled``, ``_name``, etc.), so we strip them before passing
        the schema to the Gateway.

        Also strips ``None`` values for known optional keys like
        ``main_tcs`` — ramses_rf's validator rejects ``null`` even though
        the key is ``vol.Optional``.

        For HVAC (ventilation) devices (FAN, prefix ``30:``), ramses_rf
        requires at least one of ``remotes``/``sensors`` keys to be
        present.  We inject ``remotes: []`` when neither exists so that
        minimal schema entries like ``{"30:160000": {}}`` validate.
        """

        def _strip_traits(obj: Any) -> Any:
            """Recursively strip _ prefixed keys from dicts."""
            if isinstance(obj, dict):
                return {
                    k: _strip_traits(v)
                    for k, v in obj.items()
                    if not str(k).startswith("_")
                }
            return obj

        # First pass: collect _disabled/_skipped device IDs so we can remove
        # them from orphan lists, and collect un-disabled trait-only devices
        # so we can add them to orphans (so ramses_rf creates them)
        ctl_id = schema.get(SZ_MAIN_TCS)
        disabled_ids: set[str] = set()
        skipped_ids: set[str] = set()
        undisabled_ids: set[str] = set()
        for k, v in schema.items():
            if isinstance(v, dict) and _DEVICE_ID_RE.match(str(k)) and str(k) != ctl_id:
                if v.get(SZ_TR_DISABLED) is True:
                    disabled_ids.add(str(k))
                elif v.get(SZ_TR_SKIPPED) is True:
                    skipped_ids.add(str(k))
                elif v.get(SZ_TR_DISABLED) is False or v.get(SZ_TR_SKIPPED) is False:
                    # Explicitly un-disabled or un-skipped — needs to be in
                    # orphans so ramses_rf creates it
                    undisabled_ids.add(str(k))

        result: dict[str, Any] = {}
        for k, v in schema.items():
            if k in RamsesCoordinator._SCHEMA_EXTENSION_KEYS or v is None:
                continue
            # Remove _disabled and _skipped devices from orphan lists
            if k in (SZ_ORPHANS_HEAT, SZ_ORPHANS_HVAC) and isinstance(v, list):
                v = [d for d in v if d not in disabled_ids and d not in skipped_ids]
            # Track if the original had _ keys before stripping
            had_traits = isinstance(v, dict) and any(
                str(k2).startswith("_") for k2 in v
            )
            # Strip _ prefixed keys from values
            v = _strip_traits(v)
            # Inject remotes: [] for HVAC (FAN, 30:) devices that lack
            # remotes/sensors — ramses_rf requires at least one of them
            if (
                isinstance(v, dict)
                and _DEVICE_ID_RE.match(str(k))
                and str(k).startswith("30:")
                and "remotes" not in v
                and "sensors" not in v
            ):
                v = {**v, "remotes": []}
            # Drop trait-only entries (had _ keys, now empty after stripping)
            if (
                had_traits
                and isinstance(v, dict)
                and _DEVICE_ID_RE.match(str(k))
                and not v
            ):
                # Un-disabled trait-only entry: add to orphans instead
                # (ramses_rf would reject the empty dict)
                continue
            # Drop empty device entries (no traits, no topology) —
            # ramses_rf rejects empty dicts for device IDs.  Add to orphans
            # instead so the device is still created.
            if (
                not had_traits
                and isinstance(v, dict)
                and _DEVICE_ID_RE.match(str(k))
                and str(k) != ctl_id
                and not v
                and str(k) not in disabled_ids
                and str(k) not in skipped_ids
            ):
                undisabled_ids.add(str(k))
                continue
            result[k] = v

        # Add un-disabled trait-only devices to orphans so ramses_rf creates them.
        # When a user changes _disabled from true to false, the device entry
        # becomes trait-only (no topology keys).  Without this, the device would
        # be dropped entirely and ramses_rf would not create it.
        if undisabled_ids:
            heat_orphans = set(result.get(SZ_ORPHANS_HEAT, []))
            hvac_orphans = set(result.get(SZ_ORPHANS_HVAC, []))
            for dev_id in undisabled_ids:
                if dev_id.startswith("30:"):
                    hvac_orphans.add(dev_id)
                else:
                    heat_orphans.add(dev_id)
            if heat_orphans:
                result[SZ_ORPHANS_HEAT] = sorted(heat_orphans)
            if hvac_orphans:
                result[SZ_ORPHANS_HVAC] = sorted(hvac_orphans)

        return result

    @staticmethod
    def _derive_known_list_from_schema(
        schema: dict[str, Any],
        *,
        user_overrides: dict[str, Any] | None = None,
        schema_is_ssot: bool = False,
    ) -> dict[str, Any]:
        """Derive a known_list from the schema structure.

        Walks the schema (same logic as ``_extract_device_ids_from_schema``
        in services.py) and returns a known_list dict where each device ID
        maps to an empty traits dict ``{}``.  This is enough for
        ``enforce_known_list`` to allow the device through — ramses_rf will
        infer the class from the address prefix and message codes.

        If *user_overrides* is provided, those entries take precedence for
        any traits the user has set (alias, faked, class, scheme, bound).

        When *schema_is_ssot* is True (passive scan mode), devices that are
        in user_overrides but NOT in the schema are silently dropped — the
        schema is the single source of truth, and stale known_list entries
        must not re-create devices the user has cleared.  When False (legacy
        mode), those devices are kept for backward compatibility.

        :param schema: The global schema dict (may contain extension keys).
        :param user_overrides: Optional known_list entries from config that
            override the derived defaults.
        :param schema_is_ssot: When True, drop known_list-only devices (not
            in schema) instead of keeping them for backward compatibility.
        :return: A known_list dict suitable for ``GatewayConfig.known_list``.
        """
        # Collect all device IDs from the schema structure
        device_ids: set[str] = set()

        # Main TCS (the CTL)
        if ctl_id := schema.get(SZ_MAIN_TCS):
            device_ids.add(ctl_id)

        for key, value in schema.items():
            # Skip non-device-id keys and our extension keys
            if key in RamsesCoordinator._SCHEMA_EXTENSION_KEYS:
                continue
            if key in (
                SZ_MAIN_TCS,
                SZ_ORPHANS_HEAT,
                SZ_ORPHANS_HVAC,
                "transport_constructor",
            ):
                continue
            if not _DEVICE_ID_RE.match(str(key)):
                continue

            # key is a device_id (CTL or FAN)
            device_ids.add(str(key))

            if not isinstance(value, dict):
                continue

            # Heat TCS structure
            if isinstance(value.get(SZ_SYSTEM), dict):
                if app_id := value[SZ_SYSTEM].get(SZ_APPLIANCE_CONTROL):
                    device_ids.add(app_id)

            if isinstance(value.get(SZ_DHW_SYSTEM), dict):
                dhw = value[SZ_DHW_SYSTEM]
                if sensor_id := dhw.get(SZ_SENSOR):
                    device_ids.add(sensor_id)
                if valve_id := dhw.get(SZ_DHW_VALVE):
                    device_ids.add(valve_id)
                if valve_id := dhw.get(SZ_HTG_VALVE):
                    device_ids.add(valve_id)

            if isinstance(value.get(SZ_UFH_SYSTEM), dict):
                for ufc_id in value[SZ_UFH_SYSTEM]:
                    if _DEVICE_ID_RE.match(str(ufc_id)):
                        device_ids.add(str(ufc_id))

            if isinstance(value.get(SZ_ZONES), dict):
                for zone_data in value[SZ_ZONES].values():
                    if not isinstance(zone_data, dict):
                        continue
                    if sensor_id := zone_data.get(SZ_SENSOR):
                        device_ids.add(sensor_id)
                    for act_id in zone_data.get(SZ_ACTUATORS, []):
                        device_ids.add(act_id)

            for orphan_id in value.get(SZ_ORPHANS, []):
                device_ids.add(orphan_id)

            # HVAC structure
            for remote_id in value.get(SZ_REMOTES, []):
                device_ids.add(remote_id)
            for sensor_id in value.get(SZ_SENSORS, []):
                device_ids.add(sensor_id)

        # Global orphans
        for orphan_id in schema.get(SZ_ORPHANS_HEAT, []):
            device_ids.add(orphan_id)
        for orphan_id in schema.get(SZ_ORPHANS_HVAC, []):
            device_ids.add(orphan_id)

        # Build the known_list, excluding _disabled and _skipped devices
        excluded: set[str] = set()
        for key, value in schema.items():
            if (
                isinstance(value, dict)
                and _DEVICE_ID_RE.match(str(key))
                and (
                    value.get(SZ_TR_DISABLED) is True
                    or value.get(SZ_TR_SKIPPED) is True
                )
            ):
                excluded.add(str(key))

        known_list: dict[str, Any] = {}
        for device_id in device_ids:
            if device_id in excluded:
                continue
            # Extract _ traits from the device's top-level schema entry
            entry = schema.get(device_id)
            traits: dict[str, Any] = {}
            if isinstance(entry, dict):
                if entry.get(SZ_TR_CLASS):
                    traits["class"] = entry[SZ_TR_CLASS]
                if entry.get(SZ_TR_ALIAS):
                    traits["alias"] = entry[SZ_TR_ALIAS]
                if entry.get(SZ_TR_NAME):
                    # _name maps to alias for ramses_rf (display name)
                    traits.setdefault("alias", entry[SZ_TR_NAME])
            known_list[device_id] = traits

        # Apply user overrides (deep merge: user traits win)
        if user_overrides:
            for device_id, traits in user_overrides.items():
                if device_id in excluded:
                    continue
                if device_id not in known_list:
                    if schema_is_ssot:
                        # Schema is SSOT: drop known_list-only devices.
                        # They are stale entries from before the schema was
                        # cleared — keeping them would re-create devices the
                        # user just removed via fresh start / clear cache.
                        #
                        # Exception: the HGI (gateway) must always be in the
                        # known_list — it is never in the schema (it's the
                        # scanner, not a scanned device) but enforce_known_list
                        # would reject its own packets without it.
                        is_hgi = (
                            isinstance(traits, dict) and traits.get("class") == "HGI"
                        )
                        if not is_hgi:
                            continue
                    # Legacy mode: keep for backward compatibility
                    known_list[device_id] = (
                        dict(traits) if isinstance(traits, dict) else traits
                    )
                elif isinstance(traits, dict) and isinstance(
                    known_list[device_id], dict
                ):
                    known_list[device_id] = {**known_list[device_id], **traits}

        return known_list

    def _create_client(self, schema: dict[str, Any]) -> Gateway:
        """Create and configure a new RAMSES client instance."""

        raw_config = self.options.get(CONF_RAMSES_RF, {}).copy()

        # When passive scan is enabled, force enforce_known_list so ramses_rf
        # doesn't auto-create devices from traffic — the only path to entity
        # creation should be through accept_discovered_device.
        advanced = self.entry.options.get(CONF_ADVANCED_FEATURES, {})
        if advanced.get(CONF_PASSIVE_SCAN, False):
            if not raw_config.get(SZ_ENFORCE_KNOWN_LIST):
                _LOGGER.warning(
                    "Passive scan is enabled but enforce_known_list is off — "
                    "forcing enforce_known_list=True to prevent auto-creation "
                    "of entities from traffic. Accept discovered devices via "
                    "the accept_discovered_device service instead."
                )
                raw_config[SZ_ENFORCE_KNOWN_LIST] = True

        engine_kwargs: dict[str, Any] = {}
        gateway_kwargs: dict[str, Any] = {}

        engine_fields = {f.name for f in dataclasses.fields(EngineConfig)}
        gateway_fields = {f.name for f in dataclasses.fields(GatewayConfig)}

        for k, v in raw_config.items():
            if k in engine_fields:
                engine_kwargs[k] = v
            elif k in gateway_fields and k != "engine":
                gateway_kwargs[k] = v

        engine_kwargs["app_context"] = self.hass

        # ── Schema as single source of truth ──────────────────────────
        # Derive known_list from the schema (device IDs from topology),
        # then merge user overrides (alias, faked, class, scheme, bound).
        user_known_list = self.options.get(SZ_KNOWN_LIST, {})
        # When passive scan is enabled, the schema is SSOT — stale
        # known_list entries must not re-create cleared devices.
        schema_is_ssot = bool(advanced.get(CONF_PASSIVE_SCAN, False))
        derived_known_list = self._derive_known_list_from_schema(
            schema, user_overrides=user_known_list, schema_is_ssot=schema_is_ssot
        )
        # Strip commands from traits (ramses_rf doesn't accept them)
        sanitized_known_list = {
            device_id: (
                {k: v for k, v in traits.items() if k != CONF_COMMANDS}
                if isinstance(traits, dict)
                else traits
            )
            for device_id, traits in derived_known_list.items()
        }
        # Device traits (class/alias/faked/bound/scheme) are consumed by
        # ramses_rf DeviceRegistry via GatewayConfig.known_list.
        gateway_kwargs["known_list"] = sanitized_known_list

        packet_log = self.options.get(SZ_PACKET_LOG, {})
        engine_kwargs["packet_log"] = packet_log

        # Strip ramses_cc-only extension keys before passing to ramses_rf
        gateway_kwargs["schema"] = self._strip_schema_extensions(schema)

        gateway_timeout = self.options.get(CONF_GATEWAY_TIMEOUT)
        if gateway_timeout is not None:
            gateway_kwargs["gateway_timeout"] = gateway_timeout

        # Detect the transport type from port_name / flags.
        _serial_port_opts = self.options.get(SZ_SERIAL_PORT, {})
        _port_name_raw = _serial_port_opts.get(SZ_PORT_NAME, "")
        _is_zigbee = isinstance(_port_name_raw, str) and _port_name_raw.startswith(
            "zigbee://"
        )
        _is_mqtt_ha_port = (
            isinstance(_port_name_raw, str) and _port_name_raw == "mqtt_ha"
        )
        _is_mqtt_flag = bool(self.options.get(CONF_MQTT_USE_HA))

        if not _port_name_raw:
            mqtt_entries = self.hass.config_entries.async_entries("mqtt")
            if mqtt_entries:
                _LOGGER.warning(
                    "No serial_port configured; defaulting to Home Assistant MQTT transport. "
                    "Please re-open the Ramses RF options and re-save the chosen transport."
                )
                _serial_port_opts[SZ_PORT_NAME] = "mqtt_ha"
                _port_name_raw = "mqtt_ha"
                _is_mqtt_ha_port = True
                _is_mqtt_flag = True
            else:
                raise ConfigEntryNotReady(
                    "No serial port configured. Open the Ramses RF options flow to select a transport."
                )

        _is_mqtt_ha = _is_mqtt_flag or _is_mqtt_ha_port

        if _is_zigbee:
            # ZigbeeTransport — handled natively by transport_factory in ramses_tx.
            # No MQTT broker is required; no RamsesMqttBridge is created.
            # hass reaches ZigbeeTransport via GatewayConfig.app_context (PR #505).
            engine_config = EngineConfig(**engine_kwargs)
            gwy_config = GatewayConfig(engine=engine_config, **gateway_kwargs)

            return Gateway(
                port_name=_port_name_raw,
                config=gwy_config,
                loop=self.hass.loop,
            )

        if _is_mqtt_ha:
            # RamsesMqttBridge path — uses HA MQTT
            if not self.hass.config_entries.async_entries("mqtt"):
                raise ConfigEntryNotReady(
                    "Home Assistant MQTT integration is not set up"
                )

            # Retrieve config options
            mqtt_topic = self.options.get(CONF_MQTT_TOPIC, DEFAULT_MQTT_TOPIC)
            hgi_id = self.options.get(CONF_MQTT_HGI_ID, DEFAULT_HGI_ID)

            self.mqtt_bridge = RamsesMqttBridge(self.hass, mqtt_topic, hgi_id)

            # Ensure the bridge unsubscribes from MQTT on shutdown
            self.entry.async_on_unload(self.mqtt_bridge.close)

            # Pass the configured HGI ID to ramses_rf.
            engine_kwargs["hgi_id"] = hgi_id

            # Inject HGI into known_list (redundant but safe fallback — config_flow
            # handles this, but kept here to satisfy ramses_rf schema validation).
            device_entry = sanitized_known_list.setdefault(hgi_id, {})
            device_entry["class"] = "HGI"
            device_entry.setdefault("alias", "ramses_esp")

            engine_config = EngineConfig(**engine_kwargs)
            gwy_config = GatewayConfig(engine=engine_config, **gateway_kwargs)

            return Gateway(
                port_name=_port_name_raw or "mqtt",
                config=gwy_config,
                loop=self.hass.loop,
                transport_constructor=self.mqtt_bridge.async_transport_factory,
            )

        # Standard Serial/USB setup
        port_name, port_config = extract_serial_port(self.options[SZ_SERIAL_PORT])
        engine_kwargs["port_config"] = port_config

        engine_config = EngineConfig(**engine_kwargs)
        gwy_config = GatewayConfig(engine=engine_config, **gateway_kwargs)

        return Gateway(
            port_name=port_name,
            config=gwy_config,
            loop=self.hass.loop,
        )

    async def _async_stop_client(self) -> None:
        """Safely stop the RAMSES client, catching transport exceptions on teardown."""
        if not self.client:
            return

        _LOGGER.debug("Coordinator: Initiating safe shutdown of RAMSES client")
        try:
            # This triggers ramses_tx teardown and logger buffer flushes
            await self.client.stop()
        except serial.SerialException as err:
            _LOGGER.debug(
                "Serial port disconnected or busy during teardown (likely due to buffer flush): %s",
                err,
            )
        except (
            exc.TransportError,
            TimeoutError,
        ) as err:
            _LOGGER.debug(
                "Transport timeout/error during RAMSES client shutdown: %s", err
            )
        except Exception as err:
            _LOGGER.warning("Unexpected error while stopping RAMSES client: %s", err)

    async def _async_save_on_unload(self) -> None:
        """Save client state during unload, skipping topology sync.

        During unload (e.g. reload, fresh start), the learned topology from
        the dying coordinator must NOT be written back to the config entry —
        that would overwrite a freshly-cleared schema and defeat the fresh
        start.  We still save packets and discovery state.
        """
        self._skip_topology_sync = True
        try:
            await self.async_save_client_state()
        finally:
            self._skip_topology_sync = False

    async def async_save_client_state(self, _: dt | None = None) -> None:
        """Save the current state of the RAMSES client to persistent storage.

        :param _: Optional datetime argument from async_track_time_interval.
        """

        if not self.client:
            _LOGGER.debug("Cannot save state: Client not initialized")
            return

        # Support both async (new) and sync (old) client.get_state()
        # Cast to Any prevents Pylance from inferring Never on the else block
        result = cast(Any, self.client.get_state())

        if inspect.isawaitable(result):
            schema, packets = await result
        else:
            schema, packets = result

        _LOGGER.info("Saving the client state cache (packets, schema)")

        # Sync learned topology from ramses_rf back to the config entry.
        # The learned schema (from gateway.schema()) may have richer topology
        # (zones, bindings) than the config entry schema.  If so, write it back.
        # Skip during unload (fresh start / reload) so we don't overwrite a
        # freshly-cleared schema with stale learned topology.
        if not self._skip_topology_sync:
            config_schema = self.options.get(CONF_SCHEMA, {})
            enriched = sync_learned_topology(config_schema, schema)
            if enriched is not None:
                _LOGGER.info("Learned topology is richer than config, syncing back")
                new_options = dict(self.options)
                new_options[CONF_SCHEMA] = enriched
                self.options = new_options
                # Suppress the reload that async_update_entry would trigger,
                # since the running coordinator already has the updated options
                # and a reload would tear down the transport while pending
                # _send_cmd tasks are still in flight (causing lingering tasks).
                self._suppress_reload = True
                try:
                    self.hass.config_entries.async_update_entry(
                        self.entry, options=new_options
                    )
                finally:
                    self._suppress_reload = False
        else:
            # During unload: save the config schema (not the learned schema)
            # to .storage, so the cached schema doesn't override a freshly-
            # cleared config schema on the next restart.  The learned schema
            # from the dying coordinator is stale topology that the user may
            # have just cleared — it must not survive in the cache.
            schema = self.options.get(CONF_SCHEMA, {})

        # Explicitly declare intermediate dict to solve Pylance 'Never is not iterable'
        remotes_from_entities: dict[str, Any] = {
            k: getattr(v, "_commands", {})
            for k, v in self._entities.items()
            if hasattr(v, "_commands")
        }
        remotes = self._remotes | remotes_from_entities

        discovery_state = (
            self.discovery_manager.export_state()
            if self.discovery_manager
            else getattr(self, "_cached_discovery_state", None)
        )

        _LOGGER.info(
            "Saving state: discovery_manager=%s, cached=%s, discovery_devices=%d",
            bool(self.discovery_manager),
            bool(getattr(self, "_cached_discovery_state", None)),
            len(discovery_state.get("devices", {})) if discovery_state else 0,
        )

        await self.store.async_save(schema, packets, remotes, discovery_state)

    def _get_device(self, device_id: str) -> Any | None:
        """Get a device by ID."""
        if dev := next((d for d in self._devices if d.id == device_id), None):
            return dev
        if self.client and hasattr(self.client, "device_registry"):
            return self.client.device_registry.device_by_id.get(cast(Any, device_id))
        return None

    def async_register_platform(
        self,
        platform: EntityPlatform,
        add_new_devices: Callable[[RamsesRFEntity], None],
    ) -> None:
        """Register a platform that has entities with the coordinator.

        :param platform: The HA platform instance (e.g. climate, sensor).
        :param add_new_devices: Callback to add new devices to HA.
        """
        platform_str = str(getattr(platform, "domain", platform))
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
        """Unload all platforms associated with this integration.

        :return: True if all platforms unloaded successfully.
        """
        tasks: list[Coroutine[Any, Any, bool]] = [
            self.hass.config_entries.async_forward_entry_unload(self.entry, platform)
            for platform, task in self._platform_setup_tasks.items()
            if not task.cancel()
        ]
        result = all(await asyncio.gather(*tasks))
        _LOGGER.debug("Platform unload completed with result: %s", result)
        return result

    async def _async_update_device(self, device: RamsesRFEntity) -> None:
        """
        Update device information in the device registry.

        :param device: The RamsesRF entity to update.
        :type device: RamsesRFEntity
        :return: None
        :rtype: None
        """

        # Safely resolve the device name, handling properties, methods, and coroutines
        device_name: str | None = None
        name_attr = getattr(device, "name", None)

        if name_attr:
            raw_name: Any = name_attr
            if callable(raw_name):
                with suppress(TypeError):
                    raw_name = raw_name()

            if inspect.isawaitable(raw_name):
                raw_name = await raw_name

            device_name = str(raw_name) if raw_name else None

        # Fallback names if the device doesn't supply a valid one
        if not device_name:
            if isinstance(device, System):
                device_name = f"Controller {device.id}"
            elif getattr(device, "_SLUG", None):
                device_name = f"{getattr(device, '_SLUG', None)} {device.id}"
            else:
                device_name = str(device.id)

        info: dict[str, Any] | None = None
        state_store = getattr(device, "state_store", None)
        if state_store:
            info = await state_store._msg_value_code(Code._10E0)

        model: str | None = (
            info.get("description") if info else getattr(device, "_SLUG", None)
        )

        device_registry = dr.async_get(self.hass)

        via_device: tuple[str, str] | None = None
        if isinstance(device, Zone) and device.tcs:
            _LOGGER.info(f"ZONE {model} via_device SET to {device.tcs.id}")
            via_device = (DOMAIN, str(device.tcs.id))
        elif isinstance(device, Child) and getattr(device, "_parent", None):
            parent = getattr(device, "_parent", None)
            parent_id = getattr(parent, "id", None) if parent else None
            _LOGGER.info(f"CHILD {model} via_device SET to {parent_id}")
            if parent_id:
                via_device = (DOMAIN, str(parent_id))
        else:
            via_device = None

        # Conditionally assemble kwargs to protect HA TypedDict strict checks
        kwargs: dict[str, Any] = {}
        if via_device is not None:
            kwargs["via_device"] = via_device

        device_info = DeviceInfo(
            identifiers={(DOMAIN, str(device.id))},
            name=device_name,
            manufacturer=None,
            model=model,
            serial_number=str(device.id),
            **kwargs,
        )

        if self._device_info.get(str(device.id)) == device_info:
            return

        self._device_info[str(device.id)] = device_info

        device_registry.async_get_or_create(
            config_entry_id=self.entry.entry_id, **device_info
        )

    async def _async_update_data(self) -> None:
        """Fetch data from the RAMSES RF client."""
        _LOGGER.debug("Coordinator: _async_update_data called (Heartbeat)")
        if not self.client:
            _LOGGER.debug(
                "Coordinator: (_async_update_data) Client is None, skipping update"
            )
            return

        # The Coordinator is now only responsible for updating entities that already exist.
        # If ramses_rf pushes updates via callbacks, you might not even need logic here.
        # But if you need to poll for specific values (e.g. fault status), do it here.

        return None

    async def _async_discovery_task(self, _now: dt | None = None) -> None:
        """Wrapper to call discovery from the interval listener."""
        try:
            await self._discover_new_entities()
        except Exception as err:
            _LOGGER.error("Discovery error: %s", err)

    async def _discover_new_entities(self) -> None:
        """Discover new devices in the client and register them with HA."""
        if not self.client:
            return

        gwy: Gateway = self.client

        engine = getattr(gwy, "_engine", None)
        transport = getattr(engine, "_transport", None) or getattr(
            gwy, "_transport", None
        )
        active_hgi_id = None
        if transport is not None:
            with suppress(AttributeError, KeyError, TypeError):
                active_hgi_id = transport.get_extra_info(SZ_ACTIVE_HGI)
        if not active_hgi_id:
            active_hgi_id = getattr(engine, "_hgi_id", None)
        if (
            isinstance(active_hgi_id, str)
            and _DEVICE_ID_RE.match(active_hgi_id)
            and active_hgi_id not in gwy.device_registry.device_by_id
        ):
            with suppress(Exception):
                gwy.device_registry.get_device(cast(Any, active_hgi_id))

        # Snapshot the lists to avoid RuntimeError if ramses_rf updates them continuously
        # This fixes the silent failure where list changes size during iteration
        current_devices = list(gwy.device_registry.devices)
        current_systems = list(gwy.device_registry.systems)

        # --- DIAGNOSTIC LOGGING ---
        # This will reveal if ramses_rf has actually found any devices.
        _LOGGER.info(
            "Discovery: Devices=%s, Systems=%s",
            len(current_devices),
            len(current_systems),
        )
        if len(current_devices) > 0:
            _LOGGER.debug("Discovered Devices: %s", [d.id for d in current_devices])

        async def async_add_entities(
            platform: str, devices: Sequence[RamsesRFEntity]
        ) -> None:
            if not devices:
                return
            await self._async_setup_platform(platform)
            async_dispatcher_send(
                self.hass, SIGNAL_NEW_DEVICES.format(platform), devices
            )

        def find_new_entities(
            known: list[_T_Entity], current: list[_T_Entity]
        ) -> tuple[list[_T_Entity], list[_T_Entity]]:
            new = [x for x in current if x not in known]
            return known + new, new

        # Explicit typing ensures we bypass list invariance issues without casting
        current_evo_systems: list[System] = [
            s for s in current_systems if isinstance(s, Evohome)
        ]
        self._systems, new_systems = find_new_entities(
            self._systems, current_evo_systems
        )

        current_zones: list[Zone] = [
            z for s in current_systems if isinstance(s, Evohome) for z in s.zones
        ]
        self._zones, new_zones = find_new_entities(self._zones, current_zones)

        # Cast element directly in comprehension to securely enforce list[Zone]
        current_dhws: list[Zone] = [
            cast(Zone, s.dhw)
            for s in current_systems
            if isinstance(s, Evohome) and s.dhw
        ]
        self._dhws, new_dhws = find_new_entities(self._dhws, current_dhws)

        self._devices, new_devices = find_new_entities(self._devices, current_devices)

        # Process new devices for fan logic
        # Systems/DHWs must be processed before Devices to ensure via_device parents exist
        for device in new_systems + new_dhws + new_zones + new_devices:
            await self.fan_handler.async_setup_fan_device(cast(Device, device))
            # Register device in registry once upon discovery
            await self._async_update_device(device)

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
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_bind_device(call)

    async def async_force_update(self, _: ServiceCall) -> None:
        """Force an immediate update of all device states.

        :param _: Unused service call argument.
        """
        await self.async_refresh()

    async def async_sync_topology(self, _: ServiceCall) -> None:
        """Sync learned topology to the config entry immediately.

        Triggers the same save + sync_learned_topology cycle that normally
        runs every 5 minutes (SAVE_STATE_INTERVAL), so users don't have to
        wait after ramses_rf has learned new topology (e.g. from 000C).

        :param _: Unused service call argument.
        """
        _LOGGER.info("Manual topology sync requested (sync_topology service)")
        await self.async_save_client_state()

    async def async_send_packet(self, call: ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_send_packet(call)

    async def async_discover_known_devices(self, call: ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_discover_known_devices(call)

    async def async_get_discovered_devices(self, call: ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_get_discovered_devices(call)

    async def async_accept_discovered_device(self, call: ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_accept_discovered_device(call)

    async def async_discard_discovered_device(self, call: ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_discard_discovered_device(call)

    async def async_remove_discovered_device(self, call: ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_remove_discovered_device(call)

    async def async_enable_discovered_device(self, call: ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_enable_discovered_device(call)

    async def async_disable_discovered_device(self, call: ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_disable_discovered_device(call)

    async def async_add_faked_rem(self, call: ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call object containing parameters.
        """
        await self.service_handler.async_add_faked_rem(call)

    async def async_get_fan_param(self, call: dict[str, Any] | ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call or dictionary containing parameters.
        """
        await self.service_handler.async_get_fan_param(call)

    async def _async_run_fan_param_sequence(
        self, call: dict[str, Any] | ServiceCall
    ) -> None:
        """Delegate to Service Handler to run the fan parameter sequence.

        :param call: The service call or dictionary containing parameters.
        """
        await self.service_handler._async_run_fan_param_sequence(call)

    def get_all_fan_params(self, call: dict[str, Any] | ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call or dictionary containing parameters.
        """
        # Note: get_all_fan_params is not async, it wraps the async call in a task
        self.hass.async_create_task(
            self.service_handler._async_run_fan_param_sequence(call)
        )

    async def async_set_fan_param(self, call: dict[str, Any] | ServiceCall) -> None:
        """Delegate to Service Handler.

        :param call: The service call or dictionary containing parameters.
        """
        await self.service_handler.async_set_fan_param(call)
