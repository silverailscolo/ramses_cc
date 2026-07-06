"""Schemas for RAMSES integration."""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import timedelta as td
from typing import Any, Final

import voluptuous as vol  # type: ignore[import-untyped, unused-ignore]
from homeassistant.const import CONF_SCAN_INTERVAL
from homeassistant.helpers import config_validation as cv

from ramses_rf.config import sch_global_traits_dict_factory
from ramses_rf.helpers import deep_merge, is_subset, shrink
from ramses_rf.schemas import (
    SCH_GATEWAY_CONFIG,
    SCH_GLOBAL_SCHEMAS_DICT,
    SCH_RESTORE_CACHE_DICT,
    SZ_APPLIANCE_CONTROL,
    SZ_BOUND_TO,
    SZ_CLASS,
    SZ_CONFIG,
    SZ_DHW_SYSTEM,
    SZ_MAIN_TCS,
    SZ_ORPHANS,
    SZ_ORPHANS_HEAT,
    SZ_ORPHANS_HVAC,
    SZ_REMOTES,
    SZ_RESTORE_CACHE,
    SZ_SENSOR,
    SZ_SENSORS,
    SZ_SYSTEM,
)
from ramses_tx.const import (
    COMMAND_REGEX,
    DEFAULT_GAP_DURATION,
    # DEFAULT_NUM_REPEATS,  # use 3 in ramses_cc Actions, not 0 like ramses_tx
    MAX_GAP_DURATION,  # renamed from local MAX_DELAY_SECS
    MAX_NUM_REPEATS,
    MIN_GAP_DURATION,  # renamed from local MIN_DELAY_SECS
    MIN_NUM_REPEATS,
    SZ_ZONES,
)
from ramses_tx.schemas import (
    SCH_ENGINE_DICT,
    SZ_BLOCK_LIST,
    SZ_KNOWN_LIST,
    SZ_PORT_CONFIG,
    SZ_SERIAL_PORT,
    extract_serial_port,
    sch_packet_log_dict_factory,
    sch_serial_port_dict_factory,
)

from .const import (
    ATTR_ACTIVE,
    ATTR_CO2_LEVEL,
    ATTR_COMMAND,
    ATTR_DELAY_SECS,
    ATTR_DEVICE_ID,
    ATTR_DIFFERENTIAL,
    ATTR_DURATION,
    ATTR_INDOOR_HUMIDITY,
    ATTR_LOCAL_OVERRIDE,
    ATTR_MAX_TEMP,
    ATTR_MIN_TEMP,
    ATTR_MODE,
    ATTR_MULTIROOM,
    ATTR_NUM_ENTRIES,
    ATTR_NUM_REPEATS,
    ATTR_OPENWINDOW,
    ATTR_OVERRUN,
    ATTR_PERIOD,
    ATTR_SCHEDULE,
    ATTR_SETPOINT,
    ATTR_TEMPERATURE,
    ATTR_TIMEOUT,
    ATTR_UNTIL,
    CONF_ADVANCED_FEATURES,
    CONF_AUTO_NOTIFY,
    CONF_COMMANDS,
    CONF_DEV_MODE,
    CONF_LOST_THRESHOLD,
    CONF_MESSAGE_EVENTS,
    CONF_PASSIVE_SCAN,
    CONF_RAMSES_RF,
    CONF_SEND_PACKET,
    CONF_UNKNOWN_CODES,
    SZ_DEVICE_COMMENTS,
    SystemMode,
    ZoneMode,
)

_SchemaT = dict[str, Any]

_LOGGER = logging.getLogger(__name__)

# send_command service action
DEFAULT_NUM_REPEATS: Final[int] = 3  # override ramses_rf DEFAULT_NUM_REPEATS

# Configuration schema for Integration/domain
SCAN_INTERVAL_DEFAULT = td(seconds=60)
SCAN_INTERVAL_MINIMUM = td(seconds=3)

# Schema regex matches
_SCH_DEVICE_ID = cv.matches_regex(r"^[0-9]{2}:[0-9]{6}$")
_SCH_CMD_CODE = cv.matches_regex(r"^[0-9A-F]{4}$")
_SCH_DOM_IDX = cv.matches_regex(r"^[0-9A-F]{2}$")
_SCH_PARAM_ID = vol.All(cv.string, cv.matches_regex(r"^[0-9A-F]{2}$"))
_SCH_COMMAND = cv.matches_regex(COMMAND_REGEX.pattern)

SCH_ADVANCED_FEATURES = vol.Schema(
    {
        vol.Optional(CONF_SEND_PACKET, default=False): cv.boolean,
        vol.Optional(CONF_MESSAGE_EVENTS, default=None): vol.Any(None, cv.is_regex),
        vol.Optional(CONF_DEV_MODE): cv.boolean,
        vol.Optional(CONF_UNKNOWN_CODES): cv.boolean,
        vol.Optional(CONF_PASSIVE_SCAN, default=False): cv.boolean,
        vol.Optional(CONF_AUTO_NOTIFY, default=True): cv.boolean,
        vol.Optional(CONF_LOST_THRESHOLD, default=7): vol.All(
            cv.positive_int, vol.Range(min=1, max=90)
        ),
    }
)

# Define the traits for FAN devices
FAN_TRAITS = {
    vol.Optional(SZ_BOUND_TO): vol.Any(None, _SCH_DEVICE_ID),
    vol.Optional(CONF_COMMANDS): dict,
}

SCH_GLOBAL_TRAITS_DICT, SCH_TRAITS = sch_global_traits_dict_factory(
    hvac_traits=FAN_TRAITS
)

SCH_GATEWAY_CONFIG = SCH_GATEWAY_CONFIG.extend(
    SCH_ENGINE_DICT,
    extra=vol.PREVENT_EXTRA,
)

SCH_PACKET_LOG = sch_packet_log_dict_factory(default_backups=7)

SCH_DOMAIN_CONFIG = (
    vol.Schema(
        {
            vol.Optional(CONF_RAMSES_RF, default={}): SCH_GATEWAY_CONFIG,
            vol.Optional(CONF_SCAN_INTERVAL, default=SCAN_INTERVAL_DEFAULT): vol.All(
                cv.time_period, vol.Range(min=SCAN_INTERVAL_MINIMUM)
            ),
            vol.Optional(CONF_ADVANCED_FEATURES, default={}): SCH_ADVANCED_FEATURES,
        },
        extra=vol.PREVENT_EXTRA,  # will be system, orphan schemas for ramses_rf
    )
    .extend(SCH_GLOBAL_SCHEMAS_DICT)
    .extend(SCH_GLOBAL_TRAITS_DICT)
    .extend(sch_packet_log_dict_factory(default_backups=7))
    .extend(SCH_RESTORE_CACHE_DICT)
    .extend(sch_serial_port_dict_factory())
)

SCH_MINIMUM_TCS = vol.Schema(
    {
        vol.Optional(SZ_SYSTEM): vol.Schema(
            {vol.Required(SZ_APPLIANCE_CONTROL): vol.Match(r"^10:[0-9]{6}$")}
        ),
        vol.Optional(SZ_ZONES, default={}): vol.Schema(
            {
                vol.Required(str): vol.Schema(
                    {vol.Required(SZ_SENSOR): vol.Match(r"^01:[0-9]{6}$")}
                )
            }
        ),
    },
    extra=vol.PREVENT_EXTRA,
)


def normalise_config(config: _SchemaT) -> tuple[str, _SchemaT, _SchemaT]:
    """Return a port/client_config/coordinator_config for the library.

    Extracts and separates the configuration into three parts: the serial port name,
    the configuration for the ramses_rf library (client), and the configuration
    for the HA coordinator (including polling intervals and remote commands).

    :param config: The raw configuration dictionary from Home Assistant.
    :return: A tuple containing:
        - The serial port name (str).
        - The client/library configuration dictionary (_SchemaT).
        - The coordinator configuration dictionary (_SchemaT).
    """

    config = deepcopy(config)

    config[SZ_CONFIG] = config.pop(CONF_RAMSES_RF)

    port_name, port_config = extract_serial_port(config.pop(SZ_SERIAL_PORT))

    # Check if 'v' is truthy (not None) before calling .get()
    # This prevents crashes when a known_list entry is null (e.g. "01:123456": null)
    remote_commands = {
        k: v.pop(CONF_COMMANDS)
        for k, v in config[SZ_KNOWN_LIST].items()
        if v and v.get(CONF_COMMANDS)
    }

    coordinator_keys = (CONF_SCAN_INTERVAL, CONF_ADVANCED_FEATURES, SZ_RESTORE_CACHE)
    return (
        port_name,
        {k: v for k, v in config.items() if k not in coordinator_keys}
        | {SZ_PORT_CONFIG: port_config},
        {k: v for k, v in config.items() if k in coordinator_keys}
        | {"remotes": remote_commands},
    )


def strip_traits_for_validation(schema: _SchemaT) -> _SchemaT:
    """Strip ``_`` prefixed keys and trait-only entries for schema validation.

    ramses_rf's ``SCH_GLOBAL_SCHEMAS`` validator rejects ``_`` prefixed keys
    (user-authored traits like ``_disabled``, ``_name``, ``_alias``) and
    trait-only top-level entries (e.g. ``{"04:111111": {"_disabled": True}}``).

    This function recursively removes all ``_`` prefixed keys from the schema
    and removes top-level device-ID keys whose value would be empty after
    stripping (i.e. trait-only entries).  The result is safe to pass to
    ``SCH_GLOBAL_SCHEMAS`` for validation.

    :param schema: The full schema dict (with traits).
    :return: A cleaned schema dict without ``_`` keys, safe for validation.
    """
    import re

    _DEVICE_ID_RE = re.compile(r"^[0-9]{2}:[0-9]{6}$")

    def _strip_traits(obj: Any) -> Any:
        """Recursively strip _ prefixed keys from dicts."""
        if isinstance(obj, dict):
            return {
                k: _strip_traits(v)
                for k, v in obj.items()
                if not str(k).startswith("_")
            }
        return obj

    cleaned: _SchemaT = {}
    for key, value in schema.items():
        # Track if the original had _ keys before stripping
        had_traits = isinstance(value, dict) and any(
            str(k).startswith("_") for k in value
        )
        # Strip _ keys from the value
        stripped = _strip_traits(value)

        # If this is a device-ID key that had traits and is now empty
        # after stripping, it was a trait-only entry — drop it
        if (
            had_traits
            and isinstance(value, dict)
            and _DEVICE_ID_RE.match(str(key))
            and not stripped
        ):
            continue

        cleaned[key] = stripped

    return cleaned


def merge_schemas(config_schema: _SchemaT, cached_schema: _SchemaT) -> _SchemaT | None:
    """Return the config schema deep merged into the cached schema.

    Attempts to combine the user-defined configuration schema with the
    schema restored from the persistence cache. It prefers the cached schema
    if it is a superset of the config.

    :param config_schema: The schema defined in the integration configuration.
    :param cached_schema: The schema restored from the client state cache.
    :return: A merged schema dictionary if successful, or None if the cached
        schema is incompatible or less complete than the config.
    """
    if not isinstance(config_schema, dict) or not isinstance(cached_schema, dict):
        _LOGGER.warning("merge_schemas: non-dict input, skipping merge")
        return None

    if is_subset(shrink(config_schema), shrink(cached_schema)):
        _LOGGER.info("Using the cached schema")
        return cached_schema

    merged_schema: _SchemaT = deep_merge(config_schema, cached_schema)  # 1st precedent

    if is_subset(shrink(config_schema), shrink(merged_schema)):
        _LOGGER.info("Using a merged schema")
        return merged_schema

    _LOGGER.info("Cached schema is a subset of config schema. Skipping cached.")
    return None


# Schema keys that hold device IDs in list form
_LIST_KEYS = frozenset({SZ_ORPHANS_HEAT, SZ_ORPHANS_HVAC, "orphans"})
# Schema keys that hold a single device ID as a scalar
_SCALAR_KEYS = frozenset(
    {SZ_SENSOR, SZ_APPLIANCE_CONTROL, "hotwater_valve", "heating_valve"}
)
# Schema keys that hold lists of device IDs inside zone/TCS entries
_ZONE_LIST_KEYS = frozenset({"actuators", "remotes", "sensors"})

# Keys that identify an HVAC entry (FAN with remotes/sensors)
_HVAC_ENTRY_KEYS = frozenset({SZ_REMOTES, SZ_SENSORS})
# Top-level keys that are HVAC-related
_HVAC_TOP_KEYS = frozenset({SZ_ORPHANS_HVAC})


def extract_hvac_schema(schema: _SchemaT) -> _SchemaT:
    """Extract HVAC-only entries from a full schema.

    Returns a dict containing only the HVAC-related parts:
    - Top-level ``orphans_hvac`` list
    - FAN entries (any top-level key whose value dict has ``remotes``
      or ``sensors``)

    :param schema: The full schema dict.
    :return: A schema dict containing only HVAC entries.
    """
    hvac: dict[str, Any] = {}
    if not isinstance(schema, dict):
        return hvac

    for key, val in schema.items():
        if key in _HVAC_TOP_KEYS:
            hvac[key] = val
            continue
        if isinstance(val, dict) and _HVAC_ENTRY_KEYS & set(val):
            hvac[key] = val

    return hvac


def merge_hvac_schema(config_schema: _SchemaT, hvac_schema: _SchemaT) -> _SchemaT:
    """Merge cached HVAC schema entries into a config schema.

    HVAC entries (FAN with remotes/sensors, orphans_hvac) are lost when
    ramses_rf restarts because ``load_fan`` is a stub.  This helper
    merges them back from the separate HVAC cache.

    - FAN entries: union the ``remotes`` and ``sensors`` lists
    - ``orphans_hvac``: union the lists

    :param config_schema: The config schema to merge into.
    :param hvac_schema: The cached HVAC schema to merge from.
    :return: A new schema with HVAC entries merged in.
    """
    if not hvac_schema or not isinstance(hvac_schema, dict):
        return config_schema

    result = deepcopy(config_schema)
    changed = False

    for key, val in hvac_schema.items():
        if key == SZ_ORPHANS_HVAC:
            existing = set(result.get(SZ_ORPHANS_HVAC, []))
            new = set(val) if isinstance(val, list) else set()
            merged = sorted(existing | new)
            if merged != list(result.get(SZ_ORPHANS_HVAC, [])):
                result[SZ_ORPHANS_HVAC] = merged
                changed = True
            continue

        if not isinstance(val, dict):
            continue

        config_entry = result.get(key, {})
        if not isinstance(config_entry, dict):
            config_entry = {}

        for list_key in (SZ_REMOTES, SZ_SENSORS):
            cached_list = val.get(list_key, [])
            if not cached_list:
                continue
            existing = set(config_entry.get(list_key, []))
            new = [d for d in cached_list if d not in existing]
            if new:
                config_entry[list_key] = sorted(existing | set(new))
                changed = True

        if config_entry:
            result[key] = config_entry

    return result if changed else config_schema


def remove_device_from_schema(schema: _SchemaT, device_id: str) -> _SchemaT:
    """Remove a device_id from anywhere in the schema.

    Searches all locations where a device_id can appear:
    - Top-level orphan lists (orphans_heat, orphans_hvac, orphans)
    - Zone sensor/actuators inside a TCS entry
    - DHW sensor/valves inside a TCS entry
    - appliance_control inside a TCS system
    - remotes/sensors inside an HVAC entry

    Does NOT remove the device's own top-level key (e.g. ``"32:153289": {}``)
    — the caller will merge a new fragment that updates it.

    :param schema: The schema dict to clean.
    :param device_id: The device ID to remove.
    :return: A new schema dict with the device removed from its old location.
    """
    new_schema = deepcopy(schema)

    # 1. Remove from top-level orphan lists
    for key in _LIST_KEYS:
        if key in new_schema and isinstance(new_schema[key], list):
            new_schema[key] = [d for d in new_schema[key] if d != device_id]
            if not new_schema[key]:
                del new_schema[key]

    # 2. Search TCS/HVAC entries for the device
    for tcs_id, tcs_entry in list(new_schema.items()):
        if not isinstance(tcs_entry, dict) or tcs_id in _LIST_KEYS:
            continue
        if tcs_id in (SZ_MAIN_TCS, SZ_DEVICE_COMMENTS):
            continue

        # 2a. Check system.appliance_control
        sys_entry = tcs_entry.get(SZ_SYSTEM, {})
        if isinstance(sys_entry, dict):
            for scalar_key in _SCALAR_KEYS:
                if scalar_key in sys_entry and sys_entry[scalar_key] == device_id:
                    sys_entry[scalar_key] = None

        # 2b. Check orphans list inside TCS
        if SZ_ORPHANS in tcs_entry and isinstance(tcs_entry[SZ_ORPHANS], list):
            tcs_entry[SZ_ORPHANS] = [d for d in tcs_entry[SZ_ORPHANS] if d != device_id]
            if not tcs_entry[SZ_ORPHANS]:
                del tcs_entry[SZ_ORPHANS]

        # 2c. Check zones for sensor and actuators
        zones = tcs_entry.get(SZ_ZONES, {})
        if isinstance(zones, dict):
            for _zone_idx, zone in list(zones.items()):
                if not isinstance(zone, dict):
                    continue
                # sensor is a scalar
                if zone.get(SZ_SENSOR) == device_id:
                    zone[SZ_SENSOR] = None
                # actuators is a list
                if "actuators" in zone and isinstance(zone["actuators"], list):
                    zone["actuators"] = [d for d in zone["actuators"] if d != device_id]
                    if not zone["actuators"]:
                        del zone["actuators"]

        # 2d. Check DHW system for sensor and valves
        dhw = tcs_entry.get(SZ_DHW_SYSTEM, {})
        if isinstance(dhw, dict):
            for scalar_key in (SZ_SENSOR, "hotwater_valve", "heating_valve"):
                if scalar_key in dhw and dhw[scalar_key] == device_id:
                    dhw[scalar_key] = None

        # 2e. Check HVAC remotes/sensors lists
        for list_key in _ZONE_LIST_KEYS:
            if list_key in tcs_entry and isinstance(tcs_entry[list_key], list):
                tcs_entry[list_key] = [d for d in tcs_entry[list_key] if d != device_id]
                if not tcs_entry[list_key]:
                    del tcs_entry[list_key]

    return new_schema


def sync_learned_topology(
    config_schema: _SchemaT, learned_schema: _SchemaT
) -> _SchemaT | None:
    """Sync learned topology from ramses_rf back into the config schema.

    Compares the learned schema (from ``gateway.schema()``) with the config
    entry schema.  If the learned schema has richer topology (devices in
    zones that config has in orphans, new zones, appliance_control), returns
    an enriched config schema.

    Preserves user-authored keys (``_name``, ``_alias``, ``_class``,
    ``_enabled``) and the ``device_comments`` list.

    :param config_schema: The current config entry schema (user intent).
    :param learned_schema: The learned topology from ``gateway.schema()``.
    :return: An enriched schema dict if changes were made, or None if the
        config schema already matches or is richer than the learned topology.
    """
    if not learned_schema or not isinstance(learned_schema, dict):
        return None
    if not isinstance(config_schema, dict):
        return None

    new_schema = deepcopy(config_schema)
    changed = False

    # Keys that are config-only and must be preserved as-is
    config_only_keys = {SZ_DEVICE_COMMENTS, SZ_MAIN_TCS}

    # 1. Sync TCS entries (zones, appliance_control, DHW, orphans)
    for tcs_id, learned_entry in learned_schema.items():
        if not isinstance(learned_entry, dict) or tcs_id in config_only_keys:
            continue
        if tcs_id in (SZ_ORPHANS_HEAT, SZ_ORPHANS_HVAC):
            continue

        config_entry = new_schema.get(tcs_id, {})
        if not isinstance(config_entry, dict):
            config_entry = {}

        # 1a. Sync appliance_control
        learned_sys = learned_entry.get(SZ_SYSTEM, {})
        if isinstance(learned_sys, dict):
            learned_app = learned_sys.get(SZ_APPLIANCE_CONTROL)
            if learned_app:
                config_sys = config_entry.setdefault(SZ_SYSTEM, {})
                if config_sys.get(SZ_APPLIANCE_CONTROL) != learned_app:
                    config_sys[SZ_APPLIANCE_CONTROL] = learned_app
                    changed = True

        # 1b. Sync zones — this is the key enrichment
        learned_zones = learned_entry.get(SZ_ZONES, {})
        if isinstance(learned_zones, dict):
            config_zones = config_entry.get(SZ_ZONES)
            if not isinstance(config_zones, dict):
                config_zones = {}
                config_entry[SZ_ZONES] = config_zones
            for zone_idx, learned_zone in learned_zones.items():
                if not isinstance(learned_zone, dict):
                    continue
                config_zone = config_zones.setdefault(zone_idx, {})
                # Sync sensor (only if config doesn't already have one)
                learned_sensor = learned_zone.get(SZ_SENSOR)
                if learned_sensor and not config_zone.get(SZ_SENSOR):
                    config_zone[SZ_SENSOR] = learned_sensor
                    changed = True
                # Sync actuators (union, don't overwrite)
                learned_actuators = learned_zone.get("actuators", [])
                if learned_actuators:
                    existing = set(config_zone.get("actuators", []))
                    new_actuators = [a for a in learned_actuators if a not in existing]
                    if new_actuators:
                        config_zone["actuators"] = sorted(existing | set(new_actuators))
                        changed = True
                # Sync class if learned has it and config doesn't
                learned_class = learned_zone.get(SZ_CLASS)
                if learned_class and SZ_CLASS not in config_zone:
                    config_zone[SZ_CLASS] = learned_class
                    changed = True

        # 1c. Sync DHW system
        learned_dhw = learned_entry.get(SZ_DHW_SYSTEM, {})
        if isinstance(learned_dhw, dict) and learned_dhw:
            config_dhw = config_entry.setdefault(SZ_DHW_SYSTEM, {})
            learned_dhw_sensor = learned_dhw.get(SZ_SENSOR)
            if learned_dhw_sensor and not config_dhw.get(SZ_SENSOR):
                config_dhw[SZ_SENSOR] = learned_dhw_sensor
                changed = True

        # 1d. Sync TCS-level orphans (only remove devices now in zones)
        learned_tcs_orphans = set(learned_entry.get(SZ_ORPHANS, []))
        config_tcs_orphans = set(config_entry.get(SZ_ORPHANS, []))
        if learned_tcs_orphans != config_tcs_orphans:
            # Only remove from config orphans if they're in a zone now
            all_zone_devices: set[str] = set()
            for zone in config_entry.get(SZ_ZONES, {}).values():
                if isinstance(zone, dict):
                    if zone.get(SZ_SENSOR):
                        all_zone_devices.add(zone[SZ_SENSOR])
                    all_zone_devices.update(zone.get("actuators", []))
            to_remove = config_tcs_orphans & all_zone_devices
            if to_remove:
                remaining = sorted(config_tcs_orphans - to_remove)
                if remaining:
                    config_entry[SZ_ORPHANS] = remaining
                else:
                    config_entry.pop(SZ_ORPHANS, None)
                changed = True

        # 1e. Zone→zone and zone→DHW reassignment — clean old locations
        # After syncing zones from learned schema, a device may now appear
        # in both its old config zone AND its new learned zone (or DHW).
        # Build maps of where the learned schema places each device, then
        # scan all config zones and remove devices that the learned schema
        # placed in a different location.
        learned_device_zones: dict[str, str] = {}
        learned_zones_map = learned_entry.get(SZ_ZONES, {})
        if isinstance(learned_zones_map, dict):
            for lz_idx, lz in learned_zones_map.items():
                if not isinstance(lz, dict):
                    continue
                sensor = lz.get(SZ_SENSOR)
                if isinstance(sensor, str):
                    learned_device_zones[sensor] = lz_idx
                for act in lz.get("actuators", []):
                    if isinstance(act, str):
                        learned_device_zones[act] = lz_idx

        # Also collect devices the learned schema places in DHW — these
        # should be removed from config zones (zone→DHW move).
        learned_dhw_devices: set[str] = set()
        learned_dhw_entry = learned_entry.get(SZ_DHW_SYSTEM, {})
        if isinstance(learned_dhw_entry, dict):
            dhw_sensor = learned_dhw_entry.get(SZ_SENSOR)
            if isinstance(dhw_sensor, str):
                learned_dhw_devices.add(dhw_sensor)
            for valve_key in ("hotwater_valve", "heating_valve"):
                valve = learned_dhw_entry.get(valve_key)
                if isinstance(valve, str):
                    learned_dhw_devices.add(valve)

        if (learned_device_zones or learned_dhw_devices) and isinstance(
            config_entry.get(SZ_ZONES), dict
        ):
            for cz_idx, cz in list(config_entry[SZ_ZONES].items()):
                if not isinstance(cz, dict):
                    continue
                # Check sensor — remove if learned placed it in a
                # different zone or in DHW
                cz_sensor = cz.get(SZ_SENSOR)
                if (
                    isinstance(cz_sensor, str)
                    and cz_sensor
                    and (
                        (
                            cz_sensor in learned_device_zones
                            and learned_device_zones[cz_sensor] != cz_idx
                        )
                        or cz_sensor in learned_dhw_devices
                    )
                ):
                    cz[SZ_SENSOR] = None
                    changed = True
                # Check actuators — same logic
                cz_actuators = cz.get("actuators", [])
                if cz_actuators:
                    new_acts = [
                        a
                        for a in cz_actuators
                        if isinstance(a, str)
                        and (
                            a not in learned_device_zones
                            or learned_device_zones[a] == cz_idx
                        )
                        and a not in learned_dhw_devices
                    ]
                    if len(new_acts) != len(cz_actuators):
                        if new_acts:
                            cz["actuators"] = new_acts
                        else:
                            cz.pop("actuators", None)
                        changed = True

        # 1f. DHW→zone reassignment — clear DHW sensor/valves if the
        # learned schema now has the device in a zone instead of DHW.
        if learned_device_zones and isinstance(config_entry.get(SZ_DHW_SYSTEM), dict):
            config_dhw = config_entry[SZ_DHW_SYSTEM]
            # Clear DHW sensor if learned placed it in a zone
            dhw_sensor = config_dhw.get(SZ_SENSOR)
            if dhw_sensor and dhw_sensor in learned_device_zones:
                config_dhw[SZ_SENSOR] = None
                changed = True
            # Clear DHW valves if learned placed them in a zone
            for valve_key in ("hotwater_valve", "heating_valve"):
                valve = config_dhw.get(valve_key)
                if valve and valve in learned_device_zones:
                    config_dhw[valve_key] = None
                    changed = True

        new_schema[tcs_id] = config_entry

    # 2. Sync top-level orphans_heat — remove devices now in zones or DHW
    config_heat_orphans = set(new_schema.get(SZ_ORPHANS_HEAT, []))
    learned_heat_orphans = set(learned_schema.get(SZ_ORPHANS_HEAT, []))
    if config_heat_orphans and config_heat_orphans != learned_heat_orphans:
        # Find devices that are in config orphans but in a zone or DHW in learned
        all_learned_zone_devices: set[str] = set()
        for learned_entry in learned_schema.values():
            if not isinstance(learned_entry, dict):
                continue
            for zone in learned_entry.get(SZ_ZONES, {}).values():
                if isinstance(zone, dict):
                    sensor = zone.get(SZ_SENSOR)
                    if isinstance(sensor, str):
                        all_learned_zone_devices.add(sensor)
                    for a in zone.get("actuators", []):
                        if isinstance(a, str):
                            all_learned_zone_devices.add(a)
            # Also check DHW sensor and valves
            learned_dhw = learned_entry.get(SZ_DHW_SYSTEM, {})
            if isinstance(learned_dhw, dict):
                dhw_sensor = learned_dhw.get(SZ_SENSOR)
                if isinstance(dhw_sensor, str):
                    all_learned_zone_devices.add(dhw_sensor)
                for valve_key in ("hotwater_valve", "heating_valve"):
                    valve = learned_dhw.get(valve_key)
                    if isinstance(valve, str):
                        all_learned_zone_devices.add(valve)
        to_remove = config_heat_orphans & all_learned_zone_devices
        if to_remove:
            remaining = sorted(
                d for d in (config_heat_orphans - to_remove) if isinstance(d, str)
            )
            if remaining:
                new_schema[SZ_ORPHANS_HEAT] = remaining
            else:
                new_schema.pop(SZ_ORPHANS_HEAT, None)
            changed = True

    # 3. Sync top-level orphans_hvac — remove devices now in HVAC entries
    config_hvac_orphans = set(new_schema.get(SZ_ORPHANS_HVAC, []))
    learned_hvac_orphans = set(learned_schema.get(SZ_ORPHANS_HVAC, []))
    if config_hvac_orphans and config_hvac_orphans != learned_hvac_orphans:
        # Find devices in config orphans that are in an HVAC entry in learned
        all_hvac_entry_devices: set[str] = set()
        for key, val in learned_schema.items():
            if not isinstance(val, dict):
                continue
            if key in config_only_keys or key in (SZ_ORPHANS_HEAT, SZ_ORPHANS_HVAC):
                continue
            # HVAC entries have remotes/sensors lists
            for list_key in _ZONE_LIST_KEYS:
                if list_key in val and isinstance(val[list_key], list):
                    all_hvac_entry_devices.update(val[list_key])
        to_remove = config_hvac_orphans & all_hvac_entry_devices
        if to_remove:
            remaining = sorted(config_hvac_orphans - to_remove)
            if remaining:
                new_schema[SZ_ORPHANS_HVAC] = remaining
            else:
                new_schema.pop(SZ_ORPHANS_HVAC, None)
            changed = True

    if not changed:
        return None

    _LOGGER.info("Synced learned topology to config schema")
    return new_schema


def schema_is_minimal(schema: _SchemaT) -> bool:
    """Return True if the schema is minimal (i.e. no optional keys).

    Validates if the provided schema meets the minimum structural requirements
    for a Temperature Control System (TCS) without containing unnecessary
    or optional definition keys.

    :param schema: The schema dictionary to validate.
    :return: True if the schema is a valid minimal TCS schema, False otherwise.
    """

    key: str
    sch: _SchemaT

    for key, sch in schema.items():
        if key in (
            SZ_BLOCK_LIST,
            SZ_KNOWN_LIST,
            SZ_MAIN_TCS,
            SZ_ORPHANS_HEAT,
            SZ_ORPHANS_HVAC,
            SZ_DEVICE_COMMENTS,
            "transport_constructor",
        ):
            continue

        try:
            _ = SCH_MINIMUM_TCS(shrink(sch))
        except vol.Invalid:
            return False

        if SZ_ZONES in sch and list(sch[SZ_ZONES].values())[0][SZ_SENSOR] != key:
            return False

    return True


SCH_NO_SVC_PARAMS = vol.Schema({}, extra=vol.PREVENT_EXTRA)
SCH_NO_ENTITY_SVC_PARAMS = cv.make_entity_service_schema(
    {},
    extra=vol.PREVENT_EXTRA,
)


# services for ramses_cc integration

_SCH_BINDING = vol.Schema({vol.Required(_SCH_CMD_CODE): vol.Any(None, _SCH_DOM_IDX)})

SCH_BIND_DEVICE = vol.Schema(
    {
        vol.Required("device_id"): _SCH_DEVICE_ID,
        vol.Required("offer"): vol.All(_SCH_BINDING, vol.Length(min=1)),
        vol.Optional("confirm", default={}): vol.Any(
            {}, vol.All(_SCH_BINDING, vol.Length(min=1))
        ),
        vol.Optional("device_info", default=None): vol.Any(None, _SCH_COMMAND),
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_SEND_PACKET = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): _SCH_DEVICE_ID,
        vol.Optional("from_id"): _SCH_DEVICE_ID,
        vol.Required("verb"): vol.In((" I", "I", "RQ", "RP", " W", "W")),
        vol.Required("code"): cv.matches_regex(r"^[0-9A-F]{4}$"),
        vol.Required("payload"): cv.matches_regex(r"^([0-9A-F][0-9A-F]){1,48}$"),
    }
)

SVC_BIND_DEVICE: Final = "bind_device"
SVC_FORCE_UPDATE: Final = "force_update"
SVC_SEND_PACKET: Final = "send_packet"
SVC_SYNC_TOPOLOGY: Final = "sync_topology"

SCH_DISCOVER_KNOWN_DEVICES = vol.Schema(
    {
        vol.Optional("device_id"): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)

# Discovery scan service schemas

SCH_GET_DISCOVERED_DEVICES = vol.Schema(
    {
        vol.Optional("status"): vol.In(
            ("new", "accepted", "discarded", "removed", "lost")
        ),
        vol.Optional("enabled"): cv.boolean,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_ACCEPT_DISCOVERED_DEVICE = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): _SCH_DEVICE_ID,
        vol.Optional("owner"): vol.All(str, vol.Length(max=50)),
        vol.Optional("schema_entry"): dict,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_DISCARD_DISCOVERED_DEVICE = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_REMOVE_DISCOVERED_DEVICE = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_ENABLE_DISCOVERED_DEVICE = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_DISABLE_DISCOVERED_DEVICE = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_ADD_FAKED_REM = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): _SCH_DEVICE_ID,
        vol.Required("bound_to"): _SCH_DEVICE_ID,
        vol.Optional("alias"): vol.All(str, vol.Length(max=50)),
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_REMOVE_DEVICE = vol.Schema(
    {
        vol.Required(ATTR_DEVICE_ID): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)


# services for sensor platform

MIN_CO2_LEVEL: Final[int] = 300
MAX_CO2_LEVEL: Final[int] = 9999

SVC_PUT_CO2_LEVEL: Final = "put_co2_level"
SCH_PUT_CO2_LEVEL = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_CO2_LEVEL): vol.All(
            cv.positive_int,
            vol.Range(min=MIN_CO2_LEVEL, max=MAX_CO2_LEVEL),
        ),
    },
    extra=vol.PREVENT_EXTRA,
)

MIN_DHW_TEMP: Final[float] = 0
MAX_DHW_TEMP: Final[float] = 99

SVC_PUT_DHW_TEMP: Final = "put_dhw_temp"
SCH_PUT_DHW_TEMP = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_TEMPERATURE): vol.All(
            vol.Coerce(float),
            vol.Range(min=MIN_DHW_TEMP, max=MAX_DHW_TEMP),
        ),
    },
    extra=vol.PREVENT_EXTRA,
)

MIN_INDOOR_HUMIDITY: Final[float] = 0
MAX_INDOOR_HUMIDITY: Final[float] = 100

SVC_PUT_INDOOR_HUMIDITY: Final = "put_indoor_humidity"
SCH_PUT_INDOOR_HUMIDITY = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_INDOOR_HUMIDITY): vol.All(
            cv.positive_float,
            vol.Range(min=MIN_INDOOR_HUMIDITY, max=MAX_INDOOR_HUMIDITY),
        ),
    },
    extra=vol.PREVENT_EXTRA,
)

MIN_ROOM_TEMP: Final[float] = -20
MAX_ROOM_TEMP: Final[float] = 60

SVC_PUT_ROOM_TEMP: Final = "put_room_temp"
SCH_PUT_ROOM_TEMP = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_TEMPERATURE): vol.All(
            vol.Coerce(float),
            vol.Range(min=MIN_ROOM_TEMP, max=MAX_ROOM_TEMP),
        ),
    },
    extra=vol.PREVENT_EXTRA,
)

SVCS_RAMSES_SENSOR = {
    SVC_PUT_CO2_LEVEL: SCH_PUT_CO2_LEVEL,
    SVC_PUT_DHW_TEMP: SCH_PUT_DHW_TEMP,
    SVC_PUT_INDOOR_HUMIDITY: SCH_PUT_INDOOR_HUMIDITY,
    SVC_PUT_ROOM_TEMP: SCH_PUT_ROOM_TEMP,
}

# services for climate platform

SCH_DURATION = vol.All(  # of time (<=24h)
    cv.time_period,
    vol.Range(min=td(hours=1), max=td(hours=24)),
)
SCH_PERIOD = vol.All(  # of days (0-99)
    cv.time_period, vol.Range(min=td(days=0), max=td(days=99))
)

SVC_SET_SYSTEM_MODE: Final = "set_system_mode"
SCH_SET_SYSTEM_MODE = cv.make_entity_service_schema(
    # nested schemas not allowed after HA 2025.9, extra check moved to climate.py
    {
        vol.Required(ATTR_MODE): vol.In(SystemMode),
        vol.Optional(ATTR_DURATION): vol.Any(SCH_DURATION, None),
        # canBeTemporary: true, timingMode: Duration
        vol.Optional(ATTR_PERIOD): vol.Any(SCH_PERIOD, None),
        # Period: None is indefinitely; 0 is the end of today, 1 is end of tomorrow
    }
)

SCH_SET_SYSTEM_MODE_EXTRA = vol.Schema(  # original Entity Service action validation schema
    # vol.Msg(  # TODO turn on if good checks are working 8-2025
    vol.Any(
        {  # A also: Off, Heat, Cool (for pre-evohome)
            vol.Required(ATTR_MODE): vol.In(
                [SystemMode.AUTO, SystemMode.HEAT_OFF, SystemMode.RESET]
            )
        },
        {  # B
            vol.Required(ATTR_MODE): vol.In([SystemMode.ECO_BOOST]),
            vol.Optional(ATTR_DURATION): vol.Any(SCH_DURATION, None),
        },  # duration: : None is indefinitely; 0 is invalid
        {  # C canBeTemporary: true, timingMode: Period
            vol.Required(ATTR_MODE): vol.In(
                [
                    SystemMode.AWAY,
                    SystemMode.CUSTOM,
                    SystemMode.DAY_OFF,
                    SystemMode.DAY_OFF_ECO,
                ]
            ),
            vol.Optional(ATTR_PERIOD): vol.Any(SCH_PERIOD, None),
        },  # Period: None is indefinitely; 0 is the end of today, 1 is end of tomorrow
    ),
    #     msg="Invalid ramses_cc Zone Mode entry in Entity Service call",
    # ),
    extra=vol.PREVENT_EXTRA,
)

DEFAULT_MIN_TEMP: Final[float] = 5
MIN_MIN_TEMP: Final[float] = 5
MAX_MIN_TEMP: Final[float] = 21

DEFAULT_MAX_TEMP: Final[float] = 35
MIN_MAX_TEMP: Final[float] = 21
MAX_MAX_TEMP: Final[float] = 35

SVC_SET_ZONE_CONFIG: Final = "set_zone_config"
SCH_SET_ZONE_CONFIG = cv.make_entity_service_schema(
    {
        vol.Optional(ATTR_MAX_TEMP, default=DEFAULT_MAX_TEMP): vol.All(
            cv.positive_float, vol.Range(min=MIN_MAX_TEMP, max=MAX_MAX_TEMP)
        ),
        vol.Optional(ATTR_MIN_TEMP, default=DEFAULT_MIN_TEMP): vol.All(
            cv.positive_float, vol.Range(min=MIN_MIN_TEMP, max=MAX_MIN_TEMP)
        ),
        vol.Optional(ATTR_LOCAL_OVERRIDE, default=True): cv.boolean,
        vol.Optional(ATTR_OPENWINDOW, default=True): cv.boolean,
        vol.Optional(ATTR_MULTIROOM, default=True): cv.boolean,
    }
)

SVC_SET_ZONE_MODE: Final = "set_zone_mode"
SCH_SET_ZONE_MODE = cv.make_entity_service_schema(
    # nested schemas not allowed after HA 2025.9, extra check moved to climate.py
    {
        vol.Required(ATTR_MODE): vol.In(
            [
                ZoneMode.SCHEDULE,
                ZoneMode.PERMANENT,
                ZoneMode.ADVANCED,
                ZoneMode.TEMPORARY,
            ]
        ),
        vol.Optional(ATTR_SETPOINT): vol.All(
            cv.positive_float, vol.Range(min=5, max=35)
        ),
        vol.Optional(ATTR_UNTIL): cv.datetime,
        vol.Optional(ATTR_DURATION): vol.All(
            cv.time_period,
            vol.Range(min=td(minutes=5), max=td(days=1)),
        ),
    }
)

SCH_SET_ZONE_MODE_EXTRA = (
    vol.Schema(  # original Entity Service action validation schema
        # vol.Msg(  # TODO turn msg on if checks are working 10-2025
        vol.Any(
            {  # A
                vol.Required(ATTR_MODE): vol.In([ZoneMode.SCHEDULE]),
                # only mode with no setpoint
            },
            {  # B
                vol.Required(ATTR_MODE): vol.In(
                    [ZoneMode.PERMANENT, ZoneMode.ADVANCED]
                ),
                vol.Required(ATTR_SETPOINT): vol.All(
                    cv.positive_float, vol.Range(min=5, max=35)
                ),
            },
            {  # C
                vol.Required(ATTR_MODE): vol.In([ZoneMode.TEMPORARY]),
                vol.Required(ATTR_SETPOINT): vol.All(
                    cv.positive_float, vol.Range(min=5, max=35)
                ),
                vol.Required(ATTR_DURATION, default=td(hours=1)): vol.All(
                    cv.time_period,
                    vol.Range(min=td(minutes=5), max=td(days=1)),
                ),
            },
            {  # D
                vol.Required(ATTR_MODE): vol.In([ZoneMode.TEMPORARY]),
                vol.Required(ATTR_SETPOINT): vol.All(
                    cv.positive_float, vol.Range(min=5, max=35)
                ),
                vol.Required(ATTR_UNTIL): cv.datetime,
            },
        ),
        #     msg="Invalid ramses_cc Zone Mode entry in Entity Service call",
        # ),
        extra=vol.PREVENT_EXTRA,
    )
)

SVC_SET_ZONE_SCHEDULE: Final = "set_zone_schedule"
SCH_SET_ZONE_SCHEDULE = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_SCHEDULE): cv.string,
    }
)

DEFAULT_NUM_ENTRIES: Final[float] = 8
MIN_NUM_ENTRIES: Final[float] = 1
MAX_NUM_ENTRIES: Final[float] = 64

SVC_GET_SYSTEM_FAULTS: Final = "get_system_faults"
SCH_GET_SYSTEM_FAULTS = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_NUM_ENTRIES, default=DEFAULT_NUM_ENTRIES): vol.All(
            cv.positive_int, vol.Range(min=MIN_NUM_ENTRIES, max=MAX_NUM_ENTRIES)
        ),
    }
)

# Service schema for getting and setting hvac fan parameters (using ramses_rf implementation)
SVC_GET_FAN_PARAM: Final = "get_fan_param"
SVC_GET_FAN_CLIM_PARAM: Final = "get_fan_clim_param"
SVC_GET_FAN_REM_PARAM: Final = "get_fan_rem_param"
SVC_SET_FAN_PARAM: Final = "set_fan_param"
SVC_SET_FAN_CLIM_PARAM: Final = "set_fan_clim_param"
SVC_SET_FAN_REM_PARAM: Final = "set_fan_rem_param"
SVC_UPDATE_FAN_PARAMS: Final = "update_fan_params"

_TARGET_FIELDS = {
    vol.Optional("entity_id"): cv.entity_ids,
    vol.Optional("device_id"): cv.ensure_list_csv,
    vol.Optional("area_id"): cv.ensure_list_csv,
    vol.Optional("device"): vol.Any(None, cv.ensure_list_csv),
}

SCH_GET_FAN_PARAM = cv.make_entity_service_schema(
    {
        **_TARGET_FIELDS,
        vol.Required("param_id"): _SCH_PARAM_ID,
        vol.Optional("from_id"): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_GET_FAN_REM_PARAM = cv.make_entity_service_schema(
    {
        **_TARGET_FIELDS,
        vol.Required("param_id"): _SCH_PARAM_ID,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_SET_FAN_PARAM = cv.make_entity_service_schema(
    {
        **_TARGET_FIELDS,
        vol.Required("param_id"): _SCH_PARAM_ID,
        vol.Required("value"): cv.string,
        vol.Optional("from_id"): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_SET_FAN_REM_PARAM = cv.make_entity_service_schema(
    {
        **_TARGET_FIELDS,
        vol.Required("param_id"): _SCH_PARAM_ID,
        vol.Required("value"): cv.string,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_UPDATE_FAN_PARAMS = cv.make_entity_service_schema(
    {
        **_TARGET_FIELDS,
        vol.Optional("from_id"): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)

SCH_GET_FAN_PARAM_DOMAIN = vol.Schema(
    {
        vol.Optional("device"): vol.Any(None, cv.ensure_list_csv),
        vol.Optional("device_id"): vol.Any(None, cv.string),
        vol.Required("param_id"): _SCH_PARAM_ID,
        vol.Optional("from_id"): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)
SCH_SET_FAN_PARAM_DOMAIN = vol.Schema(
    {
        vol.Optional("device"): vol.Any(None, cv.ensure_list_csv),
        vol.Optional("device_id"): vol.Any(None, cv.string),
        vol.Required("param_id"): _SCH_PARAM_ID,
        vol.Required("value"): cv.string,
        vol.Optional("from_id"): _SCH_DEVICE_ID,
    },
    extra=vol.PREVENT_EXTRA,
)
SCH_UPDATE_FAN_PARAMS_DOMAIN = SCH_UPDATE_FAN_PARAMS


# services without their own schema
SVC_FAKE_ZONE_TEMP: Final = "fake_zone_temp"
SVC_GET_ZONE_SCHEDULE: Final = "get_zone_schedule"
SVC_RESET_SYSTEM_MODE: Final = "reset_system_mode"
SVC_RESET_ZONE_CONFIG: Final = "reset_zone_config"
SVC_RESET_ZONE_MODE: Final = "reset_zone_mode"

SVCS_RAMSES_CLIMATE = {
    SVC_FAKE_ZONE_TEMP: SCH_PUT_ROOM_TEMP,  # a convenience for SVC_PUT_ROOM_TEMP
    SVC_SET_SYSTEM_MODE: SCH_SET_SYSTEM_MODE,
    SVC_SET_ZONE_CONFIG: SCH_SET_ZONE_CONFIG,
    SVC_SET_ZONE_MODE: SCH_SET_ZONE_MODE,
    SVC_RESET_SYSTEM_MODE: SCH_NO_ENTITY_SVC_PARAMS,
    SVC_RESET_ZONE_CONFIG: SCH_NO_ENTITY_SVC_PARAMS,
    SVC_RESET_ZONE_MODE: SCH_NO_ENTITY_SVC_PARAMS,
    SVC_GET_ZONE_SCHEDULE: SCH_NO_ENTITY_SVC_PARAMS,
    SVC_SET_ZONE_SCHEDULE: SCH_SET_ZONE_SCHEDULE,
    SVC_GET_SYSTEM_FAULTS: SCH_GET_SYSTEM_FAULTS,
    SVC_GET_FAN_CLIM_PARAM: SCH_GET_FAN_PARAM,  # UI fan_param actions
    SVC_SET_FAN_CLIM_PARAM: SCH_SET_FAN_PARAM,
    SVC_UPDATE_FAN_PARAMS: SCH_UPDATE_FAN_PARAMS,
}

# services for water_heater platform

SVC_SET_DHW_MODE: Final = "set_dhw_mode"
SCH_SET_DHW_MODE = cv.make_entity_service_schema(
    # nested schemas not allowed after HA 2025.9, extra check moved to climate.py
    {
        vol.Required(ATTR_MODE): vol.In(
            [
                ZoneMode.SCHEDULE,
                ZoneMode.PERMANENT,
                ZoneMode.ADVANCED,
                ZoneMode.TEMPORARY,
            ]
        ),
        vol.Optional(ATTR_ACTIVE): cv.boolean,
        vol.Optional(ATTR_UNTIL): cv.datetime,
        vol.Optional(ATTR_DURATION): vol.All(
            cv.time_period,
            vol.Range(min=td(minutes=5), max=td(days=1)),
        ),
    }
)

SCH_SET_DHW_MODE_EXTRA = vol.Schema(  # original Entity Service action validation schema
    # vol.Msg(  # TODO turn on if good checks are working 8-2025
    vol.Any(
        {  # A
            vol.Required(ATTR_MODE): vol.In([ZoneMode.SCHEDULE]),
            # only mode with no active
        },
        {
            vol.Required(ATTR_MODE): vol.In([ZoneMode.PERMANENT, ZoneMode.ADVANCED]),
            vol.Required(ATTR_ACTIVE): cv.boolean,
        },
        {  # B a.k.a DHW boost
            vol.Required(ATTR_MODE): vol.In([ZoneMode.TEMPORARY]),
            vol.Required(ATTR_ACTIVE): True,  # TODO: vol.Any(truthy)
            vol.Required(ATTR_DURATION, default=td(hours=1)): vol.All(
                cv.time_period,
                vol.Range(min=td(minutes=5), max=td(days=1)),
            ),
        },
        {  # C
            vol.Required(ATTR_MODE): vol.In([ZoneMode.TEMPORARY]),
            vol.Required(ATTR_ACTIVE): cv.boolean,
            vol.Required(ATTR_DURATION): vol.All(
                cv.time_period,
                vol.Range(min=td(minutes=5), max=td(days=1)),
            ),
        },
        {  # D
            vol.Required(ATTR_MODE): vol.In([ZoneMode.TEMPORARY]),
            vol.Required(ATTR_ACTIVE): cv.boolean,
            vol.Required(ATTR_UNTIL): cv.datetime,
        },
    ),
    #     msg="Invalid ramses_cc Zone Mode entry in Entity Service call",
    # ),
    extra=vol.PREVENT_EXTRA,
)

DEFAULT_DHW_SETPOINT: Final[float] = 50  # degrees celsius, float
MIN_DHW_SETPOINT: Final[float] = 30
MAX_DHW_SETPOINT: Final[float] = 85

DEFAULT_OVERRUN: Final[int] = 5  # minutes, int
MIN_OVERRUN: Final[int] = 0
MAX_OVERRUN: Final[int] = 10

DEFAULT_DIFFERENTIAL: Final[float] = 10  # degrees celsius, float
MIN_DIFFERENTIAL: Final[float] = 1
MAX_DIFFERENTIAL: Final[float] = 10

SVC_SET_DHW_PARAMS: Final = "set_dhw_params"
SCH_SET_DHW_PARAMS = cv.make_entity_service_schema(
    {
        vol.Optional(ATTR_SETPOINT, default=DEFAULT_DHW_SETPOINT): vol.All(
            cv.positive_float, vol.Range(min=MIN_DHW_SETPOINT, max=MAX_DHW_SETPOINT)
        ),
        vol.Optional(ATTR_OVERRUN, default=DEFAULT_OVERRUN): vol.All(
            cv.positive_int, vol.Range(min=MIN_OVERRUN, max=MAX_OVERRUN)
        ),
        vol.Optional(ATTR_DIFFERENTIAL, default=DEFAULT_DIFFERENTIAL): vol.All(
            cv.positive_float, vol.Range(min=MIN_DIFFERENTIAL, max=MAX_DIFFERENTIAL)
        ),
    }
)

SVC_SET_DHW_SCHEDULE: Final = "set_dhw_schedule"
SCH_SET_DHW_SCHEDULE = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_SCHEDULE): cv.string,
    }
)

SVC_FAKE_DHW_TEMP: Final = "fake_dhw_temp"
SVC_GET_DHW_SCHEDULE: Final = "get_dhw_schedule"
SVC_RESET_DHW_MODE: Final = "reset_dhw_mode"
SVC_RESET_DHW_PARAMS: Final = "reset_dhw_params"
SVC_SET_DHW_BOOST: Final = "set_dhw_boost"

SVCS_RAMSES_WATER_HEATER = {
    SVC_FAKE_DHW_TEMP: SCH_PUT_DHW_TEMP,  # a convenience for SVC_PUT_DHW_TEMP
    SVC_RESET_DHW_MODE: SCH_NO_ENTITY_SVC_PARAMS,
    SVC_RESET_DHW_PARAMS: SCH_NO_ENTITY_SVC_PARAMS,
    SVC_SET_DHW_BOOST: SCH_NO_ENTITY_SVC_PARAMS,
    SVC_SET_DHW_MODE: SCH_SET_DHW_MODE,
    SVC_SET_DHW_PARAMS: SCH_SET_DHW_PARAMS,
    SVC_GET_DHW_SCHEDULE: SCH_NO_ENTITY_SVC_PARAMS,
    SVC_SET_DHW_SCHEDULE: SCH_SET_DHW_SCHEDULE,
}

# services for remote platform

DEFAULT_TIMEOUT: Final[int] = 60
MIN_TIMEOUT: Final[int] = 30
MAX_TIMEOUT: Final[int] = 300

SVC_LEARN_COMMAND: Final = "learn_command"
SCH_LEARN_COMMAND = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_COMMAND): cv.string,
        vol.Required(ATTR_TIMEOUT, default=DEFAULT_TIMEOUT): vol.All(
            cv.positive_int, vol.Range(min=MIN_TIMEOUT, max=MAX_TIMEOUT)
        ),
    },
)

# hvac services

# add_command (inject a packet without RF learning loop)
SVC_ADD_COMMAND: Final = "add_command"
SCH_ADD_COMMAND = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_COMMAND): cv.string,
        vol.Required("packet_string"): cv.string,
    }
)

SVC_SEND_COMMAND: Final = "send_command"
SCH_SEND_COMMAND = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_COMMAND): cv.string,
        vol.Required(ATTR_NUM_REPEATS, default=3): vol.All(
            cv.positive_int,
            vol.Range(min=MIN_NUM_REPEATS, max=MAX_NUM_REPEATS),
        ),
        vol.Required(ATTR_DELAY_SECS, default=DEFAULT_GAP_DURATION): vol.All(
            cv.positive_float,
            vol.Range(min=MIN_GAP_DURATION, max=MAX_GAP_DURATION),
        ),
    },
)

SVC_DELETE_COMMAND: Final = "delete_command"
SCH_DELETE_COMMAND = cv.make_entity_service_schema(
    {
        vol.Required(ATTR_COMMAND): cv.string,
    },
)

SVCS_RAMSES_REMOTE = {
    SVC_DELETE_COMMAND: SCH_DELETE_COMMAND,
    SVC_ADD_COMMAND: SCH_ADD_COMMAND,
    SVC_LEARN_COMMAND: SCH_LEARN_COMMAND,
    SVC_SEND_COMMAND: SCH_SEND_COMMAND,
    SVC_GET_FAN_REM_PARAM: SCH_GET_FAN_REM_PARAM,
    SVC_SET_FAN_REM_PARAM: SCH_SET_FAN_REM_PARAM,
}

# Service schemas for number platform
SVCS_RAMSES_NUMBER: dict[str, Any] = {
    # set_fan_param is registered as a coordinator/domain service (not an entity service)
}
