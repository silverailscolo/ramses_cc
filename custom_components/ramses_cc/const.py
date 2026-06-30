"""Constants for RAMSES integration."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from homeassistant.const import CONF_SCAN_INTERVAL as CONF_SCAN_INTERVAL

from ramses_rf.protocol.ramses import _2411_PARAMS_SCHEMA as _2411_PARAMS_SCHEMA
from ramses_rf.schemas import SZ_BOUND_TO as SZ_BOUND_TO, SZ_SCHEMA as SZ_SCHEMA
from ramses_tx.const import SZ_IS_EVOFW3 as SZ_IS_EVOFW3
from ramses_tx.schemas import (
    SZ_BUFFER_CAPACITY as SZ_BUFFER_CAPACITY,
    SZ_ENFORCE_KNOWN_LIST as SZ_ENFORCE_KNOWN_LIST,
    SZ_FLUSH_INTERVAL as SZ_FLUSH_INTERVAL,
    SZ_KNOWN_LIST as SZ_KNOWN_LIST,
    SZ_PACKET_LOG as SZ_PACKET_LOG,
    SZ_PACKET_LOG_PATH as SZ_PACKET_LOG_PATH,
    SZ_PACKET_LOG_PREFIX as SZ_PACKET_LOG_PREFIX,
    SZ_PACKET_LOG_RETENTION_DAYS as SZ_PACKET_LOG_RETENTION_DAYS,
    SZ_PORT_NAME as SZ_PORT_NAME,
    SZ_SERIAL_PORT as SZ_SERIAL_PORT,
)

DOMAIN: Final = "ramses_cc"

STORAGE_VERSION: Final[int] = 1
STORAGE_KEY: Final = DOMAIN

# Dispatcher signals
SIGNAL_NEW_DEVICES: Final = f"{DOMAIN}_new_devices_" + "{}"
SIGNAL_UPDATE: Final = f"{DOMAIN}_update"

# Config
CONF_ADVANCED_FEATURES: Final = "advanced_features"
CONF_COMMANDS: Final = "commands"
CONF_DEV_MODE: Final = "dev_mode"
CONF_GATEWAY_TIMEOUT: Final = "gateway_timeout"
CONF_MESSAGE_EVENTS: Final = "message_events"
CONF_MQTT_USE_HA: Final = "mqtt_use_ha"
CONF_MQTT_HGI_ID: Final = "mqtt_hgi_id"
CONF_MQTT_TOPIC: Final = "mqtt_topic"
CONF_RAMSES_RF: Final = "ramses_rf"
CONF_SCHEMA: Final = "schema"
CONF_SEND_PACKET: Final = "send_packet"
CONF_UNKNOWN_CODES: Final = "unknown_codes"
CONF_DISCOVER_KNOWN_DEVICES: Final = "discover_known_devices"

# Defaults
DEFAULT_MQTT_TOPIC: Final = "RAMSES/GATEWAY"
DEFAULT_HGI_ID: Final = "18:000730"

# State
SZ_CLIENT_STATE: Final = "client_state"
SZ_PACKETS: Final = "packets"
SZ_REMOTES: Final = "remotes"

# Entity/service attributes
ATTR_ACTIVE: Final = "active"
ATTR_ACTIVE_FAULTS: Final = "active_faults"
ATTR_ACTUATOR: Final = "enabled"
ATTR_BATTERY: Final = "battery_low"
ATTR_BATTERY_LEVEL: Final = "battery_level"
ATTR_CO2_LEVEL: Final = "co2_level"
ATTR_COMMAND: Final = "command"
ATTR_DELAY_SECS: Final = "delay_secs"
ATTR_DEVICE_ID: Final = "device_id"
ATTR_DIFFERENTIAL: Final = "differential"
ATTR_DURATION: Final = "duration"
ATTR_FAN_RATE: Final = "fan_rate"
ATTR_FAULT_LOG: Final = "fault_log"
ATTR_HEAT_DEMAND: Final = "heat_demand"
ATTR_HUMIDITY: Final = "relative_humidity"
ATTR_INDOOR_HUMIDITY: Final = "indoor_humidity"
ATTR_LATEST_EVENT: Final = "latest_event"
ATTR_LATEST_FAULT: Final = "latest_fault"
ATTR_LOCAL_OVERRIDE: Final = "local_override"
ATTR_MAX_TEMP: Final = "max_temp"
ATTR_MIN_TEMP: Final = "min_temp"
ATTR_MODE: Final = "mode"
ATTR_MULTIROOM: Final = "multiroom_mode"
ATTR_NUM_ENTRIES: Final = "num_entries"
ATTR_NUM_REPEATS: Final = "num_repeats"
ATTR_OPENWINDOW: Final = "openwindow_function"
ATTR_OVERRUN: Final = "overrun"
ATTR_PERIOD: Final = "period"
ATTR_RELAY_DEMAND: Final = "relay_demand"
ATTR_SCHEDULE: Final = "schedule"
ATTR_SETPOINT: Final = "setpoint"
ATTR_SYSTEM_MODE: Final = "system_mode"
ATTR_TEMPERATURE: Final = "temperature"
ATTR_TIMEOUT: Final = "timeout"
ATTR_UNTIL: Final = "until"
ATTR_WINDOW: Final = "window_open"
ATTR_WORKING_SCHEMA: Final = "working_schema"

# Unofficial presets
PRESET_CUSTOM: Final = "custom"
PRESET_TEMPORARY: Final = "temporary"
PRESET_PERMANENT: Final = "permanent"


# Volume Flow Rate units, these specific unit are not defined in HA v2024.1
class UnitOfVolumeFlowRate(StrEnum):
    """Volume flow rate units (defined by integration)."""

    LITERS_PER_MINUTE = "L/min"
    LITERS_PER_SECOND = "L/s"


class SystemMode(StrEnum):
    """System modes."""

    AUTO = "auto"
    AWAY = "away"
    CUSTOM = "custom"
    DAY_OFF = "day_off"
    DAY_OFF_ECO = "day_off_eco"  # set to Eco when DayOff ends
    ECO_BOOST = "eco_boost"  # Eco, or Boost
    HEAT_OFF = "heat_off"
    RESET = "auto_with_reset"


class ZoneMode(StrEnum):
    """Zone modes."""

    SCHEDULE = "follow_schedule"
    ADVANCED = "advanced_override"  # until the next setpoint
    PERMANENT = "permanent_override"  # indefinitely
    COUNTDOWN = "countdown_override"  # for a number of minutes (max 1,215)
    TEMPORARY = "temporary_override"  # until a given date/time


# ─────────────────────────────────────────────────────────────────────────────
# Discovery: RQ codes for probing known_list devices
# ─────────────────────────────────────────────────────────────────────────────
# Maps device class (from known_list) to the RQ code that will get an RP
# response and (where possible) trigger topology builder promotion.
# Codes chosen from _DEV_KLASSES_HEAT / _DEV_KLASSES_HVAC in ramses_rf.
#
# Format: (code_hex, payload)
#   code_hex: 4-char hex code string (e.g. "31DA")
#   payload:  hex payload string for the RQ
DISCOVERY_CODES_BY_CLASS: Final[dict[str, tuple[str, str]]] = {
    # HVAC
    "FAN": ("31DA", "00"),  # RP 31DA → promotes to FAN
    "REM": ("10D0", "00"),  # RP 10D0 — detection only (promotes on I 22F1)
    "CO2": ("10E0", "00"),  # RP 10E0 — detection only (promotes on I 1298)
    # Heat
    "CTL": ("2E04", "00"),  # system mode
    "OTB": ("3220", "00"),  # RP 3220 → promotes to OTB
    "BDR": ("0008", "00"),  # RP 0008 (prefix 13: auto-promotes)
    "UFC": ("000C", "00"),  # RP 000C (prefix 02: not in topology builder)
    "TRV": ("0016", "00"),  # RP 0016 (prefix 04: auto-promotes)
    "DHW": ("10A0", "00"),  # RP 10A0 from type 07 → promotes to DHW
    "THM": ("2309", "00"),  # RQ 2309 (prefix 12:/22:/34: auto-promotes)
    "RFG": ("10E0", "00"),  # RP 10E0
    "PRG": ("1090", "00"),  # RP 1090
}

# Fallback RQ code by address prefix when no class is specified in known_list.
# Based on address type → likely device type mapping.
DISCOVERY_CODES_BY_PREFIX: Final[dict[str, tuple[str, str]]] = {
    "01": ("2E04", "00"),  # CTL
    "02": ("000C", "00"),  # UFC
    "03": ("0001", "00"),  # HCW (no RP codes, use generic probe)
    "04": ("0016", "00"),  # TRV
    "07": ("10A0", "00"),  # DHW
    "10": ("3220", "00"),  # OTB
    "12": ("2309", "00"),  # THM
    "13": ("0008", "00"),  # BDR
    "18": ("10E0", "00"),  # RFG / HGI
    "22": ("2309", "00"),  # THM
    "23": ("1090", "00"),  # PRG
    "29": ("10D0", "00"),  # REM
    "30": ("10E0", "00"),  # RFG
    "32": ("31DA", "00"),  # FAN / CO2
    "34": ("2309", "00"),  # THM (RND)
}

# Generic probe code used as first attempt when no class or prefix match.
DISCOVERY_GENERIC_CODE: Final[tuple[str, str]] = ("0001", "00")

# Non-responder advice by device class/prefix for warning logs.
DISCOVERY_ADVICE: Final[dict[str, str]] = {
    "REM": "press a button on the remote to wake it, then run discover_known_devices again",
    "CO2": "check device is powered, CO2 sensors broadcast periodically — wait or run again later",
    "HUM": "no RQ probe available — wait for natural broadcast or specify class in known_list",
    "TRV": "adjust the temperature dial on the valve to wake it, then run again",
}

# Delay (seconds) after sending all RQs before checking which devices responded.
DISCOVERY_RESPONSE_DELAY: Final[float] = 3.0

# Service name
SVC_DISCOVER_KNOWN_DEVICES: Final = "discover_known_devices"
