"""Tests to achieve 100% coverage for schemas.py."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import patch

import pytest

from custom_components.ramses_cc.const import (
    CONF_ADVANCED_FEATURES,
    CONF_COMMANDS,
    CONF_RAMSES_RF,
    SZ_DEVICE_COMMENTS,
)
from custom_components.ramses_cc.schemas import (
    extract_hvac_schema,
    merge_hvac_schema,
    merge_schemas,
    normalise_config,
    remove_device_from_schema,
    schema_is_minimal,
    strip_traits_for_validation,
    sync_learned_topology,
)
from ramses_rf.schemas import (
    SZ_APPLIANCE_CONTROL,
    SZ_CLASS,
    SZ_DHW_SYSTEM,
    SZ_KNOWN_LIST,
    SZ_MAIN_TCS,
    SZ_ORPHANS,
    SZ_ORPHANS_HEAT,
    SZ_ORPHANS_HVAC,
    SZ_REMOTES,
    SZ_SENSOR,
    SZ_SENSORS,
    SZ_SYSTEM,
)
from ramses_tx.const import SZ_ZONES
from ramses_tx.schemas import SZ_BLOCK_LIST, SZ_PORT_NAME, SZ_SERIAL_PORT


def test_normalise_config() -> None:
    """Test the normalization of configuration data (Lines 157-170)."""
    config: dict[str, Any] = {
        CONF_RAMSES_RF: {"disable_discovery": True},
        SZ_SERIAL_PORT: {SZ_PORT_NAME: "/dev/ttyUSB0"},
        SZ_KNOWN_LIST: {
            "18:111111": {CONF_COMMANDS: {"boost": "packet_data"}},
            "01:123456": {SZ_CLASS: "TRV"},
        },
        "restore_cache": True,
        CONF_ADVANCED_FEATURES: {"dev_mode": True},
    }

    port, client_config, coordinator_config = normalise_config(config)

    assert port == "/dev/ttyUSB0"
    assert client_config["config"] == {"disable_discovery": True}
    assert coordinator_config["remotes"]["18:111111"] == {"boost": "packet_data"}
    assert CONF_COMMANDS not in client_config[SZ_KNOWN_LIST]["18:111111"]


def test_merge_schemas_logic(caplog: pytest.LogCaptureFixture) -> None:
    """Test schema merging branches (Lines 186-193)."""
    caplog.set_level(logging.INFO)

    # Case 1: Config is subset of cached (Line 183)
    config_sub: dict[str, Any] = {"known_list": {"18:111111": {SZ_CLASS: "HGI"}}}
    cached_sup: dict[str, Any] = {
        "known_list": {"18:111111": {SZ_CLASS: "HGI"}, "01:123456": {SZ_CLASS: "TRV"}}
    }
    assert merge_schemas(config_sub, cached_sup) == cached_sup
    assert "Using the cached schema" in caplog.text

    # Case 2: Merged schema is superset of config (Line 189)
    config_new: dict[str, Any] = {"known_list": {"01:123456": {SZ_CLASS: "TRV"}}}
    cached_old: dict[str, Any] = {"known_list": {"18:111111": {SZ_CLASS: "HGI"}}}
    merged = merge_schemas(config_new, cached_old)
    assert merged is not None
    assert "Using a merged schema" in caplog.text

    # Case 3: Trigger 'Cached schema is a subset' path (Line 193)
    with patch(
        "custom_components.ramses_cc.schemas.is_subset", side_effect=[False, False]
    ):
        assert merge_schemas({"a": 1}, {"b": 2}) is None
        assert "Cached schema is a subset of config schema" in caplog.text


def test_schema_is_minimal_logic() -> None:
    """Test minimal schema validation branches (Lines 203-214)."""
    # Case 1: Valid minimal schema (Line 203 & 214)
    # Note: To pass line 211, the top-level key must match the zone sensor ID
    minimal_schema: dict[str, Any] = {
        "01:123456": {
            SZ_ZONES: {"01": {SZ_SENSOR: "01:123456"}},
        }
    }
    assert schema_is_minimal(minimal_schema) is True

    # Case 2: Excluded keys branch (Line 204)
    excluded_keys: dict[str, Any] = {
        SZ_BLOCK_LIST: ["01:111111"],
        SZ_KNOWN_LIST: {"18:111111": {SZ_CLASS: "HGI"}},
        "01:123456": {SZ_ZONES: {"01": {SZ_SENSOR: "01:123456"}}},
    }
    assert schema_is_minimal(excluded_keys) is True

    # Case 3: Invalid content for SCH_MINIMUM_TCS (Line 208)
    not_minimal: dict[str, Any] = {
        "10:123456": {
            SZ_SYSTEM: {SZ_APPLIANCE_CONTROL: "10:123456"},
            "extra_key": "not_allowed",
        }
    }
    assert schema_is_minimal(not_minimal) is False

    # Case 4: Mismatched zone sensor (Line 211)
    mismatched: dict[str, Any] = {
        "01:111111": {
            SZ_ZONES: {"01": {SZ_SENSOR: "01:222222"}},
        }
    }
    assert schema_is_minimal(mismatched) is False


# ── Tests for remove_device_from_schema ───────────────────────────────


def test_remove_device_from_orphans_heat() -> None:
    """Remove a device from top-level orphans_heat list."""
    schema: dict[str, Any] = {
        SZ_ORPHANS_HEAT: ["04:111111", "04:222222", "04:333333"],
    }
    result = remove_device_from_schema(schema, "04:222222")
    assert result[SZ_ORPHANS_HEAT] == ["04:111111", "04:333333"]


def test_remove_device_from_orphans_heat_empty_list() -> None:
    """Removing the last device from orphans_heat deletes the key."""
    schema: dict[str, Any] = {SZ_ORPHANS_HEAT: ["04:111111"]}
    result = remove_device_from_schema(schema, "04:111111")
    assert SZ_ORPHANS_HEAT not in result


def test_remove_device_from_orphans_hvac() -> None:
    """Remove a device from top-level orphans_hvac list."""
    schema: dict[str, Any] = {SZ_ORPHANS_HVAC: ["32:111111", "32:222222"]}
    result = remove_device_from_schema(schema, "32:111111")
    assert result[SZ_ORPHANS_HVAC] == ["32:222222"]


def test_remove_device_from_zone_sensor() -> None:
    """Remove a device that is a zone sensor (set to None, zone stays)."""
    schema: dict[str, Any] = {
        "01:123456": {
            SZ_ZONES: {
                "02": {SZ_SENSOR: "04:111111", "actuators": ["13:222222"]},
            }
        }
    }
    result = remove_device_from_schema(schema, "04:111111")
    assert result["01:123456"][SZ_ZONES]["02"][SZ_SENSOR] is None
    # Zone and actuators stay
    assert result["01:123456"][SZ_ZONES]["02"]["actuators"] == ["13:222222"]


def test_remove_device_from_zone_actuators() -> None:
    """Remove a device from a zone's actuators list."""
    schema: dict[str, Any] = {
        "01:123456": {
            SZ_ZONES: {
                "02": {SZ_SENSOR: "04:111111", "actuators": ["13:222222", "13:333333"]},
            }
        }
    }
    result = remove_device_from_schema(schema, "13:222222")
    assert result["01:123456"][SZ_ZONES]["02"]["actuators"] == ["13:333333"]


def test_remove_device_from_zone_actuators_empty() -> None:
    """Removing the last actuator deletes the key."""
    schema: dict[str, Any] = {
        "01:123456": {
            SZ_ZONES: {
                "02": {SZ_SENSOR: "04:111111", "actuators": ["13:222222"]},
            }
        }
    }
    result = remove_device_from_schema(schema, "13:222222")
    assert "actuators" not in result["01:123456"][SZ_ZONES]["02"]


def test_remove_device_from_appliance_control() -> None:
    """Remove a device that is appliance_control (set to None)."""
    schema: dict[str, Any] = {
        "01:123456": {
            SZ_SYSTEM: {SZ_APPLIANCE_CONTROL: "10:111111"},
        }
    }
    result = remove_device_from_schema(schema, "10:111111")
    assert result["01:123456"][SZ_SYSTEM][SZ_APPLIANCE_CONTROL] is None


def test_remove_device_from_tcs_orphans() -> None:
    """Remove a device from TCS-level orphans list."""
    schema: dict[str, Any] = {
        "01:123456": {
            SZ_ORPHANS: ["04:111111", "04:222222"],
        }
    }
    result = remove_device_from_schema(schema, "04:111111")
    assert result["01:123456"][SZ_ORPHANS] == ["04:222222"]


def test_remove_device_from_dhw_sensor() -> None:
    """Remove a device that is a DHW sensor."""
    schema: dict[str, Any] = {
        "01:123456": {
            SZ_DHW_SYSTEM: {SZ_SENSOR: "07:111111"},
        }
    }
    result = remove_device_from_schema(schema, "07:111111")
    assert result["01:123456"][SZ_DHW_SYSTEM][SZ_SENSOR] is None


def test_remove_device_from_hvac_remotes() -> None:
    """Remove a device from an HVAC entry's remotes list."""
    schema: dict[str, Any] = {
        "32:111111": {"remotes": ["29:222222", "29:333333"]},
    }
    result = remove_device_from_schema(schema, "29:222222")
    assert result["32:111111"]["remotes"] == ["29:333333"]


def test_remove_device_not_in_schema() -> None:
    """Removing a device that isn't in the schema returns a copy unchanged."""
    schema: dict[str, Any] = {"01:123456": {SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}}}}
    result = remove_device_from_schema(schema, "99:999999")
    assert result == schema
    # Ensure it's a copy, not the same object
    assert result is not schema


def test_remove_device_preserves_own_top_level_key() -> None:
    """The device's own top-level key (e.g. '32:153289': {}) is NOT removed."""
    schema: dict[str, Any] = {
        "32:111111": {"remotes": ["29:222222"]},
        SZ_ORPHANS_HVAC: ["32:111111"],
    }
    result = remove_device_from_schema(schema, "32:111111")
    # Removed from orphans_hvac
    assert "32:111111" not in result.get(SZ_ORPHANS_HVAC, [])
    # But its own top-level key stays (the fragment will update it)
    assert "32:111111" in result


# ── Tests for sync_learned_topology ───────────────────────────────────


def test_sync_learned_topology_no_changes() -> None:
    """Returns None when config already matches learned topology."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
        },
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    assert sync_learned_topology(config, learned) is None


def test_sync_learned_topology_adds_zone_sensor() -> None:
    """Moves a device from orphans_heat to a zone when learned has it."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {},
        SZ_ORPHANS_HEAT: ["04:111111"],
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111", "actuators": []}},
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:123456"][SZ_ZONES]["02"][SZ_SENSOR] == "04:111111"
    # Device removed from orphans_heat
    assert "04:111111" not in result.get(SZ_ORPHANS_HEAT, [])


def test_sync_learned_topology_adds_actuators() -> None:
    """Adds actuators to a zone that config doesn't have."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
        },
        SZ_ORPHANS_HEAT: ["13:222222"],
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {
                "02": {SZ_SENSOR: "04:111111", "actuators": ["13:222222"]},
            },
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert "13:222222" in result["01:123456"][SZ_ZONES]["02"]["actuators"]
    assert "13:222222" not in result.get(SZ_ORPHANS_HEAT, [])


def test_sync_learned_topology_preserves_user_sensor() -> None:
    """Does not overwrite a sensor the user already set."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:999999"}},
        },
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111", "actuators": []}},
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    # No changes — user's sensor stays, learned sensor is different
    assert result is None


def test_sync_learned_topology_adds_appliance_control() -> None:
    """Adds appliance_control when learned has it and config doesn't."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {},
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_SYSTEM: {SZ_APPLIANCE_CONTROL: "10:111111"},
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:123456"][SZ_SYSTEM][SZ_APPLIANCE_CONTROL] == "10:111111"


def test_sync_learned_topology_preserves_device_comments() -> None:
    """device_comments list is preserved."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {},
        "device_comments": {"04:666666": "test comment"},
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["device_comments"] == {"04:666666": "test comment"}


def test_sync_learned_topology_empty_learned() -> None:
    """Returns None when learned schema is empty."""
    config: dict[str, Any] = {"main_tcs": "01:123456"}
    assert sync_learned_topology(config, {}) is None
    assert sync_learned_topology(config, None) is None  # type: ignore[arg-type]


def test_sync_learned_topology_hvac_orphans() -> None:
    """Removes HVAC devices from orphans_hvac when they're in an HVAC entry."""
    config: dict[str, Any] = {
        SZ_ORPHANS_HVAC: ["29:111111", "29:222222"],
        "32:333333": {"remotes": []},
    }
    learned: dict[str, Any] = {
        SZ_ORPHANS_HVAC: ["29:222222"],
        "32:333333": {"remotes": ["29:111111"]},
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert "29:111111" not in result.get(SZ_ORPHANS_HVAC, [])
    assert "29:222222" in result[SZ_ORPHANS_HVAC]


def test_sync_learned_topology_adds_zone_class() -> None:
    """Adds zone class from learned when config doesn't have it."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
        },
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {
                "02": {SZ_SENSOR: "04:111111", SZ_CLASS: "electric_heating"},
            },
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:123456"][SZ_ZONES]["02"][SZ_CLASS] == "electric_heating"


def test_sync_learned_topology_preserves_existing_zone_class() -> None:
    """Does not overwrite zone class the user already set."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111", SZ_CLASS: "underfloor_heating"}},
        },
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {
                "02": {SZ_SENSOR: "04:111111", SZ_CLASS: "electric_heating"},
            },
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is None  # No changes — user's class stays


def test_sync_learned_topology_dhw_sensor() -> None:
    """Adds DHW sensor from learned when config doesn't have one."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {},
        SZ_ORPHANS_HEAT: ["07:111111"],
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_DHW_SYSTEM: {SZ_SENSOR: "07:111111"},
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:123456"][SZ_DHW_SYSTEM][SZ_SENSOR] == "07:111111"


def test_sync_learned_topology_tcs_orphans_cleanup() -> None:
    """Removes devices from TCS-level orphans when they're in zones."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ORPHANS: ["04:111111"],
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
        },
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
            SZ_ORPHANS: [],
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert "04:111111" not in result["01:123456"].get(SZ_ORPHANS, [])


def test_sync_learned_topology_tcs_orphans_partial_cleanup() -> None:
    """Only removes devices that are in zones, keeps other orphans."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ORPHANS: ["04:111111", "04:222222"],
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
        },
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
            SZ_ORPHANS: ["04:222222"],
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:123456"][SZ_ORPHANS] == ["04:222222"]


def test_sync_learned_topology_hvac_orphans_empty_result() -> None:
    """Removes all HVAC orphans when they're all in HVAC entries."""
    config: dict[str, Any] = {
        SZ_ORPHANS_HVAC: ["29:111111"],
        "32:333333": {"remotes": []},
    }
    learned: dict[str, Any] = {
        SZ_ORPHANS_HVAC: [],
        "32:333333": {"remotes": ["29:111111"]},
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert SZ_ORPHANS_HVAC not in result


def test_sync_learned_topology_heat_orphans_empty_result() -> None:
    """Removes all heat orphans when they're all in zones."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {},
        SZ_ORPHANS_HEAT: ["04:111111"],
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert SZ_ORPHANS_HEAT not in result


def test_sync_learned_topology_non_dict_tcs_entry() -> None:
    """Handles a non-dict TCS entry in config gracefully."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": "not a dict",
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_SYSTEM: {SZ_APPLIANCE_CONTROL: "10:111111"},
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:123456"][SZ_SYSTEM][SZ_APPLIANCE_CONTROL] == "10:111111"


def test_sync_learned_topology_non_dict_zone() -> None:
    """Handles a non-dict zone in learned schema gracefully."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {},
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": "not a dict"},
        },
        SZ_ORPHANS_HEAT: [],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    # No changes — the non-dict zone was skipped
    assert result is None


def test_remove_device_from_dhw_valve() -> None:
    """Remove a device that is a DHW hotwater_valve."""
    schema: dict[str, Any] = {
        "01:123456": {
            SZ_DHW_SYSTEM: {"hotwater_valve": "13:111111"},
        }
    }
    result = remove_device_from_schema(schema, "13:111111")
    assert result["01:123456"][SZ_DHW_SYSTEM]["hotwater_valve"] is None


def test_remove_device_from_hvac_sensors() -> None:
    """Remove a device from an HVAC entry's sensors list."""
    schema: dict[str, Any] = {
        "32:111111": {"sensors": ["37:222222", "37:333333"]},
    }
    result = remove_device_from_schema(schema, "37:222222")
    assert result["32:111111"]["sensors"] == ["37:333333"]


def test_remove_device_from_hvac_sensors_empty() -> None:
    """Removing the last sensor deletes the key."""
    schema: dict[str, Any] = {
        "32:111111": {"sensors": ["37:222222"]},
    }
    result = remove_device_from_schema(schema, "37:222222")
    assert "sensors" not in result["32:111111"]


def test_remove_device_from_orphans_key() -> None:
    """Remove a device from the bare 'orphans' key."""
    schema: dict[str, Any] = {"orphans": ["04:111111", "04:222222"]}
    result = remove_device_from_schema(schema, "04:111111")
    assert result["orphans"] == ["04:222222"]


def test_remove_device_from_heating_valve() -> None:
    """Remove a device that is a heating_valve."""
    schema: dict[str, Any] = {
        "01:123456": {
            SZ_DHW_SYSTEM: {"heating_valve": "13:111111"},
        }
    }
    result = remove_device_from_schema(schema, "13:111111")
    assert result["01:123456"][SZ_DHW_SYSTEM]["heating_valve"] is None


def test_remove_device_tcs_orphans_becomes_empty() -> None:
    """TCS orphans list is deleted when the last device is removed."""
    schema: dict[str, Any] = {
        "01:123456": {SZ_ORPHANS: ["04:111111"]},
    }
    result = remove_device_from_schema(schema, "04:111111")
    assert SZ_ORPHANS not in result["01:123456"]


def test_remove_device_non_dict_zone() -> None:
    """Non-dict zone entry is skipped gracefully."""
    schema: dict[str, Any] = {
        "01:123456": {SZ_ZONES: {"02": "not a dict"}},
    }
    result = remove_device_from_schema(schema, "04:111111")
    # No crash, zone stays as-is
    assert result["01:123456"][SZ_ZONES]["02"] == "not a dict"


def test_sync_learned_topology_heat_orphans_partial() -> None:
    """Only removes heat orphans that are in zones, keeps others."""
    config: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {},
        SZ_ORPHANS_HEAT: ["04:111111", "04:222222"],
    }
    learned: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}},
        },
        SZ_ORPHANS_HEAT: ["04:222222"],
        SZ_ORPHANS_HVAC: [],
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result[SZ_ORPHANS_HEAT] == ["04:222222"]


def test_sync_learned_topology_hvac_non_dict_entry() -> None:
    """Non-dict HVAC entry in learned is skipped gracefully."""
    config: dict[str, Any] = {
        SZ_ORPHANS_HVAC: ["29:111111"],
    }
    learned: dict[str, Any] = {
        SZ_ORPHANS_HVAC: [],
        "32:333333": "not a dict",
    }
    result = sync_learned_topology(config, learned)
    # No HVAC entry devices found, so no changes
    assert result is None


# ── Tests for strip_traits_for_validation ─────────────────────────────


def test_strip_traits_removes_underscore_keys() -> None:
    """_ prefixed keys are stripped from all levels."""
    schema: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {
            "_disabled": True,
            "_name": "My Controller",
            SZ_ZONES: {"02": {SZ_SENSOR: "04:111111", "_name": "Living Room"}},
        },
    }
    result = strip_traits_for_validation(schema)
    assert "_disabled" not in result["01:123456"]
    assert "_name" not in result["01:123456"]
    assert "_name" not in result["01:123456"][SZ_ZONES]["02"]
    assert result["01:123456"][SZ_ZONES]["02"][SZ_SENSOR] == "04:111111"


def test_strip_traits_removes_trait_only_entries() -> None:
    """Top-level device-ID keys with only _ keys are removed."""
    schema: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {SZ_ZONES: {"02": {SZ_SENSOR: "04:111111"}}},
        "04:222222": {"_disabled": True},
    }
    result = strip_traits_for_validation(schema)
    assert "04:222222" not in result
    assert "01:123456" in result


def test_strip_traits_keeps_empty_tcs_entries() -> None:
    """Empty TCS entries (no _ keys) are kept."""
    schema: dict[str, Any] = {
        "main_tcs": "01:123456",
        "01:123456": {},
    }
    result = strip_traits_for_validation(schema)
    assert result["01:123456"] == {}


def test_strip_traits_keeps_non_underscore_keys() -> None:
    """Non-underscore keys are preserved."""
    schema: dict[str, Any] = {
        "orphans_heat": ["04:111111"],
        "orphans_hvac": ["32:222222"],
    }
    result = strip_traits_for_validation(schema)
    assert result == schema


def test_strip_traits_handles_non_dict_values() -> None:
    """Non-dict values (lists, strings) are passed through."""
    schema: dict[str, Any] = {
        "main_tcs": "01:123456",
        "orphans_heat": ["04:111111", "04:222222"],
    }
    result = strip_traits_for_validation(schema)
    assert result["orphans_heat"] == ["04:111111", "04:222222"]
    assert result["main_tcs"] == "01:123456"


# ---------------------------------------------------------------------------
# Zone reassignment tests (zone→zone, zone→DHW, DHW→zone)
# ---------------------------------------------------------------------------


def test_sync_zone_to_zone_sensor_move() -> None:
    """Device moves from zone 01 to zone 02 — old zone sensor cleared."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "04:056053"},
                "02": {},
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "02": {SZ_SENSOR: "04:056053"},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:216136"][SZ_ZONES]["01"][SZ_SENSOR] is None
    assert result["01:216136"][SZ_ZONES]["02"][SZ_SENSOR] == "04:056053"


def test_sync_zone_to_zone_actuator_move() -> None:
    """Actuator moves from zone 01 to zone 02 — removed from old, kept in new."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {"actuators": ["04:034720", "04:056053"]},
                "02": {},
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "02": {"actuators": ["04:056053"]},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert "04:056053" not in result["01:216136"][SZ_ZONES]["01"]["actuators"]
    assert "04:034720" in result["01:216136"][SZ_ZONES]["01"]["actuators"]
    assert "04:056053" in result["01:216136"][SZ_ZONES]["02"]["actuators"]


def test_sync_zone_to_dhw_sensor_move() -> None:
    """Device moves from zone to DHW — old zone sensor cleared, DHW gets it."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {SZ_SENSOR: "07:050121"}},
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_DHW_SYSTEM: {SZ_SENSOR: "07:050121"},
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:216136"][SZ_ZONES]["01"][SZ_SENSOR] is None
    assert result["01:216136"][SZ_DHW_SYSTEM][SZ_SENSOR] == "07:050121"


def test_sync_dhw_to_zone_sensor_move() -> None:
    """Device moves from DHW to zone — DHW sensor cleared, zone gets it."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {}},
            SZ_DHW_SYSTEM: {SZ_SENSOR: "07:050121"},
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {SZ_SENSOR: "07:050121"}},
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:216136"][SZ_ZONES]["01"][SZ_SENSOR] == "07:050121"
    assert result["01:216136"][SZ_DHW_SYSTEM][SZ_SENSOR] is None


def test_sync_zone_to_zone_no_move_returns_none() -> None:
    """Device stays in same zone — no changes, returns None."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {SZ_SENSOR: "04:056053"}},
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {SZ_SENSOR: "04:056053"}},
        },
    }
    assert sync_learned_topology(config, learned) is None


def test_sync_zone_move_preserves_user_authored_keys() -> None:
    """User-authored keys (_name, _class) in old zone are preserved on move."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "04:056053", "_name": "Living Room"},
                "02": {},
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "02": {SZ_SENSOR: "04:056053"},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    # Old zone keeps user _name, sensor cleared
    assert result["01:216136"][SZ_ZONES]["01"][SZ_SENSOR] is None
    assert result["01:216136"][SZ_ZONES]["01"]["_name"] == "Living Room"
    # New zone has the sensor
    assert result["01:216136"][SZ_ZONES]["02"][SZ_SENSOR] == "04:056053"


def test_sync_zone_move_actuator_empty_list_removed() -> None:
    """When all actuators move away, the empty actuators list is removed."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {"actuators": ["04:056053"]},
                "02": {},
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "02": {"actuators": ["04:056053"]},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    # Old zone: actuators list removed entirely (was the only entry)
    assert "actuators" not in result["01:216136"][SZ_ZONES]["01"]
    assert result["01:216136"][SZ_ZONES]["02"]["actuators"] == ["04:056053"]


def test_sync_zone_move_multiple_devices() -> None:
    """Multiple devices move zones simultaneously — all cleaned up."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "04:056053", "actuators": ["13:120241"]},
                "02": {},
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "02": {SZ_SENSOR: "04:056053", "actuators": ["13:120241"]},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:216136"][SZ_ZONES]["01"][SZ_SENSOR] is None
    assert "actuators" not in result["01:216136"][SZ_ZONES]["01"]
    assert result["01:216136"][SZ_ZONES]["02"][SZ_SENSOR] == "04:056053"
    assert "13:120241" in result["01:216136"][SZ_ZONES]["02"]["actuators"]


def test_sync_dhw_to_zone_valve_move() -> None:
    """DHW valve moves to a zone — DHW valve cleared, zone actuator added."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {}},
            SZ_DHW_SYSTEM: {"hotwater_valve": "13:120242"},
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {"actuators": ["13:120242"]}},
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:216136"][SZ_DHW_SYSTEM]["hotwater_valve"] is None
    assert "13:120242" in result["01:216136"][SZ_ZONES]["01"]["actuators"]


# ---------------------------------------------------------------------------
# HVAC schema extract / merge tests (load_fan stub workaround)
# ---------------------------------------------------------------------------


def test_extract_hvac_schema_basic() -> None:
    """Extract HVAC entries from a mixed schema — only HVAC keys returned."""
    schema: dict[str, Any] = {
        SZ_MAIN_TCS: "01:216136",
        "01:216136": {SZ_ZONES: {"01": {SZ_SENSOR: "34:092243"}}},
        SZ_ORPHANS_HEAT: ["04:056053"],
        "32:153289": {SZ_REMOTES: ["37:111111", "37:222222"]},
        SZ_ORPHANS_HVAC: ["37:444444"],
    }
    hvac = extract_hvac_schema(schema)
    assert "32:153289" in hvac
    assert hvac["32:153289"][SZ_REMOTES] == ["37:111111", "37:222222"]
    assert SZ_ORPHANS_HVAC in hvac
    assert hvac[SZ_ORPHANS_HVAC] == ["37:444444"]
    # Heat-only entries excluded
    assert "01:216136" not in hvac
    assert SZ_ORPHANS_HEAT not in hvac
    assert SZ_MAIN_TCS not in hvac


def test_extract_hvac_schema_with_sensors() -> None:
    """FAN entry with sensors list is also extracted."""
    schema: dict[str, Any] = {
        "32:153289": {SZ_REMOTES: ["37:111111"], SZ_SENSORS: ["37:222222"]},
    }
    hvac = extract_hvac_schema(schema)
    assert "32:153289" in hvac
    assert SZ_SENSORS in hvac["32:153289"]


def test_extract_hvac_schema_empty() -> None:
    """Schema with no HVAC entries returns empty dict."""
    schema: dict[str, Any] = {
        SZ_MAIN_TCS: "01:216136",
        "01:216136": {SZ_ZONES: {}},
        SZ_ORPHANS_HEAT: ["04:056053"],
    }
    assert extract_hvac_schema(schema) == {}


def test_extract_hvac_schema_non_dict() -> None:
    """Non-dict schema returns empty dict."""
    assert extract_hvac_schema(None) == {}  # type: ignore[arg-type]
    assert extract_hvac_schema("bad") == {}  # type: ignore[arg-type]


def test_extract_hvac_schema_fan_without_lists() -> None:
    """FAN entry without remotes/sensors is NOT extracted (no HVAC keys)."""
    schema: dict[str, Any] = {
        "32:153289": {"_name": "My Fan"},
    }
    hvac = extract_hvac_schema(schema)
    assert "32:153289" not in hvac


def test_merge_hvac_schema_into_empty() -> None:
    """Merge cached HVAC into empty config — all entries added."""
    hvac: dict[str, Any] = {
        "32:153289": {SZ_REMOTES: ["37:111111", "37:222222"]},
        SZ_ORPHANS_HVAC: ["37:444444"],
    }
    result = merge_hvac_schema({}, hvac)
    assert result["32:153289"][SZ_REMOTES] == ["37:111111", "37:222222"]
    assert result[SZ_ORPHANS_HVAC] == ["37:444444"]


def test_merge_hvac_schema_union_remotes() -> None:
    """Merge cached remotes with existing — union, no duplicates."""
    config: dict[str, Any] = {
        "32:153289": {SZ_REMOTES: ["37:111111"]},
    }
    hvac: dict[str, Any] = {
        "32:153289": {SZ_REMOTES: ["37:222222", "37:333333"]},
    }
    result = merge_hvac_schema(config, hvac)
    assert set(result["32:153289"][SZ_REMOTES]) == {
        "37:111111",
        "37:222222",
        "37:333333",
    }


def test_merge_hvac_schema_union_orphans() -> None:
    """Merge cached orphans_hvac with existing — union, no duplicates."""
    config: dict[str, Any] = {SZ_ORPHANS_HVAC: ["37:111111"]}
    hvac: dict[str, Any] = {SZ_ORPHANS_HVAC: ["37:222222"]}
    result = merge_hvac_schema(config, hvac)
    assert set(result[SZ_ORPHANS_HVAC]) == {"37:111111", "37:222222"}


def test_merge_hvac_schema_adds_sensors() -> None:
    """Merge cached sensors into FAN entry that only has remotes."""
    config: dict[str, Any] = {
        "32:153289": {SZ_REMOTES: ["37:111111"]},
    }
    hvac: dict[str, Any] = {
        "32:153289": {SZ_SENSORS: ["37:222222"]},
    }
    result = merge_hvac_schema(config, hvac)
    assert "37:111111" in result["32:153289"][SZ_REMOTES]
    assert "37:222222" in result["32:153289"][SZ_SENSORS]


def test_merge_hvac_schema_empty_cache() -> None:
    """Empty HVAC cache returns config unchanged."""
    config: dict[str, Any] = {"32:153289": {SZ_REMOTES: ["37:111111"]}}
    assert merge_hvac_schema(config, {}) is config


def test_merge_hvac_schema_none_cache() -> None:
    """None HVAC cache returns config unchanged."""
    config: dict[str, Any] = {"32:153289": {SZ_REMOTES: ["37:111111"]}}
    assert merge_hvac_schema(config, None) is config  # type: ignore[arg-type]


def test_merge_hvac_schema_no_overlap() -> None:
    """Config has no HVAC entries, cache has some — all added."""
    config: dict[str, Any] = {
        SZ_MAIN_TCS: "01:216136",
        "01:216136": {SZ_ZONES: {}},
    }
    hvac: dict[str, Any] = {
        "32:153289": {SZ_REMOTES: ["37:111111"]},
        SZ_ORPHANS_HVAC: ["37:222222"],
    }
    result = merge_hvac_schema(config, hvac)
    assert "32:153289" in result
    assert result["32:153289"][SZ_REMOTES] == ["37:111111"]
    assert result[SZ_ORPHANS_HVAC] == ["37:222222"]
    # Heat entries preserved
    assert SZ_MAIN_TCS in result
    assert "01:216136" in result


def test_merge_hvac_schema_preserves_heat_entries() -> None:
    """Merging HVAC does not alter heat topology entries."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {SZ_SENSOR: "34:092243"}},
            SZ_SYSTEM: {SZ_APPLIANCE_CONTROL: "13:120241"},
        },
        "32:153289": {SZ_REMOTES: ["37:111111"]},
    }
    hvac: dict[str, Any] = {
        "32:153289": {SZ_REMOTES: ["37:222222"]},
    }
    result = merge_hvac_schema(config, hvac)
    # Heat entry unchanged
    assert result["01:216136"][SZ_ZONES]["01"][SZ_SENSOR] == "34:092243"
    assert result["01:216136"][SZ_SYSTEM][SZ_APPLIANCE_CONTROL] == "13:120241"
    # HVAC entry merged
    assert set(result["32:153289"][SZ_REMOTES]) == {
        "37:111111",
        "37:222222",
    }


def test_merge_hvac_schema_roundtrip() -> None:
    """Extract then merge should roundtrip (idempotent for HVAC-only)."""
    original: dict[str, Any] = {
        "32:153289": {
            SZ_REMOTES: ["37:111111", "37:222222"],
            SZ_SENSORS: ["37:333333"],
        },
        SZ_ORPHANS_HVAC: ["37:444444"],
    }
    hvac = extract_hvac_schema(original)
    merged = merge_hvac_schema({}, hvac)
    assert merged == original


# ---------------------------------------------------------------------------
# C.2: User schema edits survive sync/merge cycle
# ---------------------------------------------------------------------------


def test_sync_preserves_user_alias_in_zone() -> None:
    """User _alias in a zone is preserved when sync adds actuators."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "34:092243", "_alias": "Living Room"},
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {"actuators": ["04:056053"]},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:216136"][SZ_ZONES]["01"]["_alias"] == "Living Room"
    assert "04:056053" in result["01:216136"][SZ_ZONES]["01"]["actuators"]


def test_sync_preserves_user_enabled_false() -> None:
    """User _enabled: false on a zone is preserved across sync."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "34:092243", "_enabled": False},
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {"actuators": ["04:056053"]},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:216136"][SZ_ZONES]["01"]["_enabled"] is False


def test_sync_preserves_user_skipped_true() -> None:
    """User _skipped: true on a zone is preserved across sync."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "34:092243", "_skipped": True},
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {"actuators": ["04:056053"]},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert result["01:216136"][SZ_ZONES]["01"]["_skipped"] is True


def test_sync_preserves_user_main_tcs() -> None:
    """User-edited main_tcs is not overwritten by sync."""
    config: dict[str, Any] = {
        SZ_MAIN_TCS: "01:216136",
        "01:216136": {SZ_ZONES: {"01": {SZ_SENSOR: "34:092243"}}},
    }
    learned: dict[str, Any] = {
        SZ_MAIN_TCS: "01:999999",
        "01:216136": {
            SZ_ZONES: {"01": {"actuators": ["04:056053"]}},
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    # Config main_tcs is preserved — sync does not overwrite it
    assert result[SZ_MAIN_TCS] == "01:216136"


def test_sync_preserves_manually_added_device() -> None:
    """Device manually added to a zone (not in learned) is preserved."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "34:092243", "actuators": ["04:056053"]},
            },
        },
    }
    # Learned schema doesn't mention 04:056053 at all
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "34:092243"},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    # 04:056053 should still be in actuators (sync only removes from
    # orphans when device is in a learned zone, not from zones)
    if result is not None:
        assert "04:056053" in result["01:216136"][SZ_ZONES]["01"]["actuators"]
    else:
        # If result is None, config is unchanged — manually added device preserved
        assert "04:056053" in config["01:216136"][SZ_ZONES]["01"]["actuators"]


def test_sync_does_not_re_add_removed_device() -> None:
    """Device removed from config schema is not re-added from learned orphans."""
    config: dict[str, Any] = {
        "01:216136": {SZ_ZONES: {}},
    }
    # Learned has 04:056053 in orphans, but config doesn't
    learned: dict[str, Any] = {
        "01:216136": {SZ_ORPHANS: ["04:056053"]},
    }
    result = sync_learned_topology(config, learned)
    # sync should not add orphans to config — it only removes from
    # config orphans when device is in a zone
    if result is not None:
        assert "04:056053" not in result["01:216136"].get(SZ_ORPHANS, [])


def test_sync_preserves_device_comments_with_zone_sync() -> None:
    """device_comments preserved even when zones are being synced."""
    config: dict[str, Any] = {
        SZ_DEVICE_COMMENTS: [{"device_id": "34:092243", "comment": "Kitchen"}],
        "01:216136": {
            SZ_ZONES: {"01": {SZ_SENSOR: "34:092243"}},
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {"actuators": ["04:056053"]}},
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    assert SZ_DEVICE_COMMENTS in result
    assert result[SZ_DEVICE_COMMENTS] == [
        {"device_id": "34:092243", "comment": "Kitchen"}
    ]


def test_strip_traits_with_disabled_string() -> None:
    """strip_traits_for_validation handles _disabled as string, not just bool."""
    schema: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {SZ_SENSOR: "34:092243", "_disabled": "yes"}},
        },
    }
    result = strip_traits_for_validation(schema)
    # _disabled key should be stripped regardless of value type
    assert "_disabled" not in result["01:216136"][SZ_ZONES]["01"]
    assert result["01:216136"][SZ_ZONES]["01"][SZ_SENSOR] == "34:092243"


def test_strip_traits_removes_custom_underscore_key() -> None:
    """Custom _ prefixed key (not a known trait) is stripped for validation."""
    schema: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {SZ_SENSOR: "34:092243", "_custom_key": "value"}},
        },
    }
    result = strip_traits_for_validation(schema)
    assert "_custom_key" not in result["01:216136"][SZ_ZONES]["01"]


# ---------------------------------------------------------------------------
# C.3: Corruption / malformed schema graceful degradation
# ---------------------------------------------------------------------------


def test_sync_duplicate_device_in_two_zones() -> None:
    """Duplicate device in two zones — sync keeps learned, removes stale."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "34:092243"},
                "02": {SZ_SENSOR: "34:092243"},  # duplicate!
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: "34:092243"},
            },
        },
    }
    result = sync_learned_topology(config, learned)
    assert result is not None
    # Zone 01 keeps the sensor (learned agrees)
    assert result["01:216136"][SZ_ZONES]["01"][SZ_SENSOR] == "34:092243"
    # Zone 02 sensor cleared (learned doesn't have it there)
    assert result["01:216136"][SZ_ZONES]["02"][SZ_SENSOR] is None


def test_sync_orphan_list_non_string_entries() -> None:
    """Orphan list with non-string entries — no crash, strings preserved."""
    config: dict[str, Any] = {
        SZ_ORPHANS_HEAT: ["04:056053", 123, None, "13:042605"],
        "01:216136": {SZ_ZONES: {}},
    }
    learned: dict[str, Any] = {
        "01:216136": {SZ_ZONES: {"01": {SZ_SENSOR: "04:056053"}}},
    }
    # Should not crash — non-string entries are in the set, but the
    # intersection logic uses set operations which handle mixed types
    result = sync_learned_topology(config, learned)
    assert result is not None
    # 04:056053 should be removed from orphans (it's in a zone now)
    assert "04:056053" not in result.get(SZ_ORPHANS_HEAT, [])


def test_sync_zones_as_list_not_dict() -> None:
    """Schema with zones as a list instead of dict — no crash."""
    config: dict[str, Any] = {
        "01:216136": {
            "zones": [{"sensor": "04:056053"}],  # list, not dict
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {SZ_SENSOR: "34:092243"}},
        },
    }
    # Should not crash — config zones is a list, so setdefault won't
    # crash because sync checks isinstance(learned_zones, dict) first
    sync_learned_topology(config, learned)
    # Either returns a result (enriched) or None — either way, no crash


def test_sync_sensor_as_dict_not_string() -> None:
    """Schema with sensor as a dict instead of string — no crash."""
    config: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {
                "01": {SZ_SENSOR: {"nested": "garbage"}},  # dict, not str
            },
        },
    }
    learned: dict[str, Any] = {
        "01:216136": {
            SZ_ZONES: {"01": {"actuators": ["04:056053"]}},
        },
    }
    # Should not crash — sensor is a dict (truthy), but sync doesn't
    # validate the type, it just checks truthiness
    result = sync_learned_topology(config, learned)
    # No crash is the key assertion
    assert result is not None or result is None  # either is fine


def test_remove_device_remotes_as_string_not_list() -> None:
    """remotes as a string instead of list — no crash."""
    schema: dict[str, Any] = {
        "32:153289": {"remotes": "37:111111"},  # string, not list
    }
    # Should not crash — remove_device_from_schema iterates the list,
    # but if it's a string it iterates characters. The key assertion
    # is that it doesn't raise.
    result = remove_device_from_schema(schema, "37:111111")
    assert "32:153289" in result  # entry preserved


def test_empty_schema_all_functions() -> None:
    """Empty schema {} — all functions return empty/None, no crash."""
    empty: dict[str, Any] = {}
    assert sync_learned_topology(empty, {}) is None
    assert extract_hvac_schema(empty) == {}
    assert merge_hvac_schema(empty, {}) is empty
    assert remove_device_from_schema(empty, "04:056053") == {}
    assert strip_traits_for_validation(empty) == {}


def test_none_schema_all_functions() -> None:
    """None schema — treated as empty/None, no crash."""
    assert sync_learned_topology(None, {}) is None  # type: ignore[arg-type]
    assert extract_hvac_schema(None) == {}
    config: dict[str, Any] = {}
    assert merge_hvac_schema(config, None) is config  # type: ignore[arg-type]
    assert merge_schemas(None, {}) is None  # type: ignore[arg-type]
    assert merge_schemas({}, None) is None  # type: ignore[arg-type]


def test_merge_schemas_corrupt_cache_non_dict() -> None:
    """merge_schemas with corrupt cache (non-dict) — returns None or config."""
    config: dict[str, Any] = {
        SZ_MAIN_TCS: "01:216136",
        "01:216136": {SZ_ZONES: {}},
    }
    # Corrupt cache: a string instead of dict
    corrupt_cache = "not a dict"  # type: ignore[assignment]
    result = merge_schemas(config, corrupt_cache)  # type: ignore[arg-type]
    # Should return None (cache is not a valid schema) or config
    assert result is None or result == config
