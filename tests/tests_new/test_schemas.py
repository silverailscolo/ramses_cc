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
)
from custom_components.ramses_cc.schemas import (
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
    SZ_ORPHANS,
    SZ_ORPHANS_HEAT,
    SZ_ORPHANS_HVAC,
    SZ_SENSOR,
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
