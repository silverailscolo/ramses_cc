import pytest
from voluptuous import MultipleInvalid

from custom_components.ramses_cc.schemas import (
    _SCH_CMD_CODE,
    _SCH_DEVICE_ID,
    SCH_DOMAIN_CONFIG,
    SCH_MINIMUM_TCS,
)


def test_device_id_schema() -> None:
    """Test the device ID schema."""
    valid_ids = [
        "01:123456",
        "32:999999",
        "10:000000",
    ]

    invalid_ids = [
        "01:12345",  # Too short
        "01:1234567",  # Too long
        "01:12345x",  # Invalid character
        "1:123456",  # First part too short
        "011:123456",  # First part too long
    ]

    for valid_id in valid_ids:
        assert _SCH_DEVICE_ID(valid_id) == valid_id

    for invalid_id in invalid_ids:
        with pytest.raises(MultipleInvalid):
            _SCH_DEVICE_ID(invalid_id)


def test_command_code_schema() -> None:
    """Test the command code schema."""
    valid_codes = [
        "0000",
        "FFFF",
        "1234",
        "ABCD",
    ]

    invalid_codes = [
        "000",  # Too short
        "00000",  # Too long
        "123X",  # Invalid character
        "123",  # Too short
        "12345",  # Too long
    ]

    for valid_code in valid_codes:
        assert _SCH_CMD_CODE(valid_code) == valid_code

    for invalid_code in invalid_codes:
        with pytest.raises(MultipleInvalid):
            _SCH_CMD_CODE(invalid_code)


def test_domain_config_schema() -> None:
    """Test the domain configuration schema."""
    valid_config = {
        "ramses_rf": {"port_config": {"serial_port": "/dev/ttyUSB0"}},
        "scan_interval": 60,
        "advanced_features": {
            "send_packet": True,
            "dev_mode": False,
            "unknown_codes": True,
        },
        "known_list": {"01:123456": {"class": "FAN", "param_remote": "32:999999"}},
    }

    invalid_config = {
        "ramses_rf": {"port_config": {"serial_port": "/dev/ttyUSB0"}},
        "scan_interval": 2,  # Below minimum
        "advanced_features": {
            "send_packet": "not_a_bool",  # Invalid type
            "dev_mode": False,
            "unknown_codes": True,
        },
        "known_list": {
            "01:123456": {
                "class": "FAN",
                "param_remote": "invalid_id",  # Invalid device ID
            }
        },
    }

    # Test valid config
    try:
        SCH_DOMAIN_CONFIG(valid_config)
    except MultipleInvalid as e:
        pytest.fail(f"Valid config should not raise errors: {e}")

    # Test invalid config
    with pytest.raises(MultipleInvalid):
        SCH_DOMAIN_CONFIG(invalid_config)


def test_minimum_tcs_schema() -> None:
    """Test the minimum TCS schema."""
    valid_tcs = {
        "system": {"appliance_control": "10:123456"},
        "zones": {"zone1": {"sensor": "01:123456"}},
    }

    invalid_tcs = {
        "system": {
            "appliance_control": "invalid_id"  # Invalid device ID
        },
        "zones": {
            "zone1": {
                "sensor": "01:12345"  # Invalid device ID
            }
        },
    }

    # Test valid TCS
    try:
        SCH_MINIMUM_TCS(valid_tcs)
    except MultipleInvalid as e:
        pytest.fail(f"Valid TCS should not raise errors: {e}")

    # Test invalid TCS
    with pytest.raises(MultipleInvalid):
        SCH_MINIMUM_TCS(invalid_tcs)


def test_param_remote_id_in_known_list():
    """Test that param_remote_id is properly validated in known_list."""
    valid_config = {
        "known_list": {"01:123456": {"class": "FAN", "param_remote": "32:999999"}}
    }

    invalid_config = {
        "known_list": {
            "01:123456": {
                "class": "FAN",
                "param_remote": "invalid_id",  # Invalid device ID
            }
        }
    }

    try:
        SCH_DOMAIN_CONFIG(valid_config)
    except MultipleInvalid as e:
        pytest.fail(f"Valid config with param_remote should not raise errors: {e}")

    with pytest.raises(MultipleInvalid):
        SCH_DOMAIN_CONFIG(invalid_config)
