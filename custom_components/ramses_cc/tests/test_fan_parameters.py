from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

# Import the fan_parameters module with type ignore since it might not exist yet
from custom_components.ramses_cc import fan_parameters  # type: ignore[attr-defined]
from custom_components.ramses_cc.broker import RamsesBroker

# Create a type alias for the device class
RamsesFanParametersDevice = fan_parameters.RamsesFanParametersDevice  # type: ignore[attr-defined, no-any-return]


@pytest.fixture
async def mock_broker() -> RamsesBroker:
    """Mock broker instance."""
    broker = MagicMock(spec=RamsesBroker)
    broker.register_fan_param_handler = AsyncMock()
    broker.unregister_fan_param_handler = AsyncMock()
    broker.async_get_fan_param = AsyncMock()
    return broker


@pytest.fixture
async def mock_device(mock_broker: RamsesBroker) -> RamsesFanParametersDevice:
    """Mock fan device instance."""
    return RamsesFanParametersDevice(
        broker=mock_broker,
        device_id="01:123456",
        name="Test Fan",
        fake_device_id="FA:KE01",
        remote_device_id="32:999999",
    )


async def test_device_initialization(mock_device: RamsesFanParametersDevice) -> None:
    """Test device initialization."""
    assert mock_device.fan_device_id == "01:123456"
    assert mock_device.name == "Test Fan Fan Parameters"
    assert mock_device.fake_device_id == "FA:KE01"
    assert mock_device.remote_device_id == "32:999999"
    assert mock_device.device_id == "01:123456"


async def test_parameter_update(mock_device: RamsesFanParametersDevice) -> None:
    """Test parameter update handling."""
    # Register callback
    callback = AsyncMock()
    unregister = mock_device.register_callback(callback)

    # Update parameter
    await mock_device._handle_parameter_update("4E", "1")

    # Verify callback was called
    callback.assert_called_once_with("4E", "1")

    # Verify parameter value stored
    assert mock_device._param_values["4E"] == "1"

    # Test unregistering callback
    unregister()
    await mock_device._handle_parameter_update("4E", "2")
    callback.assert_called_once()  # Should still be 1 call


async def test_get_parameter_value(
    mock_device: RamsesFanParametersDevice, mock_broker: RamsesBroker
) -> None:
    """Test getting parameter value."""
    # Test getting parameter that exists
    mock_device._param_values["4E"] = "1"
    assert mock_device.get_parameter_value("4E") == "1"

    # Test getting non-existent parameter
    assert mock_device.get_parameter_value("4F") is None

    # Test async get parameter
    mock_broker.async_get_fan_param.return_value = None
    await mock_device.async_get_parameter_value("4E")
    mock_broker.async_get_fan_param.assert_called_once()


async def test_device_registration(
    mock_device: RamsesFanParametersDevice, mock_broker: RamsesBroker
) -> None:
    """Test device registration and unregistration."""
    # Test registration
    await mock_device.async_register()
    mock_broker.register_fan_param_handler.assert_called_once()

    # Test unregistration
    await mock_device.async_unregister()
    mock_broker.unregister_fan_param_handler.assert_called_once()


async def test_cleanup(
    mock_device: RamsesFanParametersDevice, mock_broker: RamsesBroker
) -> None:
    """Test device cleanup."""
    # Add some entities and callbacks
    mock_entity = MagicMock()
    mock_entity.async_will_remove_from_hass = AsyncMock()
    mock_device.entities.append(mock_entity)

    callback = AsyncMock()
    mock_device.register_callback(callback)

    # Perform cleanup
    await mock_device.async_will_remove_from_hass()

    # Verify entity cleanup
    mock_entity.async_will_remove_from_hass.assert_called_once()
    assert len(mock_device.entities) == 0

    # Verify callback cleanup
    assert len(mock_device._update_callbacks) == 0

    # Verify broker unregistration
    mock_broker.unregister_fan_param_handler.assert_called_once()


async def test_async_setup_entry(
    hass: HomeAssistant, mock_broker: RamsesBroker
) -> None:
    """Test async setup entry."""
    entry = MagicMock(spec=ConfigEntry)
    entry.options = {"param_remote_id": {"01:123456": "32:999999"}}

    with patch(
        "custom_components.ramses_cc.fan_parameters.RamsesFanParametersDevice"
    ) as mock_device_class:
        mock_device = MagicMock()
        mock_device_class.return_value = mock_device

        # Import here to avoid circular import
        from custom_components.ramses_cc import async_setup_entry

        await async_setup_entry(hass, entry, AsyncMock())

        # Verify device was created with correct parameters
        mock_device_class.assert_called_once_with(
            broker=mock_broker,
            device_id="01:123456",
            name="Test Fan",
            fake_device_id=None,
            remote_device_id="32:999999",
        )

        # Verify device was registered
        mock_device.async_register.assert_called_once()

        # Verify entities were added
        mock_device.async_add_entities.assert_called_once()
