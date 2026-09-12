# --- START OF FILE test_binary_sensor.py ---

"""Tests for the ramses_cc binary_sensor platform."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.components.binary_sensor import BinarySensorDeviceClass
from homeassistant.core import HomeAssistant

from custom_components.ramses_cc.binary_sensor import (
    BINARY_SENSOR_DESCRIPTIONS,
    SZ_BATTERY_LEVEL,
    SZ_BATTERY_LOW,
    SZ_BATTERY_STATE,
    RamsesBatteryBinarySensor,
    RamsesBinarySensor,
    RamsesBinarySensorEntityDescription,
    RamsesGatewayBinarySensor,
    RamsesLogbookBinarySensor,
    RamsesPoolChildBinarySensor,
    RamsesPoolStatusSensor,
    RamsesSystemBinarySensor,
    _add_pool_status_entities,
    async_setup_entry,
)
from ramses_rf.const import SZ_FILTER_DIRTY, SZ_FROST_CYCLE, SZ_HAS_FAULT
from ramses_rf.devices import HgiGateway, HvacVentilator
from ramses_rf.systems.tcs import Logbook, System
from ramses_tx.const import SZ_IS_EVOFW3


@pytest.fixture
def mock_coordinator() -> MagicMock:
    """Return a mock RamsesCoordinator.

    :return: A mock object simulating the RamsesCoordinator.
    :rtype: MagicMock
    """
    coordinator = MagicMock()
    coordinator.async_register_platform = MagicMock()
    coordinator.is_pool_enabled = False
    return coordinator


async def test_async_setup_entry(
    hass: HomeAssistant, mock_coordinator: MagicMock
) -> None:
    """Test the platform setup and entity creation callback.

    :param hass: The Home Assistant instance.
    :type hass: HomeAssistant
    :param mock_coordinator: The mock coordinator fixture.
    :type mock_coordinator: MagicMock
    """
    # Arrange
    entry = MagicMock()
    entry.runtime_data = mock_coordinator

    mock_add_entities = MagicMock()
    mock_device = MagicMock(spec=HgiGateway)
    mock_device.id = "18:123456"

    # Act
    with patch("custom_components.ramses_cc.binary_sensor.entity_platform"):
        await async_setup_entry(hass, entry, mock_add_entities)

    add_callback = mock_coordinator.async_register_platform.call_args[0][1]
    add_callback([mock_device])
    created_entities = mock_add_entities.call_args[0][0]

    # Assert
    assert mock_coordinator.async_register_platform.called
    assert mock_add_entities.called
    assert len(created_entities) == 1
    assert isinstance(created_entities[0], RamsesGatewayBinarySensor)


def test_pool_status_entities_add_children_on_coordinator_update() -> None:
    """Pool child sensors are added when identity appears after setup."""
    coordinator = MagicMock()
    coordinator.is_pool_enabled = True
    coordinator.entry.entry_id = "entry-one"
    coordinator.entry.async_on_unload = MagicMock()
    coordinator.get_pool_child_status.side_effect = [
        [],
        [
            {
                "child_id": "0",
                "hgi_id": "18:001111",
                "connected": True,
                "availability": "ONLINE",
                "accepted": True,
                "send_ready": True,
            }
        ],
    ]
    remove_listener = MagicMock()
    coordinator.async_add_listener.return_value = remove_listener
    async_add_entities = MagicMock()

    _add_pool_status_entities(coordinator, async_add_entities)
    listener = coordinator.async_add_listener.call_args.args[0]
    listener()

    initial_entities = async_add_entities.call_args_list[0].args[0]
    added_entities = async_add_entities.call_args_list[1].args[0]
    assert isinstance(initial_entities[0], RamsesPoolStatusSensor)
    assert isinstance(added_entities[0], RamsesPoolChildBinarySensor)
    assert added_entities[0].unique_id == (
        "entry-one_pool_child_18:001111_online"
    )
    coordinator.entry.async_on_unload.assert_called_once_with(remove_listener)


def test_pool_aggregate_requires_eligible_child() -> None:
    """An online receive-only child must not make the pool healthy."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-two"
    coordinator.last_update_success = True
    coordinator.get_pool_child_status.return_value = [
        {
            "child_id": "0",
            "hgi_id": "18:001111",
            "connected": True,
            "availability": "ONLINE",
            "accepted": False,
            "send_ready": True,
        }
    ]
    entity = RamsesPoolStatusSensor(coordinator)

    assert entity.unique_id == "entry-two_pool_status_online"
    assert entity.is_on is False
    assert entity.extra_state_attributes["eligible"] == 0


def test_hvac_diagnostic_binary_sensor_descriptions() -> None:
    """Expose ventilator diagnostic flags with safe HA semantics."""
    descriptions = {
        description.key: description
        for description in BINARY_SENSOR_DESCRIPTIONS
        if description.key in (SZ_FILTER_DIRTY, SZ_FROST_CYCLE, SZ_HAS_FAULT)
    }

    assert set(descriptions) == {
        SZ_FILTER_DIRTY,
        SZ_FROST_CYCLE,
        SZ_HAS_FAULT,
    }
    assert all(
        description.ramses_rf_class is HvacVentilator
        for description in descriptions.values()
    )
    assert (
        descriptions[SZ_FILTER_DIRTY].device_class
        is BinarySensorDeviceClass.PROBLEM
    )
    assert (
        descriptions[SZ_HAS_FAULT].device_class
        is BinarySensorDeviceClass.PROBLEM
    )
    assert descriptions[SZ_FROST_CYCLE].icon == "mdi:snowflake"
    assert descriptions[SZ_FROST_CYCLE].icon_off == "mdi:snowflake-off"


@patch("custom_components.ramses_cc.binary_sensor.resolve_async_attr")
async def test_generic_binary_sensor(
    mock_resolve_async_attr: MagicMock, mock_coordinator: MagicMock
) -> None:
    """Test RamsesBinarySensor base class logic.

    :param mock_resolve_async_attr: Mock for the async resolver.
    :type mock_resolve_async_attr: MagicMock
    :param mock_coordinator: The mock coordinator fixture.
    :type mock_coordinator: MagicMock
    """
    # Arrange
    description = RamsesBinarySensorEntityDescription(
        key="test_sensor",
        ramses_rf_attr="test_attr",
        name="Test Sensor",
        icon="mdi:test",
        icon_off="mdi:test-off",
    )

    mock_device = MagicMock()
    mock_device.id = "01:123456"
    # Mock library availability delegation so the base RamsesEntity evaluates it correctly
    mock_device.is_available = True

    # Act
    sensor: RamsesBinarySensor = RamsesBinarySensor(
        mock_coordinator, mock_device, description
    )
    avail_state = sensor.available

    # Assert
    assert sensor.unique_id == "01:123456-test_sensor"
    # Assign to a variable first to satisfy Mypy
    assert avail_state is True

    # Act (is_on true)
    mock_resolve_async_attr.return_value = True
    state_1 = sensor.is_on
    icon_1 = sensor.icon

    # Assert
    assert state_1 is True
    assert icon_1 == "mdi:test"

    # Act (is_on false)
    mock_resolve_async_attr.return_value = False
    state_2 = sensor.is_on
    icon_2 = sensor.icon

    # Assert
    assert state_2 is False
    assert icon_2 == "mdi:test-off"

    # Act (Duck-Typing backwards compat)
    mock_resolve_async_attr.return_value = True
    state_3 = sensor.is_on

    # Assert
    assert state_3 is True


@patch("custom_components.ramses_cc.binary_sensor.resolve_async_attr")
async def test_battery_binary_sensor(
    mock_resolve_async_attr: MagicMock, mock_coordinator: MagicMock
) -> None:
    """Test RamsesBatteryBinarySensor.

    :param mock_resolve_async_attr: Mock for the async resolver.
    :type mock_resolve_async_attr: MagicMock
    :param mock_coordinator: The mock coordinator fixture.
    :type mock_coordinator: MagicMock
    """
    # Arrange
    description = RamsesBinarySensorEntityDescription(
        key="test_battery",
        ramses_rf_attr=SZ_BATTERY_LOW,
        name="Test Battery",
        device_class=BinarySensorDeviceClass.BATTERY,
        ramses_cc_class=RamsesBatteryBinarySensor,
    )

    mock_device = MagicMock()
    mock_device.id = "04:123456"

    sensor: RamsesBatteryBinarySensor = RamsesBatteryBinarySensor(
        mock_coordinator, mock_device, description
    )

    # Helper to mock multiple async attribute resolutions on the same entity
    def mock_resolve_state(entity: Any, device: Any, attr: str) -> Any:
        if attr == SZ_BATTERY_STATE:
            return {
                SZ_BATTERY_LEVEL: 0.5,
                SZ_BATTERY_LOW: True,
            }
        if attr == SZ_BATTERY_LOW:
            return True
        return None

    # Act (Battery state present)
    # 1. Battery state present - Mocked via async resolve
    mock_resolve_async_attr.side_effect = mock_resolve_state
    state_1 = sensor.is_on
    attrs = sensor.extra_state_attributes

    # Assert
    assert state_1 is True
    assert attrs[SZ_BATTERY_LEVEL] == 0.5

    # Act (Battery state missing)
    # 2. Battery state missing
    mock_resolve_async_attr.side_effect = lambda e, d, a: None
    attrs_2 = sensor.extra_state_attributes

    # Assert
    assert attrs_2[SZ_BATTERY_LEVEL] == "N/A"


async def test_logbook_binary_sensor_availability(
    mock_coordinator: MagicMock,
) -> None:
    """Test RamsesLogbookBinarySensor availability delegates to device.

    :param mock_coordinator: The mock coordinator fixture.
    :type mock_coordinator: MagicMock
    """
    # Arrange
    description = RamsesBinarySensorEntityDescription(
        key="active_fault",
        name="Active fault",
        ramses_rf_attr="active_faults",
        ramses_cc_class=RamsesLogbookBinarySensor,
        device_class=BinarySensorDeviceClass.PROBLEM,
    )

    mock_device = MagicMock(spec=Logbook)
    mock_device.id = "01:123456"
    mock_device.state_store = MagicMock()

    sensor: RamsesLogbookBinarySensor = RamsesLogbookBinarySensor(
        mock_coordinator, mock_device, description
    )

    # Act (Case A: Device unavailable)
    # Case A: Device is not available (Delegated to library's is_available property)
    mock_device.is_available = False

    # Assert
    assert sensor.available is False

    # Act (Case B: Device available)
    # Case B: Device is available (Delegated to library's is_available property)
    mock_device.is_available = True

    # Assert
    assert sensor.available is True


@patch("custom_components.ramses_cc.binary_sensor.resolve_async_attr")
async def test_logbook_binary_sensor_state(
    mock_resolve_async_attr: MagicMock, mock_coordinator: MagicMock
) -> None:
    """Test RamsesLogbookBinarySensor state based on faults.

    :param mock_resolve_async_attr: Mock for the async resolver.
    :type mock_resolve_async_attr: MagicMock
    :param mock_coordinator: The mock coordinator fixture.
    :type mock_coordinator: MagicMock
    """
    # Arrange
    description = RamsesBinarySensorEntityDescription(
        key="active_fault",
        name="Active fault",
        ramses_rf_attr="active_faults",
        ramses_cc_class=RamsesLogbookBinarySensor,
        device_class=BinarySensorDeviceClass.PROBLEM,
    )

    mock_device = MagicMock(spec=Logbook)
    mock_device.id = "01:123456"

    sensor: RamsesLogbookBinarySensor = RamsesLogbookBinarySensor(
        mock_coordinator, mock_device, description
    )

    # Act (is_on = False)
    # 1. Test is_on = False (No faults)
    mock_resolve_async_attr.return_value = []
    initial_state = sensor.is_on

    # Assert
    assert initial_state is False

    # Act (is_on = True)
    # 2. Test is_on = True (Has faults)
    mock_resolve_async_attr.return_value = [{"fault": "error"}]
    final_state = sensor.is_on

    # Assert
    assert final_state is True

    # Act (is_on = False, issue 841)
    # 3. Test is_on = False when active_faults is None (empty/unloaded log)
    #    must report off, not unknown (regression vs 0.54.3)
    mock_resolve_async_attr.return_value = None
    none_state = sensor.is_on

    # Assert
    assert none_state is False


async def test_system_binary_sensor_availability(
    mock_coordinator: MagicMock,
) -> None:
    """Test RamsesSystemBinarySensor availability delegates to device.

    :param mock_coordinator: The mock coordinator fixture.
    :type mock_coordinator: MagicMock
    """
    # Arrange
    description = RamsesBinarySensorEntityDescription(
        key="status",
        ramses_rf_attr="id",
        name="System status",
        ramses_cc_class=RamsesSystemBinarySensor,
        device_class=BinarySensorDeviceClass.PROBLEM,
    )

    mock_device = MagicMock(spec=System)
    mock_device.id = "01:123456"
    mock_device.state_store = MagicMock()

    sensor: RamsesSystemBinarySensor = RamsesSystemBinarySensor(
        mock_coordinator, mock_device, description
    )

    # Act & Assert (Case A: Device unavailable)
    # Case A: Device is not available (Delegated to library's is_available property)
    mock_device.is_available = False
    assert sensor.available is False

    # Act & Assert (Case B: Device available)
    # Case B: Device is available (Delegated to library's is_available property)
    mock_device.is_available = True
    assert sensor.available is True


@patch("custom_components.ramses_cc.binary_sensor.resolve_async_attr")
async def test_gateway_binary_sensor_attrs(
    mock_resolve_async_attr: MagicMock, mock_coordinator: MagicMock
) -> None:
    """Test RamsesGatewayBinarySensor attribute caching and async schema resolution.

    :param mock_resolve_async_attr: Mock for the attribute resolver helper.
    :type mock_resolve_async_attr: MagicMock
    :param mock_coordinator: The mock coordinator fixture.
    :type mock_coordinator: MagicMock
    """
    # Arrange
    description = RamsesBinarySensorEntityDescription(
        key="status",
        ramses_rf_attr="is_active",
        name="Gateway status",
        ramses_cc_class=RamsesGatewayBinarySensor,
        device_class=BinarySensorDeviceClass.PROBLEM,
    )

    mock_device = MagicMock(spec=HgiGateway)
    mock_device.id = "18:123456"

    # Set up the gateway mock to match the expected structure
    gwy = MagicMock()
    gwy.tcs.id = "01:111111"

    # Mock the resolve_async_attr helper to return a safe schema dict
    mock_resolve_async_attr.return_value = {"system_schema": "test"}

    # Mock the Phase 2.77 configuration facade
    gwy.config = MagicMock()
    gwy.config.known_list = {
        "10:1": {"alias": "test", "class": "RAD", "faked": True}
    }

    gwy._engine = MagicMock()
    gwy._engine._enforce_known_list = True
    gwy._engine._exclude = {}
    gwy._engine._transport.get_extra_info.return_value = True

    mock_device._gateway = gwy

    sensor: RamsesGatewayBinarySensor = RamsesGatewayBinarySensor(
        mock_coordinator, mock_device, description
    )

    # Act
    # Fetch attributes (should cache and utilize the mocked async helper)
    attrs = sensor.extra_state_attributes

    # Assert
    mock_resolve_async_attr.assert_called_once_with(
        sensor, gwy.tcs, "_schema_min"
    )

    assert attrs["config"]["enforce_known_list"] is True
    assert "01:111111" in attrs["schema"]
    assert attrs["schema"]["01:111111"] == {"system_schema": "test"}
    assert attrs[SZ_IS_EVOFW3] is True

    # Verify filtering/shrinking of known_list
    known = attrs["known_list"][0]["10:1"]
    assert known["alias"] == "test"
    assert known["class"] == "RAD"
    assert known["faked"] is True


async def test_gateway_binary_sensor_state(
    mock_coordinator: MagicMock,
) -> None:
    """Test RamsesGatewayBinarySensor is_on state logic.

    :param mock_coordinator: The mock coordinator fixture.
    :type mock_coordinator: MagicMock
    """
    # Arrange
    description = RamsesBinarySensorEntityDescription(
        key="status",
        ramses_rf_attr="is_active",
        name="Gateway status",
        ramses_cc_class=RamsesGatewayBinarySensor,
        device_class=BinarySensorDeviceClass.PROBLEM,
    )

    mock_device = MagicMock(spec=HgiGateway)
    mock_device.id = "18:123456"

    sensor: RamsesGatewayBinarySensor = RamsesGatewayBinarySensor(
        mock_coordinator, mock_device, description
    )

    # Act & Assert
    # 1. Case A: Gateway active (OK) -> is_on False (no problem)
    mock_device.is_active = True
    is_on_check_a = sensor.is_on
    assert is_on_check_a is False

    # 2. Case B: Gateway inactive (dead) -> is_on True (problem)
    mock_device.is_active = False
    is_on_check_b = sensor.is_on
    assert is_on_check_b is True


@patch("custom_components.ramses_cc.binary_sensor.resolve_async_attr")
async def test_logbook_async_added_to_hass(
    mock_resolve: MagicMock,
    mock_coordinator: MagicMock,
) -> None:
    """Test RamsesLogbookBinarySensor.async_added_to_hass polling logic."""
    # Arrange
    description = RamsesBinarySensorEntityDescription(
        key="active_fault",
        name="Active fault",
        ramses_rf_attr="active_faults",
        ramses_cc_class=RamsesLogbookBinarySensor,
        device_class=BinarySensorDeviceClass.PROBLEM,
    )
    mock_device = MagicMock(spec=Logbook)
    mock_device.id = "01:123456"
    mock_device._tcs = MagicMock()
    mock_device._tcs.id = "01:123456"
    mock_device._tcs.get_faultlog = AsyncMock()

    sensor: RamsesLogbookBinarySensor = RamsesLogbookBinarySensor(
        mock_coordinator, mock_device, description
    )

    # Act (active_faults None)
    # 1. active_faults is None, tcs has get_faultlog
    mock_resolve.return_value = None
    with patch(
        "custom_components.ramses_cc.binary_sensor."
        "RamsesBinarySensor.async_added_to_hass"
    ):
        await sensor.async_added_to_hass()

    # Assert
    mock_device._tcs.get_faultlog.assert_awaited_once_with(
        limit=1, force_refresh=True
    )

    # Arrange (active_faults present)
    # 2. active_faults is not None (should not poll)
    mock_device._tcs.get_faultlog.reset_mock()
    mock_resolve.return_value = [{"fault": "error"}]

    # Act
    with patch(
        "custom_components.ramses_cc.binary_sensor."
        "RamsesBinarySensor.async_added_to_hass"
    ):
        await sensor.async_added_to_hass()

    # Assert
    mock_device._tcs.get_faultlog.assert_not_called()


def test_bypass_binary_sensor_extra_attributes(
    mock_coordinator: MagicMock,
) -> None:
    """Test RamsesBypassBinarySensor extra_state_attributes includes bypass position."""
    from custom_components.ramses_cc.binary_sensor import (
        SZ_BYPASS_POSITION,
        RamsesBypassBinarySensor,
    )

    description = RamsesBinarySensorEntityDescription(
        key="bypass", ramses_rf_attr=SZ_BYPASS_POSITION
    )
    mock_device = MagicMock()
    mock_device.id = "32:112233"
    setattr(mock_device, SZ_BYPASS_POSITION, 0.45)

    sensor = RamsesBypassBinarySensor(
        mock_coordinator, mock_device, description
    )
    attrs = sensor.extra_state_attributes
    assert attrs[SZ_BYPASS_POSITION] == 0.45


async def test_logbook_binary_sensor_polling_error(
    mock_coordinator: MagicMock,
) -> None:
    """Test RamsesLogbookBinarySensor handles exception during get_faultlog."""
    description = RamsesBinarySensorEntityDescription(
        key="logbook", ramses_rf_attr="active_faults"
    )
    mock_device = MagicMock(spec=Logbook)
    mock_device.id = "01:123456"
    mock_tcs = MagicMock()
    mock_tcs.get_faultlog = AsyncMock(side_effect=RuntimeError("Bus busy"))
    mock_device._tcs = mock_tcs

    sensor = RamsesLogbookBinarySensor(
        mock_coordinator, mock_device, description
    )
    with (
        patch(
            "custom_components.ramses_cc.binary_sensor.resolve_async_attr",
            return_value=None,
        ),
        patch(
            "custom_components.ramses_cc.binary_sensor.RamsesBinarySensor.async_added_to_hass",
            AsyncMock(),
        ),
    ):
        await sensor.async_added_to_hass()
    mock_tcs.get_faultlog.assert_awaited_once()


def test_system_binary_sensor_is_on(
    mock_coordinator: MagicMock,
) -> None:
    """Test RamsesSystemBinarySensor is_on inversion and None propagation."""
    description = RamsesBinarySensorEntityDescription(
        key="system_sensor", ramses_rf_attr="system_status"
    )
    mock_device = MagicMock(spec=System)
    mock_device.id = "01:123456"

    sensor = RamsesSystemBinarySensor(
        mock_coordinator, mock_device, description
    )

    with patch(
        "custom_components.ramses_cc.binary_sensor.RamsesBinarySensor.is_on",
        new_callable=MagicMock,
    ) as mock_is_on:
        # 1. When super().is_on is True -> is_on returns False
        mock_is_on.__get__ = MagicMock(return_value=True)
        assert sensor.is_on is False

        # 2. When super().is_on is False -> is_on returns True
        mock_is_on.__get__ = MagicMock(return_value=False)
        assert sensor.is_on is True

        # 3. When super().is_on is None -> is_on returns None
        mock_is_on.__get__ = MagicMock(return_value=None)
        assert sensor.is_on is None


# -- Per-HGI pool status entity tests (issue 1119) --------------------------


def test_migrate_old_pool_entities_removes_orphans() -> None:
    """Old non-entry-scoped pool entities are removed from the registry."""
    from custom_components.ramses_cc.binary_sensor import (
        _migrate_old_pool_entities,
    )

    hass = MagicMock()
    ent_reg = MagicMock()

    old_child = MagicMock()
    old_child.unique_id = "pool_child_18:012345_online"
    old_child.entity_id = "binary_sensor.old_child"
    old_status = MagicMock()
    old_status.unique_id = "pool_status_online"
    old_status.entity_id = "binary_sensor.old_status"
    new_child = MagicMock()
    new_child.unique_id = "entry-one_pool_child_18:012345_online"
    new_child.entity_id = "binary_sensor.new_child"
    unrelated = MagicMock()
    unrelated.unique_id = "some_other_entity"
    unrelated.entity_id = "binary_sensor.other"

    ent_reg.entities = {
        "binary_sensor.old_child": old_child,
        "binary_sensor.old_status": old_status,
        "binary_sensor.new_child": new_child,
        "binary_sensor.other": unrelated,
    }

    with patch(
        "custom_components.ramses_cc.binary_sensor.er.async_get",
        return_value=ent_reg,
    ):
        _migrate_old_pool_entities(hass, "entry-one")

    removed_ids = {c.args[0] for c in ent_reg.async_remove.call_args_list}
    assert "binary_sensor.old_child" in removed_ids
    assert "binary_sensor.old_status" in removed_ids
    assert "binary_sensor.new_child" not in removed_ids
    assert "binary_sensor.other" not in removed_ids


def test_add_pool_status_entities_skips_when_pool_disabled() -> None:
    """No entities are created when the pool is not enabled."""
    coordinator = MagicMock()
    coordinator.is_pool_enabled = False
    async_add_entities = MagicMock()

    _add_pool_status_entities(coordinator, async_add_entities)

    async_add_entities.assert_not_called()


def test_add_pool_status_entities_skips_missing_hgi_id() -> None:
    """Children without hgi_id are skipped."""
    coordinator = MagicMock()
    coordinator.is_pool_enabled = True
    coordinator.entry.entry_id = "entry-x"
    coordinator.entry.async_on_unload = MagicMock()
    coordinator.get_pool_child_status.return_value = [
        {"child_id": "0", "hgi_id": None, "connected": True},
        {"child_id": "1", "hgi_id": "18:002222", "connected": True},
    ]
    coordinator.async_add_listener.return_value = MagicMock()
    async_add_entities = MagicMock()

    _add_pool_status_entities(coordinator, async_add_entities)

    # Only one child entity (the one with hgi_id)
    added = async_add_entities.call_args_list[1].args[0]
    assert len(added) == 1
    assert added[0]._hgi_id == "18:002222"


def test_add_pool_status_entities_dedupes_same_hgi_id() -> None:
    """Multiple children with the same HGI ID produce one sensor."""
    coordinator = MagicMock()
    coordinator.is_pool_enabled = True
    coordinator.entry.entry_id = "entry-d"
    coordinator.entry.async_on_unload = MagicMock()
    coordinator.get_pool_child_status.return_value = [
        {"child_id": "0", "hgi_id": "18:003333", "connected": True},
        {"child_id": "1", "hgi_id": "18:003333", "connected": False},
    ]
    coordinator.async_add_listener.return_value = MagicMock()
    async_add_entities = MagicMock()

    _add_pool_status_entities(coordinator, async_add_entities)

    added = async_add_entities.call_args_list[1].args[0]
    assert len(added) == 1


def test_pool_child_binary_sensor_is_on() -> None:
    """Pool child sensor is_on reflects connected + ONLINE."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.last_update_success = True
    coordinator.get_pool_child_status.return_value = [
        {
            "child_id": "0",
            "hgi_id": "18:004444",
            "connected": True,
            "availability": "ONLINE",
            "accepted": True,
            "send_ready": True,
        }
    ]
    sensor = RamsesPoolChildBinarySensor(
        coordinator,
        "18:004444",
        "0",
        coordinator.get_pool_child_status()[0],
    )
    assert sensor.is_on is True
    assert sensor.available is True


def test_pool_child_binary_sensor_is_on_offline() -> None:
    """Pool child sensor is_on is False when availability != ONLINE."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.last_update_success = True
    coordinator.get_pool_child_status.return_value = [
        {
            "child_id": "0",
            "hgi_id": "18:004444",
            "connected": True,
            "availability": "OFFLINE",
            "accepted": True,
            "send_ready": True,
        }
    ]
    sensor = RamsesPoolChildBinarySensor(
        coordinator,
        "18:004444",
        "0",
        coordinator.get_pool_child_status()[0],
    )
    assert sensor.is_on is False


def test_pool_child_binary_sensor_is_on_no_status() -> None:
    """Pool child sensor is_on is None when no matching child is found."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.last_update_success = True
    coordinator.get_pool_child_status.return_value = []
    sensor = RamsesPoolChildBinarySensor(coordinator, "18:004444", "0", {})
    assert sensor.is_on is None


def test_pool_child_binary_sensor_available() -> None:
    """Pool child sensor available follows coordinator last_update_success."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.last_update_success = False
    sensor = RamsesPoolChildBinarySensor(coordinator, "18:004444", "0", {})
    assert sensor.available is False


def test_pool_child_binary_sensor_extra_state_attributes() -> None:
    """Extra state attributes expose child monitoring fields."""
    status = {
        "child_id": "0",
        "hgi_id": "18:004444",
        "port_name": "/dev/ttyUSB0",
        "connected": True,
        "availability": "ONLINE",
        "accepted": True,
        "send_ready": True,
        "callback_driven": False,
        "pkts_received": 42,
        "consecutive_errors": 0,
        "last_pkt_time": "2026-09-12T12:00:00",
    }
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.get_pool_child_status.return_value = [status]
    sensor = RamsesPoolChildBinarySensor(coordinator, "18:004444", "0", status)

    attrs = sensor.extra_state_attributes
    assert attrs["hgi_id"] == "18:004444"
    assert attrs["port_name"] == "/dev/ttyUSB0"
    assert attrs["pkts_received"] == 42


def test_pool_child_find_status_prefers_online() -> None:
    """_find_status prefers connected+online over first match."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.get_pool_child_status.return_value = [
        {
            "child_id": "0",
            "hgi_id": "18:004444",
            "connected": False,
            "availability": "OFFLINE",
        },
        {
            "child_id": "1",
            "hgi_id": "18:004444",
            "connected": True,
            "availability": "ONLINE",
        },
    ]
    sensor = RamsesPoolChildBinarySensor(coordinator, "18:004444", "0", {})
    result = sensor._find_status()
    assert result is not None
    assert result["child_id"] == "1"


def test_pool_child_find_status_returns_first_when_none_online() -> None:
    """_find_status returns first match when no child is online."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.get_pool_child_status.return_value = [
        {
            "child_id": "0",
            "hgi_id": "18:004444",
            "connected": False,
            "availability": "OFFLINE",
        },
    ]
    sensor = RamsesPoolChildBinarySensor(coordinator, "18:004444", "0", {})
    result = sensor._find_status()
    assert result is not None
    assert result["child_id"] == "0"


def test_pool_child_handle_coordinator_update() -> None:
    """_handle_coordinator_update stores status and writes state."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.get_pool_child_status.return_value = [
        {
            "child_id": "0",
            "hgi_id": "18:004444",
            "connected": True,
            "availability": "ONLINE",
        }
    ]
    sensor = RamsesPoolChildBinarySensor(coordinator, "18:004444", "0", {})
    with patch.object(sensor, "async_write_ha_state") as mock_write:
        sensor._handle_coordinator_update()
        mock_write.assert_called_once()


async def test_pool_child_async_added_to_hass() -> None:
    """async_added_to_hass registers coordinator listener."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.async_add_listener.return_value = MagicMock()
    sensor = RamsesPoolChildBinarySensor(coordinator, "18:004444", "0", {})
    with patch(
        "custom_components.ramses_cc.binary_sensor.BinarySensorEntity.async_added_to_hass",
        AsyncMock(),
    ):
        await sensor.async_added_to_hass()
    coordinator.async_add_listener.assert_called_once()


def test_pool_status_sensor_is_on_true() -> None:
    """Pool status sensor is_on True when eligible child exists."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-s"
    coordinator.last_update_success = True
    coordinator.get_pool_child_status.return_value = [
        {
            "child_id": "0",
            "hgi_id": "18:005555",
            "connected": True,
            "availability": "ONLINE",
            "accepted": True,
            "send_ready": True,
        }
    ]
    sensor = RamsesPoolStatusSensor(coordinator)
    assert sensor.is_on is True
    assert sensor.available is True


def test_pool_status_sensor_is_on_none_empty() -> None:
    """Pool status sensor is_on is None when no children exist."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-s"
    coordinator.last_update_success = True
    coordinator.get_pool_child_status.return_value = []
    sensor = RamsesPoolStatusSensor(coordinator)
    assert sensor.is_on is None


def test_pool_status_sensor_available_false() -> None:
    """Pool status sensor available is False when coordinator fails."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-s"
    coordinator.last_update_success = False
    sensor = RamsesPoolStatusSensor(coordinator)
    assert sensor.available is False


def test_pool_status_sensor_extra_state_attributes() -> None:
    """Pool status sensor extra attributes expose aggregate counts."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-s"
    coordinator.get_pool_child_status.return_value = [
        {
            "child_id": "0",
            "hgi_id": "18:005555",
            "connected": True,
            "availability": "ONLINE",
            "accepted": True,
            "send_ready": True,
        },
        {
            "child_id": "1",
            "hgi_id": "18:006666",
            "connected": True,
            "availability": "OFFLINE",
            "accepted": False,
            "send_ready": False,
        },
    ]
    sensor = RamsesPoolStatusSensor(coordinator)
    attrs = sensor.extra_state_attributes
    assert attrs["children"] == 2
    assert attrs["connected"] == 2
    assert attrs["online"] == 1
    assert attrs["send_ready"] == 1
    assert attrs["eligible"] == 1


def test_pool_status_sensor_handle_coordinator_update() -> None:
    """Pool status sensor _handle_coordinator_update writes state."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-s"
    sensor = RamsesPoolStatusSensor(coordinator)
    with patch.object(sensor, "async_write_ha_state") as mock_write:
        sensor._handle_coordinator_update()
        mock_write.assert_called_once()


async def test_pool_status_sensor_async_added_to_hass() -> None:
    """Pool status sensor async_added_to_hass registers listener."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-s"
    coordinator.async_add_listener.return_value = MagicMock()
    sensor = RamsesPoolStatusSensor(coordinator)
    with patch(
        "custom_components.ramses_cc.binary_sensor.BinarySensorEntity.async_added_to_hass",
        AsyncMock(),
    ):
        await sensor.async_added_to_hass()
    coordinator.async_add_listener.assert_called_once()


def test_pool_child_find_status_skips_other_hgi() -> None:
    """_find_status skips children with a different HGI ID."""
    coordinator = MagicMock()
    coordinator.entry.entry_id = "entry-c"
    coordinator.get_pool_child_status.return_value = [
        {
            "child_id": "0",
            "hgi_id": "18:999999",  # different HGI
            "connected": True,
            "availability": "ONLINE",
        },
        {
            "child_id": "1",
            "hgi_id": "18:004444",  # matching HGI
            "connected": True,
            "availability": "ONLINE",
        },
    ]
    sensor = RamsesPoolChildBinarySensor(coordinator, "18:004444", "0", {})
    result = sensor._find_status()
    assert result is not None
    assert result["hgi_id"] == "18:004444"


def test_gateway_binary_sensor_available_always_true() -> None:
    """RamsesGatewayBinarySensor.available always returns True."""
    description = RamsesBinarySensorEntityDescription(
        key="status",
        ramses_rf_attr="is_active",
        name="Gateway status",
        ramses_cc_class=RamsesGatewayBinarySensor,
        device_class=BinarySensorDeviceClass.PROBLEM,
    )
    mock_device = MagicMock(spec=HgiGateway)
    mock_device.id = "18:123456"
    coordinator = MagicMock()
    sensor = RamsesGatewayBinarySensor(coordinator, mock_device, description)
    assert sensor.available is True


# -- Gateway binary sensor fallback paths ------------------------------------


async def test_gateway_binary_sensor_extra_attrs_fallback_known_list() -> None:
    """Gateway attrs fall back to engine._include when gwy_config has no dict."""
    description = RamsesBinarySensorEntityDescription(
        key="status",
        ramses_rf_attr="is_active",
        name="Gateway status",
        ramses_cc_class=RamsesGatewayBinarySensor,
        device_class=BinarySensorDeviceClass.PROBLEM,
    )
    mock_device = MagicMock(spec=HgiGateway)
    mock_device.id = "18:123456"
    mock_device.tcs = None

    gwy = MagicMock()
    gwy.config = MagicMock()
    gwy.config.known_list = None  # not a dict → triggers fallback
    gwy._engine = MagicMock()
    gwy._engine._include = {"10:2": {"alias": "fb"}}
    gwy._engine._enforce_known_list = True
    gwy._engine._transport = MagicMock()
    gwy._engine._transport.get_extra_info.return_value = False
    gwy._engine._exclude = {}
    mock_device._gateway = gwy

    coordinator = MagicMock()
    sensor = RamsesGatewayBinarySensor(coordinator, mock_device, description)
    attrs = sensor.extra_state_attributes
    assert "10:2" in attrs["known_list"][0]


async def test_gateway_binary_sensor_extra_attrs_fallback_include_not_dict() -> (
    None
):
    """Gateway attrs fall back to gwy._include when engine._include is not dict."""
    description = RamsesBinarySensorEntityDescription(
        key="status",
        ramses_rf_attr="is_active",
        name="Gateway status",
        ramses_cc_class=RamsesGatewayBinarySensor,
        device_class=BinarySensorDeviceClass.PROBLEM,
    )
    mock_device = MagicMock(spec=HgiGateway)
    mock_device.id = "18:123456"
    mock_device.tcs = None

    gwy = MagicMock()
    gwy.config = MagicMock()
    gwy.config.known_list = None
    gwy._engine = MagicMock()
    gwy._engine._include = "not_a_dict"  # not a dict → deeper fallback
    gwy._engine._enforce_known_list = "not_a_bool"  # not a bool → fallback
    gwy._engine._transport = None  # no transport → fallback to gwy._transport
    gwy._transport = MagicMock()
    gwy._transport.get_extra_info.return_value = True
    gwy._include = {"10:3": {"alias": "fb2"}}
    gwy._enforce_known_list = True
    gwy._exclude = {}
    mock_device._gateway = gwy

    coordinator = MagicMock()
    sensor = RamsesGatewayBinarySensor(coordinator, mock_device, description)
    attrs = sensor.extra_state_attributes
    assert "10:3" in attrs["known_list"][0]
    assert attrs["config"]["enforce_known_list"] is True
