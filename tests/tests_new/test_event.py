"""Tests for the RamsesEvent class."""

import re
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from custom_components.ramses_cc.const import (
    CONF_ADVANCED_FEATURES,
    CONF_MESSAGE_EVENTS,
    DOMAIN,
)
from custom_components.ramses_cc.event import (
    RamsesEvent,
    RamsesEventData,
    RamsesEventType,
    RamsesLearnEvent,
    RamsesRegexEvent,
    async_setup_entry,
)


# Mock Message class
@dataclass
class MockMessage:
    src: MagicMock
    dst: MagicMock | None
    dtm: MagicMock
    verb: str
    code: str
    payload: str
    _pkt: str


# Mock Coordinator
class MockCoordinator:
    def __init__(self, learn_device_id=None):
        self.client = MagicMock()
        self.learn_device_id = learn_device_id


# Mock HomeAssistant
@pytest.fixture
def mock_hass():
    return MagicMock(spec=HomeAssistant, data={})


# Mock ConfigEntry
@pytest.fixture
def mock_config_entry():
    return MagicMock(spec=ConfigEntry, entry_id="123")


# Test RamsesEventData
def test_ramses_event_data():
    data = RamsesEventData(
        type="test",
        device_id="dev1",
        dtm="2023-01-01",
        src="src1",
        dst="dst1",
        verb="RP",
        code="code1",
        payload="payload1",
        packet="packet1",
    )
    assert data.type == "test"
    assert data.device_id == "dev1"


# Test RamsesEventType
def test_ramses_event_type():
    assert f"{DOMAIN}_learn" == RamsesEventType.LEARN
    assert f"{DOMAIN}_regex_match" == RamsesEventType.REGEX


# Test RamsesEvent
@pytest.mark.asyncio
async def test_ramses_event_init(mock_hass):
    coordinator = MockCoordinator()
    event = RamsesEvent(
        coordinator,
        mock_hass,
        {"type": RamsesEventType.LEARN},
        event_callback=MagicMock(),
    )
    assert event._type == RamsesEventType.LEARN


@pytest.mark.asyncio
async def test_ramses_event_update_data(mock_hass):
    coordinator = MockCoordinator()
    event = RamsesEvent(
        coordinator,
        mock_hass,
        {"type": RamsesEventType.LEARN},
        event_callback=MagicMock(),
    )
    event.update_data({"type": RamsesEventType.REGEX, "extra": "data"})
    assert event._type == RamsesEventType.REGEX
    assert event._data == {"type": RamsesEventType.REGEX, "extra": "data"}


@pytest.mark.asyncio
async def test_ramses_event_async_added_to_hass(mock_hass):
    coordinator = MockCoordinator()
    mock_callback = MagicMock()
    event = RamsesEvent(
        coordinator,
        mock_hass,
        {"type": RamsesEventType.LEARN},
        event_callback=mock_callback,
    )
    coordinator.client.add_msg_handler.return_value = MagicMock()
    await event.async_added_to_hass()
    assert event._remove is not None


@pytest.mark.asyncio
async def test_ramses_event_async_will_remove_from_hass(mock_hass):
    coordinator = MockCoordinator()
    mock_remove = MagicMock()
    event = RamsesEvent(
        coordinator,
        mock_hass,
        {"type": RamsesEventType.LEARN},
        event_callback=MagicMock(),
    )
    event._remove = mock_remove
    await event.async_will_remove_from_hass()
    mock_remove.assert_called_once()


# Test RamsesLearnEvent
@pytest.mark.asyncio
async def test_ramses_learn_event_init(mock_hass):
    coordinator = MockCoordinator(learn_device_id="dev1")
    event = RamsesLearnEvent(coordinator, mock_hass, {"type": RamsesEventType.LEARN})
    assert event._attr_event_types == [RamsesEventType.LEARN]
    assert event._attr_unique_id == "learn_event"


@pytest.mark.asyncio
async def test_ramses_learn_event_async_process_msg(mock_hass):
    coordinator = MockCoordinator(learn_device_id="dev1")
    event = RamsesLearnEvent(coordinator, mock_hass, {"type": RamsesEventType.LEARN})
    msg = MockMessage(
        src=MagicMock(id="dev1"),
        dst=MagicMock(id="dev2"),
        dtm=MagicMock(isoformat=MagicMock(return_value="2023-01-01")),
        verb="verb1",
        code="code1",
        payload="payload1",
        _pkt="packet1",
    )
    with patch.object(event, "update_data") as mock_update:
        event._event_callback(msg)
        mock_update.assert_called_once_with(
            {
                "type": RamsesEventType.LEARN,
                "src": "dev1",
                "code": "code1",
                "packet": "packet1",
            }
        )


# Test RamsesRegexEvent
@pytest.mark.asyncio
async def test_ramses_regex_event_init(mock_hass):
    coordinator = MockCoordinator()
    regex = re.compile("test")
    event = RamsesRegexEvent(
        coordinator, mock_hass, {"type": RamsesEventType.REGEX}, regex=regex
    )
    assert event._attr_event_types == [RamsesEventType.LEARN, RamsesEventType.REGEX]
    assert event.regex == regex


@pytest.mark.asyncio
async def test_ramses_regex_event_async_process_msg(mock_hass):
    coordinator = MockCoordinator()
    regex = re.compile("payload1")
    event = RamsesRegexEvent(
        coordinator, mock_hass, {"type": RamsesEventType.REGEX}, regex=regex
    )
    msg = MockMessage(
        src=MagicMock(id="dev1"),
        dst=MagicMock(id="dev2"),
        dtm=MagicMock(isoformat=MagicMock(return_value="2023-01-01")),
        verb="verb1",
        code="code1",
        payload="payload1",
        _pkt="packet1",
    )
    with patch.object(event, "update_data") as mock_update:
        event._event_callback(msg)
        mock_update.assert_called_once_with(
            {
                "type": RamsesEventType.REGEX,
                "device_id": "dev1",
                "dtm": "2023-01-01",
                "src": "dev1",
                "dst": "dev2",
                "verb": "verb1",
                "code": "code1",
                "payload": "payload1",
                "packet": "packet1",
            }
        )


# Test async_setup_entry
@pytest.mark.asyncio
async def test_async_setup_entry(mock_hass, mock_config_entry):
    mock_config_entry.options = {CONF_ADVANCED_FEATURES: {CONF_MESSAGE_EVENTS: "test"}}
    mock_hass.data[DOMAIN] = {mock_config_entry.entry_id: MockCoordinator()}
    add_entity_callback = MagicMock(new_entities=None)
    await async_setup_entry(mock_hass, mock_config_entry, add_entity_callback)

    assert add_entity_callback == ["12"]
    assert len(add_entity_callback.new_entities) == 2
