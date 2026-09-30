"""Tests for the RamsesEvent class."""

import re
from datetime import UTC, datetime as dt
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

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
from ramses_tx.dtos import PacketDTO


# Mock Coordinator
@pytest.fixture
def mock_coordinator(learn_device_id: str | None = None) -> MagicMock:
    """A mock object simulating the RamsesCoordinator."""
    coordinator = MagicMock()
    coordinator.async_register_platform = MagicMock()
    coordinator.learn_device_id = learn_device_id
    return coordinator


# Mock HomeAssistant
@pytest.fixture
def mock_hass() -> MagicMock:
    return MagicMock(spec=HomeAssistant, data={})


# Mock ConfigEntry
@pytest.fixture
def mock_config_entry() -> MagicMock:
    return MagicMock(spec=ConfigEntry, entry_id="123")


# Test RamsesEventData
def test_ramses_event_data() -> None:
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
def test_ramses_event_type() -> None:
    assert f"{DOMAIN}_learn" == RamsesEventType.LEARN
    assert f"{DOMAIN}_regex_match" == RamsesEventType.REGEX


# Test RamsesEvent
@pytest.mark.asyncio
async def test_ramses_event_init(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    event = RamsesEvent(
        mock_coordinator,
        mock_hass,
        {"type": RamsesEventType.LEARN},
        event_callback=MagicMock(),
    )
    assert event._type == RamsesEventType.LEARN
    assert event.has_entity_name is True


@pytest.mark.asyncio
async def test_ramses_event_update_data(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    event = RamsesEvent(
        mock_coordinator,
        mock_hass,
        {"type": RamsesEventType.LEARN},
        event_callback=MagicMock(),
    )
    with patch.object(event, "async_write_ha_state") as mock_write:
        event.update_data({"type": RamsesEventType.LEARN, "extra": "data"})
    mock_write.assert_called_once()
    assert event._type == RamsesEventType.LEARN
    assert event._data == {"type": RamsesEventType.LEARN, "extra": "data"}


@pytest.mark.asyncio
async def test_ramses_event_update_data_error(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    event = RamsesEvent(
        mock_coordinator,
        mock_hass,
        {"type": RamsesEventType.LEARN},
        event_callback=MagicMock(),
    )

    # Expect error
    with pytest.raises(HomeAssistantError):
        event.update_data({"type": RamsesEventType.REGEX, "extra": "data"})


@pytest.mark.asyncio
async def test_ramses_event_async_added_to_hass(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    mock_callback = MagicMock()
    event = RamsesEvent(
        mock_coordinator,
        mock_hass,
        {"type": RamsesEventType.LEARN},
        event_callback=mock_callback,
    )
    mock_coordinator.client.add_msg_handler.return_value = MagicMock()
    await event.async_added_to_hass()
    assert event._remove is not None


@pytest.mark.asyncio
async def test_ramses_event_async_will_remove_from_hass(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    mock_remove = MagicMock()
    event = RamsesEvent(
        mock_coordinator,
        mock_hass,
        {"type": RamsesEventType.LEARN},
        event_callback=MagicMock(),
    )
    event._remove = mock_remove
    await event.async_will_remove_from_hass()
    mock_remove.assert_called_once()


# Test RamsesLearnEvent
@pytest.mark.asyncio
async def test_ramses_learn_event_init(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    mock_coordinator.learn_device_id = "01:111111"
    event = RamsesLearnEvent(
        mock_coordinator, mock_hass, {"type": RamsesEventType.LEARN}
    )
    assert event._attr_event_types == [RamsesEventType.LEARN]
    assert event._attr_unique_id == "learn_event"
    assert event._attr_translation_key == "ramses_cc_learn_event"
    assert event.has_entity_name is True


@pytest.mark.asyncio
async def test_ramses_learn_event_async_process_msg(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    mock_coordinator.learn_device_id = "01:111111"
    event = RamsesLearnEvent(
        mock_coordinator, mock_hass, {"type": RamsesEventType.LEARN}
    )
    dto = PacketDTO(
        timestamp=dt(2023, 1, 1, 12, 0, tzinfo=UTC),
        rssi="000",
        verb=" I",
        seq="000",
        addr1="01:111111",
        addr2="01:222222",
        addr3="--:------",
        code="1234",
        length="003",
        payload="001122",
    )
    with patch.object(event, "update_data") as mock_update:
        event._event_callback(dto)
        expected_packet = (
            " I 000 01:111111 01:222222 --:------ 1234 003 001122"
        )
        mock_update.assert_called_once_with(
            {
                "type": RamsesEventType.LEARN,
                "src": "01:111111",
                "code": "1234",
                "packet": expected_packet,
            }
        )


def _make_dto(src: str = "01:111111", dst: str = "01:222222") -> PacketDTO:
    """Create a PacketDTO for testing."""
    return PacketDTO(
        timestamp=dt(2023, 1, 1, 12, 0, tzinfo=UTC),
        rssi="000",
        verb=" I",
        seq="000",
        addr1=src,
        addr2=dst,
        addr3="--:------",
        code="1234",
        length="003",
        payload="001122",
    )


@pytest.mark.asyncio
async def test_signal_update_update_data_still_synchronous(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    """The learn event update_data call must remain synchronous (not
    deferred) — only SIGNAL_UPDATE is deferred."""
    mock_coordinator.learn_device_id = "01:111111"
    event = RamsesLearnEvent(
        mock_coordinator, mock_hass, {"type": RamsesEventType.LEARN}
    )
    dto = _make_dto()

    with patch.object(event, "update_data") as mock_update:
        event._event_callback(dto)

        # update_data must have been called synchronously, before any
        # async task runs
        mock_update.assert_called_once()


# Test RamsesRegexEvent
@pytest.mark.asyncio
async def test_ramses_regex_event_init(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    regex = re.compile("test")
    event = RamsesRegexEvent(
        mock_coordinator,
        mock_hass,
        {"type": RamsesEventType.REGEX},
        regex=regex,
    )
    assert event._attr_event_types == [RamsesEventType.REGEX]
    assert event.regex == regex
    assert event._attr_unique_id == "regex_event"
    assert event._attr_translation_key == "ramses_cc_regex_event"
    assert event.has_entity_name is True


@pytest.mark.asyncio
async def test_ramses_regex_event_async_process_msg(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    regex = re.compile("001122")
    event = RamsesRegexEvent(
        mock_coordinator,
        mock_hass,
        {"type": RamsesEventType.REGEX},
        regex=regex,
    )
    dto = PacketDTO(
        timestamp=dt(2023, 1, 1, 12, 0, tzinfo=UTC),
        rssi="000",
        verb=" I",
        seq="000",
        addr1="01:111111",
        addr2="01:222222",
        addr3="--:------",
        code="1234",
        length="003",
        payload="001122",
    )
    with patch.object(event, "update_data") as mock_update:
        event._event_callback(dto)
        expected_packet = (
            " I 000 01:111111 01:222222 --:------ 1234 003 001122"
        )
        mock_update.assert_called_once_with(
            {
                "type": RamsesEventType.REGEX,
                "device_id": "01:111111",
                "dtm": "2023-01-01T12:00:00+00:00",
                "src": "01:111111",
                "dst": "01:222222",
                "verb": " I",
                "code": "1234",
                "payload": {
                    "zone_index": "00",
                    "_payload": "001122",
                    "_value": 43.86,
                    "seqx_num": "000",
                },
                "packet": expected_packet,
            }
        )


@pytest.mark.asyncio
async def test_ramses_regex_event_bytes_in_dict_payload(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    """Test regex event converts bytes values in dict payload to hex."""
    regex = re.compile("001122")
    event = RamsesRegexEvent(
        mock_coordinator,
        mock_hass,
        {"type": RamsesEventType.REGEX},
        regex=regex,
    )
    dto = PacketDTO(
        timestamp=dt(2023, 1, 1, 12, 0, tzinfo=UTC),
        rssi="000",
        verb=" I",
        seq="000",
        addr1="01:111111",
        addr2="01:222222",
        addr3="--:------",
        code="1234",
        length="003",
        payload="001122",
    )
    with patch.object(event, "update_data") as mock_update:
        event._event_callback(dto)
        call_args = mock_update.call_args[0][0]
        # The payload should be a dict (parsed from the packet)
        assert isinstance(call_args["payload"], dict)


@pytest.mark.asyncio
async def test_ramses_regex_event_bytes_payload(
    mock_hass: MagicMock, mock_coordinator: MagicMock
) -> None:
    """Test regex event converts raw bytes payload to hex string."""
    regex = re.compile("001122")
    event = RamsesRegexEvent(
        mock_coordinator,
        mock_hass,
        {"type": RamsesEventType.REGEX},
        regex=regex,
    )
    dto = PacketDTO(
        timestamp=dt(2023, 1, 1, 12, 0, tzinfo=UTC),
        rssi="000",
        verb=" I",
        seq="000",
        addr1="01:111111",
        addr2="01:222222",
        addr3="--:------",
        code="1234",
        length="003",
        payload="001122",
    )
    # Patch msg.payload to return bytes directly
    with (
        patch.object(event, "update_data") as mock_update,
        patch(
            "ramses_rf.messages.base.Message.payload",
            new_callable=PropertyMock,
            return_value=b"\x00\x11\x22",
        ),
    ):
        event._event_callback(dto)
        call_args = mock_update.call_args[0][0]
        # bytes payload should be converted to hex string
        assert call_args["payload"] == "001122"


# next 3 moved here from test_init events 0.55.6


async def test_domain_event_platform(
    hass: HomeAssistant, mock_coordinator: MagicMock
) -> None:
    """Test the event platform setup and entity creation callback.

    :param hass: The Home Assistant instance.
    :param mock_config_entry: The mock config fixture.
    """
    entry = MagicMock()
    entry.entry_id = "test_entry"
    entry.options = {CONF_ADVANCED_FEATURES: {CONF_MESSAGE_EVENTS: None}}
    entry.runtime_data = mock_coordinator

    mock_add_entities = MagicMock()

    await async_setup_entry(hass, entry, mock_add_entities)

    assert mock_add_entities.called
    # Verify 2 events created
    assert len(mock_add_entities.call_args) == 2

    # Create a mock device that matches one of the descriptions
    mock_event = MagicMock(spec=RamsesEvent)
    mock_event.id = "test_event"
    mock_event.data = {"type": "tst"}
    mock_event.coordinator = mock_coordinator
    mock_event.hass = hass

    # Call the callback with the mock event
    mock_add_entities([mock_event])

    # Verify async_add_entities was called with the created entity
    assert mock_add_entities.called
    created_entities = mock_add_entities.call_args[0][0]
    assert len(created_entities) == 1
    assert isinstance(created_entities[0], RamsesEvent)
    assert created_entities[0].data["type"] == "tst"


async def test_domain_events(
    hass: HomeAssistant, mock_coordinator: MagicMock
) -> None:
    """Regex/learn event entities fire on matching packets via state changes.

    Events are delivered as HA event-entity state changes (triggered via
    ``update_data``), not bus events — the callback is ``_event_callback``,
    registered through ``client.add_msg_handler``.
    """
    entry = MagicMock()
    entry.entry_id = "events_test_entry"
    entry.options = {CONF_ADVANCED_FEATURES: {CONF_MESSAGE_EVENTS: ".*"}}
    entry.runtime_data = mock_coordinator

    entities: list[RamsesEvent] = []
    await async_setup_entry(hass, entry, entities.extend)

    regex_entity = next(e for e in entities if isinstance(e, RamsesRegexEvent))
    learn_entity = next(e for e in entities if isinstance(e, RamsesLearnEvent))

    msg = PacketDTO(
        timestamp=dt(2023, 1, 1, 12, 0, tzinfo=UTC),
        rssi="000",
        verb=" I",
        seq="000",
        addr1="01:111111",
        addr2="01:222222",
        addr3="--:------",
        code="1234",
        length="003",
        payload="001122",
    )

    # 1. message_events regex ".*" matches every packet
    with patch.object(regex_entity, "update_data") as mock_regex_update:
        regex_entity._event_callback(msg)
    mock_regex_update.assert_called_once()
    regex_data = mock_regex_update.call_args[0][0]
    assert regex_data["type"] == RamsesEventType.REGEX
    assert regex_data["src"] == "01:111111"
    assert regex_data["code"] == "1234"

    # 2. learn event fires when the coordinator is in learn mode for src
    mock_coordinator.learn_device_id = "01:111111"
    with patch.object(learn_entity, "update_data") as mock_learn_update:
        learn_entity._event_callback(msg)
    mock_learn_update.assert_called_once()
    learn_data = mock_learn_update.call_args[0][0]
    assert learn_data["type"] == RamsesEventType.LEARN
    assert learn_data["src"] == "01:111111"


async def test_domain_events_no_config(
    hass: HomeAssistant, mock_coordinator: MagicMock
) -> None:
    """Without message_events configured, the regex event never fires."""
    entry = MagicMock()
    entry.entry_id = "events_test_entry"
    entry.options = {}  # no advanced features / message events
    entry.runtime_data = mock_coordinator

    entities: list[RamsesEvent] = []
    await async_setup_entry(hass, entry, entities.extend)

    regex_entity = next(e for e in entities if isinstance(e, RamsesRegexEvent))
    learn_entity = next(e for e in entities if isinstance(e, RamsesLearnEvent))

    msg = PacketDTO(
        timestamp=dt(2023, 1, 1, 12, 0, tzinfo=UTC),
        rssi="000",
        verb=" I",
        seq="000",
        addr1="01:111111",
        addr2="01:222222",
        addr3="--:------",
        code="1234",
        length="003",
        payload="001122",
    )

    # No regex compiled -> callback is a no-op
    with patch.object(regex_entity, "update_data") as mock_regex_update:
        regex_entity._event_callback(msg)
    mock_regex_update.assert_not_called()

    # Learn event also silent while learn_device_id is unset
    with patch.object(learn_entity, "update_data") as mock_learn_update:
        learn_entity._event_callback(msg)
    mock_learn_update.assert_not_called()
