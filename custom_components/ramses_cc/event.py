"""Event platform for Ramses RF events."""

# see https://github.com/home-assistant/core/blob/dev/homeassistant/components/
from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from re import Pattern
from typing import TYPE_CHECKING, Any, Final

from homeassistant.components.event import EventEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import CONF_ADVANCED_FEATURES, CONF_MESSAGE_EVENTS, DOMAIN, SIGNAL_UPDATE
from .coordinator import RamsesCoordinator

if TYPE_CHECKING:
    from ramses_tx.const import Code
    from ramses_tx.message import Message

_LOGGER = logging.getLogger(__name__)

# adapted from Event platform for Home Assistant Backup integration.
# https://github.com/home-assistant/core/blob/dev/homeassistant/components/backup/event.py
ATTR_FAILED_REASON: Final[str] = "failed_reason"


@dataclass(frozen=True, kw_only=True, slots=True)
class RamsesEventData:
    """Class to hold RamsesEvent data."""

    type: str
    device_id: str | None
    dtm: str | None
    src: str | None
    dst: str | None
    verb: str | None
    code: Code | None
    payload: str | None
    packet: str | None


class RamsesEventType(StrEnum):
    """Create ramses_cc state enum."""

    LEARN = f"{DOMAIN}_learn"
    REGEX = f"{DOMAIN}_regex_match"


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Event set up for Ramses RF entry system-wide events."""

    coordinator = hass.data[DOMAIN][config_entry.entry_id]

    features: dict[str, Any] = config_entry.options.get(CONF_ADVANCED_FEATURES, {})
    if message_events := features.get(CONF_MESSAGE_EVENTS):
        message_events_regex = re.compile(message_events)
    else:
        message_events_regex = None

    async_add_entities(
        [
            RamsesLearnEvent(
                coordinator,
                hass,
                data={"type": RamsesEventType.LEARN},
            ),
            RamsesRegexEvent(
                coordinator,
                hass,
                data={"type": RamsesEventType.REGEX},
                regex=message_events_regex,
            ),
        ]
    )


class RamsesEvent(EventEntity):
    """Representation of a Ramses RF event."""

    def __init__(
        self,
        coordinator: RamsesCoordinator,
        hass: HomeAssistant,
        data: dict[str, Any],
        event_callback: Any,
    ) -> None:
        """Initialize the event."""
        self._coordinator: RamsesCoordinator = coordinator
        self._hass = hass
        self._type: str = data["type"]  # data.pop("type")
        self._data = data
        self._event_callback = event_callback
        self._remove: Callable[[], None] | None = None
        super().__init__()
        _LOGGER.debug("RamsesEvent init completed for %s", self._type)

    def update_data(self, data: dict[str, Any]) -> None:
        """Update the event from async_process_msg()."""
        self._type = data["type"]  # data.pop("type")
        self._data = data
        self._async_handle_event(self._type)

    @callback
    def _async_handle_event(self, event: str) -> None:
        """Handle the ramses event."""

        _LOGGER.debug("handle event %s", self._type)
        # self.hass.bus.fire(event, self._data)
        self._trigger_event(
            event,
            {
                "extra_data": self._data,
            },
        )
        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        """Register callbacks with the coordinator and store result to allow their removal."""
        if self._coordinator.client:
            await super().async_added_to_hass()
            _LOGGER.debug("RamsesEvent added_to_hass completed")
            self._remove = self._coordinator.client.add_msg_handler(
                self._event_callback
            )

    async def async_will_remove_from_hass(self) -> None:
        """Deregister callbacks with the coordinator."""
        if self._coordinator.client and self._remove is not None:
            self._remove()
        await super().async_will_remove_from_hass()


class RamsesLearnEvent(RamsesEvent):
    """Representation of a Ramses RF Learn event."""

    _attr_event_types = [
        RamsesEventType.LEARN,
    ]
    _attr_unique_id = "learn_event"
    _attr_translation_key = "learn_event"

    def __init__(
        self,
        coordinator: RamsesCoordinator,
        hass: HomeAssistant,
        data: dict[str, Any],
    ) -> None:
        """Initialize the event.

        :param coordinator: The data update coordinator for the integration.
        :param hass: The Home Assistant instance.
        :param data: Supporting data to send with the event
        """

        @callback
        def async_process_msg(msg: Message, *args: Any, **kwargs: Any) -> None:
            """Process a message from the event bus and pass it on."""

            async_dispatcher_send(self._hass, f"{SIGNAL_UPDATE}_{msg.src.id}")
            if msg.dst and msg.dst.id != msg.src.id:
                async_dispatcher_send(self._hass, f"{SIGNAL_UPDATE}_{msg.dst.id}")

            if (
                coordinator.learn_device_id
                and coordinator.learn_device_id == msg.src.id
            ):
                event_data = {
                    "type": RamsesEventType.LEARN,
                    "src": msg.src.id,
                    "code": msg.code,
                    "packet": str(msg._pkt),
                }
                # TODO: change to }_event and read that type in coordinator.learn_device_id
                self.update_data(event_data)
                # was: hass.bus.async_fire(RamsesEventType.LEARN, event_data)

        super().__init__(coordinator, hass, data, event_callback=async_process_msg)
        # TODO: adapt remote.py#async_learn_command


class RamsesRegexEvent(RamsesEvent):
    """Representation of a Ramses RF Learn event."""

    _attr_event_types = [
        RamsesEventType.REGEX,
    ]
    _attr_unique_id = "regex_event"
    _attr_translation_key = "regex_event"

    def __init__(
        self,
        coordinator: RamsesCoordinator,
        hass: HomeAssistant,
        data: dict[str, Any],
        regex: Pattern[str] | None = None,
    ) -> None:
        """Initialize the event.

        :param coordinator: The data update coordinator for the integration.
        :param hass: The Home Assistant instance.
        :param data: Supporting data to send with the event
        :param regex: The regular expression to match against
        """

        self.regex = regex

        @callback
        def async_process_msg(msg: Message, *args: Any, **kwargs: Any) -> None:
            """Process a message from the event bus and pass it on."""

            async_dispatcher_send(self._hass, f"{SIGNAL_UPDATE}_{msg.src.id}")
            if msg.dst and msg.dst.id != msg.src.id:
                async_dispatcher_send(self._hass, f"{SIGNAL_UPDATE}_{msg.dst.id}")

            # filter msg by advanced_config regex, fire an event if a match
            if regex and regex.search(f"{msg!r}"):
                event_data = {
                    "type": RamsesEventType.REGEX,
                    "device_id": msg.src.id,
                    "dtm": msg.dtm.isoformat(),
                    "src": msg.src.id,
                    "dst": msg.dst.id,
                    "verb": msg.verb,
                    "code": msg.code,
                    "payload": msg.payload,
                    "packet": str(msg._pkt),
                }
                self.update_data(event_data)

        super().__init__(coordinator, hass, data, event_callback=async_process_msg)
