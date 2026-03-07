"""Support for Ramses RF events."""
# adapted from Lutron
# https://github.com/home-assistant/core/blob/dev/homeassistant/components/lutron/event.py

import logging
from collections.abc import Callable
from enum import StrEnum

# from . import ATTR_ACTION, ATTR_FULL_ID, ATTR_UUID, LutronConfigEntry
# from .entity import LutronKeypad
from typing import Any  # , TYPE_CHECKING

# from pylutron import Button, Keypad, Lutron, LutronEvent
from homeassistant.components.event import EventEntity

# from homeassistant.config_entries import ConfigEntry
# from homeassistant.const import ATTR_ID
from homeassistant.core import callback  # ,HomeAssistant

# from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
# from homeassistant.util import slugify
from .coordinator import RamsesCoordinator

_LOGGER = logging.getLogger(__name__)


class RamsesEventType(StrEnum):
    """Ramses RF event types."""

    SINGLE_PRESS = "single_press"
    PRESS = "press"
    RELEASE = "release"


LEGACY_EVENT_TYPES: dict[RamsesEventType, str] = {
    RamsesEventType.SINGLE_PRESS: "single",
    RamsesEventType.PRESS: "pressed",
    RamsesEventType.RELEASE: "released",
}


# async def async_setup_entry(
#     hass: HomeAssistant,
#     config_entry: ConfigEntry,
#     async_add_entities: AddConfigEntryEntitiesCallback,
# ) -> None:
#     """Set up the Ramses RF event platform."""
#     entry_data = config_entry.runtime_data
#
#     async_add_entities(
#         RamsesEventEntity(area_name, keypad, button, entry_data.client)
#         for area_name, keypad, button in entry_data.buttons
#     )


# class RamsesEventEntity(LutronKeypad, EventEntity):
#     """Representation of a Lutron keypad button."""
#
#     _attr_translation_key = "button"
#
#     def __init__(
#         self,
#         area_name: str,
#         keypad: Keypad,
#         button: Button,
#         controller: Lutron,
#     ) -> None:
#         """Initialize the button."""
#         super().__init__(area_name, button, controller, keypad)
#         if (name := button.name) == "Unknown Button":
#             name += f" {button.number}"
#         self._attr_name = name
#         self._has_release_event = (
#             button.button_type is not None and "RaiseLower" in button.button_type
#         )
#         if self._has_release_event:
#             self._attr_event_types = [RamsesEventType.PRESS, RamsesEventType.RELEASE]
#         else:
#             self._attr_event_types = [RamsesEventType.SINGLE_PRESS]
#
#         self._full_id = slugify(f"{area_name} {name}")
#         self._id = slugify(name)
#
#     async def async_added_to_hass(self) -> None:
#         """Register callbacks."""
#         await super().async_added_to_hass()
#         self.async_on_remove(self._lutron_device.subscribe(self.handle_event, None))
#
#     @callback
#     def handle_event(
#         self, button: Button, _context: None, event: LutronEvent, _params: dict
#     ) -> None:
#         """Handle received event."""
#         action: RamsesEventType | None = None
#         if self._has_release_event:
#             if event == Button.Event.PRESSED:
#                 action = RamsesEventType.PRESS
#             else:
#                 action = RamsesEventType.RELEASE
#         elif event == Button.Event.PRESSED:
#             action = RamsesEventType.SINGLE_PRESS
#
#         if action:
#             data = {
#                 ATTR_ID: self._id,
#                 ATTR_ACTION: LEGACY_EVENT_TYPES[action],
#                 ATTR_FULL_ID: self._full_id,
#                 ATTR_UUID: button.uuid,
#             }
#             self.hass.bus.fire("lutron_event", data)
#             self._trigger_event(action)
#             self.schedule_update_ha_state()


class RamsesEventEntity(EventEntity):
    _attr_event_types = ["ramses_cc_regex_match", "ramses_cc_learn"]

    def __init__(
        self, coordinator: RamsesCoordinator, data: dict[str, Any], event_callback: Any
    ) -> None:
        """Initialize the event.

        :param coordinator: The data update coordinator for the integration.
        :param data: Supporting data to send with the event
        """
        self._coordinator: RamsesCoordinator = coordinator
        self._type: str = data.pop("type")
        self._data = data
        self._event_callback = event_callback
        self._remove: Callable[[], None] | None = None

        super().__init__()
        _LOGGER.debug("EBR RamsesEvent init completed for %s", self._type)

    def update(
        self,
        data: dict[str, Any],
    ) -> None:
        self._type = data.pop("type")
        self._data = data
        self._async_handle_event(self._type)

    @callback
    def _async_handle_event(self, event: str) -> None:
        """Handle the ramses event."""
        self._trigger_event(event, {"extra_data": self._data})
        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        """Register callbacks with the coordinator and store result to allow their removal."""
        if self._coordinator.client:
            _LOGGER.debug("EBR RamsesEvent added_to_hass completed")
            self._remove = self._coordinator.client.add_msg_handler(
                self._event_callback
            )

    async def async_will_remove_from_hass(self) -> None:
        """Deregister callbacks with the coordinator."""
        if self._coordinator.client and self._remove is not None:
            self._remove()
