"""Provides triggers for Ramses RF."""

from homeassistant.const import STATE_HOME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.trigger import (
    Trigger,
    make_entity_origin_state_trigger,
    make_entity_target_state_trigger,
)

from .const import DOMAIN

TRIGGERS: dict[str, type[Trigger]] = {
    "ramses_cc_learn": make_entity_target_state_trigger(DOMAIN, STATE_HOME),
    "ramses_cc_regex_match": make_entity_origin_state_trigger(
        DOMAIN, from_state=STATE_HOME
    ),
}


async def async_get_triggers(hass: HomeAssistant) -> dict[str, type[Trigger]]:
    """Return the triggers for Ramses RF."""
    return TRIGGERS
