"""Provides triggers for Ramses RF."""

import logging

from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity import get_supported_features
from homeassistant.helpers.trigger import Trigger, make_entity_target_state_trigger

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


def supports_feature(hass: HomeAssistant, entity_id: str, features: int) -> bool:
    """Test if an entity supports the specified features."""
    try:
        return bool(get_supported_features(hass, entity_id) & features)
    except HomeAssistantError:
        return False


TRIGGERS: dict[str, type[Trigger]] = {
    f"{DOMAIN}_learn": make_entity_target_state_trigger(DOMAIN, "learn"),
    f"{DOMAIN}_regex_match": make_entity_target_state_trigger(DOMAIN, "regex"),
}


async def async_get_triggers(hass: HomeAssistant) -> dict[str, type[Trigger]]:
    """Return the triggers for Ramses RF."""
    _LOGGER.debug("EBR RamsesTriggers fetched")
    return TRIGGERS
