"""Support for RAMSES RF select entities.

Provides a System Mode select entity on the evohome-type controller
device, giving direct access to all native system modes (2E04) —
including those not mapped to a climate preset such as ``eco_boost``,
``day_off_eco`` and ``auto_with_reset`` (issue 1323).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from homeassistant.components.select import (
    SelectEntity,
    SelectEntityDescription,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers.entity_platform import (
    AddEntitiesCallback,
    EntityPlatform,
    async_get_current_platform,
)

from ramses_rf.const import SZ_SYSTEM_MODE
from ramses_rf.systems.tcs import Evohome
from ramses_tx.exceptions import (
    ProtocolSendFailed,
    ProtocolTimeoutError,
    RamsesException,
    TransportError,
)

from .const import DOMAIN, SystemMode
from .entity import RamsesEntity, RamsesEntityDescription
from .helpers import fields_to_aware, resolve_async_attr
from .typing import RamsesConfigEntry

if TYPE_CHECKING:
    from .coordinator import RamsesCoordinator

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class RamsesSelectEntityDescription(
    RamsesEntityDescription, SelectEntityDescription
):
    """Class describing Ramses select entities."""

    ramses_cc_class: type[RamsesSystemModeSelect]
    ramses_rf_class: type[Evohome]


class RamsesSystemModeSelect(RamsesEntity, SelectEntity):
    """System mode select for an evohome-type controller (TCS, 2E04)."""

    _device: Evohome

    _attr_icon: str = "mdi:thermostat-cog"
    _attr_options: list[str] = [str(mode) for mode in SystemMode]

    def __init__(
        self,
        coordinator: RamsesCoordinator,
        device: Evohome,
        entity_description: RamsesSelectEntityDescription,
    ) -> None:
        """Initialize a system mode select entity.

        :param coordinator: The Ramses coordinator.
        :param device: The Evohome system.
        :param entity_description: The entity description.
        """
        super().__init__(coordinator, device, entity_description)
        self._attr_unique_id = f"{device.id}-{entity_description.key}"

    @property
    def current_option(self) -> str | None:
        """Return the current system mode, or None if not yet known."""
        system_mode = resolve_async_attr(self, self._device, "system_mode")
        if not system_mode:
            return None
        return system_mode.get(SZ_SYSTEM_MODE)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return the integration-specific state attributes.

        Adds the ``until`` datetime for temporary modes (away, day_off,
        custom, day_off_eco and eco_boost may carry an expiry).
        """
        attrs = super().extra_state_attributes
        system_mode = resolve_async_attr(self, self._device, "system_mode")
        if system_mode and "until" in system_mode:
            attrs["until"] = fields_to_aware(system_mode["until"])
        return attrs

    async def async_select_option(self, option: str) -> None:
        """Set the system mode on the controller.

        The mode is set indefinitely; temporary variants can be set via
        the ``ramses_cc.set_system_mode`` entity service instead.

        :param option: The system mode to set.
        :raises ServiceValidationError: If the option is not a valid
            system mode.
        :raises HomeAssistantError: If the command fails.
        """
        try:
            mode = SystemMode(option)
        except ValueError as err:
            raise ServiceValidationError(
                translation_domain=DOMAIN,
                translation_key="validation_error",
                translation_placeholders={"error": str(err)},
            ) from err

        try:
            await self._device.set_mode(mode, until=None)
            self.async_write_ha_state()
        except (
            RamsesException,
            ProtocolSendFailed,
            ProtocolTimeoutError,
            TimeoutError,
            TransportError,
        ) as err:
            raise HomeAssistantError(
                f"Failed to set system mode: {err}"
            ) from err


SELECT_DESCRIPTIONS: tuple[RamsesSelectEntityDescription, ...] = (
    RamsesSelectEntityDescription(
        key="system_mode",
        translation_key="system_mode",
        ramses_rf_class=Evohome,
        ramses_cc_class=RamsesSystemModeSelect,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: RamsesConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the RAMSES select platform from a config entry.

    Creates one system mode select per evohome-type controller, both for
    systems already known at setup and for those discovered later (via
    the coordinator's new-device dispatch).

    :param hass: The Home Assistant instance.
    :param entry: The config entry.
    :param async_add_entities: The callback to add entities.
    """
    coordinator: RamsesCoordinator = entry.runtime_data
    platform: EntityPlatform = async_get_current_platform()

    @callback
    def add_devices(devices: Any) -> None:
        """Create select entities for the given systems."""
        entities = [
            description.ramses_cc_class(
                coordinator,
                device,
                description,
            )
            for device in devices
            for description in SELECT_DESCRIPTIONS
            if isinstance(device, description.ramses_rf_class)
        ]
        async_add_entities(entities)

    coordinator.async_register_platform(platform, add_devices)
