"""Support for RAMSES RF button entities.

Buttons are stateless momentary entities that trigger an action when
pressed. This module provides:
- gateway-level buttons that invoke the integration's own
  domain-wide services (see ./services.yaml), and
- per-FAN buttons that reset the filter counter (W 10D0) via the
  ``reset_filter_counter`` entity service (see ./remote.py).

.. rubric:: Module Functions

.. py:function:: async_setup_entry(...)

    Set up the RAMSES button platform from a config entry. Creates
    gateway-level service buttons immediately, and registers a
    device-discovery callback with the coordinator so that per-FAN
    filter-reset buttons are created as soon as both the FAN device
    and its climate entity are available.

.. rubric:: Class Structure

.. code-block:: text

    RamsesButtonBase (RamsesEntity, ButtonEntity)
    └── RamsesButtonEntityDescription (
            RamsesEntityDescription, ButtonEntityDescription
        )
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from homeassistant.components.button import (
    ButtonEntity,
    ButtonEntityDescription,
)
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import (
    entity_platform,
    entity_registry as er,
)
from homeassistant.helpers.entity_platform import (
    AddEntitiesCallback,
    EntityPlatform,
)

from ramses_rf.devices import HgiGateway
from ramses_rf.entity import Entity as RamsesRFEntity

from .const import DOMAIN, SVC_DISCOVER_KNOWN_DEVICES
from .entity import RamsesEntity, RamsesEntityDescription
from .schemas import (
    SVC_FORCE_UPDATE,
    SVC_RESET_FILTER,
    SVC_SYNC_TOPOLOGY,
)
from .typing import RamsesConfigEntry

if TYPE_CHECKING:
    from .coordinator import RamsesCoordinator

_LOGGER = logging.getLogger(__name__)

# entity service: reset the filter counter of a FAN (W 10D0)


@dataclass(frozen=True, kw_only=True)
class RamsesButtonEntityDescription(
    RamsesEntityDescription, ButtonEntityDescription
):
    """Class describing Ramses button entities."""

    service: str | None = None  # ramses_cc service to call when pressed
    service_data: dict[str, str] | None = None
    entity_category: EntityCategory | None = EntityCategory.DIAGNOSTIC


class RamsesButtonBase(RamsesEntity, ButtonEntity):
    """Base for any RAMSES II-compatible button entity."""

    entity_description: RamsesButtonEntityDescription

    async def async_press(self) -> None:
        """Handle the button press.

        Calls the ramses_cc service named in the entity description,
        passing any configured service data (e.g. the entity_id of the
        FAN climate entity for the filter reset).
        """
        service = self.entity_description.service
        if not service:
            _LOGGER.warning(
                "Button %s has no service configured",
                self.entity_id,
            )
            return

        await self.hass.services.async_call(
            DOMAIN,
            service,
            service_data=self.entity_description.service_data or {},
            blocking=True,
        )
        _LOGGER.info(
            "Button %s called service %s",
            self.entity_id,
            service,
        )


@callback
def _fan_climate_entity_id(hass: HomeAssistant, fan_id: str) -> str | None:
    """Return the climate entity_id registered for a FAN device.

    The ``reset_filter_counter`` entity service accepts a FAN climate
    entity as its target (the handler resolves the bound REM itself).

    :param hass: The Home Assistant instance
    :param fan_id: The device id of the FAN
    :return: The climate entity_id, or None if not yet registered
    """
    ent_reg = er.async_get(hass)
    for entry in ent_reg.entities.values():
        if entry.domain == "climate" and entry.unique_id == str(fan_id):
            return entry.entity_id
    return None


@callback
def _make_filter_reset_button(
    coordinator: Any,
    fan: RamsesRFEntity,
    climate_entity_id: str,
) -> RamsesButtonBase:
    """Create a filter-reset button for a FAN device.

    :param coordinator: The RAMSES coordinator
    :param fan: The FAN device
    :param climate_entity_id: The climate entity_id of the FAN
    :return: The new button entity
    """
    description = RamsesButtonEntityDescription(
        key="reset_filter_counter",
        translation_key="reset_filter_counter",
        icon="mdi:filter-remove",
        service=SVC_RESET_FILTER,
        service_data={"entity_id": climate_entity_id},
    )
    button = RamsesButtonBase(coordinator, fan, description)
    button._attr_unique_id = f"{fan.id}-reset_filter_counter"
    return button


async def async_setup_entry(
    hass: HomeAssistant,
    entry: RamsesConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the RAMSES button platform from a config entry.

    Creates one gateway-level button per domain-wide service action,
    plus one filter-reset button per FAN (HVAC) device. Because the
    button platform may be set up before the HGIs are loaded, or the
    climate platform has registered the FAN entities, a discovery
    callback is registered with the coordinator: as devices (re)appear,
    any missing filter-reset buttons are created once their climate
    entity exists.

    :param hass: The Home Assistant instance
    :type hass: ~homeassistant.core.HomeAssistant
    :param entry: The config entry used to set up the platform
    :type entry: ~homeassistant.config_entries.ConfigEntry
    :param async_add_entities: Async function to add entities to the
        platform
    :type async_add_entities:
        ~homeassistant.helpers.entity_platform.AddEntitiesCallback
    :return: None
    :rtype: None
    """
    coordinator: RamsesCoordinator = entry.runtime_data
    platform: EntityPlatform = entity_platform.async_get_current_platform()

    # unique_ids of buttons already created (or scheduled)
    known_buttons: set[str] = set()

    _LOGGER.debug("Setting up button platform")

    entities: list[RamsesButtonBase] = []

    #
    # 1. Gateway-level service buttons
    #
    def _add_hgi_buttons(
        devices: list[RamsesRFEntity],
    ) -> list[RamsesButtonBase]:
        """Create buttons for any new HGI devices.

        :param devices: HGI devices to process
        :return: Newly created button entities (may be empty)
        """
        new_buttons: list[RamsesButtonBase] = []

        for hgi in devices:
            _LOGGER.debug("Adding HGI Button for %s", hgi.id)
            if not isinstance(hgi, HgiGateway):
                continue

            if hgi.id is not None:
                for description in (
                    RamsesButtonEntityDescription(
                        key="force_update",
                        translation_key="force_update",
                        icon="mdi:refresh",
                        service=SVC_FORCE_UPDATE,
                    ),
                    RamsesButtonEntityDescription(
                        key="sync_topology",
                        translation_key="sync_topology",
                        icon="mdi:lan-connect",
                        service=SVC_SYNC_TOPOLOGY,
                    ),
                    RamsesButtonEntityDescription(
                        key="discover_known_devices",
                        translation_key="discover_known_devices",
                        icon="mdi:magnify",
                        service=SVC_DISCOVER_KNOWN_DEVICES,
                    ),
                ):
                    button = RamsesButtonBase(coordinator, hgi, description)
                    button._attr_unique_id = f"{hgi.id}-{description.key}"
                    known_buttons.add(button._attr_unique_id)
                    entities.append(button)
            else:
                _LOGGER.warning(
                    "No gateway device found yet; skipping gateway service buttons"
                )

            new_buttons.append(
                _make_filter_reset_button(coordinator, hgi, hgi.id)
            )
        return new_buttons

    entities.extend(
        _add_hgi_buttons(list(getattr(coordinator, "devices", [])))
    )

    #
    # 2. Filter-reset buttons for FANs already known at setup time
    #
    def _add_fan_buttons(
        devices: list[RamsesRFEntity],
    ) -> list[RamsesButtonBase]:
        """Create filter-reset buttons for any new FAN devices.

        :param devices: FAN devices to process
        :return: Newly created button entities (may be empty)
        """
        new_buttons: list[RamsesButtonBase] = []

        for fan in devices:
            _LOGGER.debug("Adding FAN Button for %s", fan.id)
            if getattr(fan, "_SLUG", None) != "FAN":
                continue

            unique_id = f"{fan.id}-reset_filter_counter"
            if unique_id in known_buttons:
                continue  # already created/scheduled this setup

            climate_entity_id = _fan_climate_entity_id(hass, fan.id)
            if climate_entity_id is None:
                _LOGGER.debug(
                    "No climate entity yet for FAN %s; will retry when"
                    " devices are (re)discovered",
                    fan.id,
                )
                continue

            known_buttons.add(unique_id)
            new_buttons.append(
                _make_filter_reset_button(coordinator, fan, climate_entity_id)
            )
        return new_buttons

    entities.extend(
        _add_fan_buttons(list(getattr(coordinator, "devices", [])))
    )

    if entities:
        _LOGGER.debug("Adding %d button entities", len(entities))
        async_add_entities(entities, update_before_add=False)
    else:
        _LOGGER.debug("No button entities registered")

    #
    # 3. Platform ordering: create buttons for devices discovered
    #    later (e.g. before the climate platform registered the FAN
    #    entities, after the HGI came online or after a cache clear / re-discovery)
    #
    @callback
    def add_devices(
        devices: RamsesRFEntity
        | RamsesButtonBase
        | list[RamsesRFEntity | RamsesButtonBase],
    ) -> None:
        """Add button entities for newly discovered devices.

        The coordinator invokes this
        callback whenever devices are discovered or re-discovered.

        :param devices: Device(s) to process
        :return: None
        """
        gateway_id: str | None = None

        device_list = devices if isinstance(devices, list) else [devices]

        if all(isinstance(d, RamsesButtonBase) for d in device_list):
            # Direct entity addition (not used currently)
            async_add_entities(
                [d for d in device_list if isinstance(d, RamsesButtonBase)]
            )
            return

        hgi_buttons = _add_hgi_buttons(
            [d for d in device_list if isinstance(d, RamsesRFEntity)]
        )
        if hgi_buttons:
            _LOGGER.debug(
                "Adding %d reset buttons for newly discovered HGIs",
                len(hgi_buttons),
            )
            async_add_entities(hgi_buttons, update_before_add=False)
        else:
            _LOGGER.debug("No HGI buttons registered")

        fan_buttons = _add_fan_buttons(
            [d for d in device_list if isinstance(d, RamsesRFEntity)]
        )
        if fan_buttons:
            _LOGGER.debug(
                "Adding %d filter-reset buttons for newly discovered FANs",
                len(fan_buttons),
            )
            async_add_entities(fan_buttons, update_before_add=False)
        else:
            _LOGGER.debug("No FAN buttons registered")

    # Register the callback with the coordinator
    coordinator.async_register_platform(platform, add_devices)
