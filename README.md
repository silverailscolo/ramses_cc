![ruff](https://github.com/ramses-rf/ramses_cc/actions/workflows/check-lint.yml/badge.svg?master)
![mypy](https://github.com/ramses-rf/ramses_cc/actions/workflows/check-type.yml/badge.svg?master)
![pytest](https://github.com/ramses-rf/ramses_cc/actions/workflows/check-test.yml/badge.svg?master)
[![Coverage](https://github.com/ramses-rf/ramses_cc/actions/workflows/check-cov.yml/badge.svg?event=push)](https://github.com/ramses-rf/ramses_cc/actions/workflows/check-cov.yml)


## Overview
**ramses_cc** is a Home Assistant custom integration that works with RAMSES II-based RF 868 Mhz systems for (heating) **CH/DHW** (e.g. Honeywell Evohome) and (ventilation) **HVAC** (e.g. Itho Spider, Orcon).

This includes CH/DHW systems such as **evohome**, **Sundial**, **Hometronic**, **Chronotherm** and others.

The simplest way to know if it will work with your CH/DHW system is to identify the box connected to your boiler (or other heat source) to one of (there will be other systems that also work):
 - **R8810A** or **R8820A**: OpenTherm Bridge
 - **BDR91A** or **BDR91T**: Wireless Relay
 - **HC60NG**: Wireless Relay (older hardware version)

**ramses_cc** also works with HVAC (ventilation) systems using the same protocol, such as from **Itho**, **Orcon**, **Nuaire**, **Ventiline**, **Vasco**, etc.

It uses the [ramses_rf](https://github.com/ramses-rf/ramses_rf) client library to decode the RAMSES-II protocol used by these devices. Note that other systems may also use this protocol. YMMV!

The library requires a USB-to-RF device, either a Honeywell HGI80 (rare, expensive) or a USB/MQTT dongle running [ramses_esp](https://github.com/IndaloTech/ramses_esp) or [evofw3](https://github.com/ghoti57/evofw3), such as the one from [here](https://indalo-tech.onlineweb.shop/) or your own ESP32-S3-WROOM-1 N16R8 with a CC1100 transponder.

### Wiki

See the [wiki](https://github.com/ramses-rf/ramses_cc/wiki) for installation, configuration, troubleshooting, etc.
