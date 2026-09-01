# Usage

## Installation

To use ramses_cc, install it in Home Assistant either via HACS or using the `update.install` Action:

```yaml
   action: update.install
   target:
      entity_id: update.ramses_cc_update
   data:
      version: 0.52.1
```

### Supported Transceivers & Installation Parameters

| Parameter | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| **Transceiver Hardware** | Hardware | Yes | Honeywell HGI80, SSM-D2 / evofw3 dongle, or Indalo Tech ramses_esp (ESP32-S3). |
| **Local USB Port** | String / Path | Optional | Path to local serial port (e.g. `/dev/serial/by-id/usb-...`, `COM3`). Default baud rate `115200` (evofw3/ramses_esp) or `57600` (HGI80). |
| **Network Serial Port** | URI / URL | Optional | Remote serial socket using `rfc2217://<host>:<port>`, `socket://<host>:<port>`, or `tcp://<host>:<port>`. |
| **Home Assistant MQTT** | Service | Optional | Built-in Home Assistant MQTT integration bridge with broker topic subscription. |
| **Standalone MQTT** | URI / URL | Optional | Custom Paho MQTT broker connection string (`mqtt://[user:pass@]<host>[:port]`). |
| **Zigbee Bridge** | URI / URL | Optional | Zigbee coordinator bridge URI (`zigbee://<ieee_address>/<cluster>/<attribute>`). |

### Configuration & Options Parameters

| Option Parameter | Type | Default | Description |
| :--- | :--- | :---: | :--- |
| `scan_interval` | Integer (seconds) | `1200` | Periodic polling interval (in seconds) for supported passive parameter queries. |
| `packet_log` | File Path | None | Absolute path to a file where raw RAMSES II protocol packet logs are recorded. |
| `restore_cache` | Boolean | `True` | Restore learned device traits and schema from cache upon startup. |
| `advanced_features` | Boolean | `False` | Enable advanced entity creation (e.g. packet debug event entities). |
| `message_events` | Boolean | `False` | Dispatch RAMSES RF packet message events on Home Assistant's event bus. |
| `known_list` | Dictionary | `{}` | Explicit list of authorized device IDs and optional device class / faked traits. |
| `block_list` | Dictionary | `{}` | Explicit list of device IDs to ignore/block from discovery and packet processing. |
| `system_schema` | Dictionary | `{}` | Declarative schema defining controller (CTL), heating/cooling zones, and ventilation topology. |


## Documentation

We use [sphinx](https://www.sphinx-doc.org/en/master/usage/markdown.html) and
MyST [markup](https://myst-parser.readthedocs.io/en/latest/syntax/organising_content.html) to automatically create this code documentation from `docstr` annotations in our python code.

- Activate your virtual environment for ramses_cc as described in the [Wiki](https://github.com/ramses-rf/ramses_cc/wiki).

- Install the extra required dependencies by running ``pip install -r requirements_docs.txt`` so you can build a local set.

- Then, in a Terminal, enter `cd docs/` and run `sphinx-build -b html source build/html`.

- When the operation finishes, you can open the generated files from the `docs/build/html/` folder in a web browser.
