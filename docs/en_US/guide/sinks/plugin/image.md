# Image Sink

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Go Plugin)
The Image sink was implemented as a Go C-shared dynamic plugin (`.so`) in legacy eKuiper.

**rekuiper is written in Rust and does NOT support or load Go dynamic plugins (`.so`).** The Image sink is not implemented in rekuiper.

**Supported Alternatives in rekuiper**:
- To write binary payloads to disk, use the built-in [File Sink](../builtin/file.md) with raw binary encoding.
- To transmit image payloads across networks, use the built-in [REST Sink](../builtin/rest.md) or [MQTT Sink](../builtin/mqtt.md).
:::

## Overview (Legacy Reference Only)

In legacy eKuiper (Go), the Image sink stored raw binary image data (JPEG or PNG) to a local directory with automated age and count retention policies. This page is preserved only as an architectural reference for users who migrate from legacy Go eKuiper deployments.

## Legacy Configuration Parameters

| Property Name | Optional | Description |
| :--- | :--- | :--- |
| `path` | False | Target directory path for saved images (such as `./tmp`). |
| `format` | False | Image format: `jpeg` or `png`. |
| `maxAge` | True | Maximum retention time in hours. Default was `72`. |
| `maxCount` | True | Maximum number of stored image files. Default was `1000`. |

## Legacy Rule Example

```json
{
  "id": "rule_legacy_image",
  "sql": "SELECT * FROM camera_stream",
  "actions": [
    {
      "image": {
        "path": "/tmp/images",
        "format": "png",
        "maxCount": 1000,
        "maxAge": 72
      }
    }
  ]
}
```

## Migration Path to rekuiper

In rekuiper, write binary records to disk using the built-in File sink or post them to an external storage service using the REST sink:

```json
{
  "id": "rule_image_file",
  "sql": "SELECT image_bytes FROM camera_stream",
  "actions": [
    {
      "file": {
        "path": "data/images/capture.bin",
        "format": "binary"
      }
    }
  ]
}
```
