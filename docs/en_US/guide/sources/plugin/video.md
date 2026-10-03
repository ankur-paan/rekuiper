# Video Source Connector

::: danger Status: Unsupported in rekuiper (Legacy eKuiper Go Plugin)
The Video source connector was implemented as a Go plugin in legacy eKuiper.

**rekuiper is written in Rust and does NOT support or load Go dynamic plugins (`.so`).** The Video source connector is not implemented in rekuiper.

**Supported Alternatives in rekuiper**:
- Ingest image frame telemetry or computer vision results through the built-in [HTTP Push Source](../builtin/http_push.md) or [WebSocket Source](../builtin/websocket.md).
- For edge computer vision workflows, extract RTSP frames in an external process (such as a GStreamer or OpenCV pipeline) and send inferred metadata to rekuiper via [MQTT](../builtin/mqtt.md).
:::

## Overview (Legacy Reference Only)

In legacy eKuiper (Go), the Video source connector extracted image frames from RTSP video streams by spawning an internal `ffmpeg` process. This page is preserved only as an architectural reference for users who migrate from legacy Go eKuiper deployments.

## Legacy Configuration Overview

In legacy eKuiper, the connector configuration resided in `etc/sources/video.yaml`:

```yaml
default:
  url: rtsp://localhost:8554/stream
  interval: 1000
  codec: mjpeg
  debugResp: false
```

### Legacy Parameters

- `url`: Streaming video URL (such as `rtsp://localhost:8554/stream`).
- `interval`: Frame extraction interval in milliseconds.
- `codec`: Target video frame codec. Default was `'mjpeg'`.
- `debugResp`: Boolean flag to log FFmpeg process output.
- `inputArgs`: Map of custom arguments passed to the FFmpeg process.

## Legacy Stream Definition

```sql
CREATE STREAM video_stream () WITH (
  FORMAT = "JSON",
  CONF_KEY = "default",
  TYPE = "video"
);
```

## Migration Path to rekuiper

In rekuiper, run your frame extraction or computer vision model in a dedicated container or process. Publish the extracted telemetry (for example, bounding boxes, counts, or classifications) directly to rekuiper via MQTT:

```sql
CREATE STREAM detection_stream (
  camera_id STRING,
  object_class STRING,
  confidence FLOAT,
  ts BIGINT
) WITH (
  TYPE = "mqtt",
  DATASOURCE = "cameras/+/detections",
  FORMAT = "json"
);
```
