# Video Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

The Video source connector extracts image frames from video streams (such as RTSP streams) using the `ffmpeg` utility.

## Configuration Overview

Configure the video connector in `$rekuiper/etc/sources/video.yaml`:

```yaml
default:
  url: http://localhost:8080
  interval: 1000
  codec: mjpeg
  debugResp: false

ext:
  interval: 10000
```

> [!NOTE]
> Since version 2.4.0, the connector removes the `vformat` property and automatically uses `image2pipe` streaming mode. Existing `vformat` settings are ignored.

### Configuration Parameters

- `url`: Target streaming video URL (for example, `rtsp://localhost:8554/stream`).
- `interval`: Frame extraction interval in milliseconds.
- `codec`: Target video frame codec. Default is `'mjpeg'`.
- `debugResp`: Boolean. Set to `true` to log FFmpeg process output for diagnostic debugging. Default is `false`.
- `inputArgs`: Mapping of custom command arguments passed to FFmpeg input options (such as `-rtsp_transport` or `-fflags`).

Example with custom FFmpeg input arguments:

```yaml
default:
  url: rtsp://localhost:8554/stream
  inputArgs:
    rtsp_transport: tcp
    fflags: nobuffer
```

## Custom Configurations

Define custom configuration blocks in `video.yaml`:

```yaml
ext:
  interval: 10000
```

Reference the configuration using `CONF_KEY="ext"`:

```sql
CREATE STREAM demo () WITH (
  FORMAT = "JSON",
  CONF_KEY = "ext",
  TYPE = "video"
);
```

For stream syntax and management details, refer to [Streams Management](../../streams/overview.md).
