# Dynamic Reload Configs

By dynamically reloading [configuration](../../configuration/global_configurations.md), parameters such as debug and timezone
can be updated for running rekuiper without restarting the application.

## Reload Basic Configs

```shell
PATCH http://localhost:9081/configs
```

Request demo:

```json
{
  "debug": true,
  "consoleLog": true,
  "fileLog": true,
  "timezone": "UTC"
}
```

Current supported dynamic reloadable parameters:

- `debug`
- `consoleLog`
- `fileLog`
- `timezone`

## Shutdown rekuiper

```shell
POST http://localhost:9081/stop
```

Shut down rekuiper through REST API.
