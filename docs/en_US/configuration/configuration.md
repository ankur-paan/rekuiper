# Configuration Overview

rekuiper configurations use YAML files. You can configure parameters through configuration files, environment variables, command-line arguments, and REST APIs.

## Configuration Scope

rekuiper configurations include:

1. `etc/kuiper.yaml`: The global configuration file. Changes require restarting the rekuiper server process. Refer to [Global Configurations](./global_configurations.md) for complete parameter details.
2. `etc/sources/${source_name}.yaml`: Source configuration profiles (and `etc/mqtt_source.yaml` for MQTT sources). Refer to individual source documentation, such as the [MQTT Source Guide](../guide/sources/builtin/mqtt.md).
3. `etc/connections/connection.yaml`: Shared reusable connection profiles.

## Configuration Precedence

rekuiper resolves configuration parameters using the following hierarchy, from highest to lowest precedence:

1. Management Console and REST API
2. Environment variables
3. YAML files in the `etc` directory

YAML files define default baseline settings for bare-metal host installations.

In containerized Docker or Kubernetes deployments, use environment variables to override default settings without modifying image layers. In active production environments, update configurations dynamically through the REST API or Web Manager interface.

### Environment Variable Syntax

rekuiper maps environment variables to YAML settings using double underscore (`__`) separators.

The prefix identifies the target configuration file:
- `KUIPER`: Maps to `etc/kuiper.yaml`.
- `MQTT_SOURCE`: Maps to `etc/mqtt_source.yaml`.
- `CONNECTION`: Maps to `etc/connections/connection.yaml`.
- Any other name: Maps to `etc/sources/${source_name}.yaml`.

Example mappings:

```text
KUIPER__BASIC__DEBUG => basic.debug in etc/kuiper.yaml
MQTT_SOURCE__DEMO_CONF__QOS => demo_conf.qos in etc/mqtt_source.yaml
EDGEX__DEFAULT__PORT => default.port in etc/sources/edgex.yaml
CONNECTION__EDGEX__REDISMSGBUS__PORT => edgex.redismsgbus.port in etc/connections/connection.yaml
```

### Command-Line Arguments

The `kuiperd` binary accepts command-line flags to configure directory paths:

| Flag Name | Data Type | Description |
| :--- | :--- | :--- |
| `loadFileType` | string | Defines path resolution mode. Supported values: `relative` and `absolute`. |
| `etc` | string | Specifies the absolute directory path for configuration files. Active when `loadFileType` is `absolute`. |
| `data` | string | Specifies the absolute directory path for application state. Active when `loadFileType` is `absolute`. |
| `log` | string | Specifies the absolute directory path for log files. Active when `loadFileType` is `absolute`. |
| `plugins` | string | Specifies the absolute directory path for external plugins. Active when `loadFileType` is `absolute`. |

Example command:

```bash
./bin/kuiperd -loadFileType absolute -etc /etc/kuiper
```
