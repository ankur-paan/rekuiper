# Plugins Management

The rekuiper plugin CLI manages plugins. You can create, list, describe, register, and drop plugins.

> [!NOTE]
> Deleting a plugin requires restarting rekuiper to complete the removal. To update an existing plugin:
> 1. Delete the plugin.
> 2. Restart rekuiper.
> 3. Create the plugin using the updated binary or configuration.

## Create a Plugin

Use this command to install and register a plugin. Specify plugin definitions in JSON format:

```shell
create plugin $plugin_type $plugin_name $plugin_json | create plugin $plugin_type $plugin_name -f $plugin_def_file
```

You can create plugins using three methods:

### Specify the Definition on the Command Line

```shell
# bin/kuiper create plugin source random {"file":"http://127.0.0.1/plugins/sources/random.zip"}
```

This command creates a source plugin named `random`.

### Specify the Definition in a File

Use the `-f` flag to load the plugin definition from a file:

```shell
# bin/kuiper create plugin sink plugin1 -f /tmp/plugin1.txt
```

Example contents of `/tmp/plugin1.txt`:

```json
{
  "file": "http://127.0.0.1/plugins/sources/random.zip"
}
```

To create a function plugin that exports multiple functions, specify the function names in the `functions` array:

```shell
# bin/kuiper create plugin function mulfuncs "{\"file\":\"file:///tmp/kuiper/plugins/functions/mulfuncs.zip\",\"functions\":[\"func1\",\"func2\"]}"
```

### Specify a Local Zip File

When running `kuiperd` locally, use the `-zf` flag to point directly to a local `.zip` file:

```shell
# bin/kuiper create plugin sink plugin1 -zf ./plugin1.zip
```

### Plugin Parameters

- `plugin_type`: The plugin type. Supported values are `"source"`, `"sink"`, `"function"`, and `"portable"`.
- `plugin_name`: The unique identifier of the plugin in lowerCamelCase format (for example, `random` for `Random`).
- `file`: The URL or path of the `.zip` archive containing the compiled `.so` file and YAML metadata. For file layout rules, refer to [Plugin Extension Overview](../../extension/overview.md).
- `functions`: A string array specifying exported function names for multi-function plugins.

## Show Plugins

Use this command to list all installed plugins for a specified plugin type:

```shell
show plugins function
```

Example output:

```shell
# bin/kuiper show plugins function
function1
function2
```

## Describe a Plugin

Use this command to display metadata for an installed plugin:

```shell
describe plugin $plugin_type $plugin_name
```

Example output:

```shell
# bin/kuiper describe plugin source plugin1
{
  "name": "plugin1",
  "version": "1.0.0"
}
```

## Drop a Plugin

Use this command to delete an installed plugin:

```shell
drop plugin $plugin_type $plugin_name -s $stop
```

The `-s $stop` flag is an optional boolean. When set to `true`, the rekuiper server process stops so the file deletion takes effect. You must restart the server manually.

Example command:

```shell
# bin/kuiper drop plugin source random
Plugin random is dropped.
```

## Multi-Function Plugin Commands

Function plugins can export multiple functions. Function names must be globally unique across all plugins.

### Show User-Defined Functions

Use this command to display all registered user-defined functions:

```shell
show udfs
```

### Describe a User-Defined Function

Use this command to identify the plugin that provides a specific user-defined function:

```shell
describe udf $udf_name
```

Example output:

```json
{
  "name": "funcName",
  "plugin": "pluginName"
}
```

### Register Functions

Use this command to register exported function names for an auto-loaded plugin or when exported functions change:

```shell
register plugin function $pluginName "{\"functions\":[\"$funcName\",\"$anotherFuncName\"]}"
```

Example command:

```shell
# bin/kuiper register plugin function myPlugin "{\"functions\":[\"func1\",\"func2\",\"funcn\"]}"
```
