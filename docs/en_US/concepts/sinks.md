# Sinks

Sinks transmit data to external systems. Sinks can transmit control commands to trigger actions. Sinks can also write status data to external storage.

In a rule definition, sink types are configured as actions. A rule can specify multiple actions, and different actions can use the same sink type.

## Result Encoding

Sink output is transmitted as text. By default, rekuiper encodes output records into JSON strings.

You can customize output formatting with the `dataTemplate` property. This property uses template syntax to format records into strings.

For custom output formatting requirements, you can develop a sink extension.

## Further Reading

- [Sink Reference](../guide/sinks/overview.md)

