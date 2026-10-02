# Extensions

rekuiper includes built-in sources, sinks, and SQL functions. However, custom integrations frequently require proprietary protocols or domain-specific calculations. rekuiper provides an extension framework to customize sources, sinks, and functions.

## Extension Points

rekuiper provides three extension points:

- **Source**: Adds a custom source type to ingest data. You can reference the new source type in stream and table definitions.
- **Sink**: Adds a custom sink type to emit data. You can reference the new sink type in rule actions.
- **Function**: Adds a custom SQL function to transform data. You can use the new function in rule SQL queries.

## Extension Types

rekuiper supports three extension mechanisms:

- **[Native plugin](../extension/native/overview.md)**: Extends functionality with native shared libraries. This mechanism provides maximum performance, but requires specific compilation environments.
- **[Portable plugin](../extension/portable/overview.md)**: Implements extensions in languages such as Python or Go through independent processes. This mechanism simplifies development and deployment.
- **[External service](../extension/external/external_func.md)**: Maps existing external REST or RPC services to SQL functions through configuration files. This mechanism supports function extensions only.

## Further Reading

- [Extension Reference](../extension/overview.md)

