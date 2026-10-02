# Error Codes Reference

When a REST API request fails, rekuiper returns a structured JSON payload containing an error code and a diagnostic message.

## Error Code Descriptions

The table below lists system error codes and their associated failure categories:

| Error Code | Error Category | Description |
| :--- | :--- | :--- |
| `1000` | Undefined Error | Represents an unclassified internal system error. |
| `1002` | Resource Not Found | Indicates that the requested resource (stream, table, rule, or plugin) does not exist. |
| `1003` | I/O Error | Indicates an input or output communication failure in a source or sink connector. |
| `1004` | Encoding Error | Indicates a payload serialization, deserialization, or encoding failure. |
| `2001` | SQL Syntax Error | Indicates that the SQL statement contains syntax errors. |
| `2101` | SQL Plan Error | Indicates that the query optimizer failed to generate an execution plan. |
| `2201` | SQL Executor Error | Indicates that the runtime failed to instantiate pipeline execution operators. |
| `3000` | Stream or Table Error | Indicates an error during stream or table definition management. |
| `4000` | Rule Error | Indicates an error during rule creation, lifecycle management, or execution. |
| `5000` | Configuration Error | Indicates invalid or unparseable configuration properties. |
