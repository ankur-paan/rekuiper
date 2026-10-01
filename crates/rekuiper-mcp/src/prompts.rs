use crate::protocol::{ContentItem, PromptArgument, PromptDefinition, PromptMessage};
use serde_json::Value;

pub fn get_prompt_definitions() -> Vec<PromptDefinition> {
    vec![
        PromptDefinition {
            name: "troubleshoot_rule".to_string(),
            description: Some("Comprehensive edge streaming diagnostics: inspects rule status telemetry (source reads, sink emissions, exception counters, filter drops), validates underlying SQL syntax, examines topological execution graph, and queries active distributed trace spans to isolate pipeline failures.".to_string()),
            arguments: vec![PromptArgument {
                name: "rule_name".to_string(),
                description: Some("Exact identifier of the deployed rekuiper stream processing rule experiencing execution failures, dropped tuples, or unexpected output.".to_string()),
                required: true,
            }],
        },
        PromptDefinition {
            name: "optimize_stream_sql".to_string(),
            description: Some("Performance and memory footprint optimization for rekuiper streaming SQL queries: analyzes window clauses (Tumbling vs Hopping vs Sliding vs Session vs CountWindow), evaluates column pruning opportunities, audits predicate pushdown, and validates memory allocation on constrained edge gateways.".to_string()),
            arguments: vec![PromptArgument {
                name: "sql".to_string(),
                description: Some("The rekuiper streaming SQL query string to optimize for throughput, memory consumption, and latency.".to_string()),
                required: true,
            }],
        },
        PromptDefinition {
            name: "generate_iot_alert_rule".to_string(),
            description: Some("Generates an industrial IoT anomaly detection and alerting pipeline adhering to edge streaming best practices: includes sliding/tumbling window debouncing to eliminate alert flapping, dynamic MQTT topic publishing, and data template formatting.".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "stream_name".to_string(),
                    description: Some("Identifier of the source stream providing incoming telemetry events.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "metric".to_string(),
                    description: Some("Telemetry field or property name to evaluate (e.g. 'bearing_vibration', 'motor_temperature', 'current_draw').".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "threshold".to_string(),
                    description: Some("Threshold predicate or numeric limit triggering the alert condition (e.g. '> 85.0', '< 12.2', 'abs(vibe) > 4.5').".to_string()),
                    required: true,
                },
            ],
        },
        PromptDefinition {
            name: "create_end_to_end_pipeline".to_string(),
            description: Some("Architects a full end-to-end edge pipeline: generates CREATE STREAM DDL with format/connector specifications, continuous aggregation SQL query, MQTT action sinks with JSON templating, and mock event test payload.".to_string()),
            arguments: vec![
                PromptArgument {
                    name: "pipeline_name".to_string(),
                    description: Some("System identifier for the edge streaming pipeline and rule.".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "datasource_topic".to_string(),
                    description: Some("Inbound transport identifier or MQTT topic pattern (e.g. 'factory/+/sensor/data') from which events are consumed.".to_string()),
                    required: true,
                },
            ],
        },
        PromptDefinition {
            name: "diagnose_data_drop".to_string(),
            description: Some("Systematic root-cause diagnosis for dropped stream records: audits source schema decoding, evaluates WHERE filter conditions against sample payloads, verifies window watermark progression, and tracks sink buffer backpressure.".to_string()),
            arguments: vec![PromptArgument {
                name: "rule_name".to_string(),
                description: Some("Identifier of the rule exhibiting data loss, dropped event counts, or sink delivery gaps.".to_string()),
                required: true,
            }],
        },
    ]
}

pub fn get_prompt_messages(name: &str, args: Option<Value>) -> Result<Vec<PromptMessage>, String> {
    match name {
        "troubleshoot_rule" => {
            let rule_name = args
                .as_ref()
                .and_then(|a| a.get("rule_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown_rule");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Please troubleshoot the rekuiper stream processing rule '{}'.\n\
                         1. Use `get_rule_status` to inspect runtime counters (messages in, messages out, exceptions, dropped records).\n\
                         2. Use `get_rule` to fetch its current SQL query and sink actions.\n\
                         3. Use `validate_sql` on the query to verify expression correctness.\n\
                         4. If necessary, use `start_rule_trace` and `get_rule_traces` to inspect event execution spans.\n\
                         5. Provide clear recommendations to fix the pipeline, tune buffering, or correct data types.",
                        rule_name
                    )),
                },
            ])
        }

        "optimize_stream_sql" => {
            let sql = args
                .as_ref()
                .and_then(|a| a.get("sql"))
                .and_then(|v| v.as_str())
                .unwrap_or("SELECT * FROM demo");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Analyze and optimize the following rekuiper SQL query for high-throughput edge execution:\n\n\
                         ```sql\n{}\n```\n\n\
                         Check for:\n\
                         - Window clause efficiency (Tumbling vs Hopping vs Sliding vs CountWindow)\n\
                         - Projection overhead and column pruning\n\
                         - Missing filter predicates to drop irrelevant messages early\n\
                         - Type casting suitability and null safety\n\
                         Use `test_sql_expression` with mock data to demonstrate the difference.",
                        sql
                    )),
                },
            ])
        }

        "generate_iot_alert_rule" => {
            let stream = args
                .as_ref()
                .and_then(|a| a.get("stream_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry");
            let metric = args
                .as_ref()
                .and_then(|a| a.get("metric"))
                .and_then(|v| v.as_str())
                .unwrap_or("temp");
            let threshold = args
                .as_ref()
                .and_then(|a| a.get("threshold"))
                .and_then(|v| v.as_str())
                .unwrap_or("80.0");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Generate a production-ready rekuiper rule JSON configuration for IoT alerting:\n\
                         - Input stream: `{}`\n\
                         - Monitored field: `{}`\n\
                         - Threshold condition: `{}`\n\
                         Include a 30-second deduplication or sliding window to avoid alert spamming, and an MQTT action sink publishing to `alerts/{}/triggered`.\n\
                         Validate the SQL using `validate_sql` before outputting.",
                        stream, metric, threshold, stream
                    )),
                },
            ])
        }

        "create_end_to_end_pipeline" => {
            let pipeline_name = args
                .as_ref()
                .and_then(|a| a.get("pipeline_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("demo_pipeline");
            let topic = args
                .as_ref()
                .and_then(|a| a.get("datasource_topic"))
                .and_then(|v| v.as_str())
                .unwrap_or("devices/data");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Design an end-to-end edge pipeline named '{}' consuming from MQTT topic '{}':\n\
                         1. Provide the `create_stream` DDL statement.\n\
                         2. Provide the `create_rule` payload with windowed aggregation (min, max, avg).\n\
                         3. Provide a mock JSON event payload and verify using `test_sql_expression`.\n\
                         4. Provide instructions to deploy using `rekuiper-mcp` tools.",
                        pipeline_name, topic
                    )),
                },
            ])
        }

        "diagnose_data_drop" => {
            let rule_name = args
                .as_ref()
                .and_then(|a| a.get("rule_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown_rule");

            Ok(vec![
                PromptMessage {
                    role: "user".to_string(),
                    content: ContentItem::text(format!(
                        "Investigate why data is dropped in rekuiper rule '{}':\n\
                         1. Inspect `get_rule_status` to see if records dropped in `source_dropped` or `sink_dropped`.\n\
                         2. Fetch `get_rule_topo` to trace the node sequence.\n\
                         3. Use `test_sql_expression` to test whether sample payloads satisfy the WHERE clause.\n\
                         4. If necessary, activate `start_rule_trace` to capture dropped payloads.",
                        rule_name
                    )),
                },
            ])
        }

        other => Err(format!("Unknown prompt: '{}'", other)),
    }
}
