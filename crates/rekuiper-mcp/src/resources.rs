use crate::protocol::{ResourceContent, ResourceDefinition};
use serde_json::json;

pub fn get_resource_definitions() -> Vec<ResourceDefinition> {
    vec![
        ResourceDefinition {
            uri: "rekuiper://rules".to_string(),
            name: "Active Stream Rules".to_string(),
            description: Some("Catalog of all stream processing rules deployed in the rekuiper engine, including topology configurations, SQL queries, action sinks, and execution lifecycle states.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://streams".to_string(),
            name: "Data Streams".to_string(),
            description: Some("Registry of active streaming data sources, schemas, and transport connectors (MQTT, EdgeX, Kafka, HTTP, Simulator).".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://tables".to_string(),
            name: "Lookup Tables".to_string(),
            description: Some("Catalog of static reference and dimension lookup tables available for enrichment joins in stream processing.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://connections".to_string(),
            name: "Shared Connections".to_string(),
            description: Some("Shared reusable connection resource pool definitions (brokers, relational databases, Kafka clusters).".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://udfs".to_string(),
            name: "JavaScript UDFs".to_string(),
            description: Some("Registered custom JavaScript User Defined Functions available for scalar transformation in streaming SQL.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://plugins".to_string(),
            name: "Engine Plugins".to_string(),
            description: Some("Catalog of installed native C/Rust plugins and portable multi-language gRPC extensions.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://configs".to_string(),
            name: "Engine Configuration".to_string(),
            description: Some("Active global runtime configuration parameters, buffer sizes, logging levels, and connector defaults.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://metrics".to_string(),
            name: "Engine Metrics".to_string(),
            description: Some("Real-time engine telemetry, CPU and memory utilization, active pipeline count, and throughput counters.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://metadata/sources".to_string(),
            name: "Source Connectors Catalog".to_string(),
            description: Some("Catalog of available input source connector drivers, configuration schemas, and transport options.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://metadata/sinks".to_string(),
            name: "Sink Connectors Catalog".to_string(),
            description: Some("Catalog of available action sink drivers (MQTT, Kafka, Redis, SQL, Log, REST, File) and output formatting templates.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
        ResourceDefinition {
            uri: "rekuiper://metadata/functions".to_string(),
            name: "Built-in SQL Functions".to_string(),
            description: Some("Catalog of built-in mathematical, string, aggregate, conversion, and temporal SQL functions available in rekuiper SQL.".to_string()),
            mime_type: Some("application/json".to_string()),
        },
    ]
}

async fn fetch_endpoint(
    client: &reqwest::Client,
    base_url: &str,
    path: &str,
) -> Result<String, String> {
    let url = format!("{}{}", base_url.trim_end_matches('/'), path);
    client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("Request to {} failed: {}", url, e))?
        .text()
        .await
        .map_err(|e| format!("Failed to read response from {}: {}", url, e))
}

pub async fn read_resource(
    client: &reqwest::Client,
    base_url: &str,
    uri: &str,
) -> Result<ResourceContent, String> {
    let text = match uri {
        "rekuiper://rules" => fetch_endpoint(client, base_url, "/rules").await?,
        "rekuiper://streams" => fetch_endpoint(client, base_url, "/streams").await?,
        "rekuiper://tables" => fetch_endpoint(client, base_url, "/tables").await?,
        "rekuiper://connections" => fetch_endpoint(client, base_url, "/connections").await?,
        "rekuiper://udfs" => fetch_endpoint(client, base_url, "/udf/javascript").await?,
        "rekuiper://plugins" => fetch_endpoint(client, base_url, "/plugins/sources").await?,
        "rekuiper://configs" => fetch_endpoint(client, base_url, "/configs").await?,
        "rekuiper://metadata/sources" => fetch_endpoint(client, base_url, "/metadata/sources").await?,
        "rekuiper://metadata/sinks" => fetch_endpoint(client, base_url, "/metadata/sinks").await?,
        "rekuiper://metadata/functions" => fetch_endpoint(client, base_url, "/metadata/functions").await?,
        "rekuiper://metrics" => {
            let url = format!("{}/rules", base_url.trim_end_matches('/'));
            let resp = client.get(&url).send().await;
            let rule_count = match resp {
                Ok(r) => r.json::<Vec<String>>().await.map(|v| v.len()).unwrap_or(0),
                Err(_) => 0,
            };
            let metrics = json!({
                "engine": "rekuiper",
                "rules_active": rule_count,
                "protocol": "Model Context Protocol (MCP)",
                "status": "online"
            });
            serde_json::to_string_pretty(&metrics).unwrap_or_default()
        }
        other => return Err(format!("Resource not found: '{}'", other)),
    };

    Ok(ResourceContent {
        uri: uri.to_string(),
        mime_type: Some("application/json".to_string()),
        text,
    })
}
