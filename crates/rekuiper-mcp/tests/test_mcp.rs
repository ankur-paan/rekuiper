use rekuiper_mcp::protocol::JsonRpcRequest;
use rekuiper_mcp::McpHandler;
use serde_json::json;

#[tokio::test]
async fn test_initialize_handshake() {
    let handler = McpHandler::new("http://127.0.0.1:9081");
    let req = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(1)),
        method: "initialize".to_string(),
        params: Some(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "Antigravity", "version": "1.0.0" }
        })),
    };

    let resp = handler.handle_request(req).await.expect("Must produce response");
    assert_eq!(resp.id, Some(json!(1)));
    let result = resp.result.expect("Should have result");
    assert_eq!(result["protocolVersion"], "2024-11-05");
    assert_eq!(result["serverInfo"]["name"], "rekuiper-mcp");
}

#[tokio::test]
async fn test_tools_list() {
    let handler = McpHandler::new("http://127.0.0.1:9081");
    let req = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(2)),
        method: "tools/list".to_string(),
        params: None,
    };

    let resp = handler.handle_request(req).await.expect("Must produce response");
    let result = resp.result.expect("Should have result");
    let tools = result["tools"].as_array().expect("Tools must be an array");

    let tool_names: Vec<&str> = tools
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();

    // SQL intelligence
    assert!(tool_names.contains(&"validate_sql"));
    assert!(tool_names.contains(&"test_sql_expression"));
    assert!(tool_names.contains(&"explain_sql"));

    // Streams & Tables
    assert!(tool_names.contains(&"list_streams"));
    assert!(tool_names.contains(&"get_stream"));
    assert!(tool_names.contains(&"create_stream"));
    assert!(tool_names.contains(&"delete_stream"));
    assert!(tool_names.contains(&"push_stream_data"));
    assert!(tool_names.contains(&"list_tables"));
    assert!(tool_names.contains(&"get_table"));
    assert!(tool_names.contains(&"create_table"));
    assert!(tool_names.contains(&"delete_table"));
    assert!(tool_names.contains(&"push_table_data"));

    // Rules lifecycle
    assert!(tool_names.contains(&"list_rules"));
    assert!(tool_names.contains(&"get_rule"));
    assert!(tool_names.contains(&"create_rule"));
    assert!(tool_names.contains(&"update_rule"));
    assert!(tool_names.contains(&"delete_rule"));
    assert!(tool_names.contains(&"start_stop_rule"));
    assert!(tool_names.contains(&"bulk_start_stop_rules"));
    assert!(tool_names.contains(&"get_rule_status"));
    assert!(tool_names.contains(&"get_rule_topo"));
    assert!(tool_names.contains(&"reset_rule_state"));

    // Tracing
    assert!(tool_names.contains(&"start_rule_trace"));
    assert!(tool_names.contains(&"stop_rule_trace"));
    assert!(tool_names.contains(&"get_rule_traces"));
    assert!(tool_names.contains(&"get_trace_details"));

    // Connections & Plugins
    assert!(tool_names.contains(&"list_connections"));
    assert!(tool_names.contains(&"create_connection"));
    assert!(tool_names.contains(&"delete_connection"));
    assert!(tool_names.contains(&"list_plugins"));
    assert!(tool_names.contains(&"list_javascript_udfs"));
    assert!(tool_names.contains(&"create_javascript_udf"));
    assert!(tool_names.contains(&"delete_javascript_udf"));
    assert!(tool_names.contains(&"list_services"));

    // Config & Migration
    assert!(tool_names.contains(&"get_configs"));
    assert!(tool_names.contains(&"update_configs"));
    assert!(tool_names.contains(&"export_data"));
    assert!(tool_names.contains(&"import_data"));

    // Telemetry & Escape hatch
    assert!(tool_names.contains(&"get_engine_metrics"));
    assert!(tool_names.contains(&"ping_engine"));
    assert!(tool_names.contains(&"execute_rekuiper_api"));
}

#[tokio::test]
async fn test_offline_sql_validation() {
    let handler = McpHandler::new("http://127.0.0.1:9081");

    // 1. Valid SQL SELECT
    let req_valid = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(10)),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "validate_sql",
            "arguments": {
                "sql": "SELECT abs(vibe) AS v_abs, upper(status) AS s FROM telemetry WHERE temp > 25.0 GROUP BY TumblingWindow(ss, 10)"
            }
        })),
    };
    let resp = handler.handle_request(req_valid).await.unwrap();
    let res = resp.result.unwrap();
    assert_eq!(res["isError"], false);
    let text = res["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("\"valid\": true"));
    assert!(text.contains("\"has_window\": true"));

    // 2. Valid SQL CREATE STREAM
    let req_ddl = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(11)),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "validate_sql",
            "arguments": {
                "sql": "create stream demo () WITH (FORMAT=\"json\", DATASOURCE=\"telem\")"
            }
        })),
    };
    let resp_ddl = handler.handle_request(req_ddl).await.unwrap();
    let res_ddl = resp_ddl.result.unwrap();
    assert_eq!(res_ddl["isError"], false);
    let text_ddl = res_ddl["content"][0]["text"].as_str().unwrap();
    assert!(text_ddl.contains("\"statement_type\": \"CREATE STREAM\""));

    // 3. Invalid SQL
    let req_invalid = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(12)),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "validate_sql",
            "arguments": {
                "sql": "SELEKT INVALID FROM telemetry"
            }
        })),
    };
    let resp_err = handler.handle_request(req_invalid).await.unwrap();
    let res_err = resp_err.result.unwrap();
    assert_eq!(res_err["isError"], true);
}

#[tokio::test]
async fn test_offline_sql_expression_simulator() {
    let handler = McpHandler::new("http://127.0.0.1:9081");

    // 1. Matched row with transformations
    let req_calc = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(20)),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "test_sql_expression",
            "arguments": {
                "sql": "SELECT abs(vibe) AS v, upper(status) AS s, temp * 1.8 + 32.0 AS fahrenheit FROM demo WHERE temp > 20",
                "data": {
                    "vibe": -3.5,
                    "status": "online",
                    "temp": 25.0
                }
            }
        })),
    };
    let resp = handler.handle_request(req_calc).await.unwrap();
    let res = resp.result.unwrap();
    assert_eq!(res["isError"], false);
    let out: serde_json::Value =
        serde_json::from_str(res["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(out["status"], "matched");
    assert_eq!(out["output"]["s"], "ONLINE");
    assert_eq!(out["output"]["v"], 3.5);
    assert_eq!(out["output"]["fahrenheit"], 77.0);

    // 2. Filtered out row
    let req_filtered = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(21)),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "test_sql_expression",
            "arguments": {
                "sql": "SELECT temp FROM demo WHERE temp > 50",
                "data": { "temp": 12.0 }
            }
        })),
    };
    let resp_f = handler.handle_request(req_filtered).await.unwrap();
    let res_f = resp_f.result.unwrap();
    assert_eq!(res_f["isError"], false);
    let out_f: serde_json::Value =
        serde_json::from_str(res_f["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(out_f["status"], "filtered");
}

#[tokio::test]
async fn test_explain_sql() {
    let handler = McpHandler::new("http://127.0.0.1:9081");
    let req = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(25)),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "explain_sql",
            "arguments": {
                "sql": "SELECT temp, vibe FROM telemetry WHERE temp > 30 GROUP BY TumblingWindow(ss, 5)"
            }
        })),
    };
    let resp = handler.handle_request(req).await.unwrap();
    let res = resp.result.unwrap();
    assert_eq!(res["isError"], false);
    let text = res["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("\"valid\": true"));
    assert!(text.contains("telemetry"));
}

#[tokio::test]
async fn test_resources_and_prompts() {
    let handler = McpHandler::new("http://127.0.0.1:9081");

    // Resources list
    let req_res = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(30)),
        method: "resources/list".to_string(),
        params: None,
    };
    let resp = handler.handle_request(req_res).await.unwrap();
    let res = resp.result.unwrap();
    let uris: Vec<&str> = res["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["uri"].as_str().unwrap())
        .collect();
    assert!(uris.contains(&"rekuiper://rules"));
    assert!(uris.contains(&"rekuiper://streams"));
    assert!(uris.contains(&"rekuiper://tables"));
    assert!(uris.contains(&"rekuiper://connections"));
    assert!(uris.contains(&"rekuiper://udfs"));
    assert!(uris.contains(&"rekuiper://plugins"));
    assert!(uris.contains(&"rekuiper://configs"));
    assert!(uris.contains(&"rekuiper://metrics"));
    assert!(uris.contains(&"rekuiper://metadata/sources"));
    assert!(uris.contains(&"rekuiper://metadata/sinks"));
    assert!(uris.contains(&"rekuiper://metadata/functions"));

    // Prompts list
    let req_prompt = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        id: Some(json!(31)),
        method: "prompts/list".to_string(),
        params: None,
    };
    let resp = handler.handle_request(req_prompt).await.unwrap();
    let res = resp.result.unwrap();
    let prompts: Vec<&str> = res["prompts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(prompts.contains(&"troubleshoot_rule"));
    assert!(prompts.contains(&"optimize_stream_sql"));
    assert!(prompts.contains(&"generate_iot_alert_rule"));
    assert!(prompts.contains(&"create_end_to_end_pipeline"));
    assert!(prompts.contains(&"diagnose_data_drop"));
}
