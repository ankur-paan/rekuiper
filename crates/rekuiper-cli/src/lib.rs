use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};

#[derive(Parser, Debug)]
#[command(name = "rekuiper")]
#[command(
    about = "rekuiper CLI - High-performance edge stream processing engine client (kuiper drop-in)",
    long_about = None
)]
pub struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:9081")]
    pub url: String,

    #[arg(short = 'o', long = "output", default_value = "text")]
    pub output: String,

    #[arg(long = "json")]
    pub json: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum Commands {
    /// Ping rekuiper server
    Ping,
    /// Create a stream, table, rule, script, schema, service or plugin
    /// (`-f <file>` reads the definition from a file, eKuiper parity)
    Create {
        entity_type: String,
        #[arg(trailing_var_arg = true, num_args = 0.., allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Show streams, rules, tables, scripts, schemas, services, plugins, udfs, service_funcs
    Show {
        entity_type: String,
        extra: Option<String>,
    },
    /// Describe a stream, table, rule, script, service, schema or plugin
    /// (`describe schema <kind> <name>`, `describe plugin <kind> <name>`, `describe stream <name> -json`)
    Describe {
        entity_type: String,
        name: String,
        extra: Option<String>,
    },
    /// Explain a rule execution plan (`explain rule <name>`)
    Explain { entity_type: String, name: String },
    /// Query against streams (interactive or one-shot: `query [sql]`)
    Query {
        #[arg(trailing_var_arg = true, num_args = 0.., allow_hyphen_values = true)]
        sql: Vec<String>,
    },
    /// Drop a stream, table, rule, schema or script
    Drop {
        entity_type: String,
        name: String,
        extra: Option<String>,
    },
    /// Get status of a rule (`getstatus rule <name>`) or of data import
    /// (`getstatus import`)
    Getstatus {
        entity_type: String,
        name: Option<String>,
    },
    /// Start a rule
    Start { entity_type: String, name: String },
    /// Stop a rule
    Stop { entity_type: String, name: String },
    /// Restart a rule
    Restart { entity_type: String, name: String },
    /// Get the topology of a rule
    Gettopo { entity_type: String, name: String },
    /// Validate a rule definition (inline JSON or `-f <file>`)
    Validate {
        entity_type: String,
        #[arg(trailing_var_arg = true, num_args = 0.., allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Export a ruleset or data snapshot to a file (`export [entity] <file>`)
    Export {
        arg1: String,
        arg2: Option<String>,
        /// Restrict a data export to specific rules (`-r '["r1"]'`)
        #[arg(short = 'r', long = "rules")]
        rules: Option<String>,
    },
    /// Import a ruleset or data snapshot from a file (`import [entity] -f <file>` or `import <file>`)
    Import {
        entity_type: String,
        #[arg(trailing_var_arg = true, num_args = 0.., allow_hyphen_values = true)]
        args: Vec<String>,
    },
}

/// Split an eKuiper-style `-f <file>` / `--file <file>` flag out of raw
/// trailing CLI args (clap's `trailing_var_arg` swallows flags, so file
/// forms are extracted manually, position-independently).
pub fn split_file_flag(args: Vec<String>) -> (Vec<String>, Option<String>) {
    let mut rest = Vec::with_capacity(args.len());
    let mut file: Option<String> = None;
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        if file.is_none() && (a == "-f" || a == "--file") {
            if let Some(p) = it.next() {
                file = Some(p);
                continue;
            }
        }
        rest.push(a);
    }
    (rest, file)
}

fn fail(msg: String) -> ! {
    println!("{}", msg);
    std::process::exit(1);
}

fn read_def_file(path: &str) -> String {
    std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read file {}", path))
        .unwrap_or_else(|e| fail(format!("{:?}", e)))
}

fn normalize_plugin_kind(kind: &str) -> Option<&'static str> {
    match kind.to_ascii_lowercase().as_str() {
        "source" | "sources" => Some("sources"),
        "sink" | "sinks" => Some("sinks"),
        "function" | "functions" => Some("functions"),
        "portable" | "portables" => Some("portables"),
        "udf" | "udfs" => Some("udfs"),
        _ => None,
    }
}

async fn handle_response(res: reqwest::Response) -> Result<String> {
    let status = res.status();
    let text = res.text().await?;
    if !status.is_success() {
        let msg = if let Ok(val) = serde_json::from_str::<Value>(&text) {
            if let Some(m) = val.get("message").and_then(|m| m.as_str()) {
                m.to_string()
            } else {
                text
            }
        } else {
            text
        };
        fail(msg);
    }
    Ok(text)
}

fn print_described_fields_and_options(text: &str) {
    if let Ok(val) = serde_json::from_str::<Value>(text) {
        let fields = val
            .get("StreamFields")
            .or_else(|| val.get("Fields"))
            .and_then(|v| v.as_array());
        let options = val.get("Options").and_then(|v| v.as_object());
        println!("Fields");
        println!(
            "--------------------------------------------------------------------------------"
        );
        if let Some(f_arr) = fields {
            for f in f_arr {
                let name = f
                    .get("Name")
                    .or_else(|| f.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ty = f
                    .get("Type")
                    .or_else(|| f.get("type"))
                    .or_else(|| f.get("data_type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !name.is_empty() {
                    println!("{}  {}", name, ty);
                }
            }
        }
        println!();
        if let Some(opts) = options {
            for (k, v) in opts {
                let v_str = if let Some(s) = v.as_str() {
                    s.to_string()
                } else {
                    v.to_string()
                };
                println!("{}: {}", k, v_str);
            }
        }
    } else {
        println!("{}", text);
    }
}

async fn run_query(client: &reqwest::Client, base_url: &str, query_sql: &str) -> Result<()> {
    let session_id = format!(
        "query_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let res = client
        .post(format!("{}/ruletest", base_url))
        .json(&json!({
            "id": session_id,
            "sql": query_sql
        }))
        .send()
        .await?;
    let _ = handle_response(res).await?;

    let res = client
        .post(format!("{}/ruletest/{}/start", base_url, session_id))
        .send()
        .await?;
    let _ = handle_response(res).await?;

    let sse_res = client
        .get(format!("{}/test/{}", base_url, session_id))
        .header("Accept", "text/event-stream")
        .send()
        .await;

    if let Ok(mut resp) = sse_res {
        let read_future = async {
            while let Ok(Some(chunk)) = resp.chunk().await {
                let text = String::from_utf8_lossy(&chunk);
                for line in text.lines() {
                    if let Some(data) = line.strip_prefix("data: ") {
                        println!("{}", data);
                    } else if !line.is_empty() && !line.starts_with(':') {
                        println!("{}", line);
                    }
                }
            }
        };
        let _ = tokio::time::timeout(std::time::Duration::from_millis(1500), read_future).await;
    }

    let _ = client
        .delete(format!("{}/ruletest/{}", base_url, session_id))
        .send()
        .await;
    Ok(())
}

pub async fn run_cli() -> Result<()> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();
    let base_url = cli.url.trim_end_matches('/');
    let is_json_output = cli.json || cli.output.eq_ignore_ascii_case("json");

    match cli.command {
        Commands::Ping => {
            let res = client.get(format!("{}/ping", base_url)).send().await?;
            let text = handle_response(res).await?;
            println!("{}", text);
        }
        Commands::Create { entity_type, args } => {
            let (args, file) = split_file_flag(args);
            let inline = args.join(" ");
            let from_file = file.as_deref().map(read_def_file);
            if entity_type.eq_ignore_ascii_case("stream")
                || entity_type.eq_ignore_ascii_case("table")
            {
                let is_stream = entity_type.eq_ignore_ascii_case("stream");
                let content = from_file.unwrap_or(inline);
                if content.trim().is_empty() {
                    fail("Missing stream/table definition".to_string());
                }
                let keyword = if is_stream { "STREAM" } else { "TABLE" };
                let sql = if content.trim().to_ascii_uppercase().starts_with("CREATE") {
                    content
                } else {
                    format!("CREATE {} {}", keyword, content)
                };
                let endpoint = if is_stream { "streams" } else { "tables" };
                let res = client
                    .post(format!("{}/{}", base_url, endpoint))
                    .json(&json!({ "sql": sql }))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("rule") {
                let name = args.first().cloned().unwrap_or_default();
                let body: Value = if let Some(content) = from_file {
                    serde_json::from_str(&content).unwrap_or_else(|e| {
                        fail(format!("Rule file is not valid JSON: {}", e));
                    })
                } else if args.len() > 1 {
                    let sql_text = args[1..].join(" ");
                    match serde_json::from_str::<Value>(&sql_text) {
                        Ok(Value::Object(_)) => serde_json::from_str(&sql_text).unwrap(),
                        _ => {
                            let res = client
                                .post(format!("{}/rules", base_url))
                                .json(&json!({
                                    "id": name,
                                    "sql": sql_text,
                                    "actions": [{ "log": {} }]
                                }))
                                .send()
                                .await?;
                            let _ = handle_response(res).await?;
                            println!(
                                "Rule {} was created successfully, please use 'bin/kuiper getstatus rule {}' command to get rule status.",
                                name, name
                            );
                            return Ok(());
                        }
                    }
                } else {
                    fail("Missing rule definition".to_string());
                };
                let mut obj = body.as_object().cloned().unwrap_or_else(|| {
                    fail("Rule definition must be a JSON object".to_string());
                });
                obj.entry("id".to_string())
                    .or_insert_with(|| Value::String(name.clone()));
                let res = client
                    .post(format!("{}/rules", base_url))
                    .json(&Value::Object(obj))
                    .send()
                    .await?;
                let _ = handle_response(res).await?;
                println!(
                    "Rule {} was created successfully, please use 'bin/kuiper getstatus rule {}' command to get rule status.",
                    name, name
                );
            } else if entity_type.eq_ignore_ascii_case("script") {
                let content = from_file.unwrap_or(inline);
                let body: Value = serde_json::from_str(&content).unwrap_or_else(|e| {
                    fail(format!("Script definition is not valid JSON: {}", e))
                });
                let res = client
                    .post(format!("{}/udf/javascript", base_url))
                    .json(&body)
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("schema") {
                let kind = args.first().cloned().unwrap_or_else(|| {
                    fail("Usage: create schema <kind> <name> '<json>' | create schema <kind> -f <file>".to_string());
                });
                let body: Value = if let Some(content) = from_file {
                    if let Ok(mut parsed) = serde_json::from_str::<Value>(&content) {
                        if args.len() > 1 && parsed.is_object() {
                            if let Some(obj) = parsed.as_object_mut() {
                                obj.entry("name".to_string())
                                    .or_insert_with(|| Value::String(args[1].clone()));
                            }
                        }
                        parsed
                    } else {
                        let name = if args.len() > 1 {
                            args[1].clone()
                        } else {
                            "schema1".to_string()
                        };
                        json!({ "name": name, "content": content })
                    }
                } else if args.len() >= 3 {
                    let name = &args[1];
                    let text = args[2..].join(" ");
                    if let Ok(mut parsed) = serde_json::from_str::<Value>(&text) {
                        if let Some(obj) = parsed.as_object_mut() {
                            obj.entry("name".to_string())
                                .or_insert_with(|| Value::String(name.clone()));
                        }
                        parsed
                    } else {
                        json!({ "name": name, "content": text })
                    }
                } else if args.len() == 2 {
                    let text = &args[1];
                    serde_json::from_str(text).unwrap_or_else(|e| {
                        fail(format!("Schema definition is not valid JSON: {}", e))
                    })
                } else {
                    fail("Usage: create schema <kind> <name> '<json>' | create schema <kind> -f <file>".to_string());
                };
                let res = client
                    .post(format!("{}/schemas/{}", base_url, kind))
                    .json(&body)
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("service") {
                let content = from_file.unwrap_or(inline);
                let body: Value = serde_json::from_str(&content).unwrap_or_else(|e| {
                    fail(format!("Service definition is not valid JSON: {}", e))
                });
                let res = client
                    .post(format!("{}/services", base_url))
                    .json(&body)
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("plugin") {
                let kind_arg = args.first().cloned().unwrap_or_else(|| {
                    fail(
                        "Usage: create plugin <source|sink|function|portable> <name> -f <file>"
                            .to_string(),
                    );
                });
                let kind = normalize_plugin_kind(&kind_arg)
                    .unwrap_or_else(|| fail(format!("Unknown plugin kind: {}", kind_arg)));
                let content = from_file.unwrap_or_else(|| args[1..].join(" "));
                let body: Value = serde_json::from_str(&content).unwrap_or_else(|e| {
                    fail(format!("Plugin definition is not valid JSON: {}", e))
                });
                let res = client
                    .post(format!("{}/plugins/{}", base_url, kind))
                    .json(&body)
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Show { entity_type, extra } => {
            if entity_type.eq_ignore_ascii_case("streams")
                || entity_type.eq_ignore_ascii_case("stream")
            {
                let res = client.get(format!("{}/streams", base_url)).send().await?;
                let text = handle_response(res).await?;
                if is_json_output {
                    println!("{}", text);
                } else if let Ok(names) = serde_json::from_str::<Vec<String>>(&text) {
                    if names.is_empty() {
                        println!("No stream definitions are found.");
                    } else {
                        for name in names {
                            println!("{}", name);
                        }
                    }
                } else {
                    println!("{}", text);
                }
            } else if entity_type.eq_ignore_ascii_case("tables")
                || entity_type.eq_ignore_ascii_case("table")
            {
                let res = client.get(format!("{}/tables", base_url)).send().await?;
                let text = handle_response(res).await?;
                if is_json_output {
                    println!("{}", text);
                } else if let Ok(names) = serde_json::from_str::<Vec<String>>(&text) {
                    if names.is_empty() {
                        println!("No table definitions are found.");
                    } else {
                        for name in names {
                            println!("{}", name);
                        }
                    }
                } else {
                    println!("{}", text);
                }
            } else if entity_type.eq_ignore_ascii_case("rules")
                || entity_type.eq_ignore_ascii_case("rule")
            {
                let res = client.get(format!("{}/rules", base_url)).send().await?;
                let text = handle_response(res).await?;
                if let Ok(val) = serde_json::from_str::<Value>(&text) {
                    println!("{}", serde_json::to_string_pretty(&val).unwrap());
                } else {
                    println!("{}", text);
                }
            } else if entity_type.eq_ignore_ascii_case("scripts")
                || entity_type.eq_ignore_ascii_case("script")
            {
                let res = client
                    .get(format!("{}/udf/javascript", base_url))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("services")
                || entity_type.eq_ignore_ascii_case("service")
            {
                let res = client.get(format!("{}/services", base_url)).send().await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("service_funcs")
                || entity_type.eq_ignore_ascii_case("services_funcs")
            {
                let res = client
                    .get(format!("{}/services/functions", base_url))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("udfs")
                || entity_type.eq_ignore_ascii_case("udf")
            {
                let res = client
                    .get(format!("{}/plugins/udfs", base_url))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("plugins")
                || entity_type.eq_ignore_ascii_case("plugin")
            {
                let path = if let Some(kind) = extra {
                    let normalized = normalize_plugin_kind(&kind)
                        .unwrap_or_else(|| fail(format!("Unknown plugin kind: {}", kind)));
                    format!("plugins/{}", normalized)
                } else {
                    "plugins/sources".to_string()
                };
                let res = client.get(format!("{}/{}", base_url, path)).send().await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("schemas")
                || entity_type.eq_ignore_ascii_case("schema")
            {
                let path = if let Some(kind) = extra {
                    format!("schemas/{}", kind)
                } else {
                    "schemas".to_string()
                };
                let res = client.get(format!("{}/{}", base_url, path)).send().await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Describe {
            entity_type,
            name,
            extra,
        } => {
            let is_json = is_json_output
                || extra.as_deref() == Some("-json")
                || extra.as_deref() == Some("--json");
            if entity_type.eq_ignore_ascii_case("stream")
                || entity_type.eq_ignore_ascii_case("table")
                || entity_type.eq_ignore_ascii_case("tables")
            {
                let endpoint = if entity_type.eq_ignore_ascii_case("stream") {
                    format!("streams/{}", name)
                } else {
                    format!("tables/{}", name)
                };
                let res = client
                    .get(format!("{}/{}", base_url, endpoint))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                if is_json {
                    if let Ok(val) = serde_json::from_str::<Value>(&text) {
                        println!("{}", serde_json::to_string_pretty(&val).unwrap());
                    } else {
                        println!("{}", text);
                    }
                } else {
                    print_described_fields_and_options(&text);
                }
            } else if entity_type.eq_ignore_ascii_case("rule") {
                let res = client
                    .get(format!("{}/rules/{}", base_url, name))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                let val = serde_json::from_str::<Value>(&text);
                if let Ok(Value::Object(mut map)) = val {
                    if !map.contains_key("triggered") {
                        map.insert("triggered".to_string(), Value::Bool(false));
                    }
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&Value::Object(map)).unwrap()
                    );
                } else {
                    println!("{}", text);
                }
            } else if entity_type.eq_ignore_ascii_case("script") {
                let res = client
                    .get(format!("{}/udf/javascript/{}", base_url, name))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("service") {
                let res = client
                    .get(format!("{}/services/{}", base_url, name))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("schema") {
                let schema_name = extra.unwrap_or_else(|| {
                    fail("Usage: describe schema <kind> <name>".to_string());
                });
                let res = client
                    .get(format!("{}/schemas/{}/{}", base_url, name, schema_name))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                if let Ok(val) = serde_json::from_str::<Value>(&text) {
                    println!("{}", serde_json::to_string_pretty(&val).unwrap());
                } else {
                    println!("{}", text);
                }
            } else if entity_type.eq_ignore_ascii_case("plugin") {
                let plugin_name = extra.unwrap_or_else(|| {
                    fail(
                        "Usage: describe plugin <source|sink|function|portable> <name>".to_string(),
                    );
                });
                let kind = normalize_plugin_kind(&name)
                    .unwrap_or_else(|| fail(format!("Unknown plugin kind: {}", name)));
                let res = client
                    .get(format!("{}/plugins/{}/{}", base_url, kind, plugin_name))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else {
                fail(format!("Unknown describe entity: {}", entity_type));
            }
        }
        Commands::Explain { entity_type, name } => {
            if !entity_type.eq_ignore_ascii_case("rule") {
                fail(format!("Unknown explain entity: {}", entity_type));
            }
            let res = client
                .get(format!("{}/rules/{}/explain", base_url, name))
                .send()
                .await?;
            let text = handle_response(res).await?;
            if let Ok(val) = serde_json::from_str::<Value>(&text) {
                println!("{}", serde_json::to_string_pretty(&val).unwrap());
            } else {
                println!("{}", text);
            }
        }
        Commands::Query { sql } => {
            if sql.is_empty() {
                use std::io::{stdin, BufRead};
                println!("kuiper > ");
                let stdin_handle = stdin();
                for line in stdin_handle.lock().lines() {
                    let l = line?;
                    let trimmed = l.trim();
                    if trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("quit")
                    {
                        break;
                    }
                    if !trimmed.is_empty() {
                        run_query(&client, base_url, trimmed).await?;
                    }
                    println!("kuiper > ");
                }
            } else {
                let query_sql = sql.join(" ");
                run_query(&client, base_url, &query_sql).await?;
            }
        }
        Commands::Drop {
            entity_type,
            name,
            extra,
        } => {
            let endpoint = if entity_type.eq_ignore_ascii_case("stream") {
                format!("streams/{}", name)
            } else if entity_type.eq_ignore_ascii_case("rule") {
                format!("rules/{}", name)
            } else if entity_type.eq_ignore_ascii_case("table")
                || entity_type.eq_ignore_ascii_case("tables")
            {
                format!("tables/{}", name)
            } else if entity_type.eq_ignore_ascii_case("script")
                || entity_type.eq_ignore_ascii_case("scripts")
            {
                format!("udf/javascript/{}", name)
            } else if entity_type.eq_ignore_ascii_case("schema")
                || entity_type.eq_ignore_ascii_case("schemas")
            {
                let schema_name =
                    extra.unwrap_or_else(|| fail("Usage: drop schema <kind> <name>".to_string()));
                format!("schemas/{}/{}", name, schema_name)
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            };
            let res = client
                .delete(format!("{}/{}", base_url, endpoint))
                .send()
                .await?;
            let text = handle_response(res).await?;
            println!("{}", text);
        }
        Commands::Getstatus { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let rule = name.unwrap_or_else(|| fail("Usage: getstatus rule <name>".to_string()));
                let res = client
                    .get(format!("{}/rules/{}/status", base_url, rule))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else if entity_type.eq_ignore_ascii_case("import") {
                let res = client
                    .get(format!("{}/data/import/status", base_url))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Start { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let res = client
                    .post(format!("{}/rules/{}/start", base_url, name))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Stop { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let res = client
                    .post(format!("{}/rules/{}/stop", base_url, name))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Restart { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let res = client
                    .post(format!("{}/rules/{}/restart", base_url, name))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Gettopo { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let res = client
                    .get(format!("{}/rules/{}/topo", base_url, name))
                    .send()
                    .await?;
                let text = handle_response(res).await?;
                println!("{}", text);
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Validate { entity_type, args } => {
            let (args, file) = split_file_flag(args);
            if !entity_type.eq_ignore_ascii_case("rule") {
                fail(format!("Unknown entity type: {}", entity_type));
            }
            if args.is_empty() && file.is_none() {
                fail("Expect rule name and json.".to_string());
            }
            let body: Value = if let Some(path) = file.as_deref() {
                let content = read_def_file(path);
                let mut parsed: Value = serde_json::from_str(&content).unwrap_or_else(|e| {
                    fail(format!("Rule file is not valid JSON: {}", e));
                });
                if let Some(name) = args.first() {
                    if let Some(obj) = parsed.as_object_mut() {
                        obj.entry("id".to_string())
                            .or_insert_with(|| Value::String(name.clone()));
                    }
                }
                parsed
            } else if args.len() == 1 {
                serde_json::from_str(&args[0])
                    .unwrap_or_else(|e| fail(format!("Rule definition is not valid JSON: {}", e)))
            } else {
                let name = &args[0];
                let text = args[1..].join(" ");
                let mut parsed: Value = serde_json::from_str(&text)
                    .unwrap_or_else(|e| fail(format!("Rule definition is not valid JSON: {}", e)));
                if let Some(obj) = parsed.as_object_mut() {
                    obj.entry("id".to_string())
                        .or_insert_with(|| Value::String(name.clone()));
                }
                parsed
            };
            let res = client
                .post(format!("{}/rules/validate", base_url))
                .json(&body)
                .send()
                .await?;
            let text = handle_response(res).await?;
            println!("{}", text);
        }
        Commands::Export { arg1, arg2, rules } => {
            let (entity_type, file) = match arg2 {
                Some(f) => (arg1, f),
                None => ("data".to_string(), arg1),
            };
            let text = if entity_type.eq_ignore_ascii_case("ruleset") {
                let res = client
                    .get(format!("{}/ruleset/export", base_url))
                    .send()
                    .await?;
                handle_response(res).await?
            } else if entity_type.eq_ignore_ascii_case("data") {
                let mut body = json!({});
                if let Some(list) = rules.as_deref() {
                    let parsed: Value = serde_json::from_str(list).unwrap_or_else(|e| {
                        fail(format!("Rules filter is not valid JSON: {}", e));
                    });
                    body = json!({ "rules": parsed });
                }
                let res = client
                    .post(format!("{}/data/export", base_url))
                    .json(&body)
                    .send()
                    .await?;
                handle_response(res).await?
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            };
            std::fs::write(&file, &text)
                .with_context(|| format!("Failed to write {}", file))
                .unwrap_or_else(|e| fail(format!("{:?}", e)));
            println!("{}", text);
        }
        Commands::Import { entity_type, args } => {
            let (args, file) = split_file_flag(args);
            let (target_entity, target_file) = if entity_type.eq_ignore_ascii_case("ruleset") {
                let f = file
                    .or_else(|| args.first().cloned())
                    .unwrap_or_else(|| fail("Usage: import ruleset -f <file>".to_string()));
                ("ruleset", f)
            } else if entity_type.eq_ignore_ascii_case("data") {
                let f = file
                    .or_else(|| args.first().cloned())
                    .unwrap_or_else(|| fail("Usage: import data -f <file>".to_string()));
                ("data", f)
            } else {
                let f = file.unwrap_or(entity_type);
                ("data", f)
            };
            let content = read_def_file(&target_file);
            let body: Value = serde_json::from_str(&content)
                .unwrap_or_else(|e| fail(format!("Import file is not valid JSON: {}", e)));
            let endpoint = if target_entity.eq_ignore_ascii_case("ruleset") {
                "ruleset/import"
            } else {
                "data/import"
            };
            let res = client
                .post(format!("{}/{}", base_url, endpoint))
                .json(&body)
                .send()
                .await?;
            let text = handle_response(res).await?;
            println!("{}", text);
        }
    }

    Ok(())
}
