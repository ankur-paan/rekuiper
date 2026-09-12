use anyhow::{Context, Result};
use clap::Parser;
use rekuiper_cli::{split_file_flag, Cli, Commands};
use serde_json::{json, Value};

fn fail(msg: String) -> ! {
    eprintln!("{}", msg);
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();
    let base_url = cli.url.trim_end_matches('/');

    async fn print_response(req: reqwest::RequestBuilder) -> Result<()> {
        let res = req.send().await?;
        println!("{}", res.text().await?);
        Ok(())
    }

    match cli.command {
        Commands::Ping => {
            let res = client.get(format!("{}/ping", base_url)).send().await?;
            println!("{}", res.text().await?);
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
                print_response(
                    client
                        .post(format!("{}/{}", base_url, endpoint))
                        .json(&json!({ "sql": sql })),
                )
                .await?;
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
                            print_response(client.post(format!("{}/rules", base_url)).json(
                                &json!({
                                    "id": name,
                                    "sql": sql_text,
                                    "actions": [{ "log": {} }]
                                }),
                            ))
                            .await?;
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
                    .or_insert_with(|| Value::String(name));
                print_response(
                    client
                        .post(format!("{}/rules", base_url))
                        .json(&Value::Object(obj)),
                )
                .await?;
            } else if entity_type.eq_ignore_ascii_case("script") {
                let content = from_file.unwrap_or(inline);
                let body: Value = serde_json::from_str(&content).unwrap_or_else(|e| {
                    fail(format!("Script definition is not valid JSON: {}", e))
                });
                print_response(
                    client
                        .post(format!("{}/udf/javascript", base_url))
                        .json(&body),
                )
                .await?;
            } else if entity_type.eq_ignore_ascii_case("schema") {
                let kind = args.first().cloned().unwrap_or_else(|| {
                    fail("Usage: create schema <kind> -f <file>".to_string());
                });
                let content = from_file.unwrap_or_else(|| {
                    fail("Usage: create schema <kind> -f <file>".to_string());
                });
                let body: Value = serde_json::from_str(&content)
                    .unwrap_or_else(|e| fail(format!("Schema file is not valid JSON: {}", e)));
                print_response(
                    client
                        .post(format!("{}/schemas/{}", base_url, kind))
                        .json(&body),
                )
                .await?;
            } else if entity_type.eq_ignore_ascii_case("service") {
                let content = from_file.unwrap_or(inline);
                let body: Value = serde_json::from_str(&content).unwrap_or_else(|e| {
                    fail(format!("Service definition is not valid JSON: {}", e))
                });
                print_response(client.post(format!("{}/services", base_url)).json(&body)).await?;
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
                print_response(
                    client
                        .post(format!("{}/plugins/{}", base_url, kind))
                        .json(&body),
                )
                .await?;
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Show { entity_type } => {
            let endpoint = if entity_type.eq_ignore_ascii_case("streams") {
                "streams"
            } else if entity_type.eq_ignore_ascii_case("rules") {
                "rules"
            } else if entity_type.eq_ignore_ascii_case("tables")
                || entity_type.eq_ignore_ascii_case("table")
            {
                "tables"
            } else if entity_type.eq_ignore_ascii_case("scripts")
                || entity_type.eq_ignore_ascii_case("script")
            {
                "udf/javascript"
            } else if entity_type.eq_ignore_ascii_case("services")
                || entity_type.eq_ignore_ascii_case("service")
            {
                "services"
            } else if entity_type.eq_ignore_ascii_case("plugins")
                || entity_type.eq_ignore_ascii_case("plugin")
            {
                "plugins/sources"
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            };
            print_response(client.get(format!("{}/{}", base_url, endpoint))).await?;
        }
        Commands::Describe {
            entity_type,
            name,
            extra,
        } => {
            let path = if entity_type.eq_ignore_ascii_case("stream") {
                format!("streams/{}", name)
            } else if entity_type.eq_ignore_ascii_case("table")
                || entity_type.eq_ignore_ascii_case("tables")
            {
                format!("tables/{}", name)
            } else if entity_type.eq_ignore_ascii_case("rule") {
                format!("rules/{}", name)
            } else if entity_type.eq_ignore_ascii_case("script") {
                format!("udf/javascript/{}", name)
            } else if entity_type.eq_ignore_ascii_case("service") {
                format!("services/{}", name)
            } else if entity_type.eq_ignore_ascii_case("schema") {
                let schema_name = extra.unwrap_or_else(|| {
                    fail("Usage: describe schema <kind> <name>".to_string());
                });
                format!("schemas/{}/{}", name, schema_name)
            } else if entity_type.eq_ignore_ascii_case("plugin") {
                let plugin_name = extra.unwrap_or_else(|| {
                    fail(
                        "Usage: describe plugin <source|sink|function|portable> <name>".to_string(),
                    );
                });
                let kind = normalize_plugin_kind(&name)
                    .unwrap_or_else(|| fail(format!("Unknown plugin kind: {}", name)));
                format!("plugins/{}/{}", kind, plugin_name)
            } else {
                fail(format!("Unknown describe entity: {}", entity_type));
            };
            print_response(client.get(format!("{}/{}", base_url, path))).await?;
        }
        Commands::Drop { entity_type, name } => {
            let endpoint = if entity_type.eq_ignore_ascii_case("stream") {
                "streams"
            } else if entity_type.eq_ignore_ascii_case("rule") {
                "rules"
            } else if entity_type.eq_ignore_ascii_case("table")
                || entity_type.eq_ignore_ascii_case("tables")
            {
                "tables"
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            };
            print_response(client.delete(format!("{}/{}/{}", base_url, endpoint, name))).await?;
        }
        Commands::Getstatus { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let rule = name.unwrap_or_else(|| fail("Usage: getstatus rule <name>".to_string()));
                print_response(client.get(format!("{}/rules/{}/status", base_url, rule))).await?;
            } else if entity_type.eq_ignore_ascii_case("import") {
                print_response(client.get(format!("{}/data/import/status", base_url))).await?;
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Start { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                print_response(client.post(format!("{}/rules/{}/start", base_url, name))).await?;
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Stop { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                print_response(client.post(format!("{}/rules/{}/stop", base_url, name))).await?;
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Restart { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                print_response(client.post(format!("{}/rules/{}/restart", base_url, name))).await?;
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Gettopo { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                print_response(client.get(format!("{}/rules/{}/topo", base_url, name))).await?;
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            }
        }
        Commands::Validate {
            entity_type,
            name,
            args,
        } => {
            let (args, file) = split_file_flag(args);
            if !entity_type.eq_ignore_ascii_case("rule") {
                fail(format!("Unknown entity type: {}", entity_type));
            }
            let body: Value = if let Some(path) = file.as_deref() {
                serde_json::from_str(&read_def_file(path)).unwrap_or_else(|e| {
                    fail(format!("Rule file is not valid JSON: {}", e));
                })
            } else if !args.is_empty() {
                let text = args.join(" ");
                serde_json::from_str(&text)
                    .unwrap_or_else(|e| fail(format!("Rule definition is not valid JSON: {}", e)))
            } else {
                fail(
                    "Usage: validate rule <name> '<json>' | validate rule <name> -f <file>"
                        .to_string(),
                );
            };
            let mut obj = body.as_object().cloned().unwrap_or_else(|| {
                fail("Rule definition must be a JSON object".to_string());
            });
            obj.entry("id".to_string())
                .or_insert_with(|| Value::String(name));
            print_response(
                client
                    .post(format!("{}/rules/validate", base_url))
                    .json(&Value::Object(obj)),
            )
            .await?;
        }
        Commands::Export {
            entity_type,
            file,
            rules,
        } => {
            let text = if entity_type.eq_ignore_ascii_case("ruleset") {
                let res = client
                    .get(format!("{}/ruleset/export", base_url))
                    .send()
                    .await?;
                res.text().await?
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
                res.text().await?
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            };
            std::fs::write(&file, &text)
                .with_context(|| format!("Failed to write {}", file))
                .unwrap_or_else(|e| fail(format!("{:?}", e)));
            println!("{}", text);
        }
        Commands::Import { entity_type, args } => {
            let (_, file) = split_file_flag(args);
            let path = file.as_deref().unwrap_or_else(|| {
                fail("Usage: import <ruleset|data> -f <file>".to_string());
            });
            let content = read_def_file(path);
            let body: Value = serde_json::from_str(&content)
                .unwrap_or_else(|e| fail(format!("Import file is not valid JSON: {}", e)));
            let endpoint = if entity_type.eq_ignore_ascii_case("ruleset") {
                "ruleset/import"
            } else if entity_type.eq_ignore_ascii_case("data") {
                "data/import"
            } else {
                fail(format!("Unknown entity type: {}", entity_type));
            };
            print_response(
                client
                    .post(format!("{}/{}", base_url, endpoint))
                    .json(&body),
            )
            .await?;
        }
    }

    Ok(())
}
