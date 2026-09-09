use anyhow::Result;
use clap::{Parser, Subcommand};
use serde_json::json;

#[derive(Parser)]
#[command(name = "kuiper")]
#[command(about = "eKuiper CLI - Drop-in replacement client", long_about = None)]
struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:9081")]
    url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Ping kuiper server
    Ping,
    /// Create a stream or rule
    Create {
        entity_type: String,
        #[arg(trailing_var_arg = true, num_args = 1..)]
        args: Vec<String>,
    },
    /// Show streams or rules
    Show { entity_type: String },
    /// Describe a stream
    Describe { entity_type: String, name: String },
    /// Drop a stream or rule
    Drop { entity_type: String, name: String },
    /// Get status of a rule
    Getstatus { entity_type: String, name: String },
    /// Start a rule
    Start { entity_type: String, name: String },
    /// Stop a rule
    Stop { entity_type: String, name: String },
    /// Restart a rule
    Restart { entity_type: String, name: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();
    let base_url = cli.url.trim_end_matches('/');

    match cli.command {
        Commands::Ping => {
            let res = client.get(format!("{}/ping", base_url)).send().await?;
            println!("{}", res.text().await?);
        }
        Commands::Create { entity_type, args } => {
            if entity_type.eq_ignore_ascii_case("stream") {
                let full_cmd = args.join(" ");
                let sql = if full_cmd.to_uppercase().starts_with("CREATE STREAM") {
                    full_cmd
                } else if args.len() >= 2 {
                    args[1..].join(" ")
                } else {
                    full_cmd
                };
                let payload = json!({ "sql": sql });
                let res = client
                    .post(format!("{}/streams", base_url))
                    .json(&payload)
                    .send()
                    .await?;
                println!("{}", res.text().await?);
            } else if entity_type.eq_ignore_ascii_case("rule") {
                let id = args.first().cloned().unwrap_or_default();
                let sql = if args.len() > 1 {
                    args[1..].join(" ")
                } else {
                    "".to_string()
                };
                let payload = json!({
                    "id": id,
                    "sql": sql,
                    "actions": [{ "log": {} }]
                });
                let res = client
                    .post(format!("{}/rules", base_url))
                    .json(&payload)
                    .send()
                    .await?;
                println!("{}", res.text().await?);
            } else {
                eprintln!("Unknown entity type: {}", entity_type);
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
            } else {
                eprintln!("Unknown entity type: {}", entity_type);
                return Ok(());
            };
            let res = client
                .get(format!("{}/{}", base_url, endpoint))
                .send()
                .await?;
            println!("{}", res.text().await?);
        }
        Commands::Describe { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("stream") {
                let res = client
                    .get(format!("{}/streams/{}", base_url, name))
                    .send()
                    .await?;
                println!("{}", res.text().await?);
            } else if entity_type.eq_ignore_ascii_case("table")
                || entity_type.eq_ignore_ascii_case("tables")
            {
                let res = client
                    .get(format!("{}/tables/{}", base_url, name))
                    .send()
                    .await?;
                println!("{}", res.text().await?);
            } else {
                eprintln!("Unknown describe entity: {}", entity_type);
            }
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
                eprintln!("Unknown entity type: {}", entity_type);
                return Ok(());
            };
            let res = client
                .delete(format!("{}/{}/{}", base_url, endpoint, name))
                .send()
                .await?;
            println!("{}", res.text().await?);
        }
        Commands::Getstatus { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let res = client
                    .get(format!("{}/rules/{}/status", base_url, name))
                    .send()
                    .await?;
                println!("{}", res.text().await?);
            } else {
                eprintln!("Unknown entity type: {}", entity_type);
            }
        }
        Commands::Start { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let res = client
                    .post(format!("{}/rules/{}/start", base_url, name))
                    .send()
                    .await?;
                println!("{}", res.text().await?);
            } else {
                eprintln!("Unknown entity type: {}", entity_type);
            }
        }
        Commands::Stop { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let res = client
                    .post(format!("{}/rules/{}/stop", base_url, name))
                    .send()
                    .await?;
                println!("{}", res.text().await?);
            } else {
                eprintln!("Unknown entity type: {}", entity_type);
            }
        }
        Commands::Restart { entity_type, name } => {
            if entity_type.eq_ignore_ascii_case("rule") {
                let res = client
                    .post(format!("{}/rules/{}/restart", base_url, name))
                    .send()
                    .await?;
                println!("{}", res.text().await?);
            } else {
                eprintln!("Unknown entity type: {}", entity_type);
            }
        }
    }

    Ok(())
}
