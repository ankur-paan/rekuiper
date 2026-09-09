use anyhow::Result;
use clap::Parser;
use rekuiper_conf::PathConfig;

#[derive(Parser, Debug)]
#[command(name = "kuiperd")]
#[command(about = "eKuiper Daemon - Drop-in replacement server in Rust", long_about = None)]
struct Args {
    /// Path of etc dir
    #[arg(long, default_value = "etc")]
    etc: String,

    /// Path of data dir
    #[arg(long, default_value = "data")]
    data: String,

    /// Path of log dir
    #[arg(long, default_value = "log")]
    log: String,

    /// How to load path
    #[arg(long, default_value = "relative")]
    load_file_type: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let paths = PathConfig::new(Some(&args.etc), Some(&args.data), Some(&args.log));
    let config = paths.load_config()?;

    let version = env!("CARGO_PKG_VERSION").to_string();
    rekuiper_server::start_server(config, version).await?;

    Ok(())
}
