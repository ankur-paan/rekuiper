use anyhow::Result;
use clap::Parser;
use rekuiper_conf::PathConfig;

#[derive(Parser, Debug)]
#[command(name = "rekuiperd")]
#[command(about = "rekuiper Daemon - High-performance edge stream processing engine in Rust (kuiperd drop-in)", long_about = None)]
pub struct Args {
    /// Path of etc dir
    #[arg(long, default_value = "etc")]
    pub etc: String,

    /// Path of data dir
    #[arg(long, default_value = "data")]
    pub data: String,

    /// Path of log dir
    #[arg(long, default_value = "log")]
    pub log: String,

    /// How to load path
    #[arg(long, default_value = "relative")]
    pub load_file_type: String,
}

pub async fn run_daemon() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let paths = PathConfig::new(Some(&args.etc), Some(&args.data), Some(&args.log));
    let config = paths.load_config()?;

    let version = env!("CARGO_PKG_VERSION").to_string();
    rekuiper_server::start_server(config, version).await?;

    Ok(())
}
