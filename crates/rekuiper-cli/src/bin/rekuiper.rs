use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    rekuiper_cli::run_cli().await
}
