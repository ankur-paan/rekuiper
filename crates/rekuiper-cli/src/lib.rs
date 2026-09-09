use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "kuiper")]
#[command(about = "eKuiper CLI - Drop-in replacement client", long_about = None)]
pub struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:9081")]
    pub url: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum Commands {
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
