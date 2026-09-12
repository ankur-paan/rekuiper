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
    /// Create a stream, table, rule, script, schema, service or plugin
    /// (`-f <file>` reads the definition from a file, eKuiper parity)
    Create {
        entity_type: String,
        #[arg(trailing_var_arg = true, num_args = 0.., allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Show streams, rules, tables, scripts, schemas or services
    Show { entity_type: String },
    /// Describe a stream, table, rule, script, service, schema or plugin
    /// (`describe schema <kind> <name>`, `describe plugin <kind> <name>`)
    Describe {
        entity_type: String,
        name: String,
        extra: Option<String>,
    },
    /// Drop a stream, table or rule
    Drop { entity_type: String, name: String },
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
        name: String,
        #[arg(trailing_var_arg = true, num_args = 0.., allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Export a ruleset or data snapshot to a file
    Export {
        entity_type: String,
        file: String,
        /// Restrict a data export to specific rules (`-r '["r1"]'`)
        #[arg(short = 'r', long = "rules")]
        rules: Option<String>,
    },
    /// Import a ruleset or data snapshot from a file (`-f <file>`)
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
