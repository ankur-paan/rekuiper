use clap::Parser;
use rekuiper_cli::{Cli, Commands};

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(args).expect("CLI args should parse")
}

#[test]
fn test_cli_ping() {
    let cli = parse(&["kuiper", "ping"]);
    assert_eq!(cli.url, "http://127.0.0.1:9081");
    assert!(matches!(cli.command, Commands::Ping));
}

#[test]
fn test_cli_create_stream() {
    let cli = parse(&["kuiper", "create", "stream", "demo", "()", "WITH", "(...)"]);
    match cli.command {
        Commands::Create { entity_type, args } => {
            assert_eq!(entity_type, "stream");
            assert_eq!(args, vec!["demo", "()", "WITH", "(...)"]);
        }
        other => panic!("unexpected command: {:?}", other),
    }
}

#[test]
fn test_cli_create_rule() {
    let cli = parse(&["kuiper", "create", "rule", "rule1", "SELECT * FROM demo"]);
    match cli.command {
        Commands::Create { entity_type, args } => {
            assert_eq!(entity_type, "rule");
            assert_eq!(args, vec!["rule1", "SELECT * FROM demo"]);
        }
        other => panic!("unexpected command: {:?}", other),
    }
}

#[test]
fn test_cli_show_variants() {
    for entity in ["streams", "rules", "tables"] {
        let cli = parse(&["kuiper", "show", entity]);
        match cli.command {
            Commands::Show { entity_type } => assert_eq!(entity_type, entity),
            other => panic!("unexpected command: {:?}", other),
        }
    }
}

#[test]
fn test_cli_getstatus() {
    let cli = parse(&["kuiper", "getstatus", "rule", "rule1"]);
    match cli.command {
        Commands::Getstatus { entity_type, name } => {
            assert_eq!(entity_type, "rule");
            assert_eq!(name, "rule1");
        }
        other => panic!("unexpected command: {:?}", other),
    }
}

#[test]
fn test_cli_rule_lifecycle_commands() {
    let cli = parse(&["kuiper", "start", "rule", "rule1"]);
    match cli.command {
        Commands::Start { entity_type, name } => {
            assert_eq!(entity_type, "rule");
            assert_eq!(name, "rule1");
        }
        other => panic!("unexpected command: {:?}", other),
    }

    let cli = parse(&["kuiper", "stop", "rule", "rule1"]);
    match cli.command {
        Commands::Stop { entity_type, name } => {
            assert_eq!(entity_type, "rule");
            assert_eq!(name, "rule1");
        }
        other => panic!("unexpected command: {:?}", other),
    }

    let cli = parse(&["kuiper", "restart", "rule", "rule1"]);
    match cli.command {
        Commands::Restart { entity_type, name } => {
            assert_eq!(entity_type, "rule");
            assert_eq!(name, "rule1");
        }
        other => panic!("unexpected command: {:?}", other),
    }
}

#[test]
fn test_cli_drop_commands() {
    let cli = parse(&["kuiper", "drop", "stream", "demo"]);
    match cli.command {
        Commands::Drop { entity_type, name } => {
            assert_eq!(entity_type, "stream");
            assert_eq!(name, "demo");
        }
        other => panic!("unexpected command: {:?}", other),
    }

    let cli = parse(&["kuiper", "drop", "rule", "rule1"]);
    match cli.command {
        Commands::Drop { entity_type, name } => {
            assert_eq!(entity_type, "rule");
            assert_eq!(name, "rule1");
        }
        other => panic!("unexpected command: {:?}", other),
    }
}

#[test]
fn test_cli_custom_url() {
    let cli = parse(&["kuiper", "--url", "http://example:9999", "ping"]);
    assert_eq!(cli.url, "http://example:9999");
    assert!(matches!(cli.command, Commands::Ping));
}
