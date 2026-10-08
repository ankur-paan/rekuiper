use clap::Parser;
use rekuiper_cli::{split_file_flag, Cli, Commands};

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
fn test_cli_create_from_file() {
    // Documented `-f` file forms ride along in trailing args and are
    // extracted position-independently by `split_file_flag`.
    let cli = parse(&["kuiper", "create", "stream", "-f", "/tmp/cli/stream.txt"]);
    match cli.command {
        Commands::Create { entity_type, args } => {
            assert_eq!(entity_type, "stream");
            let (rest, file) = split_file_flag(args);
            assert!(rest.is_empty());
            assert_eq!(file.as_deref(), Some("/tmp/cli/stream.txt"));
        }
        other => panic!("unexpected command: {:?}", other),
    }
    let cli = parse(&["kuiper", "create", "rule", "r1", "-f", "/tmp/cli/rule.txt"]);
    match cli.command {
        Commands::Create { entity_type, args } => {
            assert_eq!(entity_type, "rule");
            let (rest, file) = split_file_flag(args);
            assert_eq!(rest, vec!["r1".to_string()]);
            assert_eq!(file.as_deref(), Some("/tmp/cli/rule.txt"));
        }
        other => panic!("unexpected command: {:?}", other),
    }
    // Long form and non-flag args pass through untouched.
    let (rest, file) =
        split_file_flag(vec!["a".to_string(), "--file".to_string(), "b".to_string()]);
    assert_eq!(rest, vec!["a".to_string()]);
    assert_eq!(file.as_deref(), Some("b"));
    let (rest, file) = split_file_flag(vec!["SELECT 1".to_string()]);
    assert_eq!(rest, vec!["SELECT 1".to_string()]);
    assert_eq!(file, None);
}

#[test]
fn test_cli_show_variants() {
    for entity in ["streams", "rules", "tables"] {
        let cli = parse(&["kuiper", "show", entity]);
        match cli.command {
            Commands::Show { entity_type, .. } => assert_eq!(entity_type, entity),
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
            assert_eq!(name.as_deref(), Some("rule1"));
        }
        other => panic!("unexpected command: {:?}", other),
    }
    // Bare `getstatus import` (no NAME) matches the baseline usage.
    let cli = parse(&["kuiper", "getstatus", "import"]);
    match cli.command {
        Commands::Getstatus { entity_type, name } => {
            assert_eq!(entity_type, "import");
            assert_eq!(name, None);
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
        Commands::Drop {
            entity_type, name, ..
        } => {
            assert_eq!(entity_type, "stream");
            assert_eq!(name, "demo");
        }
        other => panic!("unexpected command: {:?}", other),
    }

    let cli = parse(&["kuiper", "drop", "rule", "rule1"]);
    match cli.command {
        Commands::Drop {
            entity_type, name, ..
        } => {
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

#[test]
fn test_cli_parity_surface() {
    // describe rule/script/service/schema/plugin + gettopo/validate.
    let cli = parse(&["kuiper", "describe", "rule", "r1"]);
    match cli.command {
        Commands::Describe {
            entity_type,
            name,
            extra,
        } => {
            assert_eq!(entity_type, "rule");
            assert_eq!(name, "r1");
            assert_eq!(extra, None);
        }
        other => panic!("unexpected command: {:?}", other),
    }
    let cli = parse(&["kuiper", "describe", "schema", "protobuf", "s1"]);
    match cli.command {
        Commands::Describe {
            entity_type,
            name,
            extra,
        } => {
            assert_eq!(entity_type, "schema");
            assert_eq!(name, "protobuf");
            assert_eq!(extra.as_deref(), Some("s1"));
        }
        other => panic!("unexpected command: {:?}", other),
    }
    let cli = parse(&["kuiper", "gettopo", "rule", "r1"]);
    assert!(matches!(
        cli.command,
        Commands::Gettopo {
            entity_type: _,
            name: _
        }
    ));
    let cli = parse(&[
        "kuiper",
        "validate",
        "rule",
        "r1",
        "-f",
        "/tmp/cli/rule.txt",
    ]);
    match cli.command {
        Commands::Validate {
            entity_type, args, ..
        } => {
            assert_eq!(entity_type, "rule");
            let (_, file) = split_file_flag(args);
            assert_eq!(file.as_deref(), Some("/tmp/cli/rule.txt"));
        }
        other => panic!("unexpected command: {:?}", other),
    }
    let cli = parse(&["kuiper", "export", "ruleset", "out.json"]);
    assert!(matches!(cli.command, Commands::Export { .. }));
    let cli = parse(&["kuiper", "import", "data", "-f", "in.json"]);
    match cli.command {
        Commands::Import { entity_type, args } => {
            assert_eq!(entity_type, "data");
            let (_, file) = split_file_flag(args);
            assert_eq!(file.as_deref(), Some("in.json"));
        }
        other => panic!("unexpected command: {:?}", other),
    }
}

#[test]
fn test_cli_issue31_subcommands_and_defaults() {
    // 1. explain rule <name>
    let cli = parse(&["kuiper", "explain", "rule", "rule1"]);
    match cli.command {
        Commands::Explain { entity_type, name } => {
            assert_eq!(entity_type, "rule");
            assert_eq!(name, "rule1");
        }
        other => panic!("unexpected command: {:?}", other),
    }

    // 2. query subcommand
    let cli = parse(&["kuiper", "query", "SELECT", "*", "FROM", "demo"]);
    match cli.command {
        Commands::Query { sql } => {
            assert_eq!(sql, vec!["SELECT", "*", "FROM", "demo"]);
        }
        other => panic!("unexpected command: {:?}", other),
    }
    let cli = parse(&["kuiper", "query"]);
    match cli.command {
        Commands::Query { sql } => {
            assert!(sql.is_empty());
        }
        other => panic!("unexpected command: {:?}", other),
    }

    // 3. show plugins <kind>, show schemas <kind>, show udfs, show service_funcs
    let cli = parse(&["kuiper", "show", "plugins", "function"]);
    match cli.command {
        Commands::Show { entity_type, extra } => {
            assert_eq!(entity_type, "plugins");
            assert_eq!(extra.as_deref(), Some("function"));
        }
        other => panic!("unexpected command: {:?}", other),
    }
    let cli = parse(&["kuiper", "show", "schemas", "protobuf"]);
    match cli.command {
        Commands::Show { entity_type, extra } => {
            assert_eq!(entity_type, "schemas");
            assert_eq!(extra.as_deref(), Some("protobuf"));
        }
        other => panic!("unexpected command: {:?}", other),
    }
    let cli = parse(&["kuiper", "show", "udfs"]);
    match cli.command {
        Commands::Show { entity_type, extra } => {
            assert_eq!(entity_type, "udfs");
            assert_eq!(extra, None);
        }
        other => panic!("unexpected command: {:?}", other),
    }
    let cli = parse(&["kuiper", "show", "service_funcs"]);
    match cli.command {
        Commands::Show { entity_type, extra } => {
            assert_eq!(entity_type, "service_funcs");
            assert_eq!(extra, None);
        }
        other => panic!("unexpected command: {:?}", other),
    }

    // 4. drop schema <kind> <name>, drop script <id>
    let cli = parse(&["kuiper", "drop", "schema", "protobuf", "book"]);
    match cli.command {
        Commands::Drop {
            entity_type,
            name,
            extra,
        } => {
            assert_eq!(entity_type, "schema");
            assert_eq!(name, "protobuf");
            assert_eq!(extra.as_deref(), Some("book"));
        }
        other => panic!("unexpected command: {:?}", other),
    }
    let cli = parse(&["kuiper", "drop", "script", "s1"]);
    match cli.command {
        Commands::Drop {
            entity_type,
            name,
            extra,
        } => {
            assert_eq!(entity_type, "script");
            assert_eq!(name, "s1");
            assert_eq!(extra, None);
        }
        other => panic!("unexpected command: {:?}", other),
    }

    // 5. create schema <kind> <name> '<json>'
    let cli = parse(&[
        "kuiper",
        "create",
        "schema",
        "protobuf",
        "book",
        "{\"content\":\"test\"}",
    ]);
    match cli.command {
        Commands::Create { entity_type, args } => {
            assert_eq!(entity_type, "schema");
            assert_eq!(args, vec!["protobuf", "book", "{\"content\":\"test\"}"]);
        }
        other => panic!("unexpected command: {:?}", other),
    }

    // 6. validate rule '<json>' without explicit name
    let cli = parse(&[
        "kuiper",
        "validate",
        "rule",
        "{\"id\":\"r1\",\"sql\":\"SELECT 1\"}",
    ]);
    match cli.command {
        Commands::Validate { entity_type, args } => {
            assert_eq!(entity_type, "rule");
            assert_eq!(args, vec!["{\"id\":\"r1\",\"sql\":\"SELECT 1\"}"]);
        }
        other => panic!("unexpected command: {:?}", other),
    }

    // 7. export /path/to/file.json without entity type
    let cli = parse(&["kuiper", "export", "/tmp/x.json"]);
    match cli.command {
        Commands::Export { arg1, arg2, .. } => {
            assert_eq!(arg1, "/tmp/x.json");
            assert_eq!(arg2, None);
        }
        other => panic!("unexpected command: {:?}", other),
    }

    // 8. import /path/to/file.json without entity type
    let cli = parse(&["kuiper", "import", "/tmp/x.json"]);
    match cli.command {
        Commands::Import { entity_type, args } => {
            assert_eq!(entity_type, "/tmp/x.json");
            assert!(args.is_empty());
        }
        other => panic!("unexpected command: {:?}", other),
    }

    // 9. flags: -o json, --json
    let cli = parse(&["kuiper", "-o", "json", "show", "streams"]);
    assert_eq!(cli.output, "json");
    let cli = parse(&["kuiper", "--json", "show", "streams"]);
    assert!(cli.json);
}
