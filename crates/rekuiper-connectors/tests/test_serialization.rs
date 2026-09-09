use rekuiper_connectors::{parse_proto, BinaryCodec, DelimitedCodec, ProtobufCodec};
use rekuiper_core::SchemaManager;
use serde_json::json;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Binary codec
// ---------------------------------------------------------------------------

#[test]
fn test_binary_roundtrip() {
    let raw = b"hello rekuiper \x00\xff binary";
    let encoded = BinaryCodec::decode(raw);
    assert_eq!(
        encoded.get("_binary"),
        Some(&json!("aGVsbG8gcmVrdWlwZXIgAP8gYmluYXJ5"))
    );
    assert_eq!(BinaryCodec::encode(&encoded), raw.to_vec());

    // Strings pass through as UTF-8 bytes.
    assert_eq!(BinaryCodec::encode(&json!("ping")), b"ping".to_vec());
    // Plain JSON values round-trip through their serialized form.
    let val = json!({"temp": 25});
    assert_eq!(BinaryCodec::encode(&val), serde_json::to_vec(&val).unwrap());
    // Malformed base64 degrades to empty rather than panicking.
    assert_eq!(
        BinaryCodec::encode(&json!({"_binary": "!!!not-base64!!!"})),
        Vec::<u8>::new()
    );
}

// ---------------------------------------------------------------------------
// Delimited codec
// ---------------------------------------------------------------------------

#[test]
fn test_delimited_csv_roundtrip() {
    let codec = DelimitedCodec::new(',', vec!["temp".to_string(), "id".to_string()]);
    let mut record = HashMap::new();
    record.insert("temp".to_string(), json!(25.5));
    record.insert("id".to_string(), json!("d1"));
    assert_eq!(codec.encode(&record), "25.5,d1");

    let decoded = codec.decode("22.5,ok");
    assert_eq!(decoded.get("temp"), Some(&json!(22.5)));
    assert_eq!(decoded.get("id"), Some(&json!("ok")));
}

#[test]
fn test_delimited_tsv_and_headers() {
    // TSV without headers configured behaves positionally.
    let codec = DelimitedCodec::new('\t', vec!["a".to_string(), "b".to_string()]);
    assert_eq!(codec.encode(&HashMap::new()), "\t");
    let decoded = codec.decode("1\t2");
    assert_eq!(decoded.get("a"), Some(&json!(1)));
    assert_eq!(decoded.get("b"), Some(&json!(2)));

    // Custom pipe headers with quoting.
    let codec = DelimitedCodec::new(
        DelimitedCodec::delimiter_from_name("pipe"),
        vec!["x".to_string(), "y".to_string()],
    );
    let mut record = HashMap::new();
    record.insert("x".to_string(), json!("a|b"));
    record.insert("y".to_string(), json!(1));
    assert_eq!(codec.encode(&record), "\"a|b\"|1");
    let decoded = codec.decode("\"a|b\"|1");
    assert_eq!(decoded.get("x"), Some(&json!("a|b")));
    assert_eq!(decoded.get("y"), Some(&json!(1)));

    // Delimiter name aliases.
    assert_eq!(DelimitedCodec::delimiter_from_name("comma"), ',');
    assert_eq!(DelimitedCodec::delimiter_from_name("tab"), '\t');
    assert_eq!(DelimitedCodec::delimiter_from_name(";"), ';');
}

// ---------------------------------------------------------------------------
// Protobuf codec
// ---------------------------------------------------------------------------

const BOOK_PROTO: &str = r#"
syntax = "proto3";
package demo;

// A book in the catalog.
message Book {
  string title = 1;
  int32 price = 2;
}
"#;

fn book_message() -> rekuiper_connectors::ProtoMessage {
    let messages = parse_proto(BOOK_PROTO).expect("Book proto parses");
    assert!(messages.contains_key("Book"));
    messages["Book"].clone()
}

#[test]
fn test_protobuf_parse_book() {
    let msg = book_message();
    assert_eq!(msg.name, "Book");
    assert_eq!(msg.fields.len(), 2);
    assert_eq!(msg.fields[0].name, "title");
    assert_eq!(msg.fields[0].proto_type, "string");
    assert_eq!(msg.fields[0].number, 1);
    assert_eq!(msg.fields[1].name, "price");
    assert_eq!(msg.fields[1].proto_type, "int32");
    assert_eq!(msg.fields[1].number, 2);
}

#[test]
fn test_protobuf_decode_tutorial_vector() {
    // Official eKuiper tutorial bytes for {"title": "streaming system", "price": 123}.
    let bytes = hex_to_bytes("0a1073747265616d696e672073797374656d107b");
    let decoded = ProtobufCodec::decode(&bytes, &book_message());
    assert_eq!(decoded, json!({"title": "streaming system", "price": 123}));
}

#[test]
fn test_protobuf_encode_decode_roundtrip() {
    let msg = book_message();
    let value = json!({"title": "rekuiper", "price": 420});
    let bytes = ProtobufCodec::encode(&value, &msg);
    // title(1,LEN)"rekuiper" + price(2,VARINT)420 → deterministic bytes.
    assert_eq!(bytes, hex_to_bytes("0a0872656b756970657210a403"));
    assert_eq!(ProtobufCodec::decode(&bytes, &msg), value);

    // Missing fields are skipped; unknown keys ignored; truncated input -> Null.
    assert_eq!(
        ProtobufCodec::decode(&hex_to_bytes("0a0872656b7569706572"), &msg),
        json!({"title": "rekuiper"})
    );
    assert_eq!(
        ProtobufCodec::decode(&hex_to_bytes("0a08"), &msg),
        serde_json::Value::Null
    );
    assert_eq!(
        ProtobufCodec::encode(&json!([1, 2]), &msg),
        Vec::<u8>::new()
    );
}

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

// ---------------------------------------------------------------------------
// Schema manager
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_schema_manager_crud() {
    let manager = SchemaManager::new();
    assert!(manager.list_schemas("protobuf").is_empty());

    manager
        .register_schema(rekuiper_core::SchemaDefinition {
            name: "book".to_string(),
            kind: "protobuf".to_string(),
            content: Some(BOOK_PROTO.to_string()),
            file: None,
        })
        .await
        .unwrap();
    assert_eq!(manager.list_schemas("protobuf"), vec!["book".to_string()]);
    assert!(manager.list_schemas("json").is_empty());

    let def = manager.get_schema("protobuf", "book").expect("stored");
    assert_eq!(def.name, "book");
    assert_eq!(def.kind, "protobuf");
    assert!(def.content.unwrap().contains("message Book"));

    // Persisted definitions reload into a fresh manager.
    use rekuiper_core::KvStore;
    let kv: std::sync::Arc<dyn KvStore> = std::sync::Arc::new(rekuiper_core::MemKvStore::new());
    let manager2 = SchemaManager::new_with_kv(kv.clone());
    manager2
        .register_schema(rekuiper_core::SchemaDefinition {
            name: "book".to_string(),
            kind: "protobuf".to_string(),
            content: Some(BOOK_PROTO.to_string()),
            file: None,
        })
        .await
        .unwrap();
    let manager3 = SchemaManager::new();
    manager3.load_from_kv(&kv).await.unwrap();
    assert_eq!(manager3.list_schemas("protobuf"), vec!["book".to_string()]);

    manager3.delete_schema("protobuf", "book").await.unwrap();
    assert!(manager3.list_schemas("protobuf").is_empty());
    assert!(manager3.delete_schema("protobuf", "missing").await.is_err());
}
