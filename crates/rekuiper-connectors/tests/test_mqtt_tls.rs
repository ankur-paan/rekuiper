use rekuiper_connectors::{
    build_tls_configuration, is_secure_mqtt_scheme, is_tls_required, parse_mqtt_server_url,
    MqttConfig, MqttSink, Sink,
};
use rekuiper_core::model::StreamRecord;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("tls_fixtures")
        .join(name)
}

fn fixture_str(name: &str) -> String {
    fixture_path(name).to_string_lossy().to_string()
}

fn load_certs(name: &str) -> Vec<CertificateDer<'static>> {
    let data = fs::read(fixture_path(name)).unwrap();
    rustls_pemfile::certs(&mut Cursor::new(data))
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn load_key(name: &str) -> PrivateKeyDer<'static> {
    let data = fs::read(fixture_path(name)).unwrap();
    let mut cursor = Cursor::new(data);
    loop {
        match rustls_pemfile::read_one(&mut cursor).unwrap() {
            Some(rustls_pemfile::Item::Sec1Key(k)) => return k.into(),
            Some(rustls_pemfile::Item::Pkcs1Key(k)) => return k.into(),
            Some(rustls_pemfile::Item::Pkcs8Key(k)) => return k.into(),
            None => panic!("no key found in {}", name),
            _ => {}
        }
    }
}

async fn start_mock_tls_broker(
    cert_name: &str,
    key_name: &str,
    client_ca_name: Option<&str>,
) -> (u16, tokio::sync::oneshot::Sender<()>) {
    let certs = load_certs(cert_name);
    let key = load_key(key_name);

    let server_config = if let Some(ca) = client_ca_name {
        let ca_certs = load_certs(ca);
        let mut client_roots = RootCertStore::empty();
        for c in ca_certs {
            client_roots.add(c).unwrap();
        }
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
            .build()
            .unwrap();
        ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .unwrap()
    } else {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap()
    };

    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = listener.accept() => {
                    let (stream, _) = match res {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let mut tls_stream = match acceptor.accept(stream).await {
                            Ok(s) => s,
                            Err(_) => return,
                        };

                        let mut buf = [0u8; 1024];
                        let _n = match tls_stream.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };
                        // Reply with CONNACK on CONNECT
                        if buf[0] == 0x10 {
                            let connack = [0x20, 0x02, 0x00, 0x00];
                            let _ = tls_stream.write_all(&connack).await;
                        }

                        while let Ok(n) = tls_stream.read(&mut buf).await {
                            if n == 0 { break; }
                        }
                    });
                }
            }
        }
    });

    (port, shutdown_tx)
}

fn sample_record() -> StreamRecord {
    let mut data = HashMap::new();
    data.insert("temperature".to_string(), json!(23.5));
    data.insert("humidity".to_string(), json!(55.0));
    StreamRecord::new(data)
}

#[tokio::test]
async fn test_mqtt_tls_trusted_ca_success() {
    let (port, _shutdown) =
        start_mock_tls_broker("server_localhost.crt", "server_localhost.key", None).await;

    let config = MqttConfig {
        server: format!("ssl://127.0.0.1:{}", port),
        topic: "devices/telemetry".to_string(),
        root_ca_path: Some(fixture_str("ca1.crt")),
        ..Default::default()
    };

    let sink = MqttSink::new(config).expect("MqttSink creation failed");
    let record = sample_record();

    // Verify successful send across TLS connection
    let res = sink.send(&record).await;
    assert!(
        res.is_ok(),
        "Expected send to succeed over trusted TLS, got: {:?}",
        res
    );
}

#[tokio::test]
async fn test_mqtt_tls_untrusted_ca_fails() {
    // Server uses cert signed by CA 2
    let (port, _shutdown) =
        start_mock_tls_broker("server_untrusted.crt", "server_untrusted.key", None).await;

    // Client only trusts CA 1
    let config = MqttConfig {
        server: format!("ssl://127.0.0.1:{}", port),
        topic: "devices/telemetry".to_string(),
        root_ca_path: Some(fixture_str("ca1.crt")),
        ..Default::default()
    };

    let sink = MqttSink::new(config).expect("MqttSink creation failed");
    let record = sample_record();

    // Send should fail because the TLS handshake fails to verify the untrusted certificate
    let res = sink.send(&record).await;
    assert!(
        res.is_err(),
        "Expected send to fail with untrusted CA certificate"
    );
}

#[tokio::test]
async fn test_mqtt_tls_hostname_mismatch_fails() {
    // Server cert has SAN for mismatch.example.com
    let (port, _shutdown) =
        start_mock_tls_broker("server_mismatch.crt", "server_mismatch.key", None).await;

    // Client connects to 127.0.0.1
    let config = MqttConfig {
        server: format!("ssl://127.0.0.1:{}", port),
        topic: "devices/telemetry".to_string(),
        root_ca_path: Some(fixture_str("ca1.crt")),
        ..Default::default()
    };

    let sink = MqttSink::new(config).expect("MqttSink creation failed");
    let record = sample_record();

    // Send should fail because of hostname mismatch
    let res = sink.send(&record).await;
    assert!(res.is_err(), "Expected send to fail with hostname mismatch");
}

#[tokio::test]
async fn test_mqtt_tls_insecure_skip_verify_bypasses_mismatch() {
    // Server cert has hostname mismatch
    let (port, _shutdown) =
        start_mock_tls_broker("server_mismatch.crt", "server_mismatch.key", None).await;

    let config = MqttConfig {
        server: format!("ssl://127.0.0.1:{}", port),
        topic: "devices/telemetry".to_string(),
        insecure_skip_verify: true,
        ..Default::default()
    };

    let sink = MqttSink::new(config).expect("MqttSink creation failed");
    let record = sample_record();

    let res = sink.send(&record).await;
    assert!(
        res.is_ok(),
        "Expected send to succeed when insecure_skip_verify is true, got: {:?}",
        res
    );
}

#[tokio::test]
async fn test_mqtt_tls_mutual_tls_success() {
    // Server requires client certificate signed by CA 1
    let (port, _shutdown) = start_mock_tls_broker(
        "server_localhost.crt",
        "server_localhost.key",
        Some("ca1.crt"),
    )
    .await;

    // Client provides client certificate & private key
    let config = MqttConfig {
        server: format!("ssl://127.0.0.1:{}", port),
        topic: "devices/telemetry".to_string(),
        root_ca_path: Some(fixture_str("ca1.crt")),
        certification_path: Some(fixture_str("client.crt")),
        private_key_path: Some(fixture_str("client.key")),
        ..Default::default()
    };

    let sink = MqttSink::new(config).expect("MqttSink creation failed");
    let record = sample_record();

    let res = sink.send(&record).await;
    assert!(
        res.is_ok(),
        "Expected mTLS connection to succeed, got: {:?}",
        res
    );
}

#[tokio::test]
async fn test_mqtt_tls_mutual_tls_rejected_without_client_cert() {
    // Server requires client certificate
    let (port, _shutdown) = start_mock_tls_broker(
        "server_localhost.crt",
        "server_localhost.key",
        Some("ca1.crt"),
    )
    .await;

    // Client connects WITHOUT client cert
    let config = MqttConfig {
        server: format!("ssl://127.0.0.1:{}", port),
        topic: "devices/telemetry".to_string(),
        root_ca_path: Some(fixture_str("ca1.crt")),
        ..Default::default()
    };

    let sink = MqttSink::new(config).expect("MqttSink creation failed");
    let record = sample_record();

    let res = sink.send(&record).await;
    assert!(
        res.is_err(),
        "Expected connection to fail when server requires client certificate but client provides none"
    );
}

#[tokio::test]
async fn test_mqtt_tls_raw_pem_and_base64() {
    let (port, _shutdown) = start_mock_tls_broker(
        "server_localhost.crt",
        "server_localhost.key",
        Some("ca1.crt"),
    )
    .await;

    // Read files as strings
    let ca_pem = fs::read_to_string(fixture_path("ca1.crt")).unwrap();
    let client_crt = fs::read_to_string(fixture_path("client.crt")).unwrap();
    let client_key = fs::read_to_string(fixture_path("client.key")).unwrap();

    // Base64 encode the CA certificate
    use base64::Engine as _;
    let ca_b64 = base64::engine::general_purpose::STANDARD.encode(ca_pem.as_bytes());

    let config = MqttConfig {
        server: format!("ssl://127.0.0.1:{}", port),
        topic: "devices/telemetry".to_string(),
        root_ca_raw: Some(ca_b64),
        certification_raw: Some(client_crt),
        private_key_raw: Some(client_key),
        ..Default::default()
    };

    let sink = MqttSink::new(config).expect("MqttSink creation failed");
    let record = sample_record();

    let res = sink.send(&record).await;
    assert!(
        res.is_ok(),
        "Expected mTLS with raw/base64 PEM strings to succeed, got: {:?}",
        res
    );
}

#[test]
fn test_mqtt_tls_config_validation_errors() {
    // 1. Client cert given without private key
    let config = MqttConfig {
        server: "ssl://broker.example.com:8883".to_string(),
        certification_path: Some(fixture_str("client.crt")),
        ..Default::default()
    };
    let err = build_tls_configuration(&config).unwrap_err();
    assert!(
        err.to_string().contains("client private key is missing"),
        "Unexpected error: {}",
        err
    );

    // 2. Private key given without client cert
    let config = MqttConfig {
        server: "ssl://broker.example.com:8883".to_string(),
        private_key_path: Some(fixture_str("client.key")),
        ..Default::default()
    };
    let err = build_tls_configuration(&config).unwrap_err();
    assert!(
        err.to_string().contains("client certificate is missing"),
        "Unexpected error: {}",
        err
    );

    // 3. Non-existent cert file path
    let config = MqttConfig {
        server: "ssl://broker.example.com:8883".to_string(),
        root_ca_path: Some("non_existent_ca_file.crt".to_string()),
        ..Default::default()
    };
    let err = build_tls_configuration(&config).unwrap_err();
    assert!(
        err.to_string()
            .contains("Failed to read MQTT root CA certificate file"),
        "Unexpected error: {}",
        err
    );

    // 4. Invalid PEM content
    let config = MqttConfig {
        server: "ssl://broker.example.com:8883".to_string(),
        root_ca_raw: Some(
            "-----BEGIN CERTIFICATE-----\nINVALID\n-----END CERTIFICATE-----".to_string(),
        ),
        ..Default::default()
    };
    let err = build_tls_configuration(&config).unwrap_err();
    assert!(
        err.to_string().contains("CA") || err.to_string().contains("PEM"),
        "Unexpected error: {}",
        err
    );

    // 5. Scheme checking: secure schemes mandate TLS
    assert!(is_secure_mqtt_scheme("ssl"));
    assert!(is_secure_mqtt_scheme("tls"));
    assert!(is_secure_mqtt_scheme("tcps"));
    assert!(is_secure_mqtt_scheme("mqtts"));
    assert!(!is_secure_mqtt_scheme("tcp"));
    assert!(!is_secure_mqtt_scheme("mqtt"));

    let secure_config = MqttConfig {
        server: "ssl://broker.example.com:8883".to_string(),
        ..Default::default()
    };
    assert!(is_tls_required(&secure_config, "ssl"));

    let plain_config = MqttConfig {
        server: "tcp://broker.example.com:1883".to_string(),
        ..Default::default()
    };
    assert!(!is_tls_required(&plain_config, "tcp"));

    // Secure scheme port defaults
    assert_eq!(
        parse_mqtt_server_url("ssl://broker.example.com").unwrap().1,
        8883
    );
    assert_eq!(
        parse_mqtt_server_url("tls://broker.example.com").unwrap().1,
        8883
    );
    assert_eq!(
        parse_mqtt_server_url("tcps://broker.example.com")
            .unwrap()
            .1,
        8883
    );
    assert_eq!(
        parse_mqtt_server_url("mqtts://broker.example.com")
            .unwrap()
            .1,
        8883
    );
    assert_eq!(
        parse_mqtt_server_url("tcp://broker.example.com").unwrap().1,
        1883
    );
}

#[test]
fn test_mqtt_config_debug_redacts_secrets() {
    let config = MqttConfig {
        server: "ssl://broker.example.com:8883".to_string(),
        topic: "devices/telemetry".to_string(),
        username: Some("admin".to_string()),
        password: Some("super_secret_password".to_string()),
        private_key_raw: Some("-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEA0...".to_string()),
        certification_raw: Some("-----BEGIN CERTIFICATE-----\nMIIDdzCCAl+gAwIBAgIU...".to_string()),
        root_ca_raw: Some("-----BEGIN CERTIFICATE-----\nMIIDfTCCAmWgAwIBAgIU...".to_string()),
        ..Default::default()
    };

    let debug_str = format!("{:?}", config);
    assert!(
        !debug_str.contains("super_secret_password"),
        "Password was leaked in Debug output!"
    );
    assert!(
        !debug_str.contains("MIIEogIBAAKCAQEA0"),
        "Private key was leaked in Debug output!"
    );
    assert!(debug_str.contains("password: Some(\"******\")"));
    assert!(debug_str.contains("private_key_raw: Some(\"******\")"));
}
