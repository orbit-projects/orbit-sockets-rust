use std::{net::SocketAddr, time::Duration};

use futures_util::{SinkExt, StreamExt};
use orbit_sockets_rust::{
    Close, Message, MessageKind, ServerConfig, SocketError, bind, handler, serve,
};
use tokio::{sync::oneshot, time::timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message as WireMessage};

#[test]
fn config_bounds_and_sensitive_debug_output() {
    let mut config = ServerConfig::default();
    assert!(config.validate().is_ok());
    config.max_connections = 0;
    assert_eq!(config.validate(), Err(SocketError::Protocol));

    let message = Message::text("private-value", 1024).expect("valid message");
    assert!(!format!("{message:?}").contains("private-value"));
    let close = Close::new(1000, "sensitive reason").expect("valid close");
    assert!(!format!("{close:?}").contains("sensitive reason"));
    assert!(Close::new(1005, "").is_err());
}

#[tokio::test]
async fn echo_and_shutdown_are_bounded() {
    let listener = bind("127.0.0.1:0".parse::<SocketAddr>().expect("address"))
        .await
        .expect("listener");
    let address = listener.local_addr().expect("local address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let app = handler(|session| async move {
        let message = session.receive().await?;
        session.send(&message).await?;
        Ok(())
    });
    let config = ServerConfig {
        close_timeout: Duration::from_secs(1),
        ..ServerConfig::default()
    };
    let task = tokio::spawn(serve(listener, config, app, shutdown_rx));

    let (mut client, _) = timeout(
        Duration::from_secs(3),
        connect_async(format!("ws://{address}/ws")),
    )
    .await
    .expect("connect deadline")
    .expect("connected");
    client
        .send(WireMessage::Text("hello".into()))
        .await
        .expect("send");
    let response = timeout(Duration::from_secs(3), client.next())
        .await
        .expect("receive deadline")
        .expect("message available")
        .expect("message valid");
    match response {
        WireMessage::Text(text) => assert_eq!(text, "hello"),
        _ => panic!("expected text frame"),
    }
    let closing = timeout(Duration::from_secs(3), client.next())
        .await
        .expect("close deadline")
        .expect("close frame available")
        .expect("close frame valid");
    assert!(matches!(closing, WireMessage::Close(_)));

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(3), task)
        .await
        .expect("shutdown deadline")
        .expect("server task")
        .expect("clean shutdown");
    assert_eq!(
        MessageKind::Text,
        Message::text("x", 1).expect("text").kind()
    );
}
