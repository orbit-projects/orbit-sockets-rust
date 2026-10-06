//! Bounded standalone Tokio WebSocket listener for Orbit applications.

use std::{
    fmt,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::{
    SinkExt, StreamExt,
    future::BoxFuture,
    stream::{SplitSink, SplitStream},
};
use http::{HeaderValue, StatusCode, header};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore, oneshot},
    task::JoinSet,
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async_with_config,
    tungstenite::{
        Message as WireMessage,
        handshake::server::{ErrorResponse, Request as WsRequest, Response as WsResponse},
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};
use tokio_util::sync::CancellationToken;

pub const MAX_MESSAGE_BYTES: usize = 1_048_576;
pub const MAX_CLOSE_REASON_BYTES: usize = 123;
type WireStream = WebSocketStream<TcpStream>;
type WireSink = SplitSink<WireStream, WireMessage>;
type WireRead = SplitStream<WireStream>;

/// Stable sanitized socket failure categories.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum SocketError {
    /// The peer or server closed the session.
    Closed,
    /// A value violated the Orbit socket protocol contract.
    Protocol,
    /// A message exceeded configured bounds.
    TooLarge,
    /// A WebSocket transport operation failed.
    Transport,
}

impl fmt::Debug for SocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for SocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Closed => "socket session is closed",
            Self::Protocol => "socket protocol value is invalid",
            Self::TooLarge => "socket message exceeds configured limit",
            Self::Transport => "socket transport operation failed",
        })
    }
}

impl std::error::Error for SocketError {}

/// Validated text or binary message with payload-safe debug output.
#[derive(Clone)]
pub struct Message {
    kind: MessageKind,
    data: Arc<[u8]>,
}

/// Text or binary message kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageKind {
    /// UTF-8 text payload.
    Text,
    /// Opaque binary payload.
    Binary,
}

impl Message {
    /// Construct a bounded UTF-8 text message.
    pub fn text(value: impl Into<String>, max_bytes: usize) -> Result<Self, SocketError> {
        let bytes = value.into().into_bytes();
        Self::new(MessageKind::Text, bytes, max_bytes)
    }

    /// Construct a bounded binary message.
    pub fn binary(value: impl Into<Vec<u8>>, max_bytes: usize) -> Result<Self, SocketError> {
        Self::new(MessageKind::Binary, value.into(), max_bytes)
    }

    fn new(kind: MessageKind, data: Vec<u8>, max_bytes: usize) -> Result<Self, SocketError> {
        if data.len() > max_bytes || data.len() > MAX_MESSAGE_BYTES {
            return Err(SocketError::TooLarge);
        }
        if kind == MessageKind::Text && std::str::from_utf8(&data).is_err() {
            return Err(SocketError::Protocol);
        }
        Ok(Self {
            kind,
            data: Arc::from(data),
        })
    }

    /// Return the message kind.
    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    /// Return an immutable payload view.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Decode a text message after checking its type.
    pub fn as_text(&self) -> Result<&str, SocketError> {
        if self.kind != MessageKind::Text {
            return Err(SocketError::Protocol);
        }
        std::str::from_utf8(&self.data).map_err(|_| SocketError::Protocol)
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("kind", &self.kind)
            .field("size", &self.data.len())
            .finish()
    }
}

/// WebSocket close status and reason.
#[derive(Clone)]
pub struct Close {
    code: u16,
    reason: String,
}

impl Close {
    /// Validate a close code and bounded UTF-8 reason.
    pub fn new(code: u16, reason: impl Into<String>) -> Result<Self, SocketError> {
        let reason = reason.into();
        if !(1000..=4999).contains(&code)
            || matches!(code, 1004 | 1005 | 1006 | 1015)
            || (1016..=2999).contains(&code)
            || reason.len() > MAX_CLOSE_REASON_BYTES
        {
            return Err(SocketError::Protocol);
        }
        Ok(Self { code, reason })
    }
}

impl fmt::Debug for Close {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Close")
            .field("code", &self.code)
            .field("reason_bytes", &self.reason.len())
            .finish()
    }
}

/// Bounded listener configuration. TLS terminates at ingress; bind this listener privately.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// WebSocket URL path accepted by this service.
    pub path: String,
    /// Exact browser origins accepted by the service. Empty rejects Origin-bearing clients.
    pub allowed_origins: Vec<String>,
    /// Negotiable WebSocket subprotocols, in server preference order.
    pub subprotocols: Vec<String>,
    /// Maximum simultaneous accepted sessions.
    pub max_connections: usize,
    /// Maximum text or binary message bytes.
    pub max_message_bytes: usize,
    /// Maximum individual WebSocket frame bytes.
    pub max_frame_bytes: usize,
    /// Maximum wait for the WebSocket close handshake and server drain.
    pub close_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            path: "/ws".into(),
            allowed_origins: Vec::new(),
            subprotocols: Vec::new(),
            max_connections: 256,
            max_message_bytes: 256 * 1024,
            max_frame_bytes: 256 * 1024,
            close_timeout: Duration::from_secs(5),
        }
    }
}

impl ServerConfig {
    /// Validate path, message, frame, connection, and shutdown bounds.
    pub fn validate(&self) -> Result<(), SocketError> {
        if !self.path.starts_with('/')
            || self.path.contains(['?', '#'])
            || self.max_connections == 0
            || self.max_connections > 65_536
            || self.max_message_bytes == 0
            || self.max_message_bytes > MAX_MESSAGE_BYTES
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > self.max_message_bytes
            || self.close_timeout < Duration::from_millis(100)
            || self.close_timeout > Duration::from_secs(300)
        {
            return Err(SocketError::Protocol);
        }
        for origin in &self.allowed_origins {
            let parsed = http::Uri::try_from(origin.as_str()).map_err(|_| SocketError::Protocol)?;
            if !matches!(parsed.scheme_str(), Some("http" | "https"))
                || parsed.authority().is_none()
                || parsed.path() != ""
                || parsed.query().is_some()
                || parsed.authority().is_some_and(|a| a.as_str().contains('@'))
            {
                return Err(SocketError::Protocol);
            }
        }
        Ok(())
    }
}

/// One WebSocket session. Reads and writes are independently serialized and apply backpressure.
#[derive(Clone)]
pub struct Session {
    sink: Arc<Mutex<WireSink>>,
    read: Arc<Mutex<WireRead>>,
    closed: Arc<AtomicBool>,
    cancellation: CancellationToken,
    max_message_bytes: usize,
    close_timeout: Duration,
}

impl Session {
    /// Wait for one complete message. Cancellation propagates without losing the connection.
    pub async fn receive(&self) -> Result<Message, SocketError> {
        loop {
            let next = async { self.read.lock().await.next().await };
            tokio::select! {
                _ = self.cancellation.cancelled() => return Err(SocketError::Closed),
                message = next => match message {
                    Some(Ok(WireMessage::Text(text))) => return Message::new(MessageKind::Text, text.as_bytes().to_vec(), self.max_message_bytes),
                    Some(Ok(WireMessage::Binary(data))) => return Message::new(MessageKind::Binary, data.to_vec(), self.max_message_bytes),
                    Some(Ok(WireMessage::Close(_))) | None => {
                        self.closed.store(true, Ordering::Release);
                        return Err(SocketError::Closed);
                    }
                    Some(Ok(WireMessage::Ping(_) | WireMessage::Pong(_))) => continue,
                    Some(Err(_)) => {
                        self.closed.store(true, Ordering::Release);
                        return Err(SocketError::Transport);
                    }
                    Some(Ok(_)) => return Err(SocketError::Protocol),
                }
            }
        }
    }

    /// Send one bounded message and await sink readiness for transport backpressure.
    pub async fn send(&self, message: &Message) -> Result<(), SocketError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SocketError::Closed);
        }
        if message.data.len() > self.max_message_bytes {
            return Err(SocketError::TooLarge);
        }
        let wire = match message.kind {
            MessageKind::Text => WireMessage::Text(
                std::str::from_utf8(&message.data)
                    .map_err(|_| SocketError::Protocol)?
                    .to_owned()
                    .into(),
            ),
            MessageKind::Binary => WireMessage::Binary(message.data.to_vec().into()),
        };
        self.sink
            .lock()
            .await
            .send(wire)
            .await
            .map_err(|_| SocketError::Transport)
    }

    /// Perform a bounded WebSocket close handshake; repeated calls are harmless.
    pub async fn close(&self, close: Close) -> Result<(), SocketError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let cancellation = self.cancellation.clone();
        let close_future = async {
            let frame = CloseFrame {
                code: CloseCode::from(close.code),
                reason: close.reason.into(),
            };
            self.sink
                .lock()
                .await
                .send(WireMessage::Close(Some(frame)))
                .await
                .map_err(|_| SocketError::Transport)?;
            cancellation.cancel();
            loop {
                let next = self.read.lock().await.next().await;
                match next {
                    Some(Ok(WireMessage::Close(_))) | None => return Ok::<(), SocketError>(()),
                    Some(Ok(WireMessage::Ping(_) | WireMessage::Pong(_))) => continue,
                    Some(Ok(_)) => continue,
                    Some(Err(_)) => return Err(SocketError::Transport),
                }
            }
        };
        timeout(self.close_timeout, close_future)
            .await
            .map_err(|_| SocketError::Transport)??;
        Ok(())
    }
}

/// Async session callback used for each accepted connection.
pub type Handler =
    Arc<dyn Fn(Session) -> BoxFuture<'static, Result<(), SocketError>> + Send + Sync>;

/// Create a handler from an async function or closure.
pub fn handler<F, Fut>(function: F) -> Handler
where
    F: Fn(Session) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), SocketError>> + Send + 'static,
{
    Arc::new(move |session| Box::pin(function(session)))
}

/// Run a standalone WebSocket listener until shutdown is requested.
///
/// The listener applies a connection semaphore before protocol upgrade, validates route and origin
/// before application dispatch, disables per-message compression, caps messages and frames, and
/// drains accepted handlers within the configured timeout.
pub async fn serve(
    listener: TcpListener,
    config: ServerConfig,
    handler: Handler,
    shutdown: oneshot::Receiver<()>,
) -> Result<(), SocketError> {
    config.validate()?;
    let slots = Arc::new(Semaphore::new(config.max_connections));
    let cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|_| SocketError::Transport)?;
                let permit = match Arc::clone(&slots).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        reject_over_capacity(stream).await;
                        continue;
                    }
                };
                let cfg = config.clone();
                let app = Arc::clone(&handler);
                let token = cancellation.child_token();
                tasks.spawn(async move {
                    let _permit = permit;
                    serve_connection(stream, cfg, app, token).await;
                });
            }
        }
    }
    cancellation.cancel();
    let drain = async { while tasks.join_next().await.is_some() {} };
    timeout(config.close_timeout, drain)
        .await
        .map_err(|_| SocketError::Transport)?;
    Ok(())
}

async fn reject_over_capacity(mut stream: TcpStream) {
    let response =
        b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
    let _ = stream.write_all(response).await;
    let _ = stream.shutdown().await;
}

// The WebSocket handshake callback is defined by tungstenite and requires a large HTTP response
// value on rejection; boxing cannot change that dependency-owned callback contract.
#[allow(clippy::result_large_err)]
async fn serve_connection(
    stream: TcpStream,
    config: ServerConfig,
    app: Handler,
    cancel: CancellationToken,
) {
    let path = config.path.clone();
    let origins = config.allowed_origins.clone();
    let protocols = config.subprotocols.clone();
    let callback =
        move |request: &WsRequest, mut response: WsResponse| -> Result<WsResponse, ErrorResponse> {
            let origin_header = request.headers().get(header::ORIGIN);
            let route_valid = request.uri().path() == path;
            let origin_valid = match origin_header {
                Some(value) => value
                    .to_str()
                    .ok()
                    .is_some_and(|origin| origins.iter().any(|allowed| allowed == origin)),
                None => origins.is_empty(),
            };
            if !route_valid || !origin_valid {
                let status = if route_valid {
                    StatusCode::FORBIDDEN
                } else {
                    StatusCode::NOT_FOUND
                };
                return Err(http::Response::builder()
                    .status(status)
                    .body(Some("request rejected".to_owned()))
                    .expect("valid response"));
            }
            if !protocols.is_empty() {
                let requested: Vec<&str> = request
                    .headers()
                    .get(header::SEC_WEBSOCKET_PROTOCOL)
                    .and_then(|value| value.to_str().ok())
                    .map(|value| value.split(',').map(str::trim).collect())
                    .unwrap_or_default();
                if let Some(selected) = protocols
                    .iter()
                    .find(|protocol| requested.contains(&protocol.as_str()))
                {
                    let value = HeaderValue::try_from(selected.as_str())
                        .expect("configured protocol was validated");
                    response.headers_mut().insert(
                        http::header::HeaderName::from_static("sec-websocket-protocol"),
                        value,
                    );
                }
            }
            Ok(response)
        };
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(config.max_message_bytes))
        .max_frame_size(Some(config.max_frame_bytes));
    let socket = match accept_hdr_async_with_config(stream, callback, Some(ws_config)).await {
        Ok(socket) => socket,
        Err(_) => return,
    };
    let (sink, read) = socket.split();
    let session = Session {
        sink: Arc::new(Mutex::new(sink)),
        read: Arc::new(Mutex::new(read)),
        closed: Arc::new(AtomicBool::new(false)),
        cancellation: cancel.clone(),
        max_message_bytes: config.max_message_bytes,
        close_timeout: config.close_timeout,
    };
    tokio::select! {
        _ = cancel.cancelled() => {
            if let Ok(close) = Close::new(1001, "server shutting down") {
                let _ = session.close(close).await;
            }
        }
        result = app(session.clone()) => {
            if let Err(_sanitized_error) = result {
                if let Ok(close) = Close::new(1011, "handler failed") {
                    let _ = session.close(close).await;
                }
            } else if let Ok(close) = Close::new(1000, "") {
                let _ = session.close(close).await;
            }
        }
    }
}

/// Bind a TCP socket suitable for passing to [`serve`].
pub async fn bind(address: SocketAddr) -> Result<TcpListener, SocketError> {
    TcpListener::bind(address)
        .await
        .map_err(|_| SocketError::Transport)
}
