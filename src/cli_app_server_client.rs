use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

const MAX_HANDSHAKE_BYTES: usize = 8 * 1024;
const MAX_MESSAGE_BYTES: u64 = 1024 * 1024;
const WEBSOCKET_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub(crate) struct AppServerJsonRpcClient {
    stream: TcpStream,
    next_request_id: u64,
    random_source: File,
    pending_messages: VecDeque<Value>,
}

pub(crate) enum AppServerReceive {
    Message(Value),
    Timeout,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AppServerRequestErrorKind {
    Timeout,
    Remote,
    Closed,
    Protocol,
}

#[derive(Debug)]
pub(crate) struct AppServerRequestError {
    kind: AppServerRequestErrorKind,
    message: String,
}

impl AppServerRequestError {
    fn new(kind: AppServerRequestErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn protocol(error: anyhow::Error) -> Self {
        Self::new(AppServerRequestErrorKind::Protocol, format!("{error:#}"))
    }
}

impl fmt::Display for AppServerRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for AppServerRequestError {}

pub(crate) enum AppServerNotification {
    TurnStarted {
        thread_id: Option<String>,
    },
    TurnTerminal {
        thread_id: Option<String>,
    },
    ThreadProofInvalidated {
        thread_id: Option<String>,
    },
    ThreadActivityChanged {
        thread_id: Option<String>,
        active: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ThreadActivitySnapshot {
    Active,
    Idle,
    Missing,
    Untrusted,
}

impl AppServerJsonRpcClient {
    pub(crate) fn connect(url: &str, timeout: Duration) -> Result<Self> {
        let parsed = ParsedWsUrl::parse(url)?;
        let mut stream = connect_loopback(&parsed, timeout)?;
        stream
            .set_nodelay(true)
            .context("configure app-server websocket TCP_NODELAY")?;
        stream
            .set_read_timeout(Some(timeout))
            .context("configure app-server websocket read timeout")?;
        stream
            .set_write_timeout(Some(timeout))
            .context("configure app-server websocket write timeout")?;

        let mut random_source = File::open("/dev/urandom").context("open system random source")?;
        let websocket_key = generate_websocket_key(&mut random_source)?;
        let request = format!(
            "GET {} HTTP/1.1\r\n\
             Host: {}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n",
            parsed.path, parsed.authority, websocket_key
        );
        stream
            .write_all(request.as_bytes())
            .context("write app-server websocket handshake")?;

        let response = read_handshake_response(&mut stream, Instant::now() + timeout)?;
        validate_websocket_handshake_response(&response, &websocket_key)?;

        Ok(Self {
            stream,
            next_request_id: 1,
            random_source,
            pending_messages: VecDeque::new(),
        })
    }

    pub(crate) fn initialize(&mut self, version: &str, timeout: Duration) -> Result<Value> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "cbth_cli_passive_adapter",
                    "title": "CBTH CLI Passive Adapter",
                    "version": version,
                },
                "capabilities": {
                    "experimentalApi": true,
                    "optOutNotificationMethods": [
                        "item/agentMessage/delta",
                        "item/reasoning/summaryTextDelta",
                        "item/reasoning/textDelta",
                        "command/exec/outputDelta",
                        "item/commandExecution/outputDelta",
                        "item/fileChange/outputDelta",
                        "item/mcpToolCall/progress"
                    ]
                }
            }),
            timeout,
        )
        .map_err(anyhow::Error::new)
    }

    pub(crate) fn notify_initialized(&mut self) -> Result<()> {
        self.send_json(&json!({
            "jsonrpc": "2.0",
            "method": "initialized",
            "params": {}
        }))
    }

    pub(crate) fn request(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> std::result::Result<Value, AppServerRequestError> {
        let mut pending_messages = Vec::new();
        let result = self.request_with_message_handler(method, params, timeout, |message| {
            pending_messages.push(message);
        });
        self.pending_messages.extend(pending_messages);
        result
    }

    pub(crate) fn request_with_message_handler<F>(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
        mut handle_message: F,
    ) -> std::result::Result<Value, AppServerRequestError>
    where
        F: FnMut(Value),
    {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).ok_or_else(|| {
            AppServerRequestError::new(
                AppServerRequestErrorKind::Protocol,
                "app-server JSON-RPC request id overflow",
            )
        })?;
        self.send_json(&json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params
        }))
        .map_err(AppServerRequestError::protocol)?;

        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AppServerRequestError::new(
                    AppServerRequestErrorKind::Timeout,
                    format!("timed out waiting for app-server response to {method}"),
                ));
            }
            match self
                .recv(remaining)
                .map_err(AppServerRequestError::protocol)?
            {
                AppServerReceive::Message(message) => {
                    if message.get("id").and_then(Value::as_u64) != Some(request_id) {
                        handle_message(message);
                        continue;
                    }
                    if let Some(error) = message.get("error") {
                        return Err(AppServerRequestError::new(
                            AppServerRequestErrorKind::Remote,
                            format!("app-server {method} failed: {error}"),
                        ));
                    }
                    return Ok(message.get("result").cloned().unwrap_or(Value::Null));
                }
                AppServerReceive::Timeout => {
                    return Err(AppServerRequestError::new(
                        AppServerRequestErrorKind::Timeout,
                        format!("timed out waiting for app-server response to {method}"),
                    ));
                }
                AppServerReceive::Closed => {
                    return Err(AppServerRequestError::new(
                        AppServerRequestErrorKind::Closed,
                        format!("app-server closed while waiting for {method}"),
                    ));
                }
            }
        }
    }

    pub(crate) fn recv(&mut self, timeout: Duration) -> Result<AppServerReceive> {
        if let Some(message) = self.pending_messages.pop_front() {
            return Ok(AppServerReceive::Message(message));
        }
        let deadline = Instant::now() + timeout;
        loop {
            let Some(frame) = self.read_frame(deadline)? else {
                return Ok(AppServerReceive::Timeout);
            };
            match frame.opcode {
                0x1 => {
                    let text = String::from_utf8(frame.payload)
                        .context("decode app-server websocket text frame")?;
                    let value = serde_json::from_str(&text)
                        .context("decode app-server JSON-RPC message")?;
                    return Ok(AppServerReceive::Message(value));
                }
                0x8 => return Ok(AppServerReceive::Closed),
                0x9 => self.send_frame_until(0xA, &frame.payload, deadline)?,
                0xA => {}
                _ => {}
            }
        }
    }

    pub(crate) fn drain_pending_messages(&mut self) -> Vec<Value> {
        self.pending_messages.drain(..).collect()
    }

    fn send_json(&mut self, value: &Value) -> Result<()> {
        let text = serde_json::to_vec(value).context("encode app-server JSON-RPC message")?;
        self.send_frame(0x1, &text)
    }

    fn send_frame(&mut self, opcode: u8, payload: &[u8]) -> Result<()> {
        let frame = self.encode_frame(opcode, payload)?;
        self.stream
            .write_all(&frame)
            .context("write app-server websocket frame")
    }

    fn send_frame_until(&mut self, opcode: u8, payload: &[u8], deadline: Instant) -> Result<()> {
        let frame = self.encode_frame(opcode, payload)?;
        write_all_until(&mut self.stream, &frame, deadline)
            .context("write app-server websocket frame")
    }

    fn encode_frame(&mut self, opcode: u8, payload: &[u8]) -> Result<Vec<u8>> {
        if payload.len() as u64 > MAX_MESSAGE_BYTES {
            bail!("app-server websocket payload exceeds {MAX_MESSAGE_BYTES} bytes");
        }
        if opcode & 0x08 != 0 && payload.len() > 125 {
            bail!("app-server websocket control frame exceeds 125 bytes");
        }
        let mut mask = [0_u8; 4];
        fill_random(&mut self.random_source, &mut mask)?;

        let mut frame = Vec::with_capacity(payload.len().saturating_add(14));
        frame.push(0x80 | (opcode & 0x0F));
        match payload.len() {
            len @ 0..=125 => frame.push(0x80 | u8::try_from(len).expect("small len fits u8")),
            len @ 126..=65535 => {
                frame.push(0x80 | 126);
                frame.extend_from_slice(
                    &u16::try_from(len)
                        .expect("medium len fits u16")
                        .to_be_bytes(),
                );
            }
            len => {
                frame.push(0x80 | 127);
                frame.extend_from_slice(
                    &u64::try_from(len)
                        .expect("payload len fits u64")
                        .to_be_bytes(),
                );
            }
        }
        frame.extend_from_slice(&mask);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(idx, byte)| byte ^ mask[idx % 4]),
        );
        Ok(frame)
    }

    fn read_frame(&mut self, deadline: Instant) -> Result<Option<WebSocketFrame>> {
        let mut header = [0_u8; 2];
        if !read_exact_or_timeout(&mut self.stream, &mut header, deadline)? {
            return Ok(None);
        }
        let fin = header[0] & 0x80 != 0;
        let opcode = header[0] & 0x0F;
        if !fin {
            bail!("fragmented app-server websocket frames are not supported");
        }

        let masked = header[1] & 0x80 != 0;
        let mut length = u64::from(header[1] & 0x7F);
        if length == 126 {
            let mut bytes = [0_u8; 2];
            read_exact_required(&mut self.stream, &mut bytes, deadline)?;
            length = u64::from(u16::from_be_bytes(bytes));
        } else if length == 127 {
            let mut bytes = [0_u8; 8];
            read_exact_required(&mut self.stream, &mut bytes, deadline)?;
            length = u64::from_be_bytes(bytes);
        }
        if length > MAX_MESSAGE_BYTES {
            bail!("app-server websocket message exceeds {MAX_MESSAGE_BYTES} bytes");
        }
        if opcode & 0x08 != 0 && length > 125 {
            bail!("app-server websocket control frame exceeds 125 bytes");
        }

        let mut mask = [0_u8; 4];
        if masked {
            read_exact_required(&mut self.stream, &mut mask, deadline)?;
        }
        let mut payload =
            vec![0_u8; usize::try_from(length).expect("bounded websocket length fits usize")];
        read_exact_required(&mut self.stream, &mut payload, deadline)?;
        if masked {
            for (idx, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[idx % 4];
            }
        }
        Ok(Some(WebSocketFrame { opcode, payload }))
    }
}

pub(crate) fn decode_notification(value: &Value) -> Option<AppServerNotification> {
    let method = value.get("method")?.as_str()?;
    let params = value.get("params").unwrap_or(&Value::Null);
    match method {
        "turn/started" => Some(AppServerNotification::TurnStarted {
            thread_id: string_field(params, "threadId"),
        }),
        "turn/completed" => {
            let status = params
                .get("turn")
                .and_then(|turn| turn.get("status"))
                .and_then(Value::as_str)?;
            if is_terminal_turn_status(status) {
                Some(AppServerNotification::TurnTerminal {
                    thread_id: string_field(params, "threadId"),
                })
            } else {
                None
            }
        }
        "thread/status/changed" => {
            let thread_id = string_field(params, "threadId");
            let status_type = params
                .get("status")
                .and_then(|status| status.get("type"))
                .and_then(Value::as_str);
            match status_type {
                Some("active") => Some(AppServerNotification::ThreadActivityChanged {
                    thread_id,
                    active: true,
                }),
                Some("idle") => Some(AppServerNotification::ThreadActivityChanged {
                    thread_id,
                    active: false,
                }),
                _ => Some(AppServerNotification::ThreadProofInvalidated { thread_id }),
            }
        }
        _ => None,
    }
}

pub(crate) fn thread_result_activity_snapshot(
    result: &Value,
    bound_thread_id: &str,
) -> ThreadActivitySnapshot {
    let thread = result.get("thread").unwrap_or(result);
    if let Some(thread_id) = thread.get("id").and_then(Value::as_str)
        && thread_id != bound_thread_id
    {
        return ThreadActivitySnapshot::Untrusted;
    }
    if result.get("thread").is_some()
        && thread.get("id").and_then(Value::as_str) != Some(bound_thread_id)
    {
        return ThreadActivitySnapshot::Untrusted;
    }
    let status_type = match thread.get("status") {
        Some(status) => match status.get("type").and_then(Value::as_str) {
            Some(status_type) => Some(status_type),
            None => return ThreadActivitySnapshot::Untrusted,
        },
        None => None,
    };
    let turns = thread.get("turns").and_then(Value::as_array);
    if status_type.is_none() && turns.is_none() {
        return ThreadActivitySnapshot::Missing;
    }
    if thread.get("id").and_then(Value::as_str) != Some(bound_thread_id) {
        return ThreadActivitySnapshot::Untrusted;
    }

    if let Some(status_type) = status_type {
        return match status_type {
            "active" => ThreadActivitySnapshot::Active,
            "idle" => ThreadActivitySnapshot::Idle,
            _ => ThreadActivitySnapshot::Untrusted,
        };
    }

    let Some(turns) = turns else {
        return ThreadActivitySnapshot::Missing;
    };
    let Some(last_turn) = turns.last() else {
        return ThreadActivitySnapshot::Idle;
    };
    let Some(status) = last_turn.get("status").and_then(Value::as_str) else {
        return ThreadActivitySnapshot::Untrusted;
    };
    match status {
        "inProgress" => ThreadActivitySnapshot::Active,
        status if is_terminal_turn_status(status) => ThreadActivitySnapshot::Idle,
        _ => ThreadActivitySnapshot::Untrusted,
    }
}

fn is_terminal_turn_status(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "interrupted" | "replaced")
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

struct WebSocketFrame {
    opcode: u8,
    payload: Vec<u8>,
}

struct ParsedWsUrl {
    authority: String,
    host: String,
    port: u16,
    path: String,
}

impl ParsedWsUrl {
    fn parse(url: &str) -> Result<Self> {
        let rest = url
            .strip_prefix("ws://")
            .ok_or_else(|| anyhow::anyhow!("app-server URL must use ws://"))?;
        let (authority, path) = match rest.split_once('/') {
            Some((authority, path)) => (authority, format!("/{path}")),
            None => (rest, "/".to_owned()),
        };
        if authority.is_empty() || authority.contains('@') {
            bail!("app-server URL authority is not supported");
        }

        let (host, port) = parse_authority(authority)?;
        if !is_loopback_host(&host) {
            bail!("app-server URL host is not loopback");
        }
        Ok(Self {
            authority: authority.to_owned(),
            host,
            port,
            path,
        })
    }
}

fn parse_authority(authority: &str) -> Result<(String, u16)> {
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .ok_or_else(|| anyhow::anyhow!("invalid bracketed app-server host"))?;
        let port = match suffix.strip_prefix(':') {
            Some(port) => parse_port(port)?,
            None if suffix.is_empty() => 80,
            None => bail!("invalid bracketed app-server authority"),
        };
        return Ok((host.to_owned(), port));
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => (host, parse_port(port)?),
        Some(_) => bail!("IPv6 app-server URL hosts must be bracketed"),
        None => (authority, 80),
    };
    if host.is_empty() {
        bail!("app-server URL host is empty");
    }
    Ok((host.to_owned(), port))
}

fn parse_port(value: &str) -> Result<u16> {
    value
        .parse::<u16>()
        .with_context(|| format!("invalid app-server URL port {value:?}"))
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn connect_loopback(parsed: &ParsedWsUrl, timeout: Duration) -> Result<TcpStream> {
    let addresses: Vec<SocketAddr> = (parsed.host.as_str(), parsed.port)
        .to_socket_addrs()
        .with_context(|| format!("resolve app-server host {}", parsed.host))?
        .filter(|addr| addr.ip().is_loopback())
        .collect();
    if addresses.is_empty() {
        bail!("app-server URL resolved to no loopback addresses");
    }

    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error
        .map(anyhow::Error::new)
        .unwrap_or_else(|| anyhow::anyhow!("app-server connect failed")))
    .context("connect to app-server websocket")
}

fn read_handshake_response(stream: &mut TcpStream, deadline: Instant) -> Result<String> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1];
    while bytes.len() < MAX_HANDSHAKE_BYTES {
        read_exact_required(stream, &mut buffer, deadline)?;
        bytes.push(buffer[0]);
        if bytes.ends_with(b"\r\n\r\n") {
            return String::from_utf8(bytes).context("decode app-server websocket handshake");
        }
    }
    bail!("app-server websocket handshake exceeded {MAX_HANDSHAKE_BYTES} bytes")
}

fn validate_websocket_handshake_response(response: &str, websocket_key: &str) -> Result<()> {
    let mut lines = response.split("\r\n");
    let status = lines.next().unwrap_or_default();
    if !status.starts_with("HTTP/1.1 101 ") && !status.starts_with("HTTP/1.0 101 ") {
        bail!("app-server websocket handshake did not return HTTP 101");
    }
    let expected_accept = websocket_accept_key(websocket_key);
    let accept = lines
        .filter_map(|line| line.split_once(':'))
        .find_map(|(name, value)| {
            name.eq_ignore_ascii_case("Sec-WebSocket-Accept")
                .then(|| value.trim())
        })
        .ok_or_else(|| anyhow::anyhow!("app-server websocket handshake missing accept header"))?;
    if accept != expected_accept {
        bail!("app-server websocket handshake accept header did not match request key");
    }
    Ok(())
}

fn generate_websocket_key(random_source: &mut File) -> Result<String> {
    let mut nonce = [0_u8; 16];
    fill_random(random_source, &mut nonce)?;
    Ok(base64_encode(&nonce))
}

fn fill_random(random_source: &mut File, buffer: &mut [u8]) -> Result<()> {
    random_source
        .read_exact(buffer)
        .context("read system randomness")
}

fn websocket_accept_key(websocket_key: &str) -> String {
    let mut material = Vec::with_capacity(websocket_key.len() + WEBSOCKET_GUID.len());
    material.extend_from_slice(websocket_key.as_bytes());
    material.extend_from_slice(WEBSOCKET_GUID.as_bytes());
    base64_encode(&sha1_digest(&material))
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut chunks = bytes.chunks_exact(3);
    for chunk in &mut chunks {
        let value = (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]);
        encoded.push(TABLE[((value >> 18) & 0x3F) as usize] as char);
        encoded.push(TABLE[((value >> 12) & 0x3F) as usize] as char);
        encoded.push(TABLE[((value >> 6) & 0x3F) as usize] as char);
        encoded.push(TABLE[(value & 0x3F) as usize] as char);
    }
    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let first = remainder[0];
        let second = remainder.get(1).copied().unwrap_or(0);
        let value = (u32::from(first) << 16) | (u32::from(second) << 8);
        encoded.push(TABLE[((value >> 18) & 0x3F) as usize] as char);
        encoded.push(TABLE[((value >> 12) & 0x3F) as usize] as char);
        if remainder.len() == 2 {
            encoded.push(TABLE[((value >> 6) & 0x3F) as usize] as char);
        } else {
            encoded.push('=');
        }
        encoded.push('=');
    }
    encoded
}

fn sha1_digest(input: &[u8]) -> [u8; 20] {
    let mut message = input.to_vec();
    let bit_len = (message.len() as u64).wrapping_mul(8);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    let mut h0 = 0x67452301_u32;
    let mut h1 = 0xEFCDAB89_u32;
    let mut h2 = 0x98BADCFE_u32;
    let mut h3 = 0x10325476_u32;
    let mut h4 = 0xC3D2E1F0_u32;

    for chunk in message.chunks_exact(64) {
        let mut words = [0_u32; 80];
        for (idx, word) in words[..16].iter_mut().enumerate() {
            let offset = idx * 4;
            *word = u32::from_be_bytes([
                chunk[offset],
                chunk[offset + 1],
                chunk[offset + 2],
                chunk[offset + 3],
            ]);
        }
        for idx in 16..80 {
            words[idx] = (words[idx - 3] ^ words[idx - 8] ^ words[idx - 14] ^ words[idx - 16])
                .rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;
        for (idx, word) in words.iter().enumerate() {
            let (f, k) = match idx {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut digest = [0_u8; 20];
    for (idx, word) in [h0, h1, h2, h3, h4].iter().enumerate() {
        digest[idx * 4..idx * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

fn read_exact_required(stream: &mut TcpStream, buffer: &mut [u8], deadline: Instant) -> Result<()> {
    match read_exact_or_timeout(stream, buffer, deadline)? {
        true => Ok(()),
        false => bail!("timed out while reading app-server websocket frame"),
    }
}

fn read_exact_or_timeout(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    deadline: Instant,
) -> Result<bool> {
    let mut read = 0;
    while read < buffer.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            if read == 0 {
                return Ok(false);
            }
            bail!("timed out mid-frame while reading app-server websocket");
        }
        stream
            .set_read_timeout(Some(remaining))
            .context("configure app-server websocket read deadline")?;
        match stream.read(&mut buffer[read..]) {
            Ok(0) => bail!("app-server websocket closed"),
            Ok(n) => read += n,
            Err(error)
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
                    && read == 0 =>
            {
                if deadline.saturating_duration_since(Instant::now()).is_zero() {
                    return Ok(false);
                }
            }
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                if deadline.saturating_duration_since(Instant::now()).is_zero() {
                    bail!("timed out mid-frame while reading app-server websocket");
                }
            }
            Err(error) => return Err(error).context("read app-server websocket"),
        }
    }
    Ok(true)
}

fn write_all_until(stream: &mut TcpStream, bytes: &[u8], deadline: Instant) -> Result<()> {
    let mut written = 0;
    while written < bytes.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("timed out while writing app-server websocket frame");
        }
        stream
            .set_write_timeout(Some(remaining))
            .context("configure app-server websocket write deadline")?;
        match stream.write(&bytes[written..]) {
            Ok(0) => bail!("app-server websocket write made no progress"),
            Ok(n) => written += n,
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                if deadline.saturating_duration_since(Instant::now()).is_zero() {
                    bail!("timed out while writing app-server websocket frame");
                }
            }
            Err(error) => return Err(error).context("write app-server websocket"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    use serde_json::json;

    use super::{
        AppServerJsonRpcClient, AppServerNotification, AppServerReceive, ThreadActivitySnapshot,
        decode_notification, thread_result_activity_snapshot,
        validate_websocket_handshake_response, websocket_accept_key,
    };

    #[test]
    fn websocket_accept_key_matches_rfc_sample() {
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        assert_eq!(websocket_accept_key(key), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
        validate_websocket_handshake_response(
            "HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n",
            key,
        )
        .expect("valid handshake response");
        assert!(
            validate_websocket_handshake_response(
                "HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: fake\r\n\r\n",
                key,
            )
            .is_err()
        );
    }

    #[test]
    fn recv_timeout_is_absolute_across_control_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test websocket");
        let url = format!("ws://{}", listener.local_addr().expect("local address"));
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let result = serve_control_frame_stream(listener);
            let _ = done_tx.send(result);
        });

        let mut client =
            AppServerJsonRpcClient::connect(&url, Duration::from_secs(1)).expect("connect client");
        let started = Instant::now();
        let received = client
            .recv(Duration::from_millis(100))
            .expect("bounded recv");

        assert!(matches!(received, AppServerReceive::Timeout));
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "recv timeout was extended by control frames: {:?}",
            started.elapsed()
        );
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("server result")
            .expect("server succeeded");
    }

    #[test]
    fn recv_rejects_oversized_control_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test websocket");
        let url = format!("ws://{}", listener.local_addr().expect("local address"));
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let result = serve_oversized_ping(listener);
            let _ = done_tx.send(result);
        });

        let mut client =
            AppServerJsonRpcClient::connect(&url, Duration::from_secs(1)).expect("connect client");
        let result = client.recv(Duration::from_secs(1));

        assert!(
            result.as_ref().is_err_and(|error| error
                .to_string()
                .contains("control frame exceeds 125 bytes")),
            "unexpected oversized ping result: {:?}",
            result.err()
        );
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("server result")
            .expect("server succeeded");
    }

    fn serve_control_frame_stream(listener: TcpListener) -> Result<(), String> {
        let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
        write_test_handshake_response(&mut stream)?;
        let deadline = Instant::now() + Duration::from_millis(300);
        while Instant::now() < deadline {
            if stream.write_all(&[0x89, 0x00]).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    fn serve_oversized_ping(listener: TcpListener) -> Result<(), String> {
        let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
        write_test_handshake_response(&mut stream)?;
        let mut frame = vec![0x89, 126, 0, 126];
        frame.extend(std::iter::repeat_n(b'x', 126));
        stream.write_all(&frame).map_err(|error| error.to_string())
    }

    fn write_test_handshake_response(stream: &mut TcpStream) -> Result<(), String> {
        let request = read_test_http_request(stream)?;
        let websocket_key = request
            .split("\r\n")
            .filter_map(|line| line.split_once(':'))
            .find_map(|(name, value)| {
                name.eq_ignore_ascii_case("Sec-WebSocket-Key")
                    .then(|| value.trim().to_owned())
            })
            .ok_or_else(|| "missing websocket key".to_owned())?;
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {}\r\n\
             \r\n",
            websocket_accept_key(&websocket_key)
        );
        stream
            .write_all(response.as_bytes())
            .map_err(|error| error.to_string())
    }

    fn read_test_http_request(stream: &mut TcpStream) -> Result<String, String> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1];
        while bytes.len() < 8192 {
            stream
                .read_exact(&mut buffer)
                .map_err(|error| error.to_string())?;
            bytes.push(buffer[0]);
            if bytes.ends_with(b"\r\n\r\n") {
                return String::from_utf8(bytes).map_err(|error| error.to_string());
            }
        }
        Err("handshake request exceeded limit".to_owned())
    }

    #[test]
    fn thread_result_activity_state_reads_nested_thread_turns() {
        let result = json!({
            "thread": {
                "id": "thread-1",
                "turns": [
                    { "id": "turn-1", "status": "completed" },
                    { "id": "turn-2", "status": "inProgress" }
                ]
            }
        });

        assert_eq!(
            thread_result_activity_snapshot(&result, "thread-1"),
            ThreadActivitySnapshot::Active
        );
    }

    #[test]
    fn thread_result_activity_state_reads_top_level_turns() {
        let result = json!({
            "id": "thread-1",
            "turns": [
                { "id": "turn-1", "status": "completed" }
            ]
        });

        assert_eq!(
            thread_result_activity_snapshot(&result, "thread-1"),
            ThreadActivitySnapshot::Idle
        );
    }

    #[test]
    fn thread_result_activity_state_prefers_thread_status() {
        let active = json!({
            "thread": {
                "id": "thread-1",
                "status": { "type": "active", "activeFlags": [] },
                "turns": [
                    { "id": "turn-1", "status": "completed" }
                ]
            }
        });
        assert_eq!(
            thread_result_activity_snapshot(&active, "thread-1"),
            ThreadActivitySnapshot::Active
        );

        let idle = json!({
            "thread": {
                "id": "thread-1",
                "status": { "type": "idle" },
                "turns": [
                    { "id": "turn-1", "status": "inProgress" }
                ]
            }
        });
        assert_eq!(
            thread_result_activity_snapshot(&idle, "thread-1"),
            ThreadActivitySnapshot::Idle
        );
    }

    #[test]
    fn thread_result_activity_state_rejects_unknown_thread_status() {
        let result = json!({
            "thread": {
                "id": "thread-1",
                "status": { "type": "systemError" },
                "turns": []
            }
        });

        assert_eq!(
            thread_result_activity_snapshot(&result, "thread-1"),
            ThreadActivitySnapshot::Untrusted
        );
    }

    #[test]
    fn thread_result_activity_state_rejects_malformed_thread_status() {
        let missing_type = json!({
            "thread": {
                "id": "thread-1",
                "status": {},
                "turns": []
            }
        });
        assert_eq!(
            thread_result_activity_snapshot(&missing_type, "thread-1"),
            ThreadActivitySnapshot::Untrusted
        );

        let non_string_type = json!({
            "thread": {
                "id": "thread-1",
                "status": { "type": 1 },
                "turns": [
                    { "id": "turn-1", "status": "completed" }
                ]
            }
        });
        assert_eq!(
            thread_result_activity_snapshot(&non_string_type, "thread-1"),
            ThreadActivitySnapshot::Untrusted
        );
    }

    #[test]
    fn thread_result_activity_state_requires_turn_array() {
        let result = json!({
            "thread": {
                "id": "thread-1"
            }
        });

        assert_eq!(
            thread_result_activity_snapshot(&result, "thread-1"),
            ThreadActivitySnapshot::Missing
        );
    }

    #[test]
    fn thread_result_activity_state_rejects_unknown_turn_status() {
        let result = json!({
            "turns": [
                { "id": "turn-1", "status": "mystery" }
            ]
        });

        assert_eq!(
            thread_result_activity_snapshot(&result, "thread-1"),
            ThreadActivitySnapshot::Untrusted
        );
    }

    #[test]
    fn thread_result_activity_state_treats_replaced_as_terminal() {
        let result = json!({
            "id": "thread-1",
            "turns": [
                { "id": "turn-1", "status": "replaced" }
            ]
        });

        assert_eq!(
            thread_result_activity_snapshot(&result, "thread-1"),
            ThreadActivitySnapshot::Idle
        );
    }

    #[test]
    fn thread_result_activity_snapshot_rejects_missing_or_foreign_thread_id() {
        let top_level_foreign_without_snapshot = json!({
            "id": "thread-other"
        });
        assert_eq!(
            thread_result_activity_snapshot(&top_level_foreign_without_snapshot, "thread-1"),
            ThreadActivitySnapshot::Untrusted
        );

        let missing = json!({
            "thread": {
                "status": { "type": "idle" },
                "turns": []
            }
        });
        assert_eq!(
            thread_result_activity_snapshot(&missing, "thread-1"),
            ThreadActivitySnapshot::Untrusted
        );

        let foreign = json!({
            "thread": {
                "id": "thread-other",
                "status": { "type": "idle" },
                "turns": []
            }
        });
        assert_eq!(
            thread_result_activity_snapshot(&foreign, "thread-1"),
            ThreadActivitySnapshot::Untrusted
        );
    }

    #[test]
    fn thread_status_changed_only_treats_idle_as_idle_proof() {
        let system_error = json!({
            "method": "thread/status/changed",
            "params": {
                "threadId": "thread-1",
                "status": { "type": "systemError" }
            }
        });
        assert!(matches!(
            decode_notification(&system_error),
            Some(AppServerNotification::ThreadProofInvalidated { .. })
        ));

        let idle = json!({
            "method": "thread/status/changed",
            "params": {
                "threadId": "thread-1",
                "status": { "type": "idle" }
            }
        });
        assert!(matches!(
            decode_notification(&idle),
            Some(AppServerNotification::ThreadActivityChanged { active: false, .. })
        ));

        let malformed = json!({
            "method": "thread/status/changed",
            "params": {
                "threadId": "thread-1",
                "status": {}
            }
        });
        assert!(matches!(
            decode_notification(&malformed),
            Some(AppServerNotification::ThreadProofInvalidated { .. })
        ));
    }

    #[test]
    fn turn_completed_notification_requires_known_terminal_status() {
        let unknown = json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-1",
                "turn": { "id": "turn-1", "status": "mystery" }
            }
        });
        assert!(decode_notification(&unknown).is_none());

        let completed = json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-1",
                "turn": { "id": "turn-1", "status": "completed" }
            }
        });
        assert!(matches!(
            decode_notification(&completed),
            Some(AppServerNotification::TurnTerminal { .. })
        ));

        let replaced = json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-1",
                "turn": { "id": "turn-1", "status": "replaced" }
            }
        });
        assert!(matches!(
            decode_notification(&replaced),
            Some(AppServerNotification::TurnTerminal { .. })
        ));
    }
}
