use std::fmt;
use std::io::{self, Read, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const PLUGIN_RPC_PROTOCOL_VERSION_V1: u32 = 1;
pub const PLUGIN_RPC_SUPPORTED_PROTOCOL_VERSIONS: &[u32] = &[PLUGIN_RPC_PROTOCOL_VERSION_V1];
pub const PLUGIN_RPC_MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;
pub const PLUGIN_RPC_HELLO_METHOD: &str = "plugin.hello";

const PLUGIN_RPC_JSONRPC_VERSION: &str = "2.0";
const FRAME_LENGTH_PREFIX_BYTES: usize = 4;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRpcRequestFrame {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

impl PluginRpcRequestFrame {
    pub fn new(id: impl Into<String>, method: impl Into<String>, params: Value) -> Self {
        Self {
            jsonrpc: PLUGIN_RPC_JSONRPC_VERSION.to_owned(),
            id: id.into(),
            method: method.into(),
            params,
        }
    }

    pub fn plugin_hello(
        id: impl Into<String>,
        request: PluginHelloRequest,
    ) -> Result<Self, PluginRpcError> {
        Ok(Self::new(
            id,
            PLUGIN_RPC_HELLO_METHOD,
            serde_json::to_value(request).map_err(PluginRpcError::internal)?,
        ))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRpcResponseFrame {
    pub jsonrpc: String,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<PluginRpcError>,
}

impl PluginRpcResponseFrame {
    pub fn success(id: impl Into<String>, result: Value) -> Self {
        Self {
            jsonrpc: PLUGIN_RPC_JSONRPC_VERSION.to_owned(),
            id: id.into(),
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(id: impl Into<String>, error: PluginRpcError) -> Self {
        Self {
            jsonrpc: PLUGIN_RPC_JSONRPC_VERSION.to_owned(),
            id: id.into(),
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHelloRequest {
    pub plugin_name: String,
    pub plugin_instance_id: String,
    pub plugin_release_id: String,
    pub protocol_versions: Vec<u32>,
    #[serde(default)]
    pub capabilities: Vec<PluginCapability>,
    pub plugin_home: String,
    pub pid: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHelloResponse {
    pub protocol_version: u32,
    pub service_capabilities: Vec<ServiceCapability>,
    pub policy: PluginRpcPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daemon_endpoint: Option<DaemonEndpointHint>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginCapability {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}

impl PluginCapability {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: None,
        }
    }

    pub fn versioned(name: impl Into<String>, version: u32) -> Self {
        Self {
            name: name.into(),
            version: Some(version),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ServiceCapability {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}

impl ServiceCapability {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: None,
        }
    }

    pub fn versioned(name: impl Into<String>, version: u32) -> Self {
        Self {
            name: name.into(),
            version: Some(version),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRpcPolicy {
    pub max_frame_bytes: usize,
    pub requires_idempotency_key: bool,
}

impl Default for PluginRpcPolicy {
    fn default() -> Self {
        Self {
            max_frame_bytes: PLUGIN_RPC_MAX_FRAME_BYTES,
            requires_idempotency_key: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DaemonEndpointHint {
    pub transport: String,
    pub endpoint: String,
}

impl DaemonEndpointHint {
    pub fn uds(endpoint: impl Into<String>) -> Self {
        Self {
            transport: "uds".to_owned(),
            endpoint: endpoint.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginHandshakePolicy {
    pub supported_protocol_versions: Vec<u32>,
    pub required_plugin_capabilities: Vec<PluginCapability>,
    pub service_capabilities: Vec<ServiceCapability>,
    pub policy: PluginRpcPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daemon_endpoint: Option<DaemonEndpointHint>,
}

impl Default for PluginHandshakePolicy {
    fn default() -> Self {
        Self {
            supported_protocol_versions: PLUGIN_RPC_SUPPORTED_PROTOCOL_VERSIONS.to_vec(),
            required_plugin_capabilities: Vec::new(),
            service_capabilities: vec![
                ServiceCapability::new("plugin-rpc-v1"),
                ServiceCapability::new("plugin-hello"),
            ],
            policy: PluginRpcPolicy::default(),
            daemon_endpoint: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginRpcErrorKind {
    UnsupportedProtocol,
    MissingCapability,
    StaleLease,
    PolicyBlocked,
    TargetUnavailable,
    TransientDaemonUnavailable,
    MalformedFrame,
    FrameTooLarge,
    Io,
    InvalidRequest,
    MethodNotFound,
    Internal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PluginRpcError {
    pub kind: PluginRpcErrorKind,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl PluginRpcError {
    pub fn new(kind: PluginRpcErrorKind, message: impl Into<String>) -> Self {
        let retryable = matches!(
            kind,
            PluginRpcErrorKind::Io | PluginRpcErrorKind::TransientDaemonUnavailable
        );
        Self {
            kind,
            message: message.into(),
            retryable,
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn malformed_frame(message: impl Into<String>) -> Self {
        Self::new(PluginRpcErrorKind::MalformedFrame, message)
    }

    pub fn frame_too_large(frame_bytes: usize, max_frame_bytes: usize) -> Self {
        Self::new(
            PluginRpcErrorKind::FrameTooLarge,
            format!("plugin RPC frame is {frame_bytes} bytes, exceeds {max_frame_bytes} bytes"),
        )
        .with_details(json!({
            "frame_bytes": frame_bytes,
            "max_frame_bytes": max_frame_bytes,
        }))
    }

    pub fn io(error: io::Error) -> Self {
        Self::new(PluginRpcErrorKind::Io, error.to_string())
    }

    pub fn internal(error: impl fmt::Display) -> Self {
        Self::new(PluginRpcErrorKind::Internal, error.to_string())
    }
}

impl fmt::Display for PluginRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for PluginRpcError {}

pub fn read_plugin_rpc_frame<R, T>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<T, PluginRpcError>
where
    R: Read,
    T: DeserializeOwned,
{
    if max_frame_bytes == 0 {
        return Err(PluginRpcError::malformed_frame(
            "max frame byte budget must be greater than zero",
        ));
    }

    let mut prefix = [0_u8; FRAME_LENGTH_PREFIX_BYTES];
    read_exact_frame_bytes(reader, &mut prefix)?;
    let frame_bytes = u32::from_be_bytes(prefix) as usize;
    if frame_bytes == 0 {
        return Err(PluginRpcError::malformed_frame(
            "plugin RPC frame has zero-length payload",
        ));
    }
    if frame_bytes > max_frame_bytes {
        return Err(PluginRpcError::frame_too_large(
            frame_bytes,
            max_frame_bytes,
        ));
    }

    let mut payload = vec![0_u8; frame_bytes];
    read_exact_frame_bytes(reader, &mut payload)?;
    serde_json::from_slice(&payload)
        .map_err(|error| PluginRpcError::malformed_frame(format!("invalid JSON frame: {error}")))
}

pub fn write_plugin_rpc_frame<W, T>(
    writer: &mut W,
    frame: &T,
    max_frame_bytes: usize,
) -> Result<(), PluginRpcError>
where
    W: Write,
    T: Serialize,
{
    if max_frame_bytes == 0 {
        return Err(PluginRpcError::malformed_frame(
            "max frame byte budget must be greater than zero",
        ));
    }

    let payload = serde_json::to_vec(frame)
        .map_err(|error| PluginRpcError::malformed_frame(format!("encode JSON frame: {error}")))?;
    if payload.len() > max_frame_bytes {
        return Err(PluginRpcError::frame_too_large(
            payload.len(),
            max_frame_bytes,
        ));
    }
    if payload.len() > u32::MAX as usize {
        return Err(PluginRpcError::frame_too_large(
            payload.len(),
            u32::MAX as usize,
        ));
    }

    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(PluginRpcError::io)?;
    writer.write_all(&payload).map_err(PluginRpcError::io)?;
    writer.flush().map_err(PluginRpcError::io)
}

pub fn negotiate_plugin_hello(
    request: &PluginHelloRequest,
    policy: &PluginHandshakePolicy,
) -> Result<PluginHelloResponse, PluginRpcError> {
    let protocol_version = request
        .protocol_versions
        .iter()
        .copied()
        .filter(|version| policy.supported_protocol_versions.contains(version))
        .max()
        .ok_or_else(|| {
            PluginRpcError::new(
                PluginRpcErrorKind::UnsupportedProtocol,
                "plugin does not advertise a supported protocol version",
            )
            .with_details(json!({
                "plugin_protocol_versions": request.protocol_versions,
                "service_protocol_versions": policy.supported_protocol_versions,
            }))
        })?;

    let missing_capability = policy
        .required_plugin_capabilities
        .iter()
        .find(|required| !plugin_capabilities_contain(&request.capabilities, required));
    if let Some(required) = missing_capability {
        return Err(PluginRpcError::new(
            PluginRpcErrorKind::MissingCapability,
            format!("plugin is missing required capability '{}'", required.name),
        )
        .with_details(json!({
            "required_capability": required,
            "plugin_capabilities": request.capabilities,
        })));
    }

    Ok(PluginHelloResponse {
        protocol_version,
        service_capabilities: policy.service_capabilities.clone(),
        policy: policy.policy.clone(),
        daemon_endpoint: policy.daemon_endpoint.clone(),
    })
}

pub fn handle_plugin_hello_frame(
    frame: &PluginRpcRequestFrame,
    policy: &PluginHandshakePolicy,
) -> PluginRpcResponseFrame {
    if frame.method != PLUGIN_RPC_HELLO_METHOD {
        return PluginRpcResponseFrame::failure(
            frame.id.clone(),
            PluginRpcError::new(
                PluginRpcErrorKind::MethodNotFound,
                format!("expected {PLUGIN_RPC_HELLO_METHOD}, got {}", frame.method),
            ),
        );
    }

    let request = match serde_json::from_value::<PluginHelloRequest>(frame.params.clone()) {
        Ok(request) => request,
        Err(error) => {
            return PluginRpcResponseFrame::failure(
                frame.id.clone(),
                PluginRpcError::new(
                    PluginRpcErrorKind::InvalidRequest,
                    format!("invalid plugin hello request: {error}"),
                ),
            );
        }
    };

    match negotiate_plugin_hello(&request, policy) {
        Ok(response) => match serde_json::to_value(response) {
            Ok(result) => PluginRpcResponseFrame::success(frame.id.clone(), result),
            Err(error) => {
                PluginRpcResponseFrame::failure(frame.id.clone(), PluginRpcError::internal(error))
            }
        },
        Err(error) => PluginRpcResponseFrame::failure(frame.id.clone(), error),
    }
}

fn plugin_capabilities_contain(
    capabilities: &[PluginCapability],
    required: &PluginCapability,
) -> bool {
    capabilities.iter().any(|capability| {
        capability.name == required.name
            && required
                .version
                .is_none_or(|required_version| capability.version == Some(required_version))
    })
}

fn read_exact_frame_bytes<R: Read>(
    reader: &mut R,
    buffer: &mut [u8],
) -> Result<(), PluginRpcError> {
    reader.read_exact(buffer).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            PluginRpcError::malformed_frame("truncated plugin RPC frame")
        } else {
            PluginRpcError::io(error)
        }
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    use serde_json::json;

    use super::*;

    fn hello_request(protocol_versions: Vec<u32>) -> PluginHelloRequest {
        PluginHelloRequest {
            plugin_name: "webex".to_owned(),
            plugin_instance_id: "instance-1".to_owned(),
            plugin_release_id: "release-1".to_owned(),
            protocol_versions,
            capabilities: vec![PluginCapability::new("health")],
            plugin_home: "/tmp/cbth-plugin-webex".to_owned(),
            pid: 42,
        }
    }

    #[test]
    fn plugin_hello_negotiates_highest_common_version() {
        let request = hello_request(vec![0, PLUGIN_RPC_PROTOCOL_VERSION_V1]);
        let policy = PluginHandshakePolicy {
            required_plugin_capabilities: vec![PluginCapability::new("health")],
            service_capabilities: vec![ServiceCapability::new("service-health")],
            daemon_endpoint: Some(DaemonEndpointHint::uds("/tmp/cbth-daemon.sock")),
            ..PluginHandshakePolicy::default()
        };

        let response = negotiate_plugin_hello(&request, &policy).expect("negotiate hello");

        assert_eq!(response.protocol_version, PLUGIN_RPC_PROTOCOL_VERSION_V1);
        assert_eq!(
            response.service_capabilities,
            vec![ServiceCapability::new("service-health")]
        );
        assert_eq!(
            response.daemon_endpoint,
            Some(DaemonEndpointHint::uds("/tmp/cbth-daemon.sock"))
        );
    }

    #[test]
    fn plugin_hello_rejects_unsupported_protocol() {
        let request = hello_request(vec![999]);
        let error = negotiate_plugin_hello(&request, &PluginHandshakePolicy::default())
            .expect_err("unsupported protocol should fail");

        assert_eq!(error.kind, PluginRpcErrorKind::UnsupportedProtocol);
        assert!(error.details.is_some());
    }

    #[test]
    fn plugin_hello_rejects_missing_required_capability() {
        let request = hello_request(vec![PLUGIN_RPC_PROTOCOL_VERSION_V1]);
        let policy = PluginHandshakePolicy {
            required_plugin_capabilities: vec![PluginCapability::new("handoff")],
            ..PluginHandshakePolicy::default()
        };

        let error =
            negotiate_plugin_hello(&request, &policy).expect_err("missing capability should fail");

        assert_eq!(error.kind, PluginRpcErrorKind::MissingCapability);
        assert!(error.message.contains("handoff"));
    }

    #[test]
    fn frame_codec_roundtrips_request_on_persistent_stream() {
        let frame =
            PluginRpcRequestFrame::plugin_hello("1", hello_request(vec![1])).expect("hello frame");
        let mut stream = Vec::new();
        write_plugin_rpc_frame(&mut stream, &frame, PLUGIN_RPC_MAX_FRAME_BYTES)
            .expect("write frame");
        write_plugin_rpc_frame(&mut stream, &frame, PLUGIN_RPC_MAX_FRAME_BYTES)
            .expect("write second frame");

        let mut reader = Cursor::new(stream);
        let first: PluginRpcRequestFrame =
            read_plugin_rpc_frame(&mut reader, PLUGIN_RPC_MAX_FRAME_BYTES)
                .expect("read first frame");
        let second: PluginRpcRequestFrame =
            read_plugin_rpc_frame(&mut reader, PLUGIN_RPC_MAX_FRAME_BYTES)
                .expect("read second frame");

        assert_eq!(first, frame);
        assert_eq!(second, frame);
    }

    #[test]
    fn frame_codec_rejects_malformed_json_frame() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(1_u32).to_be_bytes());
        bytes.extend_from_slice(b"{");

        let error: PluginRpcError =
            read_plugin_rpc_frame::<_, PluginRpcRequestFrame>(&mut Cursor::new(bytes), 1024)
                .expect_err("malformed JSON should fail");

        assert_eq!(error.kind, PluginRpcErrorKind::MalformedFrame);
    }

    #[test]
    fn frame_codec_rejects_truncated_frame() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(10_u32).to_be_bytes());
        bytes.extend_from_slice(b"{}");

        let error: PluginRpcError =
            read_plugin_rpc_frame::<_, PluginRpcRequestFrame>(&mut Cursor::new(bytes), 1024)
                .expect_err("truncated frame should fail");

        assert_eq!(error.kind, PluginRpcErrorKind::MalformedFrame);
    }

    #[test]
    fn frame_codec_rejects_oversized_frame_without_reading_payload() {
        struct CountingReader {
            bytes: Cursor<Vec<u8>>,
            bytes_read: usize,
        }

        impl Read for CountingReader {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                let count = self.bytes.read(buffer)?;
                self.bytes_read += count;
                Ok(count)
            }
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(128_u32).to_be_bytes());
        bytes.extend_from_slice(&[b'x'; 128]);
        let mut reader = CountingReader {
            bytes: Cursor::new(bytes),
            bytes_read: 0,
        };

        let error: PluginRpcError =
            read_plugin_rpc_frame::<_, PluginRpcRequestFrame>(&mut reader, 16)
                .expect_err("oversized frame should fail");

        assert_eq!(error.kind, PluginRpcErrorKind::FrameTooLarge);
        assert_eq!(reader.bytes_read, FRAME_LENGTH_PREFIX_BYTES);
    }

    #[test]
    fn error_serialization_roundtrips() {
        let error = PluginRpcError::new(
            PluginRpcErrorKind::PolicyBlocked,
            "plugin policy blocks this request",
        )
        .with_details(json!({"policy": "shadow_plugin"}));

        let encoded = serde_json::to_string(&error).expect("serialize error");
        let decoded: PluginRpcError = serde_json::from_str(&encoded).expect("deserialize error");

        assert_eq!(decoded, error);
    }

    #[test]
    fn handle_plugin_hello_frame_serializes_success_response() {
        let frame = PluginRpcRequestFrame::plugin_hello(
            "hello-1",
            hello_request(vec![PLUGIN_RPC_PROTOCOL_VERSION_V1]),
        )
        .expect("hello frame");

        let response = handle_plugin_hello_frame(&frame, &PluginHandshakePolicy::default());
        let result: PluginHelloResponse =
            serde_json::from_value(response.result.expect("hello result")).expect("hello result");

        assert_eq!(response.id, "hello-1");
        assert!(response.error.is_none());
        assert_eq!(result.protocol_version, PLUGIN_RPC_PROTOCOL_VERSION_V1);
    }
}
