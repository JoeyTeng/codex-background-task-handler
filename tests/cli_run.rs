use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

fn temp_home() -> TempDir {
    let home = tempfile::tempdir().expect("temp home");
    #[cfg(unix)]
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o700)).expect("chmod temp home");
    home
}

#[cfg(unix)]
fn fake_codex_script(dir: &TempDir) -> std::path::PathBuf {
    write_fake_codex_script(&dir.path().join("fake-codex"))
}

#[cfg(unix)]
fn write_fake_codex_script(path: &Path) -> PathBuf {
    fs::write(
        path,
        r#"#!/bin/sh
log="${FAKE_CODEX_LOG:?}"
if [ "${1:-}" = "app-server" ]; then
  printf 'app-server' >> "$log"
  for arg in "$@"; do
    printf '\t%s' "$arg" >> "$log"
  done
  printf '\n' >> "$log"
  if [ -n "${FAKE_CODEX_APP_SERVER_PREFIX_BYTES:-}" ]; then
    i=0
    while [ "$i" -lt "$FAKE_CODEX_APP_SERVER_PREFIX_BYTES" ]; do
      printf x
      i=$((i + 1))
    done
  fi
  url="${FAKE_CODEX_APP_SERVER_URL:-ws://127.0.0.1:45678}"
  printf 'codex app-server\n'
  if [ -n "${FAKE_CODEX_APP_SERVER_STARTUP_SLEEP_SECONDS:-}" ]; then
    sleep "$FAKE_CODEX_APP_SERVER_STARTUP_SLEEP_SECONDS"
  fi
  printf '  listening on: %s\n' "$url"
  if [ "${FAKE_CODEX_APP_SERVER_GRANDCHILD_STDOUT:-}" = "1" ]; then
    (trap '' TERM; while :; do sleep 1; done) &
    exit 0
  fi
  while :; do
    sleep 1
  done
fi

printf 'foreground' >> "$log"
for arg in "$@"; do
  printf '\t%s' "$arg" >> "$log"
done
printf '\n' >> "$log"
if [ -n "${FAKE_CODEX_FOREGROUND_SLEEP_SECONDS:-}" ]; then
  sleep "$FAKE_CODEX_FOREGROUND_SLEEP_SECONDS"
fi
exit 0
"#,
    )
    .expect("write fake codex");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod fake codex");
    path.to_path_buf()
}

#[cfg(unix)]
fn wait_for_log_contains(path: &std::path::Path, needle: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if fs::read_to_string(path).is_ok_and(|log| log.contains(needle)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for log entry {needle:?}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
fn wait_with_timeout(mut child: Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().expect("poll child") {
            Some(_status) => return child.wait_with_output().expect("collect child output"),
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let output = child
                    .wait_with_output()
                    .expect("collect timed-out child output");
                panic!(
                    "child timed out\nstatus: {}\nstdout: {}\nstderr: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            None => thread::sleep(Duration::from_millis(50)),
        }
    }
}

#[cfg(unix)]
fn cbth_direct_json(home: &TempDir, args: &[&str]) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .env("CBTH_ALLOW_DIRECT_STORE", "1")
        .arg("--direct-store")
        .arg("--home")
        .arg(home.path())
        .args(args)
        .output()
        .expect("run cbth");

    assert!(
        output.status.success(),
        "cbth failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).expect("valid json output")
}

#[cfg(unix)]
fn wait_for_cli_activity_state(home: &TempDir, bound_thread_id: &str, expected: &str) -> i64 {
    let db_path = home.path().join("cbth.sqlite3");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if db_path.exists()
            && let Ok(conn) = Connection::open(&db_path)
        {
            let queried: rusqlite::Result<(String, i64)> = conn.query_row(
                "SELECT activity_state, activity_revision
                 FROM cli_managed_sessions
                 WHERE bound_thread_id = ?",
                [bound_thread_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            );
            if let Ok((activity_state, activity_revision)) = queried
                && activity_state == expected
            {
                return activity_revision;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {bound_thread_id} activity_state={expected}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
fn spawn_fake_app_server(thread_id: &'static str) -> (String, mpsc::Receiver<Result<(), String>>) {
    spawn_fake_app_server_with_options(thread_id, true, true, false, true, Duration::from_secs(5))
}

#[cfg(unix)]
fn spawn_fake_app_server_without_turn_snapshot(
    thread_id: &'static str,
) -> (String, mpsc::Receiver<Result<(), String>>) {
    spawn_fake_app_server_with_options(thread_id, false, false, false, true, Duration::from_secs(5))
}

#[cfg(unix)]
fn spawn_fake_app_server_started_before_read_snapshot(
    thread_id: &'static str,
) -> (String, mpsc::Receiver<Result<(), String>>) {
    spawn_fake_app_server_with_options(thread_id, true, false, true, true, Duration::from_secs(5))
}

#[cfg(unix)]
fn spawn_fake_app_server_started_before_failed_read(
    thread_id: &'static str,
) -> (String, mpsc::Receiver<Result<(), String>>) {
    spawn_fake_app_server_with_options(thread_id, true, false, true, false, Duration::from_secs(5))
}

#[cfg(unix)]
fn spawn_fake_app_server_idle_before_active_read_snapshot(
    thread_id: &'static str,
) -> (String, mpsc::Receiver<Result<(), String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake app-server");
    listener
        .set_nonblocking(true)
        .expect("set fake app-server nonblocking");
    let url = format!("ws://{}", listener.local_addr().expect("local address"));
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        let result = accept_fake_app_server_idle_before_active_read_snapshot(&listener, thread_id);
        let _ = done_tx.send(result);
    });

    (url, done_rx)
}

#[cfg(unix)]
fn spawn_fake_app_server_untrusted_read_snapshot(
    thread_id: &'static str,
) -> (String, mpsc::Receiver<Result<(), String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake app-server");
    listener
        .set_nonblocking(true)
        .expect("set fake app-server nonblocking");
    let url = format!("ws://{}", listener.local_addr().expect("local address"));
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        let result = accept_fake_app_server_untrusted_read_snapshot(&listener, thread_id);
        let _ = done_tx.send(result);
    });

    (url, done_rx)
}

#[cfg(unix)]
fn spawn_fake_app_server_resume_started_before_missing_read_snapshot(
    thread_id: &'static str,
) -> (String, mpsc::Receiver<Result<(), String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake app-server");
    listener
        .set_nonblocking(true)
        .expect("set fake app-server nonblocking");
    let url = format!("ws://{}", listener.local_addr().expect("local address"));
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        let result = accept_fake_app_server_resume_started_before_missing_read_snapshot(
            &listener, thread_id,
        );
        let _ = done_tx.send(result);
    });

    (url, done_rx)
}

#[cfg(unix)]
fn spawn_fake_app_server_reconnect_without_turn_snapshot(
    thread_id: &'static str,
) -> (String, mpsc::Receiver<Result<(), String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake app-server");
    listener
        .set_nonblocking(true)
        .expect("set fake app-server nonblocking");
    let url = format!("ws://{}", listener.local_addr().expect("local address"));
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        let result = (|| {
            accept_fake_app_server(
                &listener,
                thread_id,
                true,
                false,
                false,
                true,
                Duration::from_millis(100),
            )?;
            accept_fake_app_server(
                &listener,
                thread_id,
                false,
                false,
                false,
                true,
                Duration::from_secs(5),
            )
        })();
        let _ = done_tx.send(result);
    });

    (url, done_rx)
}

#[cfg(unix)]
fn spawn_fake_app_server_capture_passive_methods(
    thread_id: &'static str,
) -> (String, mpsc::Receiver<Result<Vec<String>, String>>) {
    spawn_fake_app_server_capture_passive_methods_for(thread_id, Duration::from_secs(2))
}

#[cfg(unix)]
fn spawn_fake_app_server_capture_passive_methods_for(
    thread_id: &'static str,
    observe_duration: Duration,
) -> (String, mpsc::Receiver<Result<Vec<String>, String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake app-server");
    listener
        .set_nonblocking(true)
        .expect("set fake app-server nonblocking");
    let url = format!("ws://{}", listener.local_addr().expect("local address"));
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        let result =
            accept_fake_app_server_capture_passive_methods(&listener, thread_id, observe_duration);
        let _ = done_tx.send(result);
    });

    (url, done_rx)
}

#[cfg(unix)]
#[derive(Clone, Copy)]
enum FakeAutoDeliveryOutcome {
    CompletedNotification,
    TwoCompletedNotifications,
    CompletedReconcile,
    FailedNotification,
    RejectedBeforeAccept,
    RejectThenComplete,
    ClosedBeforeAccept,
}

#[cfg(unix)]
fn spawn_fake_app_server_auto_delivery(
    thread_id: &'static str,
    outcome: FakeAutoDeliveryOutcome,
) -> (String, mpsc::Receiver<Result<Vec<String>, String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake app-server");
    listener
        .set_nonblocking(true)
        .expect("set fake app-server nonblocking");
    let url = format!("ws://{}", listener.local_addr().expect("local address"));
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        let result = accept_fake_app_server_auto_delivery(&listener, thread_id, outcome);
        let _ = done_tx.send(result);
    });

    (url, done_rx)
}

#[cfg(unix)]
fn spawn_fake_app_server_with_options(
    thread_id: &'static str,
    include_turns: bool,
    send_notifications: bool,
    send_started_before_read_response: bool,
    respond_to_thread_read: bool,
    hold_after_response: Duration,
) -> (String, mpsc::Receiver<Result<(), String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake app-server");
    listener
        .set_nonblocking(true)
        .expect("set fake app-server nonblocking");
    let url = format!("ws://{}", listener.local_addr().expect("local address"));
    let (done_tx, done_rx) = mpsc::channel();

    thread::spawn(move || {
        let result = accept_fake_app_server(
            &listener,
            thread_id,
            include_turns,
            send_notifications,
            send_started_before_read_response,
            respond_to_thread_read,
            hold_after_response,
        );
        let _ = done_tx.send(result);
    });

    (url, done_rx)
}

#[cfg(unix)]
fn accept_fake_app_server(
    listener: &TcpListener,
    thread_id: &'static str,
    include_turns: bool,
    send_notifications: bool,
    send_started_before_read_response: bool,
    respond_to_thread_read: bool,
    hold_after_response: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut stream, _) = loop {
        match listener.accept() {
            Ok(accepted) => break accepted,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(format!("accept fake app-server websocket: {error}")),
        }
    };

    stream
        .set_nonblocking(false)
        .map_err(|error| format!("set fake app-server stream blocking: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|error| format!("set fake app-server read timeout: {error}"))?;
    let websocket_accept = read_fake_http_upgrade(&mut stream)?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {websocket_accept}\r\n\r\n"
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|error| format!("write fake app-server handshake: {error}"))?;

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut saw_thread_read = false;
    let mut messages_seen = 0;
    while !saw_thread_read && Instant::now() < deadline {
        let message = read_fake_client_text_frame(&mut stream)
            .map_err(|error| format!("{error} after {messages_seen} fake messages"))?;
        messages_seen += 1;
        let method = message
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match method {
            "initialize" => write_fake_json_response(
                &mut stream,
                &message,
                serde_json::json!({
                    "userAgent": "fake-codex",
                    "codexHome": "/tmp/fake-codex-home",
                    "platformFamily": "unix",
                    "platformOs": "macos"
                }),
            )?,
            "initialized" => {}
            "thread/resume" => {
                write_fake_thread_response(&mut stream, &message, thread_id, include_turns)?
            }
            "thread/read" => {
                if send_started_before_read_response {
                    write_fake_server_text_frame(
                        &mut stream,
                        &serde_json::json!({
                            "jsonrpc": "2.0",
                            "method": "turn/started",
                            "params": {
                                "threadId": thread_id,
                                "turn": { "id": "turn-buffered-start", "status": "inProgress", "items": [] }
                            }
                        }),
                    )?;
                }
                if !respond_to_thread_read {
                    thread::sleep(hold_after_response);
                    return Ok(());
                }
                write_fake_thread_response(&mut stream, &message, thread_id, include_turns)?;
                saw_thread_read = true;
            }
            _ => {}
        }
    }
    if !saw_thread_read {
        return Err("fake app-server did not receive thread/read".to_owned());
    }
    if !send_notifications {
        thread::sleep(hold_after_response);
        return Ok(());
    }

    write_fake_server_text_frame(
        &mut stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "method": "turn/started",
            "params": {
                "turn": { "id": "turn-missing-thread", "status": "inProgress", "items": [] }
            }
        }),
    )?;
    write_fake_server_text_frame(
        &mut stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "threadId": "thread-other",
                "turn": { "id": "turn-foreign-thread", "status": "completed", "items": [] }
            }
        }),
    )?;
    write_fake_server_text_frame(
        &mut stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "method": "thread/status/changed",
            "params": {
                "threadId": "thread-other",
                "status": { "type": "active", "activeFlags": [] }
            }
        }),
    )?;
    write_fake_server_text_frame(
        &mut stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "method": "turn/started",
            "params": {
                "threadId": thread_id,
                "turn": { "id": "turn-passive-1", "status": "inProgress", "items": [] }
            }
        }),
    )?;
    write_fake_server_text_frame(
        &mut stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": { "id": "turn-passive-1", "status": "completed", "items": [] }
            }
        }),
    )?;
    thread::sleep(hold_after_response);
    Ok(())
}

#[cfg(unix)]
fn accept_fake_app_server_capture_passive_methods(
    listener: &TcpListener,
    thread_id: &'static str,
    observe_duration: Duration,
) -> Result<Vec<String>, String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut stream, _) = loop {
        match listener.accept() {
            Ok(accepted) => break accepted,
            Err(error) if error.kind() == ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(format!("accept fake app-server websocket: {error}")),
        }
    };

    stream
        .set_nonblocking(false)
        .map_err(|error| format!("set fake app-server stream blocking: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|error| format!("set fake app-server read timeout: {error}"))?;
    let websocket_accept = read_fake_http_upgrade(&mut stream)?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {websocket_accept}\r\n\r\n"
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|error| format!("write fake app-server handshake: {error}"))?;

    let mut methods = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut saw_thread_read = false;
    while !saw_thread_read && Instant::now() < deadline {
        let message = read_fake_client_text_frame(&mut stream)?;
        let method = record_fake_app_server_method(&mut methods, &message)?;
        match method {
            "initialize" => write_fake_json_response(
                &mut stream,
                &message,
                serde_json::json!({
                    "userAgent": "fake-codex",
                    "codexHome": "/tmp/fake-codex-home",
                    "platformFamily": "unix",
                    "platformOs": "macos"
                }),
            )?,
            "initialized" => {}
            "thread/resume" => write_fake_thread_response(&mut stream, &message, thread_id, true)?,
            "thread/read" => {
                write_fake_thread_response(&mut stream, &message, thread_id, true)?;
                saw_thread_read = true;
            }
            _ => {}
        }
    }
    if !saw_thread_read {
        return Err(format!(
            "fake app-server did not receive thread/read; methods seen: {methods:?}"
        ));
    }

    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .map_err(|error| format!("set fake app-server post-read timeout: {error}"))?;
    let observe_until = Instant::now() + observe_duration;
    while Instant::now() < observe_until {
        match try_read_fake_client_text_frame(&mut stream)? {
            Some(message) => {
                let _ = record_fake_app_server_method(&mut methods, &message)?;
            }
            None => thread::sleep(Duration::from_millis(20)),
        }
    }

    Ok(methods)
}

#[cfg(unix)]
fn accept_fake_app_server_auto_delivery(
    listener: &TcpListener,
    thread_id: &'static str,
    outcome: FakeAutoDeliveryOutcome,
) -> Result<Vec<String>, String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut stream, _) = loop {
        match listener.accept() {
            Ok(accepted) => break accepted,
            Err(error) if error.kind() == ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(format!("accept fake app-server websocket: {error}")),
        }
    };

    stream
        .set_nonblocking(false)
        .map_err(|error| format!("set fake app-server stream blocking: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .map_err(|error| format!("set fake app-server read timeout: {error}"))?;
    let websocket_accept = read_fake_http_upgrade(&mut stream)?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {websocket_accept}\r\n\r\n"
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|error| format!("write fake app-server handshake: {error}"))?;

    let mut methods = Vec::new();
    let mut turn_start_count = 0;
    let mut terminal_count = 0;
    let mut saw_pre_accept_rejection = false;
    let deadline = Instant::now() + Duration::from_secs(12);
    while Instant::now() < deadline && !fake_auto_delivery_done(outcome, terminal_count) {
        let Some(message) = try_read_fake_client_text_frame(&mut stream)? else {
            thread::sleep(Duration::from_millis(20));
            continue;
        };
        let method = record_fake_app_server_method_allowing_delivery(&mut methods, &message)?;
        match method {
            "initialize" => write_fake_json_response(
                &mut stream,
                &message,
                serde_json::json!({
                    "userAgent": "fake-codex",
                    "codexHome": "/tmp/fake-codex-home",
                    "platformFamily": "unix",
                    "platformOs": "macos"
                }),
            )?,
            "initialized" => {}
            "thread/resume" => write_fake_thread_response(&mut stream, &message, thread_id, true)?,
            "thread/read" => {
                if matches!(outcome, FakeAutoDeliveryOutcome::CompletedReconcile)
                    && methods.iter().any(|method| method == "turn/start")
                {
                    let turn_id = fake_auto_delivery_turn_id(turn_start_count);
                    write_fake_thread_turn_response(
                        &mut stream,
                        &message,
                        thread_id,
                        &turn_id,
                        "completed",
                    )?;
                    terminal_count += 1;
                } else {
                    write_fake_thread_response(&mut stream, &message, thread_id, true)?;
                }
            }
            "turn/start" => {
                turn_start_count += 1;
                let params = message.get("params").unwrap_or(&serde_json::Value::Null);
                if params.get("threadId").and_then(serde_json::Value::as_str) != Some(thread_id) {
                    return Err(format!("turn/start targeted wrong thread: {params}"));
                }
                let input = params
                    .get("input")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(|item| item.get("text"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                if !input.contains("cbth-delivery-marker:")
                    || !input.contains("Policy: trusted-all")
                {
                    return Err(format!(
                        "turn/start prompt missing marker or policy: {input}"
                    ));
                }
                match outcome {
                    FakeAutoDeliveryOutcome::RejectedBeforeAccept => {
                        write_fake_json_error(
                            &mut stream,
                            &message,
                            -32000,
                            "caller thread is not idle",
                        )?;
                        terminal_count += 1;
                    }
                    FakeAutoDeliveryOutcome::RejectThenComplete if !saw_pre_accept_rejection => {
                        saw_pre_accept_rejection = true;
                        write_fake_json_error(
                            &mut stream,
                            &message,
                            -32000,
                            "caller thread is not idle",
                        )?;
                    }
                    FakeAutoDeliveryOutcome::ClosedBeforeAccept => return Ok(methods),
                    FakeAutoDeliveryOutcome::CompletedNotification
                    | FakeAutoDeliveryOutcome::TwoCompletedNotifications
                    | FakeAutoDeliveryOutcome::CompletedReconcile
                    | FakeAutoDeliveryOutcome::FailedNotification
                    | FakeAutoDeliveryOutcome::RejectThenComplete => {
                        let turn_id = fake_auto_delivery_turn_id(turn_start_count);
                        write_fake_json_response(
                            &mut stream,
                            &message,
                            serde_json::json!({
                                "turn": {
                                    "id": turn_id.clone(),
                                    "status": "inProgress",
                                    "items": []
                                }
                            }),
                        )?;
                        thread::sleep(Duration::from_millis(100));
                        match outcome {
                            FakeAutoDeliveryOutcome::CompletedNotification
                            | FakeAutoDeliveryOutcome::TwoCompletedNotifications
                            | FakeAutoDeliveryOutcome::RejectThenComplete => {
                                write_fake_turn_completed_notification(
                                    &mut stream,
                                    thread_id,
                                    &turn_id,
                                    "completed",
                                )?;
                                terminal_count += 1;
                            }
                            FakeAutoDeliveryOutcome::FailedNotification => {
                                write_fake_turn_completed_notification(
                                    &mut stream,
                                    thread_id,
                                    &turn_id,
                                    "failed",
                                )?;
                                terminal_count += 1;
                            }
                            FakeAutoDeliveryOutcome::CompletedReconcile
                            | FakeAutoDeliveryOutcome::RejectedBeforeAccept
                            | FakeAutoDeliveryOutcome::ClosedBeforeAccept => {}
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if !methods.iter().any(|method| method == "turn/start") {
        return Err(format!(
            "fake auto-delivery app-server did not receive turn/start; methods seen: {methods:?}"
        ));
    }
    if terminal_count > 0 {
        thread::sleep(Duration::from_millis(1500));
    }
    Ok(methods)
}

#[cfg(unix)]
fn fake_auto_delivery_turn_id(turn_start_count: usize) -> String {
    format!("turn-auto-delivery-{turn_start_count}")
}

#[cfg(unix)]
fn fake_auto_delivery_done(outcome: FakeAutoDeliveryOutcome, terminal_count: usize) -> bool {
    match outcome {
        FakeAutoDeliveryOutcome::TwoCompletedNotifications => terminal_count >= 2,
        _ => terminal_count >= 1,
    }
}

#[cfg(unix)]
fn accept_fake_app_server_resume_started_before_missing_read_snapshot(
    listener: &TcpListener,
    thread_id: &'static str,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut stream, _) = loop {
        match listener.accept() {
            Ok(accepted) => break accepted,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(format!("accept fake app-server websocket: {error}")),
        }
    };

    stream
        .set_nonblocking(false)
        .map_err(|error| format!("set fake app-server stream blocking: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|error| format!("set fake app-server read timeout: {error}"))?;
    let websocket_accept = read_fake_http_upgrade(&mut stream)?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {websocket_accept}\r\n\r\n"
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|error| format!("write fake app-server handshake: {error}"))?;

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut saw_thread_read = false;
    while !saw_thread_read && Instant::now() < deadline {
        let message = read_fake_client_text_frame(&mut stream)?;
        let method = message
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match method {
            "initialize" => write_fake_json_response(
                &mut stream,
                &message,
                serde_json::json!({
                    "userAgent": "fake-codex",
                    "codexHome": "/tmp/fake-codex-home",
                    "platformFamily": "unix",
                    "platformOs": "macos"
                }),
            )?,
            "initialized" => {}
            "thread/resume" => {
                write_fake_server_text_frame(
                    &mut stream,
                    &serde_json::json!({
                        "jsonrpc": "2.0",
                        "method": "turn/started",
                        "params": {
                            "threadId": thread_id,
                            "turn": { "id": "turn-resume-window", "status": "inProgress", "items": [] }
                        }
                    }),
                )?;
                write_fake_thread_response(&mut stream, &message, thread_id, true)?;
            }
            "thread/read" => {
                write_fake_thread_response(&mut stream, &message, thread_id, false)?;
                saw_thread_read = true;
            }
            _ => {}
        }
    }
    if !saw_thread_read {
        return Err("fake app-server did not receive thread/read".to_owned());
    }
    thread::sleep(Duration::from_secs(5));
    Ok(())
}

#[cfg(unix)]
fn accept_fake_app_server_idle_before_active_read_snapshot(
    listener: &TcpListener,
    thread_id: &'static str,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut stream, _) = loop {
        match listener.accept() {
            Ok(accepted) => break accepted,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(format!("accept fake app-server websocket: {error}")),
        }
    };

    stream
        .set_nonblocking(false)
        .map_err(|error| format!("set fake app-server stream blocking: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|error| format!("set fake app-server read timeout: {error}"))?;
    let websocket_accept = read_fake_http_upgrade(&mut stream)?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {websocket_accept}\r\n\r\n"
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|error| format!("write fake app-server handshake: {error}"))?;

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut saw_thread_read = false;
    while !saw_thread_read && Instant::now() < deadline {
        let message = read_fake_client_text_frame(&mut stream)?;
        let method = message
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match method {
            "initialize" => write_fake_json_response(
                &mut stream,
                &message,
                serde_json::json!({
                    "userAgent": "fake-codex",
                    "codexHome": "/tmp/fake-codex-home",
                    "platformFamily": "unix",
                    "platformOs": "macos"
                }),
            )?,
            "initialized" => {}
            "thread/resume" => write_fake_thread_response(&mut stream, &message, thread_id, true)?,
            "thread/read" => {
                write_fake_server_text_frame(
                    &mut stream,
                    &serde_json::json!({
                        "jsonrpc": "2.0",
                        "method": "thread/status/changed",
                        "params": {
                            "threadId": thread_id,
                            "status": { "type": "idle" }
                        }
                    }),
                )?;
                write_fake_active_thread_response(&mut stream, &message, thread_id)?;
                saw_thread_read = true;
            }
            _ => {}
        }
    }
    if !saw_thread_read {
        return Err("fake app-server did not receive thread/read".to_owned());
    }
    thread::sleep(Duration::from_secs(5));
    Ok(())
}

#[cfg(unix)]
fn accept_fake_app_server_untrusted_read_snapshot(
    listener: &TcpListener,
    thread_id: &'static str,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut stream, _) = loop {
        match listener.accept() {
            Ok(accepted) => break accepted,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(format!("accept fake app-server websocket: {error}")),
        }
    };

    stream
        .set_nonblocking(false)
        .map_err(|error| format!("set fake app-server stream blocking: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|error| format!("set fake app-server read timeout: {error}"))?;
    let websocket_accept = read_fake_http_upgrade(&mut stream)?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {websocket_accept}\r\n\r\n"
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|error| format!("write fake app-server handshake: {error}"))?;

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut saw_thread_read = false;
    while !saw_thread_read && Instant::now() < deadline {
        let message = read_fake_client_text_frame(&mut stream)?;
        let method = message
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match method {
            "initialize" => write_fake_json_response(
                &mut stream,
                &message,
                serde_json::json!({
                    "userAgent": "fake-codex",
                    "codexHome": "/tmp/fake-codex-home",
                    "platformFamily": "unix",
                    "platformOs": "macos"
                }),
            )?,
            "initialized" => {}
            "thread/resume" => write_fake_thread_response(&mut stream, &message, thread_id, true)?,
            "thread/read" => {
                write_fake_json_response(
                    &mut stream,
                    &message,
                    serde_json::json!({
                        "thread": {
                            "id": thread_id,
                            "status": { "type": "systemError" },
                            "turns": []
                        }
                    }),
                )?;
                saw_thread_read = true;
            }
            _ => {}
        }
    }
    if !saw_thread_read {
        return Err("fake app-server did not receive thread/read".to_owned());
    }
    thread::sleep(Duration::from_secs(5));
    Ok(())
}

#[cfg(unix)]
fn read_fake_http_upgrade(stream: &mut TcpStream) -> Result<String, String> {
    let mut bytes = Vec::new();
    let mut byte = [0_u8; 1];
    while bytes.len() < 8192 {
        stream
            .read_exact(&mut byte)
            .map_err(|error| format!("read fake app-server handshake: {error}"))?;
        bytes.push(byte[0]);
        if bytes.ends_with(b"\r\n\r\n") {
            let request = String::from_utf8(bytes)
                .map_err(|error| format!("decode fake handshake: {error}"))?;
            let key = request
                .split("\r\n")
                .filter_map(|line| line.split_once(':'))
                .find_map(|(name, value)| {
                    name.eq_ignore_ascii_case("Sec-WebSocket-Key")
                        .then(|| value.trim())
                })
                .ok_or_else(|| "fake handshake missing websocket key".to_owned())?;
            return Ok(fake_websocket_accept_key(key));
        }
    }
    Err("fake app-server handshake exceeded limit".to_owned())
}

#[cfg(unix)]
fn fake_websocket_accept_key(websocket_key: &str) -> String {
    let mut material = Vec::with_capacity(websocket_key.len() + 36);
    material.extend_from_slice(websocket_key.as_bytes());
    material.extend_from_slice(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    fake_base64_encode(&fake_sha1_digest(&material))
}

#[cfg(unix)]
fn fake_base64_encode(bytes: &[u8]) -> String {
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

#[cfg(unix)]
fn fake_sha1_digest(input: &[u8]) -> [u8; 20] {
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

#[cfg(unix)]
fn read_fake_client_text_frame(stream: &mut TcpStream) -> Result<serde_json::Value, String> {
    let mut header = [0_u8; 2];
    stream
        .read_exact(&mut header)
        .map_err(|error| format!("read fake websocket header: {error}"))?;
    if header[0] & 0x0F != 0x1 {
        return Err(format!(
            "unexpected fake websocket opcode {}",
            header[0] & 0x0F
        ));
    }
    let masked = header[1] & 0x80 != 0;
    if !masked {
        return Err("client websocket frame was not masked".to_owned());
    }
    let mut length = u64::from(header[1] & 0x7F);
    if length == 126 {
        let mut bytes = [0_u8; 2];
        stream
            .read_exact(&mut bytes)
            .map_err(|error| format!("read fake websocket medium length: {error}"))?;
        length = u64::from(u16::from_be_bytes(bytes));
    } else if length == 127 {
        let mut bytes = [0_u8; 8];
        stream
            .read_exact(&mut bytes)
            .map_err(|error| format!("read fake websocket large length: {error}"))?;
        length = u64::from_be_bytes(bytes);
    }
    let mut mask = [0_u8; 4];
    stream
        .read_exact(&mut mask)
        .map_err(|error| format!("read fake websocket mask: {error}"))?;
    let mut payload =
        vec![0_u8; usize::try_from(length).map_err(|error| format!("frame length: {error}"))?];
    stream
        .read_exact(&mut payload)
        .map_err(|error| format!("read fake websocket payload: {error}"))?;
    for (idx, byte) in payload.iter_mut().enumerate() {
        *byte ^= mask[idx % 4];
    }
    let text = String::from_utf8(payload).map_err(|error| format!("decode fake text: {error}"))?;
    serde_json::from_str(&text).map_err(|error| format!("decode fake json: {error}"))
}

#[cfg(unix)]
fn try_read_fake_client_text_frame(
    stream: &mut TcpStream,
) -> Result<Option<serde_json::Value>, String> {
    let mut header = [0_u8; 2];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::WouldBlock
                    | ErrorKind::TimedOut
                    | ErrorKind::UnexpectedEof
                    | ErrorKind::ConnectionReset
                    | ErrorKind::BrokenPipe
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(format!("read fake websocket header: {error}")),
    }
    let opcode = header[0] & 0x0F;
    let masked = header[1] & 0x80 != 0;
    if !masked {
        return Err("client websocket frame was not masked".to_owned());
    }
    let mut length = u64::from(header[1] & 0x7F);
    if length == 126 {
        let mut bytes = [0_u8; 2];
        stream
            .read_exact(&mut bytes)
            .map_err(|error| format!("read fake websocket medium length: {error}"))?;
        length = u64::from(u16::from_be_bytes(bytes));
    } else if length == 127 {
        let mut bytes = [0_u8; 8];
        stream
            .read_exact(&mut bytes)
            .map_err(|error| format!("read fake websocket large length: {error}"))?;
        length = u64::from_be_bytes(bytes);
    }
    let mut mask = [0_u8; 4];
    stream
        .read_exact(&mut mask)
        .map_err(|error| format!("read fake websocket mask: {error}"))?;
    let mut payload =
        vec![0_u8; usize::try_from(length).map_err(|error| format!("frame length: {error}"))?];
    stream
        .read_exact(&mut payload)
        .map_err(|error| format!("read fake websocket payload: {error}"))?;
    for (idx, byte) in payload.iter_mut().enumerate() {
        *byte ^= mask[idx % 4];
    }
    if opcode != 0x1 {
        return Ok(None);
    }
    let text = String::from_utf8(payload).map_err(|error| format!("decode fake text: {error}"))?;
    serde_json::from_str(&text)
        .map(Some)
        .map_err(|error| format!("decode fake json: {error}"))
}

#[cfg(unix)]
fn record_fake_app_server_method<'a>(
    methods: &mut Vec<String>,
    message: &'a serde_json::Value,
) -> Result<&'a str, String> {
    let method = message
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    methods.push(method.to_owned());
    match method {
        "turn/start" | "turn/steer" => Err(format!(
            "fake app-server received unexpected delivery RPC {method}; methods seen: {methods:?}"
        )),
        _ => Ok(method),
    }
}

#[cfg(unix)]
fn record_fake_app_server_method_allowing_delivery<'a>(
    methods: &mut Vec<String>,
    message: &'a serde_json::Value,
) -> Result<&'a str, String> {
    let method = message
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    methods.push(method.to_owned());
    Ok(method)
}

#[cfg(unix)]
fn write_fake_thread_response(
    stream: &mut TcpStream,
    request: &serde_json::Value,
    thread_id: &str,
    include_turns: bool,
) -> Result<(), String> {
    let thread = if include_turns {
        serde_json::json!({
            "id": thread_id,
            "status": { "type": "idle" },
            "turns": []
        })
    } else {
        serde_json::json!({
            "id": thread_id
        })
    };
    write_fake_json_response(
        stream,
        request,
        serde_json::json!({
            "thread": thread
        }),
    )
}

#[cfg(unix)]
fn write_fake_thread_turn_response(
    stream: &mut TcpStream,
    request: &serde_json::Value,
    thread_id: &str,
    turn_id: &str,
    status: &str,
) -> Result<(), String> {
    write_fake_json_response(
        stream,
        request,
        serde_json::json!({
            "thread": {
                "id": thread_id,
                "status": { "type": "idle" },
                "turns": [
                    { "id": turn_id, "status": status, "items": [] }
                ]
            }
        }),
    )
}

#[cfg(unix)]
fn write_fake_active_thread_response(
    stream: &mut TcpStream,
    request: &serde_json::Value,
    thread_id: &str,
) -> Result<(), String> {
    write_fake_json_response(
        stream,
        request,
        serde_json::json!({
            "thread": {
                "id": thread_id,
                "status": { "type": "active" },
                "turns": [
                    { "id": "turn-active-snapshot", "status": "inProgress" }
                ]
            }
        }),
    )
}

#[cfg(unix)]
fn write_fake_json_error(
    stream: &mut TcpStream,
    request: &serde_json::Value,
    code: i64,
    message: &str,
) -> Result<(), String> {
    let id = request
        .get("id")
        .cloned()
        .ok_or_else(|| "fake app-server request missing id".to_owned())?;
    write_fake_server_text_frame(
        stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": code,
                "message": message
            }
        }),
    )
}

#[cfg(unix)]
fn write_fake_turn_completed_notification(
    stream: &mut TcpStream,
    thread_id: &str,
    turn_id: &str,
    status: &str,
) -> Result<(), String> {
    write_fake_server_text_frame(
        stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": {
                    "id": turn_id,
                    "status": status,
                    "items": []
                }
            }
        }),
    )
}

#[cfg(unix)]
fn write_fake_json_response(
    stream: &mut TcpStream,
    request: &serde_json::Value,
    result: serde_json::Value,
) -> Result<(), String> {
    let id = request
        .get("id")
        .cloned()
        .ok_or_else(|| "fake app-server request missing id".to_owned())?;
    write_fake_server_text_frame(
        stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }),
    )
}

#[cfg(unix)]
fn write_fake_server_text_frame(
    stream: &mut TcpStream,
    message: &serde_json::Value,
) -> Result<(), String> {
    let payload =
        serde_json::to_vec(message).map_err(|error| format!("encode fake json: {error}"))?;
    let mut frame = Vec::with_capacity(payload.len().saturating_add(10));
    frame.push(0x81);
    match payload.len() {
        len @ 0..=125 => frame.push(u8::try_from(len).expect("small payload fits u8")),
        len @ 126..=65535 => {
            frame.push(126);
            frame.extend_from_slice(&u16::try_from(len).expect("payload fits u16").to_be_bytes());
        }
        len => {
            frame.push(127);
            frame.extend_from_slice(&u64::try_from(len).expect("payload fits u64").to_be_bytes());
        }
    }
    frame.extend_from_slice(&payload);
    stream
        .write_all(&frame)
        .map_err(|error| format!("write fake websocket frame: {error}"))
}

#[cfg(unix)]
fn wait_for_fake_app_server(done_rx: mpsc::Receiver<Result<(), String>>) {
    match done_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("fake app-server failed: {error}"),
        Err(error) => panic!("timed out waiting for fake app-server: {error}"),
    }
}

#[cfg(unix)]
fn wait_for_fake_app_server_methods(
    done_rx: mpsc::Receiver<Result<Vec<String>, String>>,
) -> Vec<String> {
    match done_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(methods)) => methods,
        Ok(Err(error)) => panic!("fake app-server failed: {error}"),
        Err(error) => panic!("timed out waiting for fake app-server: {error}"),
    }
}

#[cfg(unix)]
fn submit_failed_fake_e2e_batch(home: &TempDir, thread_id: &str) -> String {
    let submitted = cbth_direct_json(
        home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            thread_id,
            "--summary",
            "fake e2e auto delivery",
            "--delivery-read-only",
            "true",
            "--delivery-requires-approval",
            "false",
            "--delivery-requires-network",
            "false",
            "--delivery-requires-write-access",
            "false",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");
    let failed = cbth_direct_json(
        home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "ready for auto delivery",
            "--max-delivery-attempts",
            "2",
        ],
    );
    failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id")
        .to_owned()
}

#[cfg(unix)]
fn run_cli_trusted_all_fake_e2e(home: &TempDir, thread_id: &str, app_server_url: &str) -> Output {
    run_cli_trusted_all_fake_e2e_with_sleep(home, thread_id, app_server_url, "7")
}

#[cfg(unix)]
fn run_cli_trusted_all_fake_e2e_with_sleep(
    home: &TempDir,
    thread_id: &str,
    app_server_url: &str,
    foreground_sleep_seconds: &str,
) -> Output {
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg(thread_id)
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--auto-delivery-policy")
        .arg("trusted-all")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", app_server_url)
        .env(
            "FAKE_CODEX_FOREGROUND_SLEEP_SECONDS",
            foreground_sleep_seconds,
        )
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[cfg(unix)]
fn wait_for_batch_close_reason(home: &TempDir, batch_id: &str, close_reason: &str) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let inspected = cbth_direct_json(home, &["batch", "inspect", "--batch-id", batch_id]);
        let batch = &inspected["batch"]["batch"];
        if batch["state"] == "closed" && batch["close_reason"] == close_reason {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for batch {batch_id} to close as {close_reason}; last batch: {batch}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
fn stop_daemon(home: &TempDir) {
    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_binds_session_starts_foreground_codex_and_stops_app_server() {
    let home = temp_home();
    let client_cwd = tempfile::tempdir().expect("client cwd");
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .arg("--")
        .arg("--model")
        .arg("gpt-test")
        .current_dir(client_cwd.path())
        .env("FAKE_CODEX_LOG", &log_path)
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let log = fs::read_to_string(&log_path).expect("read fake codex log");
    assert!(log.contains("app-server\tapp-server\t--listen\tws://127.0.0.1:0"));
    assert!(log.contains("foreground\t--remote\tws://127.0.0.1:45678\t--cd\t"));
    assert!(log.contains(&client_cwd.path().display().to_string()));
    assert!(log.contains("\t--model\tgpt-test"));

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (
        managed_session_id,
        session_state,
        session_epoch,
        activity_state,
        activity_revision,
        capability_revision,
    ): (String, String, i64, String, i64, i64) = conn
        .query_row(
            "SELECT managed_session_id, session_state, session_epoch,
                    activity_state, activity_revision, capability_revision
             FROM cli_managed_sessions
             WHERE bound_thread_id = ?",
            ["thread-cli-run"],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .expect("query managed session");
    assert!(!managed_session_id.is_empty());
    assert_eq!(session_state, "live");
    assert_eq!(session_epoch, 2);
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);
    assert_eq!(capability_revision, 0);

    let status_output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("status")
        .output()
        .expect("daemon status");
    assert!(
        status_output.status.success(),
        "daemon status failed\nstatus: {}\nstdout: {}\nstderr: {}",
        status_output.status,
        String::from_utf8_lossy(&status_output.stdout),
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status: serde_json::Value =
        serde_json::from_slice(&status_output.stdout).expect("status json");
    assert_eq!(status["cli_app_servers"], serde_json::json!([]));

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_passive_adapter_records_app_server_activity() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let (app_server_url, fake_server_done) = spawn_fake_app_server("thread-cli-run-passive");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-passive")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "2")
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_fake_app_server(fake_server_done);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (
        session_epoch,
        activity_state,
        activity_revision,
        capability_revision,
        capability_thread_resume,
        capability_turn_start,
        capability_current_state_sync,
    ): (i64, String, i64, i64, i64, i64, i64) = conn
        .query_row(
            "SELECT session_epoch, activity_state, activity_revision, capability_revision,
                    capability_thread_resume, capability_turn_start, capability_current_state_sync
             FROM cli_managed_sessions
             WHERE bound_thread_id = ?",
            ["thread-cli-run-passive"],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .expect("query managed session passive adapter state");
    assert!(session_epoch >= 2);
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);
    assert_eq!(capability_revision, 0);
    assert_eq!(capability_thread_resume, 0);
    assert_eq!(capability_turn_start, 0);
    assert_eq!(capability_current_state_sync, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_fake_e2e_passive_sync_does_not_send_delivery_rpc() {
    let home = temp_home();
    let submitted = cbth_direct_json(
        &home,
        &[
            "job",
            "submit",
            "--source-thread-id",
            "thread-cli-run-passive-delivery-guard",
            "--summary",
            "fake e2e passive guard",
            "--delivery-read-only",
            "true",
            "--delivery-requires-approval",
            "false",
            "--delivery-requires-network",
            "false",
            "--delivery-requires-write-access",
            "false",
        ],
    );
    let job_id = submitted["job"]["job_id"].as_str().expect("job id");
    let failed = cbth_direct_json(
        &home,
        &[
            "job",
            "fail",
            "--job-id",
            job_id,
            "--reason",
            "ready for passive guard",
            "--max-delivery-attempts",
            "2",
        ],
    );
    let batch_id = failed["batch"]["batch"]["batch_id"]
        .as_str()
        .expect("batch id")
        .to_owned();

    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_capture_passive_methods("thread-cli-run-passive-delivery-guard");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-passive-delivery-guard")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "1")
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let methods = match fake_server_done.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(methods)) => methods,
        Ok(Err(error)) => panic!("fake app-server failed: {error}"),
        Err(error) => panic!("timed out waiting for fake app-server: {error}"),
    };
    assert_eq!(
        methods,
        vec![
            "initialize".to_owned(),
            "initialized".to_owned(),
            "thread/resume".to_owned(),
            "thread/read".to_owned(),
        ]
    );

    let inspected = cbth_direct_json(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "open");
    assert_eq!(inspected["batch"]["batch"]["replay_policy"], "automatic");
    assert_eq!(inspected["batch"]["batch"]["delivery_attempt_count"], 0);
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let attempt_count: i64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM delivery_attempts
             WHERE source_thread_id = ?",
            ["thread-cli-run-passive-delivery-guard"],
            |row| row.get(0),
        )
        .expect("count delivery attempts");
    assert_eq!(attempt_count, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_trusted_all_auto_delivery_notification_closes_delivered() {
    let home = temp_home();
    let thread_id = "thread-cli-auto-notification";
    let batch_id = submit_failed_fake_e2e_batch(&home, thread_id);
    let (app_server_url, fake_server_done) = spawn_fake_app_server_auto_delivery(
        thread_id,
        FakeAutoDeliveryOutcome::CompletedNotification,
    );

    run_cli_trusted_all_fake_e2e(&home, thread_id, &app_server_url);
    let methods = wait_for_fake_app_server_methods(fake_server_done);
    assert!(methods.iter().any(|method| method == "turn/start"));

    let inspected = cbth_direct_json(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "closed");
    assert_eq!(inspected["batch"]["batch"]["close_reason"], "delivered");
    assert_eq!(inspected["batch"]["batch"]["delivery_attempt_count"], 1);

    let audit = cbth_direct_json(
        &home,
        &[
            "audit",
            "list",
            "--source-thread-id",
            thread_id,
            "--limit",
            "20",
        ],
    );
    let decisions = audit["audit"].as_array().expect("audit list");
    assert!(
        decisions
            .iter()
            .any(|decision| decision["decision"] == "accepted")
    );
    assert!(
        decisions
            .iter()
            .any(|decision| decision["decision"] == "observed")
    );
    stop_daemon(&home);
}

#[cfg(unix)]
#[test]
fn cli_run_trusted_all_auto_delivery_resyncs_after_terminal_for_next_head() {
    let home = temp_home();
    let thread_id = "thread-cli-auto-two-batches";
    let first_batch_id = submit_failed_fake_e2e_batch(&home, thread_id);
    let (app_server_url, fake_server_done) = spawn_fake_app_server_auto_delivery(
        thread_id,
        FakeAutoDeliveryOutcome::TwoCompletedNotifications,
    );

    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let child = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg(thread_id)
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--auto-delivery-policy")
        .arg("trusted-all")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "10")
        .spawn()
        .expect("spawn cbth cli run");

    wait_for_batch_close_reason(&home, &first_batch_id, "delivered");
    let second_batch_id = submit_failed_fake_e2e_batch(&home, thread_id);

    let output = wait_with_timeout(child, Duration::from_secs(15));
    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let methods = wait_for_fake_app_server_methods(fake_server_done);
    assert_eq!(
        methods
            .iter()
            .filter(|method| *method == "turn/start")
            .count(),
        2
    );

    for batch_id in [first_batch_id, second_batch_id] {
        let inspected = cbth_direct_json(&home, &["batch", "inspect", "--batch-id", &batch_id]);
        assert_eq!(inspected["batch"]["batch"]["state"], "closed");
        assert_eq!(inspected["batch"]["batch"]["close_reason"], "delivered");
        assert_eq!(inspected["batch"]["batch"]["delivery_attempt_count"], 1);
    }
    stop_daemon(&home);
}

#[cfg(unix)]
#[test]
fn cli_run_trusted_all_auto_delivery_skips_manual_resolution_head_without_audit() {
    let home = temp_home();
    let thread_id = "thread-cli-auto-manual-head";
    let batch_id = submit_failed_fake_e2e_batch(&home, thread_id);
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    conn.execute(
        "UPDATE batches
         SET replay_policy = 'manual_resolution_only'
         WHERE batch_id = ?",
        [&batch_id],
    )
    .expect("manualize batch");
    drop(conn);

    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_capture_passive_methods_for(thread_id, Duration::from_secs(5));

    run_cli_trusted_all_fake_e2e(&home, thread_id, &app_server_url);
    let methods = wait_for_fake_app_server_methods(fake_server_done);
    assert!(!methods.iter().any(|method| method == "turn/start"));

    let audit = cbth_direct_json(
        &home,
        &[
            "audit",
            "list",
            "--source-thread-id",
            thread_id,
            "--limit",
            "20",
        ],
    );
    assert!(audit["audit"].as_array().expect("audit list").is_empty());
    stop_daemon(&home);
}

#[cfg(unix)]
#[test]
fn cli_run_trusted_all_auto_delivery_thread_read_reconcile_closes_delivered() {
    let home = temp_home();
    let thread_id = "thread-cli-auto-reconcile";
    let batch_id = submit_failed_fake_e2e_batch(&home, thread_id);
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_auto_delivery(thread_id, FakeAutoDeliveryOutcome::CompletedReconcile);

    run_cli_trusted_all_fake_e2e(&home, thread_id, &app_server_url);
    let methods = wait_for_fake_app_server_methods(fake_server_done);
    assert!(
        methods
            .iter()
            .filter(|method| *method == "thread/read")
            .count()
            >= 2
    );

    let inspected = cbth_direct_json(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "closed");
    assert_eq!(inspected["batch"]["batch"]["close_reason"], "delivered");
    let audit = cbth_direct_json(
        &home,
        &[
            "audit",
            "list",
            "--source-thread-id",
            thread_id,
            "--limit",
            "20",
        ],
    );
    assert!(
        audit["audit"]
            .as_array()
            .expect("audit list")
            .iter()
            .any(|decision| decision["decision"] == "reconciled")
    );
    stop_daemon(&home);
}

#[cfg(unix)]
#[test]
fn cli_run_trusted_all_auto_delivery_failed_turn_manualizes_batch() {
    let home = temp_home();
    let thread_id = "thread-cli-auto-failed";
    let batch_id = submit_failed_fake_e2e_batch(&home, thread_id);
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_auto_delivery(thread_id, FakeAutoDeliveryOutcome::FailedNotification);

    run_cli_trusted_all_fake_e2e(&home, thread_id, &app_server_url);
    let methods = wait_for_fake_app_server_methods(fake_server_done);
    assert!(methods.iter().any(|method| method == "turn/start"));

    let inspected = cbth_direct_json(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "open");
    assert_eq!(
        inspected["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    let audit = cbth_direct_json(
        &home,
        &[
            "audit",
            "list",
            "--source-thread-id",
            thread_id,
            "--limit",
            "20",
        ],
    );
    assert!(
        audit["audit"]
            .as_array()
            .expect("audit list")
            .iter()
            .any(|decision| decision["decision"] == "manualized")
    );
    stop_daemon(&home);
}

#[cfg(unix)]
#[test]
fn cli_run_trusted_all_auto_delivery_reject_before_accept_is_retryable() {
    let home = temp_home();
    let thread_id = "thread-cli-auto-rejected";
    let batch_id = submit_failed_fake_e2e_batch(&home, thread_id);
    let (app_server_url, fake_server_done) = spawn_fake_app_server_auto_delivery(
        thread_id,
        FakeAutoDeliveryOutcome::RejectedBeforeAccept,
    );

    run_cli_trusted_all_fake_e2e(&home, thread_id, &app_server_url);
    let methods = wait_for_fake_app_server_methods(fake_server_done);
    assert!(methods.iter().any(|method| method == "turn/start"));

    let inspected = cbth_direct_json(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(inspected["batch"]["batch"]["state"], "open");
    assert_eq!(inspected["batch"]["batch"]["replay_policy"], "automatic");
    assert_eq!(inspected["batch"]["batch"]["delivery_attempt_count"], 0);
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (attempt_state, delivery_rpc_state): (String, String) = conn
        .query_row(
            "SELECT state, delivery_rpc_state
             FROM delivery_attempts
             WHERE source_thread_id = ?
             ORDER BY created_at DESC
             LIMIT 1",
            [thread_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("query rejected attempt");
    assert_eq!(attempt_state, "abandoned");
    assert_eq!(delivery_rpc_state, "rejected_before_accept");
    stop_daemon(&home);
}

#[cfg(unix)]
#[test]
fn cli_run_trusted_all_auto_delivery_retries_after_rejection_with_fresh_proof() {
    let home = temp_home();
    let thread_id = "thread-cli-auto-reject-then-complete";
    let batch_id = submit_failed_fake_e2e_batch(&home, thread_id);
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_auto_delivery(thread_id, FakeAutoDeliveryOutcome::RejectThenComplete);

    run_cli_trusted_all_fake_e2e_with_sleep(&home, thread_id, &app_server_url, "12");
    let methods = wait_for_fake_app_server_methods(fake_server_done);
    assert_eq!(
        methods
            .iter()
            .filter(|method| *method == "turn/start")
            .count(),
        2
    );

    let inspected = cbth_direct_json(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(
        inspected["batch"]["batch"]["state"], "closed",
        "batch after retry: {inspected}"
    );
    assert_eq!(
        inspected["batch"]["batch"]["close_reason"], "delivered",
        "batch after retry: {inspected}"
    );
    assert_eq!(inspected["batch"]["batch"]["delivery_attempt_count"], 1);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let rejected_count: i64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM delivery_attempts
             WHERE source_thread_id = ?
               AND delivery_rpc_state = 'rejected_before_accept'",
            [thread_id],
            |row| row.get(0),
        )
        .expect("count rejected attempts");
    assert_eq!(rejected_count, 1);
    stop_daemon(&home);
}

#[cfg(unix)]
#[test]
fn cli_run_trusted_all_auto_delivery_unknown_acceptance_sweeps_fail_closed() {
    let home = temp_home();
    let thread_id = "thread-cli-auto-unknown";
    let batch_id = submit_failed_fake_e2e_batch(&home, thread_id);
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_auto_delivery(thread_id, FakeAutoDeliveryOutcome::ClosedBeforeAccept);

    run_cli_trusted_all_fake_e2e(&home, thread_id, &app_server_url);
    let methods = wait_for_fake_app_server_methods(fake_server_done);
    assert!(methods.iter().any(|method| method == "turn/start"));

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (attempt_id, started_at, delivery_rpc_state): (String, i64, String) = conn
        .query_row(
            "SELECT attempt_id, delivery_rpc_started_at, delivery_rpc_state
             FROM delivery_attempts
             WHERE source_thread_id = ?
             ORDER BY created_at DESC
             LIMIT 1",
            [thread_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("query pending attempt");
    assert_eq!(delivery_rpc_state, "pending_acceptance");
    drop(conn);

    let sweep_now = (started_at + 301).to_string();
    let sweep = cbth_direct_json(&home, &["maintenance", "sweep", "--now", &sweep_now]);
    assert_eq!(sweep["sweep"]["stale_cli_acceptances_abandoned"], 1);
    let attempt = cbth_direct_json(&home, &["attempt", "inspect", "--attempt-id", &attempt_id]);
    assert_eq!(attempt["attempt"]["delivery_rpc_state"], "unknown");
    let inspected = cbth_direct_json(&home, &["batch", "inspect", "--batch-id", &batch_id]);
    assert_eq!(
        inspected["batch"]["batch"]["replay_policy"],
        "manual_resolution_only"
    );
    stop_daemon(&home);
}

#[cfg(unix)]
#[test]
fn cli_run_passive_adapter_read_snapshot_dominates_request_window_notifications() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_started_before_read_snapshot("thread-cli-run-buffered");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-buffered")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "2")
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_fake_app_server(fake_server_done);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (activity_state, activity_revision): (String, i64) = conn
        .query_row(
            "SELECT activity_state, activity_revision
             FROM cli_managed_sessions
             WHERE bound_thread_id = ?",
            ["thread-cli-run-buffered"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("query managed session passive adapter state");
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_passive_adapter_replays_resume_window_notifications_when_read_missing() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_resume_started_before_missing_read_snapshot(
            "thread-cli-run-resume-window",
        );

    let child = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-resume-window")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "7")
        .spawn()
        .expect("spawn cbth cli run");

    let activity_revision =
        wait_for_cli_activity_state(&home, "thread-cli-run-resume-window", "active");
    assert!(activity_revision >= 2);

    let output = wait_with_timeout(child, Duration::from_secs(12));
    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_fake_app_server(fake_server_done);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_passive_adapter_ignores_stale_idle_before_active_read_snapshot() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_idle_before_active_read_snapshot("thread-cli-run-stale-idle");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-stale-idle")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "2")
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_fake_app_server(fake_server_done);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (activity_state, activity_revision): (String, i64) = conn
        .query_row(
            "SELECT activity_state, activity_revision
             FROM cli_managed_sessions
             WHERE bound_thread_id = ?",
            ["thread-cli-run-stale-idle"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("query managed session passive adapter state");
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_passive_adapter_invalidates_untrusted_read_snapshot() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_untrusted_read_snapshot("thread-cli-run-untrusted-read");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-untrusted-read")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "2")
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_fake_app_server(fake_server_done);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (activity_state, activity_revision, capability_revision): (String, i64, i64) = conn
        .query_row(
            "SELECT activity_state, activity_revision, capability_revision
             FROM cli_managed_sessions
             WHERE bound_thread_id = ?",
            ["thread-cli-run-untrusted-read"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("query managed session passive adapter state");
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);
    assert_eq!(capability_revision, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_passive_adapter_invalidates_after_read_timeout() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_started_before_failed_read("thread-cli-run-failed-read");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-failed-read")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "2")
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_fake_app_server(fake_server_done);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (activity_state, activity_revision, capability_revision): (String, i64, i64) = conn
        .query_row(
            "SELECT activity_state, activity_revision, capability_revision
             FROM cli_managed_sessions
             WHERE bound_thread_id = ?",
            ["thread-cli-run-failed-read"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("query managed session passive adapter state");
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);
    assert_eq!(capability_revision, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_passive_adapter_requires_current_state_snapshot_before_idle_proof() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_without_turn_snapshot("thread-cli-run-no-snapshot");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-no-snapshot")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "1")
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_fake_app_server(fake_server_done);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (activity_state, activity_revision, capability_revision): (String, i64, i64) = conn
        .query_row(
            "SELECT activity_state, activity_revision, capability_revision
             FROM cli_managed_sessions
             WHERE bound_thread_id = ?",
            ["thread-cli-run-no-snapshot"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("query managed session passive adapter state");
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);
    assert_eq!(capability_revision, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_passive_adapter_invalidates_old_idle_after_reconnect_without_snapshot() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");
    let (app_server_url, fake_server_done) =
        spawn_fake_app_server_reconnect_without_turn_snapshot("thread-cli-run-reconnect");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-reconnect")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_URL", &app_server_url)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "3")
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_fake_app_server(fake_server_done);

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (session_epoch, activity_state, activity_revision, capability_revision): (
        i64,
        String,
        i64,
        i64,
    ) = conn
        .query_row(
            "SELECT session_epoch, activity_state, activity_revision, capability_revision
             FROM cli_managed_sessions
             WHERE bound_thread_id = ?",
            ["thread-cli-run-reconnect"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("query managed session passive adapter state");
    assert!(session_epoch >= 2);
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);
    assert_eq!(capability_revision, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_resolves_relative_path_binary_for_existing_daemon_cwd() {
    let home = temp_home();
    let daemon_cwd = tempfile::tempdir().expect("daemon cwd");
    let client_cwd = tempfile::tempdir().expect("client cwd");
    let bin_dir = client_cwd.path().join("bin");
    fs::create_dir(&bin_dir).expect("create bin dir");
    let fake_codex = write_fake_codex_script(&bin_dir.join("codex"));
    let log_path = client_cwd.path().join("fake-codex.log");

    let ensure_output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("ensure")
        .current_dir(daemon_cwd.path())
        .env("FAKE_CODEX_LOG", &log_path)
        .output()
        .expect("ensure daemon");
    assert!(
        ensure_output.status.success(),
        "daemon ensure failed\nstatus: {}\nstdout: {}\nstderr: {}",
        ensure_output.status,
        String::from_utf8_lossy(&ensure_output.stdout),
        String::from_utf8_lossy(&ensure_output.stderr)
    );

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-relative-path")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .current_dir(client_cwd.path())
        .env("PATH", "bin")
        .env("FAKE_CODEX_LOG", &log_path)
        .output()
        .expect("run cbth cli run");

    assert!(
        output.status.success(),
        "cbth cli run failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let log = fs::read_to_string(&log_path).expect("read fake codex log");
    assert!(log.contains("app-server\tapp-server\t--listen\tws://127.0.0.1:0"));
    assert!(log.contains("foreground\t--remote\tws://127.0.0.1:45678"));
    assert_eq!(fake_codex, client_cwd.path().join("bin/codex"));

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_reservation_rejects_duplicate_before_session_epoch_bump() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let first = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-reservation")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_STARTUP_SLEEP_SECONDS", "2")
        .spawn()
        .expect("spawn first cbth cli run");

    wait_for_log_contains(
        &log_path,
        "app-server\tapp-server\t--listen\tws://127.0.0.1:0",
    );

    let second = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-reservation")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .output()
        .expect("run second cbth cli run");

    assert!(
        !second.status.success(),
        "second cli run unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("active CLI app-server reservation"),
        "unexpected second stderr: {}",
        String::from_utf8_lossy(&second.stderr)
    );

    let first_output = first.wait_with_output().expect("wait for first cli run");
    assert!(
        first_output.status.success(),
        "first cli run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );

    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (session_epoch, activity_state, activity_revision, capability_revision): (
        i64,
        String,
        i64,
        i64,
    ) = conn
        .query_row(
            "SELECT session_epoch, activity_state, activity_revision, capability_revision
             FROM cli_managed_sessions WHERE bound_thread_id = ?",
            ["thread-cli-run-reservation"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("query managed session proof");
    assert_eq!(session_epoch, 2);
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);
    assert_eq!(capability_revision, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_stop_returns_when_grandchild_keeps_app_server_stdout_open() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let child = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-grandchild")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_APP_SERVER_GRANDCHILD_STDOUT", "1")
        .spawn()
        .expect("spawn cbth cli run");
    let output = wait_with_timeout(child, Duration::from_secs(5));

    assert!(
        output.status.success(),
        "cli run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_rejects_duplicate_active_thread_before_stealing_lease() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let first = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-duplicate")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("FAKE_CODEX_LOG", &log_path)
        .env("FAKE_CODEX_FOREGROUND_SLEEP_SECONDS", "2")
        .spawn()
        .expect("spawn first cbth cli run");

    wait_for_log_contains(&log_path, "foreground\t--remote\tws://127.0.0.1:45678");

    let second = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-duplicate")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .output()
        .expect("run second cbth cli run");

    assert!(
        !second.status.success(),
        "second cli run unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("already has an active CLI app-server"),
        "unexpected second stderr: {}",
        String::from_utf8_lossy(&second.stderr)
    );

    let first_output = first.wait_with_output().expect("wait for first cli run");
    assert!(
        first_output.status.success(),
        "first cli run failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );

    let log = fs::read_to_string(&log_path).expect("read fake codex log");
    assert_eq!(log.matches("foreground\t--remote").count(), 1);
    let conn = Connection::open(home.path().join("cbth.sqlite3")).expect("open db");
    let (session_epoch, activity_state, activity_revision, capability_revision): (
        i64,
        String,
        i64,
        i64,
    ) = conn
        .query_row(
            "SELECT session_epoch, activity_state, activity_revision, capability_revision
             FROM cli_managed_sessions WHERE bound_thread_id = ?",
            ["thread-cli-run-duplicate"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("query managed session proof");
    assert_eq!(session_epoch, 2);
    assert_eq!(activity_state, "unknown");
    assert_eq!(activity_revision, 0);
    assert_eq!(capability_revision, 0);

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}

#[cfg(unix)]
#[test]
fn cli_run_rejects_non_loopback_app_server_listener() {
    let home = temp_home();
    let script_dir = tempfile::tempdir().expect("script dir");
    let fake_codex = fake_codex_script(&script_dir);
    let log_path = script_dir.path().join("fake-codex.log");

    let output = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("cli")
        .arg("run")
        .arg("--bind-thread-id")
        .arg("thread-cli-run-bad-url")
        .arg("--session-allows-approval")
        .arg("false")
        .arg("--session-allows-network")
        .arg("false")
        .arg("--session-allows-write-access")
        .arg("false")
        .arg("--codex-bin")
        .arg(&fake_codex)
        .env("FAKE_CODEX_LOG", &log_path)
        .env(
            "FAKE_CODEX_APP_SERVER_URL",
            "ws://127.0.0.1:45678@remote.example",
        )
        .output()
        .expect("run cbth cli run");

    assert!(
        !output.status.success(),
        "cli run unexpectedly succeeded\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("non-loopback listener"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let log = fs::read_to_string(&log_path).expect("read fake codex log");
    assert!(!log.contains("foreground\t--remote"));

    let _ = Command::new(env!("CARGO_BIN_EXE_cbth"))
        .arg("--home")
        .arg(home.path())
        .arg("daemon")
        .arg("stop")
        .output();
}
