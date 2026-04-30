#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

struct ChildGuard(Child);

impl ChildGuard {
    fn kill_and_wait(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

#[test]
#[ignore = "requires a real codex login, model access, node, and network"]
fn live_codex_shared_app_server_smoke_is_opt_in() {
    if std::env::var("CBTH_RUN_LIVE_CODEX_E2E").as_deref() != Ok("1") {
        eprintln!("set CBTH_RUN_LIVE_CODEX_E2E=1 to run the live Codex smoke test");
        return;
    }

    let codex_bin = std::env::var_os("CBTH_LIVE_CODEX_BIN").unwrap_or_else(|| "codex".into());
    let mut child = Command::new(codex_bin)
        .arg("app-server")
        .arg("--listen")
        .arg("ws://127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn codex app-server");
    let stdout = child.stdout.take().expect("capture app-server stdout");
    let stderr = child.stderr.take().expect("capture app-server stderr");
    let mut app_server = ChildGuard(child);
    let listener_url = wait_for_app_server_listener(stdout, stderr);

    let node_bin = std::env::var_os("CBTH_LIVE_NODE_BIN").unwrap_or_else(|| "node".into());
    let timeout_ms =
        std::env::var("CBTH_LIVE_CODEX_E2E_TIMEOUT_MS").unwrap_or_else(|_| "180000".to_owned());
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/cli_shared_app_server_poc.mjs");
    let output = Command::new(node_bin)
        .arg(script)
        .arg("--url")
        .arg(&listener_url)
        .arg("--cwd")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--timeout-ms")
        .arg(&timeout_ms)
        .output()
        .expect("run live shared app-server smoke");
    app_server.kill_and_wait();

    assert!(
        output.status.success(),
        "live shared app-server smoke failed\nstatus: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn wait_for_app_server_listener(stdout: ChildStdout, stderr: ChildStderr) -> String {
    let (tx, rx) = mpsc::channel();
    spawn_listener_reader(stdout, tx.clone());
    spawn_listener_reader(stderr, tx);

    let mut closed_streams = 0;
    while closed_streams < 2 {
        match rx.recv_timeout(Duration::from_secs(15)) {
            Ok(ListenerEvent::Url(url)) => return url,
            Ok(ListenerEvent::Closed) => closed_streams += 1,
            Err(error) => panic!("timed out waiting for codex app-server listener URL: {error}"),
        }
    }
    panic!("codex app-server did not print a listener URL");
}

enum ListenerEvent {
    Url(String),
    Closed,
}

fn spawn_listener_reader<R>(stream: R, tx: mpsc::Sender<ListenerEvent>)
where
    R: std::io::Read + Send + 'static,
{
    thread::spawn(move || {
        let reader = BufReader::new(stream);
        for line in reader.lines() {
            let Ok(line) = line else {
                break;
            };
            if let Some(url) = parse_app_server_listener_url(&line) {
                let _ = tx.send(ListenerEvent::Url(url));
                return;
            }
        }
        let _ = tx.send(ListenerEvent::Closed);
    });
}

fn parse_app_server_listener_url(line: &str) -> Option<String> {
    let value = line.trim_start().strip_prefix("listening on:")?;
    let url = value.split_whitespace().next()?;
    url.starts_with("ws://").then(|| url.to_owned())
}
