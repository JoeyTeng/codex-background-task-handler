#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CODEX_BINARY = "/Applications/Codex.app/Contents/Resources/codex"


class JsonRpcLineClient:
    def __init__(self, codex_binary: str) -> None:
        self.process = subprocess.Popen(
            [codex_binary, "app-server", "--listen", "stdio://"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        if self.process.stdin is None or self.process.stdout is None or self.process.stderr is None:
            raise RuntimeError("failed to create stdio pipes for codex app-server")
        self.stdin = self.process.stdin
        self.stdout = self.process.stdout
        self.stderr = self.process.stderr
        self.next_id = 0
        self.notifications: list[dict[str, Any]] = []

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)

    def _send(self, message: dict[str, Any]) -> None:
        self.stdin.write(json.dumps(message, ensure_ascii=False) + "\n")
        self.stdin.flush()

    def _read_message(self, timeout_seconds: float = 10.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while True:
            if self.process.poll() is not None:
                stderr_output = self.stderr.read()
                raise RuntimeError(
                    f"codex app-server exited with code {self.process.returncode}: {stderr_output}"
                )
            if time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for JSON-RPC message")
            line = self.stdout.readline()
            if not line:
                time.sleep(0.05)
                continue
            return json.loads(line)

    def initialize(self) -> dict[str, Any]:
        response = self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "desktop_thread_inject_poc",
                    "title": "Desktop Thread Inject PoC",
                    "version": "0.1.0",
                },
                "capabilities": {
                    "experimentalApi": True,
                },
            },
        )
        self.notify("initialized")
        return response

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        message: dict[str, Any] = {"method": method}
        if params is not None:
            message["params"] = params
        self._send(message)

    def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_id = self.next_id
        self.next_id += 1
        message: dict[str, Any] = {"id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        self._send(message)

        while True:
            payload = self._read_message()
            if "method" in payload and "id" not in payload:
                self.notifications.append(payload)
                continue
            if payload.get("id") != request_id:
                raise RuntimeError(
                    f"unexpected JSON-RPC response id {payload.get('id')} for request {request_id}"
                )
            if "error" in payload:
                raise RuntimeError(f"{method} failed: {json.dumps(payload['error'], ensure_ascii=False)}")
            return payload["result"]

    def wait_for_notification(
        self,
        method: str,
        timeout_seconds: float = 120.0,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for notification {method}")
            payload = self._read_message(timeout_seconds=remaining)
            if "method" in payload and "id" not in payload:
                self.notifications.append(payload)
                if payload.get("method") == method:
                    return payload
                continue
            raise RuntimeError(
                f"unexpected JSON-RPC payload while waiting for notification {method}: {payload}"
            )


def make_marker(thread_id: str) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"EXTERNAL_POC_MARKER thread={thread_id} at={now}"


def file_contains(path: Path, marker: str) -> bool:
    return marker in path.read_text(encoding="utf-8")


def collect_last_lines(path: Path, limit: int = 8) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return lines[-limit:]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inject an assistant message into an existing Codex thread via a standalone app-server."
    )
    parser.add_argument("--thread-id", required=True, help="Target thread ID")
    parser.add_argument(
        "--mode",
        choices=["inject", "turn-start"],
        default="inject",
        help="Which external operation to run against the thread.",
    )
    parser.add_argument(
        "--codex-binary",
        default=DEFAULT_CODEX_BINARY,
        help=f"Path to the codex binary (default: {DEFAULT_CODEX_BINARY})",
    )
    parser.add_argument(
        "--message",
        default=None,
        help="Optional marker or expected phrase; if omitted, a unique marker will be generated.",
    )
    parser.add_argument(
        "--turn-prompt",
        default=None,
        help="Optional user prompt for --mode turn-start.",
    )
    args = parser.parse_args()

    marker = args.message or make_marker(args.thread_id)
    client = JsonRpcLineClient(args.codex_binary)

    try:
        init_result = client.initialize()
        read_before = client.request(
            "thread/read",
            {
                "threadId": args.thread_id,
                "includeTurns": True,
            },
        )
        thread_info = read_before["thread"]
        rollout_path = thread_info.get("path")
        if not rollout_path:
            raise RuntimeError("thread/read returned no rollout path")
        rollout = Path(rollout_path)
        before_line_count = len(rollout.read_text(encoding="utf-8").splitlines())

        resume_result = client.request(
            "thread/resume",
            {
                "threadId": args.thread_id,
            },
        )

        completed_params: dict[str, Any] | None = None
        turn_prompt = args.turn_prompt
        if args.mode == "inject":
            injected_item = {
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": marker,
                    }
                ],
            }
            client.request(
                "thread/inject_items",
                {
                    "threadId": args.thread_id,
                    "items": [injected_item],
                },
            )
        else:
            turn_prompt = turn_prompt or (
                "Reply with exactly this text and nothing else:\n"
                f"{marker}"
            )
            client.request(
                "turn/start",
                {
                    "threadId": args.thread_id,
                    "input": [
                        {
                            "type": "text",
                            "text": turn_prompt,
                            "textElements": [],
                        }
                    ],
                },
            )
            completed = client.wait_for_notification("turn/completed")
            completed_params = completed.get("params")

        read_after = client.request(
            "thread/read",
            {
                "threadId": args.thread_id,
                "includeTurns": True,
            },
        )
        after_line_count = len(rollout.read_text(encoding="utf-8").splitlines())

        summary = {
            "thread_id": args.thread_id,
            "marker": marker,
            "codex_binary": args.codex_binary,
            "codex_home": init_result.get("codexHome"),
            "platform_family": init_result.get("platformFamily"),
            "platform_os": init_result.get("platformOs"),
            "rollout_path": rollout_path,
            "mode": args.mode,
            "read_before_status": thread_info.get("status"),
            "resume_thread_status": resume_result.get("thread", {}).get("status"),
            "persisted_in_rollout": file_contains(rollout, marker),
            "marker_visible_in_thread_read_json": marker
            in json.dumps(read_after, ensure_ascii=False),
            "thread_read_visibility_note": (
                "thread/read(includeTurns=true) does not necessarily surface raw injected "
                "response items that are not attached to a turn"
            ),
            "before_line_count": before_line_count,
            "after_line_count": after_line_count,
            "turn_prompt": turn_prompt,
            "turn_completed": completed_params is not None,
            "turn_completed_params": completed_params,
            "notification_methods": [item.get("method") for item in client.notifications],
            "rollout_tail": collect_last_lines(rollout),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
