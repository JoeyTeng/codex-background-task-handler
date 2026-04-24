#!/usr/bin/env node

import crypto from "node:crypto";
import net from "node:net";
import process from "node:process";

const DEFAULT_URL = "ws://127.0.0.1:4313";
const DEFAULT_SLEEP_SECONDS = 10;
const DEFAULT_BASE_MARKER = "CLI_TURN_STEER_BASE_MARKER_20260422";
const DEFAULT_STEER_MARKER = "CLI_TURN_STEER_APPLIED_MARKER_20260422";

function parseArgs(argv) {
  const options = {
    url: DEFAULT_URL,
    cwd: process.cwd(),
    timeoutMs: 180000,
    sleepSeconds: DEFAULT_SLEEP_SECONDS,
    baseMarker: DEFAULT_BASE_MARKER,
    steerMarker: DEFAULT_STEER_MARKER,
    settleBeforeSteerMs: 1500,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--url") {
      options.url = argv[++index];
    } else if (arg === "--cwd") {
      options.cwd = argv[++index];
    } else if (arg === "--timeout-ms") {
      options.timeoutMs = Number(argv[++index]);
    } else if (arg === "--sleep-seconds") {
      options.sleepSeconds = Number(argv[++index]);
    } else if (arg === "--base-marker") {
      options.baseMarker = argv[++index];
    } else if (arg === "--steer-marker") {
      options.steerMarker = argv[++index];
    } else if (arg === "--settle-before-steer-ms") {
      options.settleBeforeSteerMs = Number(argv[++index]);
    } else if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node scripts/cli_turn_steer_poc.mjs [options]

Options:
  --url <ws-url>                  Websocket app-server URL (default: ${DEFAULT_URL})
  --cwd <path>                    Working directory for the thread (default: current cwd)
  --timeout-ms <ms>               Overall timeout (default: 180000)
  --sleep-seconds <sec>           Shell sleep duration for the active turn (default: ${DEFAULT_SLEEP_SECONDS})
  --base-marker <text>            Final text if steer is not applied
  --steer-marker <text>           Final text expected after steer
  --settle-before-steer-ms <ms>   Delay after turn/started before sending steer (default: 1500)
`);
}

class JsonRpcWebSocketClient {
  constructor(name, url) {
    this.name = name;
    this.url = url;
    this.socket = null;
    this.nextId = 0;
    this.pending = new Map();
    this.notifications = [];
    this.waiters = [];
    this.frameBuffer = Buffer.alloc(0);
  }

  async connect() {
    const { hostname, port, pathname } = new URL(this.url);
    const socket = net.createConnection({
      host: hostname,
      port: Number(port),
    });
    this.socket = socket;

    await new Promise((resolve, reject) => {
      let handshakeBuffer = Buffer.alloc(0);
      const websocketKey = crypto.randomBytes(16).toString("base64");
      const request = [
        `GET ${pathname || "/"} HTTP/1.1`,
        `Host: ${hostname}:${port}`,
        "Upgrade: websocket",
        "Connection: Upgrade",
        `Sec-WebSocket-Key: ${websocketKey}`,
        "Sec-WebSocket-Version: 13",
        "",
        "",
      ].join("\r\n");

      const cleanup = () => {
        socket.off("connect", onConnect);
        socket.off("data", onData);
        socket.off("error", onError);
        socket.off("close", onClose);
      };

      const onConnect = () => {
        socket.write(request);
      };

      const onData = (chunk) => {
        handshakeBuffer = Buffer.concat([handshakeBuffer, chunk]);
        const headerEnd = handshakeBuffer.indexOf("\r\n\r\n");
        if (headerEnd === -1) {
          return;
        }

        const headerText = handshakeBuffer.slice(0, headerEnd).toString("utf8");
        const statusLine = headerText.split("\r\n", 1)[0] ?? "";
        if (!statusLine.includes("101")) {
          cleanup();
          reject(new Error(`${this.name}: websocket open failed: ${statusLine || "non-101 response"}`));
          return;
        }

        const remaining = handshakeBuffer.slice(headerEnd + 4);
        cleanup();

        socket.on("data", (data) => this.handleSocketData(data));
        socket.on("error", (error) => this.failAllPending(new Error(`${this.name}: websocket runtime error: ${error.message}`)));
        socket.on("close", () => this.failAllPending(new Error(`${this.name}: websocket closed`)));

        if (remaining.length > 0) {
          this.handleSocketData(remaining);
        }

        resolve();
      };

      const onError = (error) => {
        cleanup();
        reject(new Error(`${this.name}: websocket open failed: ${error.message}`));
      };

      const onClose = () => {
        cleanup();
        reject(new Error(`${this.name}: websocket closed during handshake`));
      };

      socket.on("connect", onConnect);
      socket.on("data", onData);
      socket.on("error", onError);
      socket.on("close", onClose);
    });
  }

  close() {
    if (this.socket && !this.socket.destroyed) {
      this.socket.end();
    }
  }

  sendNotification(method, params = undefined) {
    const payload = { method };
    if (params !== undefined) {
      payload.params = params;
    }
    this.sendTextFrame(JSON.stringify(payload));
  }

  async initialize() {
    const result = await this.request("initialize", {
      clientInfo: {
        name: this.name,
        title: this.name,
        version: "0.1.0",
      },
      capabilities: {
        experimentalApi: true,
      },
    });
    this.sendNotification("initialized");
    return result;
  }

  request(method, params = undefined) {
    const id = this.nextId;
    this.nextId += 1;
    const payload = { id, method };
    if (params !== undefined) {
      payload.params = params;
    }

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject, method });
      this.sendTextFrame(JSON.stringify(payload));
    });
  }

  waitForNotification(predicate, timeoutMs) {
    const existing = this.notifications.find(predicate);
    if (existing) {
      return Promise.resolve(existing);
    }

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        this.waiters = this.waiters.filter((entry) => entry.reject !== reject);
        reject(new Error(`${this.name}: timed out waiting for matching notification`));
      }, timeoutMs);

      this.waiters.push({
        predicate,
        resolve: (value) => {
          clearTimeout(timeoutId);
          resolve(value);
        },
        reject,
      });
    });
  }

  handleMessage(raw) {
    const payload = JSON.parse(raw);

    if (Object.prototype.hasOwnProperty.call(payload, "id")) {
      const pending = this.pending.get(payload.id);
      if (!pending) {
        throw new Error(`${this.name}: unexpected response id ${payload.id}`);
      }
      this.pending.delete(payload.id);
      if (payload.error) {
        pending.reject(new Error(`${this.name}: ${pending.method} failed: ${JSON.stringify(payload.error)}`));
      } else {
        pending.resolve(payload.result);
      }
      return;
    }

    this.notifications.push(payload);
    const matching = [];
    const remaining = [];
    for (const waiter of this.waiters) {
      if (waiter.predicate(payload)) {
        matching.push(waiter);
      } else {
        remaining.push(waiter);
      }
    }
    this.waiters = remaining;
    for (const waiter of matching) {
      waiter.resolve(payload);
    }
  }

  handleSocketData(chunk) {
    this.frameBuffer = Buffer.concat([this.frameBuffer, chunk]);

    while (true) {
      const frame = decodeFrame(this.frameBuffer);
      if (!frame) {
        return;
      }

      this.frameBuffer = this.frameBuffer.subarray(frame.bytesConsumed);
      if (frame.opcode === 0x1) {
        this.handleMessage(frame.payload.toString("utf8"));
      } else if (frame.opcode === 0x8) {
        this.failAllPending(new Error(`${this.name}: websocket received close frame`));
        this.close();
        return;
      } else if (frame.opcode === 0x9) {
        this.sendControlFrame(0xA, frame.payload);
      }
    }
  }

  sendTextFrame(text) {
    this.socket.write(encodeClientFrame(0x1, Buffer.from(text, "utf8")));
  }

  sendControlFrame(opcode, payload) {
    this.socket.write(encodeClientFrame(opcode, payload));
  }

  failAllPending(error) {
    for (const { reject } of this.pending.values()) {
      reject(error);
    }
    this.pending.clear();
  }
}

function encodeClientFrame(opcode, payload) {
  const length = payload.length;
  let header;

  if (length < 126) {
    header = Buffer.alloc(2);
    header[1] = 0x80 | length;
  } else if (length < 65536) {
    header = Buffer.alloc(4);
    header[1] = 0x80 | 126;
    header.writeUInt16BE(length, 2);
  } else {
    header = Buffer.alloc(10);
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(length), 2);
  }

  header[0] = 0x80 | opcode;

  const mask = crypto.randomBytes(4);
  const maskedPayload = Buffer.alloc(length);
  for (let index = 0; index < length; index += 1) {
    maskedPayload[index] = payload[index] ^ mask[index % 4];
  }

  return Buffer.concat([header, mask, maskedPayload]);
}

function decodeFrame(buffer) {
  if (buffer.length < 2) {
    return null;
  }

  const first = buffer[0];
  const second = buffer[1];
  const fin = (first & 0x80) !== 0;
  const opcode = first & 0x0f;
  const masked = (second & 0x80) !== 0;

  let offset = 2;
  let length = second & 0x7f;

  if (length === 126) {
    if (buffer.length < offset + 2) {
      return null;
    }
    length = buffer.readUInt16BE(offset);
    offset += 2;
  } else if (length === 127) {
    if (buffer.length < offset + 8) {
      return null;
    }
    length = Number(buffer.readBigUInt64BE(offset));
    offset += 8;
  }

  let mask;
  if (masked) {
    if (buffer.length < offset + 4) {
      return null;
    }
    mask = buffer.subarray(offset, offset + 4);
    offset += 4;
  }

  if (buffer.length < offset + length) {
    return null;
  }

  let payload = buffer.subarray(offset, offset + length);
  if (masked) {
    const unmasked = Buffer.alloc(length);
    for (let index = 0; index < length; index += 1) {
      unmasked[index] = payload[index] ^ mask[index % 4];
    }
    payload = unmasked;
  }

  if (!fin) {
    throw new Error("fragmented websocket frames are not supported in this PoC");
  }

  return {
    opcode,
    payload,
    bytesConsumed: offset + length,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildStartPrompt(options) {
  return [
    `Use the shell tool to run the exact command \`sleep ${options.sleepSeconds}\`.`,
    "Do not end the current turn before that command finishes.",
    `After the command completes, reply with exactly \`${options.baseMarker}\` and nothing else.`,
  ].join(" ");
}

function buildSteerPrompt(options) {
  return [
    "Additional same-turn instruction.",
    "Keep the current turn running until the existing sleep command finishes.",
    "Do not interrupt the current turn or start a new turn.",
    `When this same turn finally completes, reply with exactly \`${options.steerMarker}\` and nothing else.`,
  ].join(" ");
}

function getTurnItems(threadRead, turnId) {
  const turns = Array.isArray(threadRead.turns) ? threadRead.turns : [];
  const matchingTurn = turns.find((turn) => turn?.id === turnId);
  return Array.isArray(matchingTurn?.items) ? matchingTurn.items : [];
}

function getLastAgentMessageText(items) {
  const agentMessages = items.filter((item) => item?.type === "agentMessage");
  const lastAgentMessage = agentMessages.at(-1);
  return typeof lastAgentMessage?.text === "string" ? lastAgentMessage.text : null;
}

function getCompletedItemsForTurn(notifications, turnId) {
  return notifications
    .filter(
      (payload) =>
        payload.method === "item/completed" && payload.params?.turnId === turnId,
    )
    .map((payload) => payload.params?.item)
    .filter(Boolean);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const frontend = new JsonRpcWebSocketClient("cli_turn_steer_frontend_poc", options.url);
  const sidecar = new JsonRpcWebSocketClient("cli_turn_steer_sidecar_poc", options.url);
  const summary = {
    url: options.url,
    cwd: options.cwd,
    sleep_seconds: options.sleepSeconds,
    base_marker: options.baseMarker,
    steer_marker: options.steerMarker,
    settle_before_steer_ms: options.settleBeforeSteerMs,
  };

  const startPrompt = buildStartPrompt(options);
  const steerPrompt = buildSteerPrompt(options);
  const deadline = AbortSignal.timeout(options.timeoutMs);

  try {
    await frontend.connect();
    await frontend.initialize();

    const threadStart = await frontend.request("thread/start", {
      cwd: options.cwd,
    });
    const threadId = threadStart.thread.id;
    summary.thread_id = threadId;

    const turnStartIssuedAt = Date.now();
    const turnStart = await frontend.request("turn/start", {
      threadId,
      cwd: options.cwd,
      approvalPolicy: "never",
      sandboxPolicy: {
        type: "workspaceWrite",
        writableRoots: [],
        networkAccess: false,
        excludeTmpdirEnvVar: false,
        excludeSlashTmp: false,
      },
      input: [
        {
          type: "text",
          text: startPrompt,
          textElements: [],
        },
      ],
    });
    const activeTurnId = turnStart.turn.id;
    summary.turn_id = activeTurnId;

    const turnStarted = await Promise.race([
      frontend.waitForNotification(
        (payload) =>
          payload.method === "turn/started" && payload.params?.turn?.id === activeTurnId,
        options.timeoutMs,
      ),
      abortPromise(deadline, "overall timeout while waiting for turn/started"),
    ]);
    summary.turn_started_notification = turnStarted.method;

    const turnCompletedPromise = frontend.waitForNotification(
      (payload) =>
        payload.method === "turn/completed" && payload.params?.turn?.id === activeTurnId,
      options.timeoutMs,
    );

    const raceResult = await Promise.race([
      turnCompletedPromise.then(() => "completed-before-steer"),
      sleep(options.settleBeforeSteerMs).then(() => "ready-to-steer"),
      abortPromise(deadline, "overall timeout before steer submission"),
    ]);
    if (raceResult !== "ready-to-steer") {
      throw new Error("active turn completed before steer could be submitted");
    }

    await sidecar.connect();
    await sidecar.initialize();

    const steerIssuedAt = Date.now();
    const steerResponse = await sidecar.request("turn/steer", {
      threadId,
      expectedTurnId: activeTurnId,
      input: [
        {
          type: "text",
          text: steerPrompt,
          textElements: [],
        },
      ],
    });

    const turnCompleted = await Promise.race([
      turnCompletedPromise,
      abortPromise(deadline, "overall timeout while waiting for turn/completed after steer"),
    ]);
    const completedAt = Date.now();

    const threadRead = await frontend.request("thread/read", {
      threadId,
      includeTurns: true,
    });
    const turnItems = getTurnItems(threadRead, activeTurnId);
    const completedItems = getCompletedItemsForTurn(frontend.notifications, activeTurnId);
    const finalAgentMessageText = getLastAgentMessageText(turnItems);
    const finalAgentMessageFromNotifications = getLastAgentMessageText(completedItems);
    const turnStartedIds = frontend.notifications
      .filter((payload) => payload.method === "turn/started")
      .map((payload) => payload.params?.turn?.id)
      .filter(Boolean);
    const uniqueTurnStartedIds = [...new Set(turnStartedIds)];

    summary.same_turn_id_after_steer = steerResponse.turnId === activeTurnId;
    summary.completed_turn_id = turnCompleted.params?.turn?.id ?? null;
    summary.turn_completed_same_turn = turnCompleted.params?.turn?.id === activeTurnId;
    summary.turn_status = turnCompleted.params?.turn?.status ?? null;
    summary.turn_duration_ms_reported = turnCompleted.params?.turn?.durationMs ?? null;
    summary.elapsed_ms_turn_start_to_complete = completedAt - turnStartIssuedAt;
    summary.elapsed_ms_steer_to_complete = completedAt - steerIssuedAt;
    summary.completed_after_steer = completedAt > steerIssuedAt;
    summary.turn_started_notification_count = turnStartedIds.length;
    summary.unique_turn_started_ids = uniqueTurnStartedIds;
    summary.no_additional_turn_started = uniqueTurnStartedIds.length === 1 && uniqueTurnStartedIds[0] === activeTurnId;
    summary.final_agent_message_text = finalAgentMessageText;
    summary.final_agent_message_from_notifications = finalAgentMessageFromNotifications;
    summary.final_message_matches_steer = finalAgentMessageText === options.steerMarker;
    summary.final_message_matches_base = finalAgentMessageText === options.baseMarker;
    summary.turn_items_count = turnItems.length;
    summary.turn_read_has_command_execution = turnItems.some((item) => item?.type === "commandExecution");
    summary.completed_items_count = completedItems.length;
    summary.notifications_have_command_execution = completedItems.some(
      (item) => item?.type === "commandExecution",
    );
    summary.final_message_matches_steer_via_notifications =
      finalAgentMessageFromNotifications === options.steerMarker;
    summary.final_message_matches_base_via_notifications =
      finalAgentMessageFromNotifications === options.baseMarker;
    summary.no_premature_completion_signal =
      summary.completed_after_steer &&
      summary.turn_completed_same_turn &&
      summary.elapsed_ms_steer_to_complete >= Math.max(2000, options.sleepSeconds * 1000 - 3000);

    console.log(JSON.stringify(summary, null, 2));
  } finally {
    frontend.close();
    sidecar.close();
  }
}

function abortPromise(signal, message) {
  return new Promise((_, reject) => {
    if (signal.aborted) {
      reject(new Error(message));
      return;
    }
    signal.addEventListener(
      "abort",
      () => {
        reject(new Error(message));
      },
      { once: true },
    );
  });
}

await main();
