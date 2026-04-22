#!/usr/bin/env node

import crypto from "node:crypto";
import net from "node:net";
import process from "node:process";

const DEFAULT_URL = "ws://127.0.0.1:4311";
const DEFAULT_MESSAGE = "Reply with exactly `CLI_SHARED_SERVER_POC_MARKER` and nothing else.";
const DEFAULT_SEED_MESSAGE =
  "Reply with exactly `CLI_SHARED_SERVER_POC_SEED` and nothing else.";

function parseArgs(argv) {
  const options = {
    url: DEFAULT_URL,
    message: DEFAULT_MESSAGE,
    cwd: process.cwd(),
    timeoutMs: 120000,
    seedMessage: DEFAULT_SEED_MESSAGE,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--url") {
      options.url = argv[++index];
    } else if (arg === "--message") {
      options.message = argv[++index];
    } else if (arg === "--cwd") {
      options.cwd = argv[++index];
    } else if (arg === "--timeout-ms") {
      options.timeoutMs = Number(argv[++index]);
    } else if (arg === "--seed-message") {
      options.seedMessage = argv[++index];
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
  console.log(`Usage: node scripts/cli_shared_app_server_poc.mjs [options]

Options:
  --url <ws-url>         Websocket app-server URL (default: ${DEFAULT_URL})
  --message <text>       Prompt sent by the sidecar client
  --seed-message <text>  Prompt sent by the frontend client before sidecar resume
  --cwd <path>           Working directory for thread/start (default: current cwd)
  --timeout-ms <ms>      Overall timeout (default: 120000)
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

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const frontend = new JsonRpcWebSocketClient("cli_shared_frontend_poc", options.url);
  const sidecar = new JsonRpcWebSocketClient("cli_shared_sidecar_poc", options.url);
  const summary = {
    url: options.url,
    cwd: options.cwd,
    seed_message: options.seedMessage,
    sidecar_message: options.message,
  };

  const deadline = AbortSignal.timeout(options.timeoutMs);

  try {
    await frontend.connect();
    await frontend.initialize();

    const threadStart = await frontend.request("thread/start", {
      cwd: options.cwd,
    });
    const threadId = threadStart.thread.id;
    summary.thread_id = threadId;

    const seedTurn = await frontend.request("turn/start", {
      threadId,
      input: [
        {
          type: "text",
          text: options.seedMessage,
          textElements: [],
        },
      ],
    });
    summary.frontend_seed_turn_id = seedTurn.turn.id;
    await Promise.race([
      frontend.waitForNotification(
        (payload) =>
          payload.method === "turn/completed" &&
          payload.params?.turn?.id === seedTurn.turn.id,
        options.timeoutMs,
      ),
      abortPromise(deadline, "overall timeout while waiting for frontend seed turn"),
    ]);

    await sidecar.connect();
    await sidecar.initialize();
    await sidecar.request("thread/resume", {
      threadId,
    });

    const sidecarTurn = await sidecar.request("turn/start", {
      threadId,
      input: [
        {
          type: "text",
          text: options.message,
          textElements: [],
        },
      ],
    });
    summary.sidecar_turn_id = sidecarTurn.turn.id;

    const frontendTurnStarted = frontend.waitForNotification(
      (payload) =>
        payload.method === "turn/started" && payload.params?.turn?.id === sidecarTurn.turn.id,
      options.timeoutMs,
    );
    const frontendTurnCompleted = frontend.waitForNotification(
      (payload) =>
        payload.method === "turn/completed" && payload.params?.turn?.id === sidecarTurn.turn.id,
      options.timeoutMs,
    );

    const startedNotification = await Promise.race([
      frontendTurnStarted,
      abortPromise(deadline, "overall timeout while waiting for turn/started"),
    ]);
    const completedNotification = await Promise.race([
      frontendTurnCompleted,
      abortPromise(deadline, "overall timeout while waiting for turn/completed"),
    ]);

    const threadRead = await frontend.request("thread/read", {
      threadId,
      includeTurns: true,
    });

    const readJson = JSON.stringify(threadRead);
    const notificationMethods = frontend.notifications.map((payload) => payload.method);
    summary.frontend_saw_turn_started = startedNotification.method === "turn/started";
    summary.frontend_saw_turn_completed = completedNotification.method === "turn/completed";
    summary.frontend_notification_methods = notificationMethods;
    summary.sidecar_turn_status = completedNotification.params?.turn?.status ?? null;
    summary.marker_visible_in_thread_read = readJson.includes("CLI_SHARED_SERVER_POC_MARKER");
    summary.turn_count = Array.isArray(threadRead.turns) ? threadRead.turns.length : null;
    summary.thread_source = threadRead.thread?.source ?? null;

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
