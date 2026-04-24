#!/usr/bin/env node

const STATUS_CONTEXT = "codex/review-gate";
const CODEX_BOT_LOGINS = new Set([
  "chatgpt-codex-connector",
  "chatgpt-codex-connector[bot]",
]);
const GATE_MARKER = "codex-review-gate";
const CODEX_CLEAN_COMMENT_PATTERNS = [
  "codex review: didn't find any major issues",
  "codex review: did not find any major issues",
];

class GateFailure extends Error {
  constructor(state, description, message) {
    super(message);
    this.name = "GateFailure";
    this.state = state;
    this.description = description;
  }
}

const config = readConfig();
const repo = parseRepo(config.repository);
const repoPath = `/repos/${encodeURIComponent(repo.owner)}/${encodeURIComponent(repo.name)}`;
const runUrl = `${config.serverUrl}/${repo.owner}/${repo.name}/actions/runs/${config.runId}`;

let statusSha = config.headSha;
let statusReady = false;

main().catch(async (error) => {
  const gateError =
    error instanceof GateFailure
      ? error
      : new GateFailure("error", "Codex review gate errored", error.message);

  if (statusSha && statusReady) {
    try {
      await setCommitStatus(gateError.state, gateError.description);
    } catch (statusError) {
      console.error(`failed to set final ${STATUS_CONTEXT} status: ${statusError.message}`);
    }
  }

  console.error(error.stack || error.message);
  process.exitCode = 1;
});

async function main() {
  const pullRequest = await loadPullRequest();
  statusSha = statusSha || pullRequest.head.sha;

  await setCommitStatus("pending", "Waiting for Codex review on current head");
  statusReady = true;

  if (pullRequest.draft) {
    console.log(`PR #${config.prNumber} is draft; leaving ${STATUS_CONTEXT} pending.`);
    return;
  }

  await failIfCurrentHeadHasCodexFindings();
  const gateComment = await ensureGateComment();
  console.log(`Watching gate comment ${gateComment.html_url || `#${gateComment.id}`} for ${statusSha}.`);

  await waitForCodexResult(gateComment);
  await setCommitStatus("success", "Codex clean review found for current head");
  console.log(`${STATUS_CONTEXT} passed for ${statusSha}.`);
}

function readConfig() {
  const token = requiredEnv("GITHUB_TOKEN");
  const repository = requiredEnv("GITHUB_REPOSITORY");
  const prNumber = Number(process.env.PR_NUMBER || "");
  const headSha = (process.env.HEAD_SHA || "").trim();

  if (!Number.isInteger(prNumber) || prNumber <= 0) {
    throw new Error("PR_NUMBER must be a positive integer");
  }

  return {
    token,
    repository,
    prNumber,
    headSha,
    apiUrl: stripTrailingSlash(process.env.GITHUB_API_URL || "https://api.github.com"),
    serverUrl: stripTrailingSlash(process.env.GITHUB_SERVER_URL || "https://github.com"),
    runId: requiredEnv("GITHUB_RUN_ID"),
    maxWaitMs: secondsEnv("MAX_WAIT_SECONDS", 1800) * 1000,
    pollIntervalMs: secondsEnv("POLL_INTERVAL_SECONDS", 30) * 1000,
  };
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function secondsEnv(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive number`);
  }
  return parsed;
}

function parseRepo(repository) {
  const parts = repository.split("/");
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new Error(`invalid GITHUB_REPOSITORY: ${repository}`);
  }
  return { owner: parts[0], name: parts[1] };
}

function stripTrailingSlash(value) {
  return value.replace(/\/+$/, "");
}

async function loadPullRequest() {
  const { data } = await request("GET", `${repoPath}/pulls/${config.prNumber}`);
  if (!statusSha) {
    statusSha = data.head.sha;
  }
  console.log(`Loaded PR #${config.prNumber}; current head is ${statusSha}.`);
  return data;
}

async function ensureGateComment() {
  const existing = await findGateComment();
  if (existing) {
    console.log(`Reusing existing gate comment ${existing.html_url || `#${existing.id}`}.`);
    return existing;
  }

  const body = [
    "@codex review",
    "",
    "<!-- codex-review-gate",
    `head=${statusSha}`,
    `run=${runUrl}`,
    "-->",
  ].join("\n");

  const { data } = await request("POST", `${repoPath}/issues/${config.prNumber}/comments`, {
    body,
  });
  console.log(`Created gate comment ${data.html_url || `#${data.id}`}.`);
  return data;
}

async function findGateComment() {
  const comments = await paginate(`${repoPath}/issues/${config.prNumber}/comments`, {
    per_page: "100",
  });

  return comments
    .filter((comment) => hasCurrentHeadMarker(comment.body || ""))
    .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())[0];
}

function hasCurrentHeadMarker(body) {
  return body.includes(GATE_MARKER) && body.includes(`head=${statusSha}`);
}

async function waitForCodexResult(gateComment) {
  const deadline = Date.now() + config.maxWaitMs;
  const gateCreatedAt = parseTimestamp(gateComment.created_at, "gate comment creation time");

  while (true) {
    await failIfPullRequestHeadChanged();
    await failIfCurrentHeadHasCodexFindings();

    const cleanSignal = await findCodexCleanSignal(gateCreatedAt);
    if (cleanSignal) {
      await failIfPullRequestHeadChanged();
      await failIfCurrentHeadHasCodexFindings();
      console.log(`Codex clean ${cleanSignal.kind} observed at ${cleanSignal.createdAt}.`);
      return;
    }

    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) {
      throw new GateFailure(
        "failure",
        "Timed out waiting for Codex clean review",
        `Timed out after ${Math.round(config.maxWaitMs / 1000)}s waiting for Codex on ${statusSha}.`,
      );
    }

    console.log(
      `No Codex clean signal yet; sleeping ${Math.round(config.pollIntervalMs / 1000)}s ` +
        `(${Math.round(remainingMs / 1000)}s remaining).`,
    );
    await sleep(Math.min(config.pollIntervalMs, remainingMs));
  }
}

async function failIfPullRequestHeadChanged() {
  const pullRequest = await loadPullRequest();
  if (pullRequest.head.sha === statusSha) {
    return;
  }

  throw new GateFailure(
    "error",
    "PR head changed while waiting for Codex",
    `PR head changed from ${statusSha} to ${pullRequest.head.sha}; this gate run is stale.`,
  );
}

async function findCodexCleanSignal(gateCreatedAt) {
  const comments = await paginate(`${repoPath}/issues/${config.prNumber}/comments`, {
    per_page: "100",
  });
  const cleanComment = comments.find((comment) => {
    const createdAt = parseTimestamp(comment.created_at, "issue comment creation time");
    return (
      createdAt >= gateCreatedAt &&
      isCodexBot(comment.user?.login) &&
      isCodexCleanComment(comment.body || "")
    );
  });
  if (cleanComment) {
    return {
      kind: "top-level comment",
      createdAt: cleanComment.created_at,
      url: cleanComment.html_url,
    };
  }

  const pullRequestReactions = await paginate(
    `${repoPath}/issues/${config.prNumber}/reactions`,
    { per_page: "100" },
  );
  const cleanReaction = pullRequestReactions.find((reaction) => {
    const createdAt = parseTimestamp(reaction.created_at, "pull request reaction creation time");
    return (
      createdAt >= gateCreatedAt &&
      isCodexBot(reaction.user?.login) &&
      reaction.content === "+1"
    );
  });
  if (cleanReaction) {
    return {
      kind: "PR body +1 reaction",
      createdAt: cleanReaction.created_at,
      url: null,
    };
  }

  return null;
}

function isCodexCleanComment(body) {
  const normalized = body.trim().toLowerCase();
  return CODEX_CLEAN_COMMENT_PATTERNS.some((pattern) => normalized.includes(pattern));
}

async function failIfCurrentHeadHasCodexFindings() {
  const findings = await findCurrentHeadCodexFindings();
  if (findings.count === 0) {
    return;
  }

  const sample = findings.samples[0];
  const suffix = sample ? ` First finding: ${sample}` : "";
  throw new GateFailure(
    "failure",
    `Codex posted ${findings.count} finding(s) on current head`,
    `Codex review found ${findings.count} inline comment(s) for ${statusSha}.${suffix}`,
  );
}

async function findCurrentHeadCodexFindings() {
  const reviews = await paginate(`${repoPath}/pulls/${config.prNumber}/reviews`, {
    per_page: "100",
  });
  const currentCodexReviews = reviews.filter(
    (review) => isCodexBot(review.user?.login) && review.commit_id === statusSha,
  );

  const samples = [];
  let count = 0;

  for (const review of currentCodexReviews) {
    const comments = await paginate(
      `${repoPath}/pulls/${config.prNumber}/reviews/${review.id}/comments`,
      { per_page: "100" },
    );
    count += comments.length;

    for (const comment of comments.slice(0, 3)) {
      const location = [comment.path, comment.line || comment.original_line]
        .filter((part) => part !== null && part !== undefined)
        .join(":");
      samples.push(location || `review ${review.id}`);
    }
  }

  return { count, samples };
}

function isCodexBot(login) {
  return CODEX_BOT_LOGINS.has(login || "");
}

function parseTimestamp(value, description) {
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    throw new Error(`invalid ${description}: ${value}`);
  }
  return parsed;
}

async function setCommitStatus(state, description) {
  await request("POST", `${repoPath}/statuses/${statusSha}`, {
    state,
    context: STATUS_CONTEXT,
    description: truncate(description, 140),
    target_url: runUrl,
  });
  console.log(`Set ${STATUS_CONTEXT}=${state}: ${description}`);
}

function truncate(value, maxLength) {
  return value.length <= maxLength ? value : `${value.slice(0, maxLength - 3)}...`;
}

async function paginate(path, query) {
  const results = [];
  let page = 1;

  while (true) {
    const { data } = await request("GET", path, { ...query, page: String(page) });
    if (!Array.isArray(data)) {
      throw new Error(`paginated endpoint did not return an array: ${path}`);
    }
    results.push(...data);
    if (data.length < Number(query.per_page || 100)) {
      return results;
    }
    page += 1;
  }
}

async function request(method, path, bodyOrQuery) {
  const url = new URL(`${config.apiUrl}${path}`);
  const options = {
    method,
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${config.token}`,
      "User-Agent": "codex-review-gate",
      "X-GitHub-Api-Version": "2022-11-28",
    },
  };

  if (method === "GET") {
    for (const [key, value] of Object.entries(bodyOrQuery || {})) {
      url.searchParams.set(key, value);
    }
  } else if (bodyOrQuery) {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(bodyOrQuery);
  }

  const response = await fetch(url, options);
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;

  if (!response.ok) {
    const message = data?.message || response.statusText;
    throw new Error(`${method} ${url.pathname} failed with ${response.status}: ${message}`);
  }

  return { data, headers: response.headers };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
