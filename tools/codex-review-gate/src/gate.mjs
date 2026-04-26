#!/usr/bin/env node

import {
  DEFAULT_CODEX_BOT_LOGINS,
  DEFAULT_TRUSTED_COMMENT_LOGINS,
  GateFailure,
  STATUS_CONTEXT,
  addSeconds,
  buildMarkerCommentBody,
  buildStateCommentBody,
  closeActiveMarker,
  collectCurrentHeadCodexFindings,
  codexAutoReviewLooksOngoing,
  createInitialState,
  findLatestTrustedMarkerComment,
  findLatestTrustedStateComment,
  hasNewEyesTransition,
  hasNewPlusOneTransition,
  isoNow,
  markerFromComment,
  normalizeState,
  parseLoginSet,
  parseStateCommentBody,
  parseTimestamp,
  reconcileStateWithMarkerComment,
  stateFromMarkerComment,
  summarizeCodexReactions,
  truncate,
  updateStateForStatus,
} from "./core.mjs";

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
  failIfLoadedPullRequestHeadChanged(pullRequest, "before starting Codex review");

  if (pullRequest.draft) {
    console.log(`PR #${config.prNumber} is draft; leaving ${STATUS_CONTEXT} pending.`);
    return;
  }

  await driveGate();
}

async function driveGate() {
  const deadline = Date.now() + config.maxWaitMs;
  let stateComment = null;
  let state = null;

  while (true) {
    await failIfPullRequestHeadChanged();
    const snapshot = await loadSnapshot();
    failIfCurrentHeadHasCodexFindings(snapshot.findings);

    ({ state, stateComment } = await ensureState(snapshot, state, stateComment));
    state = updateStateForStatus(state, {
      now: isoNow(),
      statusHead: statusSha,
      runUrl,
      status: "pending",
    });

    const bootstrapResult = await advanceBootstrap(state, stateComment, snapshot);
    state = bootstrapResult.state;
    stateComment = bootstrapResult.stateComment;
    if (bootstrapResult.kind === "wait") {
      await waitOrTimeout(deadline, bootstrapResult.description);
      continue;
    }

    const markerResult = await advanceMarker(state, stateComment, snapshot);
    state = markerResult.state;
    stateComment = markerResult.stateComment;

    if (markerResult.kind === "pass") {
      await failIfPullRequestHeadChanged("before passing Codex review gate");
      const finalSnapshot = await loadSnapshot();
      failIfCurrentHeadHasCodexFindings(finalSnapshot.findings);
      await setCommitStatus("success", "Codex +1 observed and current head has no inline findings");
      state = closeActiveMarker(state, "passed", isoNow(), {
        observedPlusOne: state.activeMarker?.observedPlusOne || snapshot.reactions.plusOne,
      });
      try {
        stateComment = await saveState(state, stateComment);
      } catch (stateError) {
        console.error(`failed to close gate marker after success: ${stateError.message}`);
      }
      console.log(`${STATUS_CONTEXT} passed for ${statusSha}.`);
      return;
    }

    if (markerResult.kind === "continue") {
      continue;
    }

    await waitOrTimeout(deadline, markerResult.description);
  }
}

async function ensureState(snapshot, previousState, previousComment) {
  if (previousState && previousComment) {
    return { state: previousState, stateComment: previousComment };
  }

  const stateComment = findLatestTrustedStateComment(snapshot.comments, config.trustedCommentLogins);
  if (stateComment) {
    const markerComment = findLatestTrustedMarkerComment(snapshot.comments, config.trustedCommentLogins);
    const reconciled = reconcileStateWithMarkerComment(
      parseStateCommentBody(stateComment.body || ""),
      markerComment,
      isoNow(),
    );
    const reconciledStateComment = reconciled.changed
      ? await saveState(reconciled.state, stateComment)
      : stateComment;

    return {
      state: reconciled.state,
      stateComment: reconciledStateComment,
    };
  }

  const markerComment = findLatestTrustedMarkerComment(snapshot.comments, config.trustedCommentLogins);
  const now = isoNow();
  const state = markerComment
    ? stateFromMarkerComment({
        markerComment,
        marker: markerFromComment(markerComment),
        now,
        statusHead: statusSha,
        runUrl,
      })
    : createInitialState({
        now,
        statusHead: statusSha,
        runUrl,
        reactions: snapshot.reactions,
        findings: snapshot.findings,
      });

  const createdStateComment = await saveState(state, null);
  return { state, stateComment: createdStateComment };
}

async function advanceBootstrap(state, stateComment, snapshot) {
  if (state.bootstrap?.status === "closed") {
    return { kind: "continue", state, stateComment };
  }

  const now = isoNow();
  const startedAt = state.bootstrap?.startedAt || now;
  const graceEndsAt = addSeconds(startedAt, config.bootstrapGraceSeconds);
  const timeoutAt = addSeconds(startedAt, config.bootstrapTimeoutSeconds);
  const graceOpen = Date.now() < parseTimestamp(graceEndsAt, "bootstrap grace deadline");
  const timeoutOpen = Date.now() < parseTimestamp(timeoutAt, "bootstrap timeout deadline");
  const autoReviewLooksOngoing = codexAutoReviewLooksOngoing(snapshot.reactions);

  state = normalizeState({
    ...state,
    updatedAt: now,
    bootstrap: {
      ...state.bootstrap,
      status: graceOpen || (autoReviewLooksOngoing && timeoutOpen) ? "open" : "closed",
      startedAt,
      graceEndsAt,
      timeoutAt,
      baseline: snapshot.reactions,
      currentHeadFindingIds: snapshot.findings.ids,
      closedAt: graceOpen || (autoReviewLooksOngoing && timeoutOpen) ? state.bootstrap?.closedAt : now,
      closeReason: graceOpen
        ? undefined
        : autoReviewLooksOngoing && timeoutOpen
          ? undefined
          : autoReviewLooksOngoing
            ? "bootstrap_timeout"
            : "bootstrap_quiet",
    },
  });

  stateComment = await saveState(state, stateComment);

  if (state.bootstrap.status === "closed") {
    console.log(`Bootstrap baseline closed: ${state.bootstrap.closeReason}.`);
    return { kind: "continue", state, stateComment };
  }

  const description = graceOpen
    ? "Waiting for initial Codex auto-review baseline grace period"
    : "Waiting for PR-open Codex auto-review to finish before gate marker";
  return { kind: "wait", description, state, stateComment };
}

async function advanceMarker(state, stateComment, snapshot) {
  if (!state.activeMarker) {
    const marker = await createGateMarker(snapshot.reactions, state);
    state = normalizeState({
      ...state,
      updatedAt: isoNow(),
      activeMarker: marker,
    });
    stateComment = await saveState(state, stateComment);
    await setCommitStatus("pending", "Waiting for Codex +1 on controlled review marker");
    return {
      kind: "wait",
      description: `Created controlled Codex marker ${marker.url || `#${marker.id}`}`,
      state,
      stateComment,
    };
  }

  let activeMarker = state.activeMarker;
  if (activeMarker.state === "pass_candidate") {
    if (activeMarker.headSha !== statusSha) {
      state = closeActiveMarker(state, "plus_one_observed_obsolete_head", isoNow(), {
        observedPlusOne: activeMarker.observedPlusOne || snapshot.reactions.plusOne,
      });
      stateComment = await saveState(state, stateComment);
      console.log(
        `Recovered Codex +1 for obsolete head ${activeMarker.headSha}; current head is ${statusSha}.`,
      );
      return { kind: "continue", state, stateComment };
    }

    return { kind: "pass", state, stateComment };
  }

  if (hasNewEyesTransition(activeMarker.baseline?.eyes, snapshot.reactions.eyes, activeMarker.createdAt)) {
    activeMarker = {
      ...activeMarker,
      state: "waiting_result",
      observedEyes: snapshot.reactions.eyes,
    };
    state = normalizeState({
      ...state,
      updatedAt: isoNow(),
      activeMarker,
    });
    stateComment = await saveState(state, stateComment);
  }

  if (
    hasNewPlusOneTransition(
      activeMarker.baseline?.plusOne,
      snapshot.reactions.plusOne,
      activeMarker.createdAt,
    )
  ) {
    if (activeMarker.headSha !== statusSha) {
      state = closeActiveMarker(state, "plus_one_observed_obsolete_head", isoNow(), {
        observedPlusOne: snapshot.reactions.plusOne,
      });
      stateComment = await saveState(state, stateComment);
      console.log(
        `Codex +1 closed marker for obsolete head ${activeMarker.headSha}; current head is ${statusSha}.`,
      );
      return { kind: "continue", state, stateComment };
    }

    state = normalizeState({
      ...state,
      updatedAt: isoNow(),
      activeMarker: {
        ...activeMarker,
        state: "pass_candidate",
        passCandidateAt: isoNow(),
        observedPlusOne: snapshot.reactions.plusOne,
      },
    });
    stateComment = await saveState(state, stateComment);
    return { kind: "pass", state, stateComment };
  }

  const markerAgeMs = Date.now() - parseTimestamp(activeMarker.createdAt, "marker creation time");
  if (markerAgeMs >= config.markerTimeoutMs) {
    state = closeActiveMarker(state, "stalled", isoNow(), {
      stalledAfterSeconds: Math.round(config.markerTimeoutMs / 1000),
      lastObservedPlusOne: snapshot.reactions.plusOne,
      lastObservedEyes: snapshot.reactions.eyes,
    });
    stateComment = await saveState(state, stateComment);
    await setCommitStatus("pending", "Codex review marker stalled; retrying with fresh baseline");
    console.log(`Marker ${activeMarker.id} stalled; re-baselining before retry.`);
    return { kind: "continue", state, stateComment };
  }

  const remainingSeconds = Math.round((config.markerTimeoutMs - markerAgeMs) / 1000);
  return {
    kind: "wait",
    description: `Waiting for Codex +1 transition (${remainingSeconds}s before marker retry)`,
    state,
    stateComment,
  };
}

async function createGateMarker(reactionBaseline, state) {
  const attempt = (state.history || []).length + 1;
  const marker = {
    version: 1,
    headSha: statusSha,
    runUrl,
    runId: config.runId,
    runAttempt: config.runAttempt,
    attempt,
    baseline: reactionBaseline,
    state: "waiting_ack",
  };

  const { data } = await request("POST", `${repoPath}/issues/${config.prNumber}/comments`, {
    body: buildMarkerCommentBody(marker),
  });

  const created = {
    ...marker,
    id: String(data.id),
    url: data.html_url || null,
    createdAt: data.created_at,
  };
  console.log(`Created controlled Codex marker ${created.url || `#${created.id}`} for ${statusSha}.`);
  return created;
}

async function saveState(state, stateComment) {
  const body = buildStateCommentBody(state);
  if (stateComment?.id) {
    const { data } = await request("PATCH", `${repoPath}/issues/comments/${stateComment.id}`, { body });
    return data;
  }

  const { data } = await request("POST", `${repoPath}/issues/${config.prNumber}/comments`, { body });
  console.log(`Created gate state comment ${data.html_url || `#${data.id}`}.`);
  return data;
}

async function loadSnapshot() {
  const [comments, issueReactions, reviewComments] = await Promise.all([
    paginate(`${repoPath}/issues/${config.prNumber}/comments`, { per_page: "100" }),
    paginate(`${repoPath}/issues/${config.prNumber}/reactions`, { per_page: "100" }),
    paginate(`${repoPath}/pulls/${config.prNumber}/comments`, { per_page: "100" }),
  ]);

  const findings = collectCurrentHeadCodexFindings(
    reviewComments,
    statusSha,
    config.codexBotLogins,
  );

  return {
    comments,
    issueReactions,
    reviewComments,
    reactions: summarizeCodexReactions(issueReactions, config.codexBotLogins),
    findings,
  };
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
    runAttempt: process.env.GITHUB_RUN_ATTEMPT || "1",
    maxWaitMs: secondsEnv("MAX_WAIT_SECONDS", 7200, { allowZero: false }) * 1000,
    markerTimeoutMs: secondsEnv("MARKER_TIMEOUT_SECONDS", 3600, { allowZero: false }) * 1000,
    pollIntervalMs: secondsEnv("POLL_INTERVAL_SECONDS", 30, { allowZero: false }) * 1000,
    bootstrapGraceSeconds: secondsEnv("BOOTSTRAP_GRACE_SECONDS", 60, { allowZero: true }),
    bootstrapTimeoutSeconds: secondsEnv("BOOTSTRAP_TIMEOUT_SECONDS", 3600, { allowZero: false }),
    codexBotLogins: parseLoginSet(process.env.CODEX_BOT_LOGINS || "", DEFAULT_CODEX_BOT_LOGINS),
    trustedCommentLogins: parseLoginSet(
      process.env.TRUSTED_COMMENT_LOGINS || "",
      DEFAULT_TRUSTED_COMMENT_LOGINS,
    ),
  };
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function secondsEnv(name, fallback, { allowZero }) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number(raw);
  const valid = Number.isFinite(parsed) && (allowZero ? parsed >= 0 : parsed > 0);
  if (!valid) {
    throw new Error(`${name} must be a ${allowZero ? "non-negative" : "positive"} number`);
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
  console.log(`Loaded PR #${config.prNumber}; PR head is ${data.head.sha}; gate head is ${statusSha}.`);
  return data;
}

async function failIfPullRequestHeadChanged(phase = "while waiting for Codex") {
  const pullRequest = await loadPullRequest();
  failIfLoadedPullRequestHeadChanged(pullRequest, phase);
}

function failIfLoadedPullRequestHeadChanged(pullRequest, phase) {
  if (pullRequest.head.sha === statusSha) {
    return;
  }

  throw new GateFailure(
    "error",
    `PR head changed ${phase}`,
    `PR head changed from ${statusSha} to ${pullRequest.head.sha}; this gate run is stale.`,
  );
}

function failIfCurrentHeadHasCodexFindings(findings) {
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

async function waitOrTimeout(deadline, description) {
  const remainingMs = deadline - Date.now();
  if (remainingMs <= 0) {
    throw new GateFailure(
      "failure",
      "Timed out waiting for Codex review signal",
      `Timed out after ${Math.round(config.maxWaitMs / 1000)}s. Last state: ${description}.`,
    );
  }

  const sleepMs = Math.min(config.pollIntervalMs, remainingMs);
  console.log(
    `${description}; sleeping ${Math.round(sleepMs / 1000)}s ` +
      `(${Math.round(remainingMs / 1000)}s remaining).`,
  );
  await sleep(sleepMs);
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
