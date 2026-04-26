export const STATUS_CONTEXT = "codex/review-gate";
export const STATE_MARKER = "codex-review-gate-state";
export const MARKER_COMMENT = "codex-review-gate-marker";
export const STATE_VERSION = 1;

export const DEFAULT_CODEX_BOT_LOGINS = new Set([
  "chatgpt-codex-connector",
  "chatgpt-codex-connector[bot]",
]);

export const DEFAULT_TRUSTED_COMMENT_LOGINS = new Set(["github-actions[bot]"]);

export class GateFailure extends Error {
  constructor(state, description, message) {
    super(message);
    this.name = "GateFailure";
    this.state = state;
    this.description = description;
  }
}

export function parseLoginSet(raw, fallback) {
  if (!raw || !raw.trim()) {
    return new Set(fallback);
  }
  return new Set(
    raw
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean),
  );
}

export function isCodexBot(login, botLogins = DEFAULT_CODEX_BOT_LOGINS) {
  return botLogins.has(login || "");
}

export function isTrustedCommentAuthor(login, trustedLogins = DEFAULT_TRUSTED_COMMENT_LOGINS) {
  return trustedLogins.has(login || "");
}

export function reactionIdentity(reaction) {
  if (!reaction) {
    return null;
  }

  return {
    id: String(reaction.id),
    content: reaction.content,
    createdAt: reaction.created_at,
    user: reaction.user?.login || "",
  };
}

export function summarizeCodexReactions(reactions, botLogins = DEFAULT_CODEX_BOT_LOGINS) {
  return {
    plusOne: selectLatestCodexReaction(reactions, "+1", botLogins),
    eyes: selectLatestCodexReaction(reactions, "eyes", botLogins),
  };
}

export function selectLatestCodexReaction(reactions, content, botLogins = DEFAULT_CODEX_BOT_LOGINS) {
  const matches = reactions
    .filter((reaction) => reaction.content === content && isCodexBot(reaction.user?.login, botLogins))
    .map(reactionIdentity);

  matches.sort((left, right) => {
    const byCreatedAt = parseTimestamp(right.createdAt, "reaction creation time") -
      parseTimestamp(left.createdAt, "reaction creation time");
    if (byCreatedAt !== 0) {
      return byCreatedAt;
    }
    return Number(right.id) - Number(left.id);
  });

  return matches[0] || null;
}

export function sameReactionIdentity(left, right) {
  if (!left || !right) {
    return !left && !right;
  }
  return String(left.id) === String(right.id) && left.createdAt === right.createdAt;
}

export function activeMarkerIsObsolete(activeMarker, statusHead) {
  return Boolean(activeMarker?.headSha && statusHead && activeMarker.headSha !== statusHead);
}

export function hasNewPlusOneTransition(baselinePlusOne, currentPlusOne, markerCreatedAt) {
  if (!currentPlusOne) {
    return false;
  }

  const currentCreatedAt = parseTimestamp(currentPlusOne.createdAt, "Codex +1 reaction creation time");
  const markerCreated = parseTimestamp(markerCreatedAt, "marker creation time");
  if (currentCreatedAt <= markerCreated) {
    return false;
  }

  return !sameReactionIdentity(baselinePlusOne, currentPlusOne);
}

export function hasNewEyesTransition(baselineEyes, currentEyes, markerCreatedAt) {
  if (!currentEyes) {
    return false;
  }

  const currentCreatedAt = parseTimestamp(currentEyes.createdAt, "Codex eyes reaction creation time");
  const markerCreated = parseTimestamp(markerCreatedAt, "marker creation time");
  if (currentCreatedAt <= markerCreated) {
    return false;
  }

  return !sameReactionIdentity(baselineEyes, currentEyes);
}

export function codexAutoReviewLooksOngoing(reactions) {
  if (!reactions.eyes) {
    return false;
  }
  if (!reactions.plusOne) {
    return true;
  }

  return (
    parseTimestamp(reactions.eyes.createdAt, "Codex eyes reaction creation time") >
    parseTimestamp(reactions.plusOne.createdAt, "Codex +1 reaction creation time")
  );
}

export function collectCurrentHeadCodexFindings(
  reviewComments,
  headSha,
  botLogins = DEFAULT_CODEX_BOT_LOGINS,
) {
  const comments = reviewComments.filter((comment) => {
    if (!isCodexBot(comment.user?.login, botLogins)) {
      return false;
    }
    return comment.commit_id === headSha || comment.original_commit_id === headSha;
  });

  const samples = comments.slice(0, 3).map((comment) => {
    const location = [comment.path, comment.line || comment.original_line]
      .filter((part) => part !== null && part !== undefined)
      .join(":");
    return location || `review comment ${comment.id}`;
  });

  return {
    count: comments.length,
    ids: comments.map((comment) => String(comment.id)),
    samples,
  };
}

export function createInitialState({ now, statusHead, runUrl, reactions, findings }) {
  return normalizeState({
    version: STATE_VERSION,
    createdAt: now,
    updatedAt: now,
    statusHead,
    bootstrap: {
      status: "open",
      startedAt: now,
      baseline: reactions,
      currentHeadFindingIds: findings.ids,
    },
    activeMarker: null,
    history: [],
    lastStatus: {
      headSha: statusHead,
      state: "pending",
      updatedAt: now,
      runUrl,
    },
  });
}

export function stateFromMarkerComment({ markerComment, marker, now, statusHead, runUrl }) {
  return normalizeState({
    version: STATE_VERSION,
    createdAt: now,
    updatedAt: now,
    statusHead,
    bootstrap: {
      status: "closed",
      startedAt: marker.createdAt,
      closedAt: marker.createdAt,
      closeReason: "reconstructed_from_marker",
      baseline: marker.baseline || { plusOne: null, eyes: null },
      currentHeadFindingIds: [],
    },
    activeMarker: {
      ...marker,
      id: String(markerComment.id),
      url: markerComment.html_url || marker.url || null,
      createdAt: markerComment.created_at || marker.createdAt,
      state: marker.state || "waiting_ack",
    },
    history: [],
    lastStatus: {
      headSha: statusHead,
      state: "pending",
      updatedAt: now,
      runUrl,
    },
  });
}

export function normalizeState(state) {
  return {
    ...state,
    version: STATE_VERSION,
    history: (state.history || []).slice(-20),
  };
}

export function closeActiveMarker(state, outcome, now, extra = {}) {
  if (!state.activeMarker) {
    return normalizeState(state);
  }

  const closedMarker = {
    ...state.activeMarker,
    state: outcome,
    outcome,
    closedAt: now,
    ...extra,
  };

  return normalizeState({
    ...state,
    updatedAt: now,
    activeMarker: null,
    history: [...(state.history || []), closedMarker],
  });
}

export function reconcileStateWithMarkerComment(state, markerComment, now) {
  const marker = markerComment ? markerFromComment(markerComment) : null;
  if (!marker || stateKnowsMarker(state, marker.id)) {
    return { state, changed: false };
  }

  if (state.activeMarker) {
    throw new GateFailure(
      "error",
      "Multiple controlled Codex markers need manual recovery",
      `Found trusted marker ${marker.id}, but state already tracks marker ${state.activeMarker.id}.`,
    );
  }

  return {
    changed: true,
    state: normalizeState({
      ...state,
      updatedAt: now,
      activeMarker: {
        ...marker,
        state: marker.state || "waiting_ack",
      },
    }),
  };
}

export function stateKnowsMarker(state, markerId) {
  if (!markerId) {
    return false;
  }
  if (String(state.activeMarker?.id || "") === String(markerId)) {
    return true;
  }
  return (state.history || []).some((marker) => String(marker.id || "") === String(markerId));
}

export function updateStateForStatus(state, { now, statusHead, runUrl, status }) {
  return normalizeState({
    ...state,
    updatedAt: now,
    statusHead,
    lastStatus: {
      headSha: statusHead,
      state: status,
      updatedAt: now,
      runUrl,
    },
  });
}

export function buildStateCommentBody(state) {
  const active = state.activeMarker;
  const summary = [
    "codex/review-gate state",
    "",
    `- head: \`${state.statusHead || "unknown"}\``,
    `- marker: \`${active ? `${active.state || "waiting"} for ${active.headSha}` : "none"}\``,
    `- updated: \`${state.updatedAt || "unknown"}\``,
  ];

  return `${summary.join("\n")}\n\n${buildHiddenJson(STATE_MARKER, normalizeState(state))}`;
}

export function parseStateCommentBody(body) {
  const parsed = parseHiddenJson(body, STATE_MARKER);
  return parsed ? normalizeState(parsed) : null;
}

export function buildMarkerCommentBody(marker) {
  return [
    "@codex review",
    "",
    buildHiddenJson(MARKER_COMMENT, {
      version: STATE_VERSION,
      headSha: marker.headSha,
      runUrl: marker.runUrl,
      runId: marker.runId,
      runAttempt: marker.runAttempt,
      attempt: marker.attempt,
      baseline: marker.baseline,
      state: marker.state || "waiting_ack",
    }),
  ].join("\n");
}

export function parseMarkerCommentBody(body) {
  const parsed = parseHiddenJson(body, MARKER_COMMENT);
  if (!parsed) {
    return null;
  }
  return {
    ...parsed,
    version: STATE_VERSION,
  };
}

export function findLatestTrustedStateComment(comments, trustedLogins = DEFAULT_TRUSTED_COMMENT_LOGINS) {
  return [...comments]
    .reverse()
    .find((comment) =>
      isTrustedCommentAuthor(comment.user?.login, trustedLogins) &&
      Boolean(parseStateCommentBody(comment.body || "")),
    ) || null;
}

export function findLatestTrustedMarkerComment(comments, trustedLogins = DEFAULT_TRUSTED_COMMENT_LOGINS) {
  return [...comments]
    .reverse()
    .find((comment) =>
      isTrustedCommentAuthor(comment.user?.login, trustedLogins) &&
      Boolean(parseMarkerCommentBody(comment.body || "")),
    ) || null;
}

export function markerFromComment(comment) {
  const marker = parseMarkerCommentBody(comment.body || "");
  if (!marker) {
    return null;
  }
  return {
    ...marker,
    id: String(comment.id),
    url: comment.html_url || null,
    createdAt: comment.created_at,
  };
}

export function buildHiddenJson(marker, value) {
  return `<!-- ${marker}\n${JSON.stringify(value, null, 2)}\n-->`;
}

export function parseHiddenJson(body, marker) {
  const pattern = new RegExp(`<!--\\s*${escapeRegExp(marker)}\\s*\\n([\\s\\S]*?)\\n\\s*-->`);
  const match = body.match(pattern);
  if (!match) {
    return null;
  }
  return JSON.parse(match[1]);
}

export function parseTimestamp(value, description) {
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    throw new Error(`invalid ${description}: ${value}`);
  }
  return parsed;
}

export function isoNow(nowMs = Date.now()) {
  return new Date(nowMs).toISOString();
}

export function addSeconds(isoTimestamp, seconds) {
  return new Date(parseTimestamp(isoTimestamp, "timestamp") + seconds * 1000).toISOString();
}

export function truncate(value, maxLength) {
  return value.length <= maxLength ? value : `${value.slice(0, maxLength - 3)}...`;
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
