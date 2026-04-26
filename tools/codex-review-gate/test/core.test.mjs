import assert from "node:assert/strict";
import test from "node:test";

import {
  activeMarkerIsObsolete,
  buildMarkerCommentBody,
  buildStateCommentBody,
  codexAutoReviewLooksOngoing,
  collectCurrentHeadCodexFindings,
  findLatestTrustedMarkerComment,
  findLatestTrustedStateComment,
  hasNewEyesTransition,
  hasNewPlusOneTransition,
  markerFromComment,
  parseStateCommentBody,
  reconcileStateWithMarkerComment,
  reactionIdentity,
  stateFromRecoveredMarkerComment,
  summarizeCodexReactions,
} from "../src/core.mjs";

test("does not reuse an unchanged +1 reaction", () => {
  const baseline = {
    id: "1",
    content: "+1",
    createdAt: "2026-04-26T10:00:00Z",
    user: "chatgpt-codex-connector[bot]",
  };

  assert.equal(
    hasNewPlusOneTransition(baseline, baseline, "2026-04-26T10:01:00Z"),
    false,
  );
});

test("requires the +1 transition to be after the marker", () => {
  const current = {
    id: "2",
    content: "+1",
    createdAt: "2026-04-26T10:00:00Z",
    user: "chatgpt-codex-connector[bot]",
  };

  assert.equal(
    hasNewPlusOneTransition(null, current, "2026-04-26T10:01:00Z"),
    false,
  );
});

test("accepts a new +1 identity after the marker", () => {
  const baseline = {
    id: "1",
    content: "+1",
    createdAt: "2026-04-26T10:00:00Z",
    user: "chatgpt-codex-connector[bot]",
  };
  const current = {
    id: "2",
    content: "+1",
    createdAt: "2026-04-26T10:05:00Z",
    user: "chatgpt-codex-connector[bot]",
  };

  assert.equal(
    hasNewPlusOneTransition(baseline, current, "2026-04-26T10:01:00Z"),
    true,
  );
});

test("detects active markers from obsolete heads", () => {
  assert.equal(activeMarkerIsObsolete({ headSha: "old" }, "new"), true);
  assert.equal(activeMarkerIsObsolete({ headSha: "new" }, "new"), false);
  assert.equal(activeMarkerIsObsolete(null, "new"), false);
});

test("treats eyes as liveness only after the marker", () => {
  const current = {
    id: "5",
    content: "eyes",
    createdAt: "2026-04-26T10:05:00Z",
    user: "chatgpt-codex-connector[bot]",
  };

  assert.equal(hasNewEyesTransition(null, current, "2026-04-26T10:01:00Z"), true);
});

test("summarizes only Codex bot PR-body reactions", () => {
  const reactions = [
    { id: 1, content: "+1", created_at: "2026-04-26T10:00:00Z", user: { login: "octocat" } },
    {
      id: 2,
      content: "+1",
      created_at: "2026-04-26T10:01:00Z",
      user: { login: "chatgpt-codex-connector[bot]" },
    },
    {
      id: 3,
      content: "eyes",
      created_at: "2026-04-26T10:02:00Z",
      user: { login: "chatgpt-codex-connector[bot]" },
    },
  ];

  assert.deepEqual(summarizeCodexReactions(reactions), {
    plusOne: reactionIdentity(reactions[1]),
    eyes: reactionIdentity(reactions[2]),
  });
});

test("round-trips hidden state metadata", () => {
  const state = {
    version: 1,
    createdAt: "2026-04-26T10:00:00Z",
    updatedAt: "2026-04-26T10:01:00Z",
    statusHead: "abc123",
    bootstrap: { status: "closed" },
    activeMarker: null,
    history: [],
  };

  assert.deepEqual(parseStateCommentBody(buildStateCommentBody(state)), state);
});

test("ignores untrusted state comments", () => {
  const trustedBody = buildStateCommentBody({
    version: 1,
    createdAt: "2026-04-26T10:00:00Z",
    updatedAt: "2026-04-26T10:01:00Z",
    statusHead: "trusted",
    bootstrap: { status: "closed" },
    activeMarker: null,
    history: [],
  });
  const attackerBody = buildStateCommentBody({
    version: 1,
    createdAt: "2026-04-26T10:00:00Z",
    updatedAt: "2026-04-26T10:01:00Z",
    statusHead: "attacker",
    bootstrap: { status: "closed" },
    activeMarker: null,
    history: [],
  });

  const comment = findLatestTrustedStateComment([
    { id: 1, body: trustedBody, user: { login: "github-actions[bot]" } },
    { id: 2, body: attackerBody, user: { login: "octocat" } },
  ]);

  assert.equal(parseStateCommentBody(comment.body).statusHead, "trusted");
});

test("finds the latest trusted marker comment", () => {
  const markerBody = buildMarkerCommentBody({
    headSha: "abc123",
    runUrl: "https://example.invalid/runs/1",
    runId: "1",
    runAttempt: "1",
    attempt: 1,
    baseline: { plusOne: null, eyes: null },
    state: "waiting_ack",
  });

  const comment = findLatestTrustedMarkerComment([
    { id: 1, body: markerBody, created_at: "2026-04-26T10:00:00Z", user: { login: "octocat" } },
    {
      id: 2,
      body: markerBody,
      html_url: "https://example.invalid/comments/2",
      created_at: "2026-04-26T10:01:00Z",
      user: { login: "github-actions[bot]" },
    },
  ]);

  assert.deepEqual(markerFromComment(comment), {
    version: 1,
    headSha: "abc123",
    runUrl: "https://example.invalid/runs/1",
    runId: "1",
    runAttempt: "1",
    attempt: 1,
    baseline: { plusOne: null, eyes: null },
    state: "waiting_ack",
    id: "2",
    url: "https://example.invalid/comments/2",
    createdAt: "2026-04-26T10:01:00Z",
  });
});

test("collects only current-head Codex inline findings", () => {
  const comments = [
    {
      id: 10,
      path: "src/lib.rs",
      line: 7,
      commit_id: "head",
      user: { login: "chatgpt-codex-connector[bot]" },
    },
    {
      id: 11,
      path: "src/old.rs",
      line: 8,
      commit_id: "old",
      user: { login: "chatgpt-codex-connector[bot]" },
    },
    {
      id: 12,
      path: "src/human.rs",
      line: 9,
      commit_id: "head",
      user: { login: "octocat" },
    },
  ];

  assert.deepEqual(collectCurrentHeadCodexFindings(comments, "head"), {
    count: 1,
    ids: ["10"],
    samples: ["src/lib.rs:7"],
  });
});

test("treats eyes newer than an old +1 as ongoing bootstrap activity", () => {
  assert.equal(
    codexAutoReviewLooksOngoing({
      plusOne: {
        id: "1",
        content: "+1",
        createdAt: "2026-04-26T10:00:00Z",
        user: "chatgpt-codex-connector[bot]",
      },
      eyes: {
        id: "2",
        content: "eyes",
        createdAt: "2026-04-26T10:05:00Z",
        user: "chatgpt-codex-connector[bot]",
      },
    }),
    true,
  );
});

test("treats +1 newer than eyes as closed bootstrap activity", () => {
  assert.equal(
    codexAutoReviewLooksOngoing({
      plusOne: {
        id: "2",
        content: "+1",
        createdAt: "2026-04-26T10:05:00Z",
        user: "chatgpt-codex-connector[bot]",
      },
      eyes: {
        id: "1",
        content: "eyes",
        createdAt: "2026-04-26T10:00:00Z",
        user: "chatgpt-codex-connector[bot]",
      },
    }),
    false,
  );
});

test("reconstructs an active marker when state was not patched after marker creation", () => {
  const markerBody = buildMarkerCommentBody({
    headSha: "abc123",
    runUrl: "https://example.invalid/runs/1",
    runId: "1",
    runAttempt: "1",
    attempt: 1,
    baseline: { plusOne: null, eyes: null },
    state: "waiting_ack",
  });
  const state = {
    version: 1,
    createdAt: "2026-04-26T10:00:00Z",
    updatedAt: "2026-04-26T10:00:00Z",
    statusHead: "abc123",
    bootstrap: { status: "closed" },
    activeMarker: null,
    history: [],
  };

  const reconciled = reconcileStateWithMarkerComment(
    state,
    {
      id: 2,
      body: markerBody,
      html_url: "https://example.invalid/comments/2",
      created_at: "2026-04-26T10:01:00Z",
      user: { login: "github-actions[bot]" },
    },
    "2026-04-26T10:02:00Z",
  );

  assert.equal(reconciled.changed, true);
  assert.equal(reconciled.state.activeMarker.id, "2");
  assert.equal(reconciled.state.activeMarker.headSha, "abc123");
});

test("does not reactivate a marker when the sticky state comment is missing", () => {
  const markerBody = buildMarkerCommentBody({
    headSha: "abc123",
    runUrl: "https://example.invalid/runs/1",
    runId: "1",
    runAttempt: "1",
    attempt: 1,
    baseline: { plusOne: null, eyes: null },
    state: "waiting_ack",
  });
  const state = stateFromRecoveredMarkerComment({
    markerComment: {
      id: 2,
      body: markerBody,
      html_url: "https://example.invalid/comments/2",
      created_at: "2026-04-26T10:01:00Z",
      user: { login: "github-actions[bot]" },
    },
    marker: {
      headSha: "abc123",
      runUrl: "https://example.invalid/runs/1",
      runId: "1",
      runAttempt: "1",
      attempt: 1,
      baseline: { plusOne: null, eyes: null },
      state: "waiting_ack",
    },
    now: "2026-04-26T10:02:00Z",
    statusHead: "abc123",
    runUrl: "https://example.invalid/runs/2",
    reactions: {
      plusOne: {
        id: "99",
        content: "+1",
        createdAt: "2026-04-26T10:01:30Z",
        user: "chatgpt-codex-connector[bot]",
      },
      eyes: null,
    },
    findings: { ids: ["finding-1"] },
  });

  assert.equal(state.activeMarker, null);
  assert.equal(state.history.length, 1);
  assert.equal(state.history[0].id, "2");
  assert.equal(state.history[0].outcome, "state_lost");
  assert.equal(state.bootstrap.baseline.plusOne.id, "99");
  assert.deepEqual(state.bootstrap.currentHeadFindingIds, ["finding-1"]);
});

test("fails closed when state and latest trusted marker disagree", () => {
  const markerBody = buildMarkerCommentBody({
    headSha: "def456",
    runUrl: "https://example.invalid/runs/2",
    runId: "2",
    runAttempt: "1",
    attempt: 2,
    baseline: { plusOne: null, eyes: null },
    state: "waiting_ack",
  });
  const state = {
    version: 1,
    createdAt: "2026-04-26T10:00:00Z",
    updatedAt: "2026-04-26T10:00:00Z",
    statusHead: "abc123",
    bootstrap: { status: "closed" },
    activeMarker: { id: "1", headSha: "abc123" },
    history: [],
  };

  assert.throws(
    () =>
      reconcileStateWithMarkerComment(
        state,
        {
          id: 2,
          body: markerBody,
          created_at: "2026-04-26T10:01:00Z",
          user: { login: "github-actions[bot]" },
        },
        "2026-04-26T10:02:00Z",
      ),
    /already tracks marker 1/,
  );
});
