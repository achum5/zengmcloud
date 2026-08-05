# Multiplayer sync: why it keeps breaking, and what to replace it with

## The short version

The sync module is 21,000 lines because it is a **replicated database**: every
device is an independent writer, changes are captured per action, shipped
through an append-only log, and re-applied elsewhere with last-write-wins per
record.

Nearly every expensive thing in it — replay windows, watermarks, epochs, apply
ordering, identity reconciliation, regression guards, divergence detection,
position stamps, stale-stamp verdicts, history repair, the recovery ladder —
exists to answer one question:

> These two databases disagree. Which one is right?

That question has no general answer. That is why every answer needs another
guard, why the guards interact, and why fixing one failure mode keeps producing
the next one.

**The game never actually needs to ask it.** BBGM multiplayer is turn-based:
exactly one device advances the league at a time, and that is already enforced
by the sim authority, the sim-day fence, and the advance claim. There is no
genuine concurrent mutation of the shared world. All of the reconciliation
machinery is solving a problem the game design already prevents.

## Where the lines actually go

31 non-test files, 13,011 lines of production code, 7,934 lines of tests.

| Concern | Lines | Verdict |
| --- | --- | --- |
| Data replication (log, capture, apply, replay, recovery, divergence) | ~9,000 | **Delete** |
| Human coordination (ready-up gates, FA board, notifications, live watching) | ~2,900 | **Keep** |
| Transport, auth, plumbing | ~1,100 | Keep, roughly halves |

The coordination code is good code solving a real problem. Nobody should touch
`draftReady.ts`, `faBoard.ts`, or the live-broadcast path. The replication code
is the part that eats leagues.

## The replacement: one writer, versioned state, immutable segments

**The invariant: a device never authors shared state it did not receive from the
room, except while it holds the baton.**

That one rule deletes divergence as a category. There is no "my copy versus your
copy" — there is version N, and a device either has it or fetches it.

### State layout

Split the league by *mutability*, not by store.

- **`live`** — the working set. All players, teams, current-season teamSeasons
  and teamStats, schedule, playoffSeries, draftPicks, negotiations,
  gameAttributes. This is what changes.
- **`history/<season>`** — everything sealed when a season closes: that season's
  games, teamSeasons, teamStats, awards, playerFeats, events. Written once,
  never rewritten.

Most of a mature league database is history. Today every snapshot re-uploads all
of it and every catch-up replays deltas that touched it.

### The manifest

One Firestore document is the whole protocol:

```json
{
  "version": 412,
  "segments": { "live": "sha-a91f…", "history/2005": "sha-3c02…" },
  "position": { "season": 2006, "phase": 3, "day": 41 },
  "baton": { "deviceId": "…", "name": "Alex", "expiresAt": 1234567890 }
}
```

### Writing — baton holder only

1. Sim locally.
2. Build the changed segments, gzip, hash.
3. Upload each blob **under its content hash**. Immutable: a write can never
   damage a payload a reader is currently allowed to fetch.
4. One Firestore transaction: compare-and-set `version` from N to N+1 and swap
   in the new hashes.

Compare-and-set on a single document is the entire concurrency control. If two
devices race, one loses the transaction and re-reads. Nothing merges.

### Reading — everyone else

1. Watch the manifest.
2. `version` changed → diff the hashes → fetch only the changed blobs, normally
   just `live`.
3. Apply atomically per store, cache silenced, payload validated first.
4. Record the version.

No watermark, no replay, no catch-up paging. "Behind" means
`localVersion < manifest.version`, and the fix is the same single bounded code
path whether a device is one version behind or four hundred.

### Non-holder actions become requests

A device without the baton does not write shared state. It appends a small
request document — `{ id, type: "setRoster", tid, order }`, `"proposeTrade"`,
`"setLineup"` — and the baton holder drains the queue into its own state before
the next advance. The results ship in the next version.

This is the piece that removes the last of the reconciliation. Today, "I set my
roster while you simmed" is two writers and a merge. As a request it is an
ordered, idempotent-by-id instruction applied by exactly one writer.

### Traffic

A `live` segment with closed-season rows stripped out of player records is a few
MB raw, roughly 300–400 KB gzipped: **one document per advance**. Today a single
simmed day publishes 3–4 chunk documents of ~300 KB for its changeset alone,
plus 10–15 log entries across all actions, plus a periodic full-database
snapshot. The new model moves *less* data, not more.

## What gets deleted

Roughly 9,000 lines become unreachable, replaced by maybe 1,200.

- `SyncEngine.ts` (2,735) — keep authority/baton, member registry, connection
  readiness. Everything about chunking, batches, watermarks, `catchUp`,
  `resyncAll`, and the outbox drain goes.
- `changeset.ts` (1,278) — gone except `DEVICE_LOCAL_STORES` and
  `DEVICE_LOCAL_GAME_ATTRIBUTES`, about 40 lines.
- `connect.ts` (2,382) — roughly halves. Watermark banking, the catch-up timer,
  the whole `checkBehindAuthority` ladder, stale-stamp persistence, the
  stranded-schedule sweeps, `isTooFarBehind`, `MANUAL_RESYNC_WINDOW_ENTRIES`.
- `FirebaseTransport.ts` (1,307) — the `changes` half goes, the `control/*` half
  stays.
- `outbox.ts` (216) — uploads are idempotent by content hash; retry is "upload
  again".
- `historyRepair.ts` (277) — demoted to a one-time migration. Sealed history
  cannot drift.
- `applyGuard.ts`, `devChangesetLogger.ts`, `worker/db/changeTracker.ts`, and
  the divergence comparisons in `leaguePosition.ts`.

## What is kept, untouched

- `draftReady.ts` (780) — ready-up gates for preseason, the lottery, each draft
  pick, the deadline, re-signing, each free agency day.
- `faBoard.ts` (382) — blind ranked free agency boards and the mood-weighted
  contested roll. A multiplayer *game mechanic*, not replication.
- `tradeDeadlineGate.ts`, `simBlockedNotify.ts`, `simDayFence.ts` and the two
  pure claim policies, `triviaScores.ts`, live broadcast, lottery reveal.
- `notifications.ts` (1,352) — kept, but it needs a new input. It currently
  reads a changeset to work out what happened. Under the new model the baton
  holder emits a small **semantic event list** alongside the version bump
  ("BOS signed X", "trade: …"). That is a far thinner and more honest change log
  than the current one, and it exists for humans rather than for state.

## Migration — staged, each stage shippable

**Stage 0 — done.** Full-state transfer made safe: content-addressed immutable
payloads, atomic per-store apply, validation before destruction, apply guard on
the restore path, compressed payloads, poisoned-checkpoint eviction. This is
not throwaway work; the new model's read path *is* this path.

**V2 core — built, ships dark (`src/worker/core/sync/v2/`).** The protocol is
now code, not prose. `protocol.ts` is the whole rulebook, pure and tested: a
device at version N may apply exactly version N+1 and nothing else - a later
version is a "gap" answered by checkpoint recovery, never by skipping.
`applyVersion.ts` is the soundness core: the data and the applied-version
marker commit in ONE IndexedDB transaction, so no kill at any moment can
manufacture a marker that lies about the data - which is the failure every
v1 wipe reduces to. Checkpoint restores write the marker LAST, so an
interrupted restore retries cleanly instead of trusting a half-restored
database. Nothing imports this module yet; it risks nothing until wired.

Remaining to wire v2 (in order): transport documents (version pointer with
compare-and-set, per-version delta docs, per-version checkpoints), the engine
loop (publish on advance, subscribe + catchUpPlan on the pointer), the
follower request queue, the per-room protocol marker + creation toggle, and
the connect branch. Capture (changeTracker), the outbox, deferred
notifications, authority claims, and every coordination feature are reused
as-is.

**Stage 1.** Split the payload into `live` + `history/<season>` segments behind a
manifest. Keep the delta log running alongside. Publishing gets cheap enough to
do on every advance instead of every 1,200 entries.

**Stage 2.** Flip the read path: devices catch up by fetching segments, not by
replaying deltas. The log stays but only `notifications.ts` reads it. The entire
recovery ladder becomes unreachable — delete it.

**Stage 3.** Replace non-holder writes with the request queue. Delete the
changeset capture/apply path and the change tracker.

**Stage 4.** Replace the log with the semantic event list. Delete the `changes`
collection.

**After Stage 2 the recurring class of bug is gone**, because no code path
remains that builds state by replaying history onto a live database.

## Risks worth stating plainly

- **The `live` segment has to be genuinely small.** Stripping closed-season
  stats and ratings rows out of player records is required, not optional, and it
  is real work. If `live` stays at tens of megabytes the whole plan fails.
- **Offline play by a non-holder stops being supported**, or becomes an
  explicitly discardable local fork. Today it is nominally supported and is one
  of the main sources of divergence. This is a product decision, not a technical
  one.
- **Firestore document caps still force chunking**, but chunks are
  content-addressed and immutable, so chunking stops being dangerous.
