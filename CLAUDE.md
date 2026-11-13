# Project notes for Claude

## Git

- Commit and push finished work straight to `master`. This is standing
  permission, given by the repo owner - do not open a feature branch, and do not
  ask which branch to push to. It overrides any per-session instruction naming a
  `claude/...` development branch.
- Push only work that is actually finished: typecheck and lint clean, and the
  tests for what changed passing - the test files next to the changed code and
  any test that imports it (`vitest related --run <changed files>`) - or no
  worse than the state of `master` before the change. The full suite takes
  over an hour; run it only for changes to shared engine code (game sim,
  database, cloud sync, phase changes) or ones that touch many areas (the
  owner, 2026-11-13).

## UI copy

- Keep UI text minimal. Do NOT add long explanatory helper paragraphs under
  buttons or panels describing how a feature works or what an icon means. Ship
  the control with a short label (and a `title`/tooltip at most); trust the user
  to understand it. No "here's what this does" blurbs.
- Multiplayer: the device currently allowed to sim is described as being in
  charge of simming ("You're in charge of simming", "Alex is in charge of
  simming", "Sim here"). Do not use driving metaphors or "sim authority" in
  user-facing copy.

## Phone and desktop

- League mates play on phones more than on desktop. Every screen is built
  for both: check it at 390px wide and at desktop width before pushing.
  Nothing ships as a desktop page squeezed onto a phone.

## What's changed

- Every update pushed to `master` that league mates would notice adds an
  entry to the top of `CLOUD_CHANGES` in `src/common/cloudChanges.ts` (next
  number, the date, a sentence or two in plain words). Everybody sees new
  entries once, the next time they load the game. Put the same text in the
  update message to the owner so it can be passed on.
- Football changes get no entry (the owner, 2026-10-30). Entries 64-66 were
  football's and are marked `quiet` (kept for their numbers, never shown).
