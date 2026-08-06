import { assert, beforeEach, describe, test } from "vitest";
import {
	claimSnapshotRestoreAttempt,
	resetSnapshotRestoreBackoff,
	shouldAttemptSnapshotRestore,
	SNAPSHOT_RESTORE_RETRY_MS,
} from "./snapshotRestoreBackoff.ts";

// THE INCIDENT: a phone in the playoffs, missing a day of data, clicking Sim
// game and watching the page die and reload. The behind-the-room healer
// restored the room's whole snapshot, failed to reach the head on the catch-up
// after it (there IS a gap - that's the complaint), fell out of the function
// without re-arming its grace window, and did the whole thing again on the next
// five-second health tick. Downloading, decompressing and parsing an entire
// league every five seconds is a tab iOS kills.

const NOW = 1_700_000_000_000;

describe("shouldAttemptSnapshotRestore", () => {
	test("with nothing attempted yet, go", () => {
		assert.strictEqual(
			shouldAttemptSnapshotRestore({ key: "a", last: undefined, now: NOW }),
			true,
		);
	});

	test("the same snapshot is not re-parsed at tick speed", () => {
		for (const elapsed of [0, 5000, 30_000, SNAPSHOT_RESTORE_RETRY_MS - 1]) {
			assert.strictEqual(
				shouldAttemptSnapshotRestore({
					key: "a",
					last: { key: "a", at: NOW },
					now: NOW + elapsed,
				}),
				false,
				`elapsed ${elapsed}`,
			);
		}
	});

	test("but a transient failure still gets another try", () => {
		assert.strictEqual(
			shouldAttemptSnapshotRestore({
				key: "a",
				last: { key: "a", at: NOW },
				now: NOW + SNAPSHOT_RESTORE_RETRY_MS,
			}),
			true,
		);
	});

	test("a freshly published snapshot is a different proposition - go now", () => {
		// The whole recovery story is "the authority publishes a good one within
		// minutes, then you restore it". Making that wait out the backoff for the
		// BAD one would defeat it.
		assert.strictEqual(
			shouldAttemptSnapshotRestore({
				key: "b",
				last: { key: "a", at: NOW },
				now: NOW + 1000,
			}),
			true,
		);
	});

	test("a stamp from the future does not lock the snapshot out", () => {
		assert.strictEqual(
			shouldAttemptSnapshotRestore({
				key: "a",
				last: { key: "a", at: NOW + 10 * SNAPSHOT_RESTORE_RETRY_MS },
				now: NOW,
			}),
			true,
		);
	});
});

describe("claimSnapshotRestoreAttempt", () => {
	beforeEach(() => {
		resetSnapshotRestoreBackoff();
	});

	test("first claim wins, the immediate retry does not", () => {
		assert.strictEqual(claimSnapshotRestoreAttempt("9:gen1"), true);
		assert.strictEqual(claimSnapshotRestoreAttempt("9:gen1"), false);
		assert.strictEqual(claimSnapshotRestoreAttempt("9:gen1"), false);
	});

	test("a new generation of the same seq is a new snapshot", () => {
		// Republishing after eviction keeps the seq and changes the generation -
		// that is precisely the fixed copy a stuck device is waiting for.
		assert.strictEqual(claimSnapshotRestoreAttempt("9:gen1"), true);
		assert.strictEqual(claimSnapshotRestoreAttempt("9:gen2"), true);
	});

	test("teardown clears it so the next session starts fresh", () => {
		assert.strictEqual(claimSnapshotRestoreAttempt("9:gen1"), true);
		assert.strictEqual(claimSnapshotRestoreAttempt("9:gen1"), false);
		resetSnapshotRestoreBackoff();
		assert.strictEqual(claimSnapshotRestoreAttempt("9:gen1"), true);
	});
});
