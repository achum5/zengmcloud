import "fake-indexeddb/auto";
import { assert, beforeEach, describe, test } from "vitest";
import { idb } from "../../db/index.ts";
import {
	claimRecoveryAttempt,
	clearRecoveryAttempt,
	MAX_UNFINISHED_ATTEMPTS,
	nextAttempt,
	readRecoveryAttempt,
	shouldSkipRecovery,
} from "./recoveryBreadcrumb.ts";

// THE INCIDENT this exists for: a phone that crashes and reloads in a loop.
// Restoring a room snapshot parses the entire league; when that is too big for
// the device the OS kills the tab, the PWA reloads, decides it still needs to
// recover, and does it again. An in-memory backoff cannot help - the crash
// resets it. That was, exactly, why the first fix for this changed nothing.

describe("recovery breadcrumb policy", () => {
	test("a leftover record for the same op is a crash, and blocks a retry", () => {
		assert.strictEqual(
			shouldSkipRecovery({ op: "a", startedAt: 0, failures: 1 }, "a"),
			true,
		);
	});

	test("nothing left behind means go", () => {
		assert.strictEqual(shouldSkipRecovery(undefined, "a"), false);
	});

	test("a different payload is a fresh proposition, not a repeat", () => {
		// The fixed checkpoint the authority republishes is a different op string,
		// and blocking it would leave the device permanently broken waiting for a
		// recovery it is no longer allowed to run.
		assert.strictEqual(
			shouldSkipRecovery(
				{ op: "snap:500:gen1", failures: 9, startedAt: 0 },
				"snap:500:gen2",
			),
			false,
		);
	});

	test("failures accumulate per op and reset on a new one", () => {
		const first = nextAttempt(undefined, "a", 100);
		assert.deepStrictEqual(first, { op: "a", startedAt: 100, failures: 1 });

		// The app died, so `first` is still on disk when we come back.
		const second = nextAttempt(first, "a", 200);
		assert.strictEqual(second.failures, 2);

		const other = nextAttempt(second, "b", 300);
		assert.strictEqual(other.failures, 1);
	});

	test("one unreturned attempt is the limit", () => {
		// Deterministic work on a fixed device: a second attempt is just a second
		// crash. Documented as a constant so changing it is a decision.
		assert.strictEqual(MAX_UNFINISHED_ATTEMPTS, 1);
	});
});

describe("recovery breadcrumb, durably", () => {
	beforeEach(async () => {
		const existing = await idb.meta.get("leagues", 1);
		if (existing) {
			delete existing.syncRecoveryAttempt;
			await idb.meta.put("leagues", existing);
		} else {
			await idb.meta.put("leagues", { lid: 1, name: "Test" } as any);
		}
	});

	test("the note is on disk BEFORE the work, so a crash cannot erase it", async () => {
		assert.strictEqual(await claimRecoveryAttempt(1, "snap:1"), true);
		const attempt = await readRecoveryAttempt(1);
		assert.strictEqual(attempt?.op, "snap:1");
		assert.strictEqual(attempt?.failures, 1);
	});

	test("surviving the work clears it, so the next launch retries freely", async () => {
		await claimRecoveryAttempt(1, "snap:1");
		await clearRecoveryAttempt(1);
		assert.strictEqual(await readRecoveryAttempt(1), undefined);
		assert.strictEqual(await claimRecoveryAttempt(1, "snap:1"), true);
	});

	test("THE LOOP: a crash leaves the note, and the next launch refuses", async () => {
		// Launch 1: claims, then the tab dies - clearRecoveryAttempt never runs.
		assert.strictEqual(await claimRecoveryAttempt(1, "snap:1"), true);

		// Launch 2 is a brand-new process. Nothing in memory survived; the note did.
		assert.strictEqual(
			await claimRecoveryAttempt(1, "snap:1"),
			false,
			"an automatic retry must not run the thing that just killed the app",
		);
		// ...and again, and again.
		assert.strictEqual(await claimRecoveryAttempt(1, "snap:1"), false);
	});

	test("Force Resync is not gated by it, but still leaves a trace", async () => {
		await claimRecoveryAttempt(1, "snap:1");
		assert.strictEqual(await claimRecoveryAttempt(1, "snap:1"), false);

		// The user deliberately presses the button.
		assert.strictEqual(
			await claimRecoveryAttempt(1, "snap:1", { gated: false }),
			true,
		);
		const attempt = await readRecoveryAttempt(1);
		assert.strictEqual(
			attempt?.failures,
			2,
			"a manual attempt is still recorded, so a capture shows it was tried",
		);
	});

	test("a republished checkpoint gets its one chance", async () => {
		await claimRecoveryAttempt(1, "snap:500:gen1");
		assert.strictEqual(await claimRecoveryAttempt(1, "snap:500:gen1"), false);
		assert.strictEqual(
			await claimRecoveryAttempt(1, "snap:500:gen2"),
			true,
			"the authority's fresh checkpoint is a different payload and deserves a try",
		);
	});

	test("clearing is scoped to the op, so two heavy jobs can't free each other", async () => {
		// A restore and a publish are both bracketed in the same league. If the
		// publish's finally cleared a restore's note, the restore would get a
		// fresh life on every launch - the loop, back again.
		await claimRecoveryAttempt(1, "snapshot-publish");
		await clearRecoveryAttempt(1, "snapshot-restore:1");
		assert.strictEqual(
			(await readRecoveryAttempt(1))?.op,
			"snapshot-publish",
			"someone else's note must survive",
		);
		await clearRecoveryAttempt(1, "snapshot-publish");
		assert.strictEqual(await readRecoveryAttempt(1), undefined);
	});

	test("THE CRASH WITH NOBODY TOUCHING IT: a phone that dies building a checkpoint stops volunteering", async () => {
		// The phone took over as sim authority. The authority publishes the room
		// checkpoint; building one reads the whole league into memory. It died
		// mid-build, reloaded, found the room still had no checkpoint, and started
		// over - forever, with the user doing nothing.
		assert.strictEqual(await claimRecoveryAttempt(1, "snapshot-publish"), true);
		// The worker is killed here - no finally runs.
		assert.strictEqual(
			await claimRecoveryAttempt(1, "snapshot-publish"),
			false,
			"the next launch must not start the build that just killed it",
		);
	});

	test("no meta row is not a reason to block a recovery", async () => {
		assert.strictEqual(await claimRecoveryAttempt(999, "snap:1"), true);
		assert.strictEqual(await claimRecoveryAttempt(undefined, "snap:1"), true);
	});
});
