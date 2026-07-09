import { assert, beforeEach, describe, test } from "vitest";
import { SyncEngine } from "./SyncEngine.ts";
import type { Changeset } from "./changeset.ts";
import type {
	Authority,
	ChangesetEntry,
	SyncMember,
	SyncSubscriber,
	SyncTransport,
} from "./types.ts";

// A minimal in-memory transport that records what got published and lets a test
// drive the "sim authority" (authority) doc, so we can check the engine's gating without
// Firebase.
class FakeTransport implements SyncTransport {
	readonly clientId: string;

	published: Omit<ChangesetEntry, "seq">[] = [];

	members: { uid: string; member: SyncMember }[] = [];

	private authority: Authority | undefined;

	private authorityCb: ((authority: Authority | undefined) => void) | undefined;

	constructor(clientId: string) {
		this.clientId = clientId;
	}

	async publish(entry: Omit<ChangesetEntry, "seq">) {
		this.published.push(entry);
	}

	subscribe(_subscriber: SyncSubscriber) {
		return () => {};
	}

	async registerMember(uid: string, member: SyncMember) {
		this.members.push({ uid, member });
	}

	async claimAuthority(holderId: string, holderName: string) {
		this.setAuthority({ holderId, holderName });
	}

	subscribeAuthority(onChange: (authority: Authority | undefined) => void) {
		this.authorityCb = onChange;
		onChange(this.authority);
		return () => {
			this.authorityCb = undefined;
		};
	}

	// Test helper: simulate the shared doc changing (e.g. someone else claims).
	setAuthority(authority: Authority | undefined) {
		this.authority = authority;
		this.authorityCb?.(authority);
	}
}

// A "bulk" changeset - more than MAX_SYNC_CHANGES (200) records, so it takes the
// sim authority-only broadcast path.
const bulkChangeset = (n = 201): Changeset => ({
	changes: Array.from({ length: n }, (_, i) => ({
		store: "players",
		id: i,
		type: "put",
		value: { pid: i },
	})),
});

const smallChangeset = (): Changeset => ({
	changes: [{ store: "players", id: 1, type: "put", value: { pid: 1 } }],
});

describe("SyncEngine sim authority (advance authority)", () => {
	let transport: FakeTransport;
	let engine: SyncEngine;

	beforeEach(() => {
		transport = new FakeTransport("me");
		engine = new SyncEngine(transport);
		engine.start();
	});

	test("starts without sim authority", () => {
		assert.strictEqual(engine.isAuthority(), false);
	});

	test("a bulk sim publishes even if the authority listener drifted (never dropped)", async () => {
		// The worker guard blocks a follower from STARTING a sim. But once a bulk
		// mutation exists locally, dropping its delta would fork the room forever -
		// so the engine publishes it regardless of what the (possibly stale)
		// authority state claims, with a warning.
		await engine.onLocalChangeset(bulkChangeset(), "playMenu.day");
		assert.ok(transport.published.length > 0);
	});

	test("a non-holder DOES broadcast their own draft pick, even if bulk", async () => {
		await engine.onLocalChangeset(bulkChangeset(), "main.draftUser");
		assert.ok(transport.published.length > 0);

		// ...and a bulk draft-advance action too (turn-based, any device).
		transport.published = [];
		await engine.onLocalChangeset(
			bulkChangeset(),
			"playMenu.untilYourNextPick",
		);
		assert.ok(transport.published.length > 0);
	});

	test("anyone can broadcast a small (non-sim) change", async () => {
		await engine.onLocalChangeset(smallChangeset(), "main.proposeTrade");
		assert.strictEqual(transport.published.length, 1);
	});

	test("claiming sim authority unlocks bulk sim broadcast", async () => {
		await engine.claimAuthority();
		assert.strictEqual(engine.isAuthority(), true);

		await engine.onLocalChangeset(bulkChangeset(), "playMenu.day");
		assert.ok(transport.published.length > 0);
	});

	test("sim authority follows the shared doc (handoff)", () => {
		transport.setAuthority({ holderId: "me", holderName: "Me" });
		assert.strictEqual(engine.isAuthority(), true);

		// Someone else takes it - we lose it.
		transport.setAuthority({ holderId: "other", holderName: "Bob" });
		assert.strictEqual(engine.isAuthority(), false);
		assert.strictEqual(engine.getAuthority()?.holderName, "Bob");
	});
});
