import "fake-indexeddb/auto";
import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { SyncEngine } from "./SyncEngine.ts";
import { setSyncEngine } from "./engineHolder.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { outbox } from "./outbox.ts";
import type {
	Authority,
	ChangesetEntry,
	SyncSubscriber,
	SyncTransport,
} from "./types.ts";
import type { SyncNotification } from "./notifications.ts";

// A push notification is a promise that the room can see the change it
// announces. These tests pin the contract that keeps the promise honest:
// pushes are bound to their changeset and fire only when it is CONFIRMED in
// the log - not when it is merely queued in a device's outbox. The incident
// behind this: a sim's push arrived on every phone ("Celtics 115") while the
// sim itself sat in the simmer's outbox for 17 minutes because the app was
// backgrounded mid-upload, so the whole room stared at a league one day
// behind its own notifications.

class Bus {
	entries: ChangesetEntry[] = [];
	private seq = 0;
	authority: Authority | undefined;
	private authorityListeners = new Set<(a: Authority | undefined) => void>();

	publish(entry: Omit<ChangesetEntry, "seq">) {
		this.entries.push({ ...entry, seq: ++this.seq });
	}

	setAuthority(a: Authority | undefined) {
		this.authority = a;
		for (const listener of this.authorityListeners) {
			listener(a);
		}
	}

	subscribeAuthority(listener: (a: Authority | undefined) => void) {
		this.authorityListeners.add(listener);
		listener(this.authority);
		return () => {
			this.authorityListeners.delete(listener);
		};
	}
}

class Transport implements SyncTransport {
	readonly clientId: string;
	private bus: Bus;
	failPublish = false;
	notifications: SyncNotification[] = [];
	busyCalls: { busyUntil: number; position?: unknown }[] = [];

	constructor(clientId: string, bus: Bus) {
		this.clientId = clientId;
		this.bus = bus;
	}

	async ping() {}

	async publish(entry: Omit<ChangesetEntry, "seq">) {
		if (this.failPublish) {
			throw new Error("publish failed");
		}
		this.bus.publish(entry);
	}

	async fetchAllEntries() {
		return [...this.bus.entries];
	}

	async fetchEntriesSince(sinceMs: number) {
		return this.bus.entries.filter((e) => e.seq > sinceMs);
	}

	async countEntriesSince(sinceMs: number) {
		return this.bus.entries.filter((e) => e.seq > sinceMs).length;
	}

	subscribe(_subscriber: SyncSubscriber) {
		return () => {};
	}

	async claimAuthority(holderId: string, holderName: string) {
		this.bus.setAuthority({ holderId, holderName });
	}

	subscribeAuthority(onChange: (a: Authority | undefined) => void) {
		return this.bus.subscribeAuthority(onChange);
	}

	async publishBusy(busyUntil: number, position?: unknown) {
		this.busyCalls.push({ busyUntil, position });
	}

	async publishNotification(notification: SyncNotification) {
		this.notifications.push(notification);
	}
}

const changeset = () => ({
	changes: [
		{
			store: "players" as any,
			id: 1,
			type: "put" as const,
			value: { pid: 1, tid: 0 },
		},
	],
});

const notification = (): SyncNotification =>
	({
		title: "Celtics (8-1) 115, Lakers (6-4) 105",
		body: "BOS Yao Ming: 28 PTS",
	}) as any;

// A tick for the fire-and-forget notification publish to settle.
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("notifications fire only when their changeset is confirmed", () => {
	beforeEach(() => {
		setSyncEngine(undefined);
		changeTracker.disable();
		changeTracker.reset();
	});

	test("a healthy publish notifies immediately", async () => {
		const bus = new Bus();
		const transport = new Transport("A", bus);
		const engine = new SyncEngine(transport);

		const outcome = await engine.onLocalChangeset(changeset(), "playMenu.day", [
			notification(),
		]);
		await settle();

		assert.strictEqual(outcome, "confirmed");
		assert.strictEqual(transport.notifications.length, 1);
	});

	// THE INCIDENT: sim finishes, upload dies, push goes out anyway. The push
	// must now wait with the data.
	test("a queued publish does NOT notify - the push waits with the data", async () => {
		const bus = new Bus();
		const transport = new Transport("A", bus);
		const engine = new SyncEngine(transport);
		transport.failPublish = true;

		const outcome = await engine.onLocalChangeset(changeset(), "playMenu.day", [
			notification(),
		]);
		await settle();

		assert.strictEqual(outcome, "queued");
		assert.strictEqual(
			transport.notifications.length,
			0,
			"the phone must not learn the score of a game the room cannot see",
		);

		// The connection comes back and the outbox drains: the push goes out with
		// the data, however late that is.
		transport.failPublish = false;
		const drained = await engine.drainOutbox();
		await settle();
		assert.ok(drained);
		assert.strictEqual(bus.entries.length, 1, "the data landed");
		assert.strictEqual(
			transport.notifications.length,
			1,
			"and the push landed with it",
		);
	});

	test("a re-drain never duplicates a push", async () => {
		const bus = new Bus();
		const transport = new Transport("A", bus);
		const engine = new SyncEngine(transport);
		transport.failPublish = true;
		await engine.onLocalChangeset(changeset(), "playMenu.day", [
			notification(),
		]);
		transport.failPublish = false;

		await engine.drainOutbox();
		await engine.drainOutbox();
		await settle();
		assert.strictEqual(transport.notifications.length, 1);
	});

	test("a changeset with no notifications drains silently", async () => {
		const bus = new Bus();
		const transport = new Transport("A", bus);
		const engine = new SyncEngine(transport);

		await engine.onLocalChangeset(changeset(), "main.updatePlayingTime");
		await settle();
		assert.strictEqual(transport.notifications.length, 0);
	});
});

describe("the position stamp only ever trails confirmed data", () => {
	beforeEach(async () => {
		setSyncEngine(undefined);
		changeTracker.disable();
		changeTracker.reset();
		// The restamp reads the league's position off the cache, so give it one.
		resetG();
		g.setWithoutSavingToDB("season", 2006);
		g.setWithoutSavingToDB("phase", 1);
		await resetCache({});
	});

	// The other half of the incident: the action wrapper stamped the room at
	// day N+1 while day N+1's data sat in the outbox, so every follower read
	// "behind the room" and ground recovery against a gap that was not in the
	// cloud. The wrapper now skips the stamp when the changeset queued; the
	// drain that finally lands it restamps here.
	test("the drain that lands a queued upload restamps the room", async () => {
		const bus = new Bus();
		const transport = new Transport("A", bus);
		const engine = new SyncEngine(transport);
		engine.start();
		await transport.claimAuthority("A", "Tester");

		transport.failPublish = true;
		await engine.onLocalChangeset(changeset(), "playMenu.day");
		transport.busyCalls.length = 0;

		transport.failPublish = false;
		const drained = await engine.drainOutbox();
		assert.ok(drained);

		// The restamp is deliberately fire-and-forget, so give it a few ticks.
		for (let i = 0; i < 20 && transport.busyCalls.length === 0; i++) {
			await settle();
		}

		const releases = transport.busyCalls.filter((c) => c.busyUntil === 0);
		assert.ok(
			releases.length >= 1,
			"landing the upload must restamp the room, or the stamp stays in the past forever",
		);
		engine.stop();
	});

	test("a follower's drain does not stamp anything", async () => {
		const bus = new Bus();
		const transport = new Transport("B", bus);
		const engine = new SyncEngine(transport);
		engine.start();
		bus.setAuthority({ holderId: "someone-else", holderName: "Other" });

		transport.failPublish = true;
		await engine.onLocalChangeset(changeset(), "main.updatePlayingTime");
		transport.busyCalls.length = 0;

		transport.failPublish = false;
		await engine.drainOutbox();
		assert.strictEqual(transport.busyCalls.length, 0);
		engine.stop();
	});
});

describe("outbox notification rows", () => {
	test("bind, take once, and never take twice", async () => {
		const code = `test-room-${Math.random()}`;
		await outbox.addNotifications(code, "entry-1", [notification()]);
		const taken = await outbox.takeNotifications("entry-1");
		assert.ok(Array.isArray(taken));
		assert.strictEqual(taken!.length, 1);
		assert.strictEqual(await outbox.takeNotifications("entry-1"), undefined);
	});

	test("taking for an unknown entry is quietly empty", async () => {
		assert.strictEqual(await outbox.takeNotifications("nope"), undefined);
	});
});
