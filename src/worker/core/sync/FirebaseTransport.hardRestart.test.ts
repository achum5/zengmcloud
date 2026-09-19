import { assert, beforeEach, describe, test, vi } from "vitest";

// A model of the SDK's provider: getFirestore() hands back the registered
// instance for the app until terminate() is CALLED on it, at which point the
// registration is dropped synchronously and the instance refuses further use.
// This is the exact contract hardRestart() leans on, so the model has to match
// it or the test proves nothing.
type FakeDb = { id: number; terminated: boolean };

let registered: FakeDb | undefined;
let nextId = 1;
const terminateCalls: FakeDb[] = [];

vi.mock("firebase/firestore", () => ({
	getFirestore: () => {
		registered ??= { id: nextId++, terminated: false };
		return registered;
	},
	terminate: (db: FakeDb) => {
		terminateCalls.push(db);
		if (registered === db) {
			registered = undefined;
		}
		db.terminated = true;
		// Mirrors a wedged client: the promise never settles.
		return new Promise<void>(() => {});
	},
	collection: (db: FakeDb, ...path: string[]) => ({ db, path }),
	doc: (db: FakeDb, ...path: string[]) => {
		if (db.terminated) {
			throw new Error(
				"FirebaseError: [code=failed-precondition]: The client has already been terminated.",
			);
		}
		return { db, path };
	},
	onSnapshot: () => () => {},
	// Everything else the module imports but this test never reaches
	addDoc: vi.fn(),
	setDoc: vi.fn(),
	getDoc: vi.fn(),
	getDocFromServer: vi.fn(),
	getCountFromServer: vi.fn(),
	disableNetwork: vi.fn(),
	enableNetwork: vi.fn(),
	getDocsFromServer: vi.fn(),
	deleteDoc: vi.fn(),
	limit: vi.fn(),
	query: vi.fn(),
	orderBy: vi.fn(),
	runTransaction: vi.fn(),
	startAfter: vi.fn(),
	where: vi.fn(),
	Timestamp: { now: () => ({ toMillis: () => Date.now() }) },
	serverTimestamp: vi.fn(),
}));

vi.mock("./firebaseApp.ts", () => ({
	getFirebaseApp: () => ({ name: "test-app" }),
}));

vi.mock("./debugLog.ts", () => ({
	syncDebugLog: vi.fn(),
}));

const { FirebaseTransport } = await import("./FirebaseTransport.ts");

describe("hardRestart", () => {
	beforeEach(() => {
		registered = undefined;
		nextId = 1;
		terminateCalls.length = 0;
	});

	test("hands the transport a client that is not the one it terminated", async () => {
		const transport = new FirebaseTransport("room", "client-A");
		const before = (transport as any).db as FakeDb;
		assert.strictEqual(before.id, 1);

		await transport.hardRestart();

		const after = (transport as any).db as FakeDb;
		assert.notStrictEqual(
			after,
			before,
			"the whole point of a hard restart is a NEW client",
		);
		assert.strictEqual(after.terminated, false);
		assert.strictEqual(before.terminated, true);
		assert.deepStrictEqual(
			terminateCalls.map((db) => db.id),
			[1],
			"exactly the old client is terminated",
		);
	});

	test("reads issued straight after the restart go to the live client", async () => {
		const transport = new FirebaseTransport("room", "client-A");
		await transport.hardRestart();

		// The first thing the engine does after a restart is put its listeners
		// back and read the pointer. With the old deferral, this is the read that
		// died with "The client has already been terminated".
		const { doc } = await import("firebase/firestore");
		assert.doesNotThrow(() =>
			doc((transport as any).db, "leagues", "room", "control", "v2state"),
		);

		// And the microtask queue draining afterwards must not change the answer
		await Promise.resolve();
		await Promise.resolve();
		assert.strictEqual(((transport as any).db as FakeDb).terminated, false);
	});

	test("a second restart in a row still produces a fresh client", async () => {
		const transport = new FirebaseTransport("room", "client-A");
		await transport.hardRestart();
		await transport.hardRestart();

		const db = (transport as any).db as FakeDb;
		assert.strictEqual(db.id, 3);
		assert.strictEqual(db.terminated, false);
		assert.deepStrictEqual(
			terminateCalls.map((x) => x.id),
			[1, 2],
		);
	});

	test("rebinds every tracked listener onto the new client", async () => {
		const transport = new FirebaseTransport("room", "client-A");
		const seen: FakeDb[] = [];
		(transport as any).tracked(() => {
			seen.push((transport as any).db);
			return () => {};
		});
		assert.strictEqual(seen.length, 1);

		await transport.hardRestart();

		assert.strictEqual(seen.length, 2, "the creator ran again");
		assert.notStrictEqual(seen[1], seen[0]);
		assert.strictEqual(seen[1], (transport as any).db);
		assert.strictEqual(seen[1]!.terminated, false);
	});
});
