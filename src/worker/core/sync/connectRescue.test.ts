import { assert, describe, test } from "vitest";
import { stallTracker, withRescue } from "./connectRescue.ts";
import { fetchDocOverRest } from "./v2/restDelta.ts";

const AFTER = 5;
const later = <T>(ms: number, value: T) =>
	new Promise<T>((resolve) => setTimeout(() => resolve(value), ms));
const failLater = (ms: number, message: string) =>
	new Promise<never>((_, reject) =>
		setTimeout(() => reject(new Error(message)), ms),
	);
const never = <T>() => new Promise<T>(() => {});

describe("connect read rescue", () => {
	test("an SDK read that answers in time is the answer - no rescue sent", async () => {
		let sent = 0;
		const value = await withRescue(
			later(1, "sdk"),
			async () => {
				sent += 1;
				return "rest";
			},
			"t",
			undefined,
			50,
		);
		assert.strictEqual(value, "sdk");
		await later(60, 0);
		assert.strictEqual(sent, 0);
	});

	test("a read the channel sits on is answered over HTTPS", async () => {
		const tracker = stallTracker();
		const value = await withRescue(
			never<string>(),
			async () => "rest",
			"t",
			tracker.stalled,
			AFTER,
		);
		assert.strictEqual(value, "rest");
		assert.isTrue(tracker.stillOut());
	});

	test("no document over HTTPS is no answer: the SDK read still decides", async () => {
		const value = await withRescue(
			later(30, "sdk"),
			async () => undefined,
			"t",
			undefined,
			AFTER,
		);
		assert.strictEqual(value, "sdk");
	});

	test("an SDK read that fails before any rescue fails as before", async () => {
		let sent = 0;
		let error: unknown;
		try {
			await withRescue(
				failLater(1, "boom"),
				async () => {
					sent += 1;
					return "rest";
				},
				"t",
				undefined,
				50,
			);
		} catch (error_) {
			error = error_;
		}
		assert.strictEqual(String(error), "Error: boom");
		assert.strictEqual(sent, 0);
	});

	test("an SDK read that fails while a rescue is out waits on the rescue", async () => {
		const value = await withRescue(
			failLater(10, "boom"),
			() => later(30, "rest"),
			"t",
			undefined,
			AFTER,
		);
		assert.strictEqual(value, "rest");
	});

	test("both failing is the SDK read's failure", async () => {
		let error: unknown;
		try {
			await withRescue(
				failLater(10, "boom"),
				() => later(30, undefined),
				"t",
				undefined,
				AFTER,
			);
		} catch (error_) {
			error = error_;
		}
		assert.strictEqual(String(error), "Error: boom");
	});

	test("a rescue that throws leaves the SDK read to answer", async () => {
		const value = await withRescue(
			later(30, "sdk"),
			() => {
				throw new Error("no fetch");
			},
			"t",
			undefined,
			AFTER,
		);
		assert.strictEqual(value, "sdk");
	});

	test("the rescue is called off once the SDK read answers", async () => {
		let aborted = false;
		const value = await withRescue(
			later(20, "sdk"),
			(signal) =>
				new Promise<string | undefined>((resolve) => {
					signal.addEventListener("abort", () => {
						aborted = true;
						resolve(undefined);
					});
				}),
			"t",
			undefined,
			AFTER,
		);
		assert.strictEqual(value, "sdk");
		assert.isTrue(aborted);
	});

	test("no rescue to hand: just the SDK read", async () => {
		const read = later(1, "sdk");
		assert.strictEqual(withRescue(read, undefined, "t"), read);
	});

	test("a stalled read stops counting as out once the channel answers it", async () => {
		const tracker = stallTracker();
		const read = later(15, "sdk");
		await withRescue(read, async () => "rest", "t", tracker.stalled, AFTER);
		assert.isTrue(tracker.stillOut());
		await read;
		await later(1, 0);
		assert.isFalse(tracker.stillOut());
	});
});

describe("a document over HTTPS", () => {
	const response = (status: number, body: unknown) =>
		({
			ok: status >= 200 && status < 300,
			status,
			json: async () => body,
		}) as unknown as Response;

	test("its fields, read from the path given, each segment encoded", async () => {
		let url = "";
		let auth = "";
		const fields = await fetchDocOverRest({
			projectId: "proj",
			path: ["leagues", "my room", "control", "v2state"],
			token: "tok",
			fetchFn: (async (u: string, init: { headers: unknown }) => {
				url = u;
				auth = (init.headers as Record<string, string>).Authorization!;
				return response(200, {
					fields: {
						version: { integerValue: "1604" },
						byName: { stringValue: "Boston" },
						at: { doubleValue: 1.5 },
					},
				});
			}) as unknown as typeof fetch,
		});
		assert.strictEqual(
			url,
			"https://firestore.googleapis.com/v1/projects/proj/databases/(default)/documents/leagues/my%20room/control/v2state",
		);
		assert.strictEqual(auth, "Bearer tok");
		assert.deepEqual(fields, { version: 1604, byName: "Boston", at: 1.5 });
	});

	test("nothing there, or refused: no answer", async () => {
		for (const status of [403, 404, 500]) {
			const fields = await fetchDocOverRest({
				projectId: "proj",
				path: ["leagues", "x"],
				token: "tok",
				fetchFn: (async () => response(status, {})) as unknown as typeof fetch,
			});
			assert.isUndefined(fields, String(status));
		}
	});
});
