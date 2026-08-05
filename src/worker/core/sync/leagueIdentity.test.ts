import { assert, describe, test } from "vitest";
import { payloadLeagueId, resolveLeagueIdentity } from "./leagueIdentity.ts";

// ---------------------------------------------------------------------------
// The room <-> league binding rule. It has to hold two opposite lines at once,
// and both have drawn blood:
//
//   - Too loose, and a league auto-reconnecting to a room that holds some other
//     league's state gets overwritten. That happened twice, to a main save.
//   - Too strict, and a real league-mate typing the room code is permanently
//     locked out of a league he has been playing in for months, with no way
//     back. That happened the day the first fix shipped.
//
// The discriminator is intent: typing the code and pressing Connect says "this
// league belongs in this room"; an automatic reconnect says nothing at all.
// ---------------------------------------------------------------------------

const room = (initial?: string) => {
	let claimed = initial;
	return {
		fetchRoomLeagueId: async () => claimed,
		// First-writer-wins, like the real transaction.
		claimRoomLeagueId: async (id: string) => {
			claimed ??= id;
			return claimed;
		},
		get claimedId() {
			return claimed;
		},
	};
};

describe("resolveLeagueIdentity", () => {
	test("a league-mate joining an established room adopts its identity", async () => {
		const r = room("dba-new");
		const outcome = await resolveLeagueIdentity({
			localId: undefined,
			explicit: true,
			...r,
		});
		assert.deepStrictEqual(outcome, { action: "adopted", id: "dba-new" });
	});

	test("the first league into an empty room claims it", async () => {
		const r = room();
		const outcome = await resolveLeagueIdentity({
			localId: undefined,
			explicit: true,
			...r,
		});
		assert.strictEqual(outcome.action, "minted");
		assert.strictEqual(
			r.claimedId,
			(outcome as any).id,
			"the room now belongs to this lineage",
		);
	});

	test("losing the first-claim race adopts the winner's identity", async () => {
		// Two devices of the same league connect at once; the transaction hands
		// the loser back whatever landed first.
		const r = room();
		await r.claimRoomLeagueId("winner");
		const outcome = await resolveLeagueIdentity({
			localId: undefined,
			explicit: true,
			fetchRoomLeagueId: async () => undefined, // read raced ahead of the claim
			claimRoomLeagueId: r.claimRoomLeagueId,
		});
		assert.deepStrictEqual(outcome, { action: "minted", id: "winner" });
	});

	test("an ordinary reconnect of the room's own league just matches", async () => {
		const r = room("dba-new");
		const outcome = await resolveLeagueIdentity({
			localId: "dba-new",
			explicit: false,
			...r,
		});
		assert.deepStrictEqual(outcome, { action: "matched", id: "dba-new" });
	});

	// THE LOCKOUT. A copy that minted its own identity - a league-mate who
	// connected once on his own, an older export, a re-created room - must
	// always be able to join by typing the code.
	test("an explicit join re-binds a league whose identity differs", async () => {
		const r = room("dba-new");
		const outcome = await resolveLeagueIdentity({
			localId: "his-own-minted-id",
			explicit: true,
			...r,
		});
		assert.deepStrictEqual(outcome, {
			action: "rebound",
			id: "dba-new",
			previous: "his-own-minted-id",
		});
	});

	// THE SPINNER. Both calls hit the network, and an unbounded hang here left
	// the app on "Connecting..." forever - the check runs before the engine
	// exists, so nothing downstream can recover it. A network failure says
	// nothing about which league owns the room, so it must resolve to
	// "unverified" and let the connect proceed; the payload provenance check
	// still refuses a wrong-league restore.
	test("a hanging binding read gives up instead of wedging the connect", async () => {
		const started = Date.now();
		const outcome = await resolveLeagueIdentity({
			localId: "dba-new",
			explicit: false,
			fetchRoomLeagueId: () => new Promise(() => {}),
			claimRoomLeagueId: () => new Promise(() => {}),
		});
		assert.strictEqual(outcome.action, "unverified");
		assert.ok(
			Date.now() - started < 20_000,
			"the check must be bounded, not open-ended",
		);
	}, 30_000);

	test("a failing claim is unverified, never a refusal", async () => {
		const outcome = await resolveLeagueIdentity({
			localId: "dba-new",
			explicit: false,
			fetchRoomLeagueId: async () => undefined,
			claimRoomLeagueId: async () => {
				throw new Error("Missing or insufficient permissions.");
			},
		});
		assert.strictEqual(
			outcome.action,
			"unverified",
			"an error must not be reported as belonging to another league",
		);
	});

	// THE INCIDENT. Same mismatch, no user intent behind it.
	test("an automatic reconnect to another league's room is refused", async () => {
		const r = room("test-league");
		const outcome = await resolveLeagueIdentity({
			localId: "dba-new",
			explicit: false,
			...r,
		});
		assert.deepStrictEqual(outcome, {
			action: "refused",
			local: "dba-new",
			room: "test-league",
		});
		assert.strictEqual(
			r.claimedId,
			"test-league",
			"a refused connect must not have taken the room over",
		);
	});
});

describe("payloadLeagueId", () => {
	const withGa = (rows: unknown[]) => ({ gameAttributes: rows });

	test("reads the identity a payload carries", () => {
		assert.strictEqual(
			payloadLeagueId(
				withGa([
					{ key: "season", value: 2006 },
					{ key: "syncLeagueId", value: "dba-new" },
				]),
			),
			"dba-new",
		);
	});

	test("a payload from before identities existed has none", () => {
		assert.strictEqual(payloadLeagueId(withGa([{ key: "season" }])), undefined);
	});

	test("junk in the identity row reads as no identity, never as a match", () => {
		assert.strictEqual(
			payloadLeagueId(withGa([{ key: "syncLeagueId", value: "" }])),
			undefined,
		);
		assert.strictEqual(
			payloadLeagueId(withGa([{ key: "syncLeagueId", value: 42 }])),
			undefined,
		);
		assert.strictEqual(payloadLeagueId(undefined), undefined);
		assert.strictEqual(payloadLeagueId({} as any), undefined);
	});
});
