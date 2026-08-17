import { assert, describe, test } from "vitest";
import { decideOwnGameSim } from "./ownGameSim.ts";

// The policy for simming your own game on a device not in charge of simming.
// The fence (simDayClaimPolicy) is the safety; this is only about avoiding
// collisions and dead ends politely. The case that motivated the simInFlight
// redefinition: one league-mate's slow live playback used to lock every OTHER
// device out of watching its own game until it ended.

const base = {
	isOwnGame: true,
	isAuthority: false,
	connectedAndReady: true,
	simInFlight: false,
	msUntilAutoSim: undefined,
	cutoffSeconds: 45,
};

describe("decideOwnGameSim", () => {
	test("your own game, nothing in the way: allowed", () => {
		assert.deepStrictEqual(decideOwnGameSim(base), { allow: true });
	});

	test("a league-mate's broadcast does NOT block - only a sim on THIS device", () => {
		// simInFlight is "a live sim playing on this device", nothing room-wide.
		// The caller (ownGameSimGate) feeds it local.liveSimGid alone, so a
		// broadcast running elsewhere never reaches this policy at all - which is
		// what lets two people watch their own games at the same time.
		assert.deepStrictEqual(decideOwnGameSim({ ...base, simInFlight: false }), {
			allow: true,
		});
		assert.strictEqual(
			decideOwnGameSim({ ...base, simInFlight: true }).allow,
			false,
		);
	});

	test("someone else's game: refused", () => {
		assert.strictEqual(
			decideOwnGameSim({ ...base, isOwnGame: false }).allow,
			false,
		);
	});

	test("too close to the scheduled auto sim: refused, else allowed", () => {
		assert.strictEqual(
			decideOwnGameSim({ ...base, msUntilAutoSim: 30_000 }).allow,
			false,
		);
		assert.deepStrictEqual(
			decideOwnGameSim({ ...base, msUntilAutoSim: 46_000 }),
			{ allow: true },
		);
		// 0 disables the window.
		assert.deepStrictEqual(
			decideOwnGameSim({ ...base, msUntilAutoSim: 10_000, cutoffSeconds: 0 }),
			{ allow: true },
		);
	});

	test("not connected: refused; the authority: always allowed", () => {
		assert.strictEqual(
			decideOwnGameSim({ ...base, connectedAndReady: false }).allow,
			false,
		);
		assert.deepStrictEqual(
			decideOwnGameSim({
				...base,
				isAuthority: true,
				isOwnGame: false,
				simInFlight: true,
			}),
			{ allow: true },
		);
	});
});
