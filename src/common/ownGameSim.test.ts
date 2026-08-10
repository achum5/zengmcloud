import { assert, describe, test } from "vitest";
import {
	DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS,
	decideOwnGameSim,
} from "./ownGameSim.ts";

const base = {
	isOwnGame: true,
	isAuthority: false,
	connectedAndReady: true,
	simInFlight: false,
	msUntilAutoSim: 10 * 60 * 1000,
	cutoffSeconds: DEFAULT_OWN_GAME_SIM_CUTOFF_SECONDS,
};

describe("decideOwnGameSim", () => {
	test("your own game, well clear of the scheduled sim, is allowed", () => {
		assert.deepEqual(decideOwnGameSim(base), { allow: true });
	});

	test("someone else's game is never yours to sim", () => {
		// The whole point of the exception is that it is exactly one gid wide.
		assert.strictEqual(
			decideOwnGameSim({ ...base, isOwnGame: false }).allow,
			false,
		);
	});

	test("the device in charge of simming plays by the normal rules", () => {
		// It does not need the exception, and must not be blocked by the cutoff -
		// the scheduled sim is its own.
		assert.deepEqual(
			decideOwnGameSim({
				...base,
				isAuthority: true,
				isOwnGame: false,
				msUntilAutoSim: 0,
			}),
			{ allow: true },
		);
	});

	test("refused inside the cutoff window", () => {
		assert.strictEqual(
			decideOwnGameSim({ ...base, msUntilAutoSim: 20 * 1000 }).allow,
			false,
		);
	});

	test("the cutoff boundary is inclusive", () => {
		assert.strictEqual(
			decideOwnGameSim({
				...base,
				cutoffSeconds: 45,
				msUntilAutoSim: 45 * 1000,
			}).allow,
			false,
		);
		assert.strictEqual(
			decideOwnGameSim({
				...base,
				cutoffSeconds: 45,
				msUntilAutoSim: 45 * 1000 + 1,
			}).allow,
			true,
		);
	});

	test("a cutoff of zero disables the window without disabling the feature", () => {
		assert.strictEqual(
			decideOwnGameSim({ ...base, cutoffSeconds: 0, msUntilAutoSim: 1 }).allow,
			true,
		);
	});

	test("nobody auto-playing means there is no race to avoid", () => {
		assert.deepEqual(decideOwnGameSim({ ...base, msUntilAutoSim: undefined }), {
			allow: true,
		});
	});

	test("a sim already running blocks a second one", () => {
		// Two at once is precisely what the fence would refuse, so do not start it.
		assert.strictEqual(
			decideOwnGameSim({ ...base, simInFlight: true }).allow,
			false,
		);
	});

	test("not connected means no", () => {
		assert.strictEqual(
			decideOwnGameSim({ ...base, connectedAndReady: false }).allow,
			false,
		);
	});

	test("every refusal explains itself", () => {
		for (const override of [
			{ isOwnGame: false },
			{ simInFlight: true },
			{ connectedAndReady: false },
			{ msUntilAutoSim: 1000 },
		]) {
			const decision = decideOwnGameSim({ ...base, ...override });
			assert.strictEqual(decision.allow, false);
			assert.ok(
				!decision.allow && decision.reason.length > 0,
				`missing reason for ${JSON.stringify(override)}`,
			);
		}
	});
});
