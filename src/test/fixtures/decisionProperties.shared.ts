import { assert, beforeEach, describe, test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { trade } from "../../worker/core/index.ts";
import { changeTracker } from "../../worker/db/changeTracker.ts";
import {
	AI_TID,
	buildValuationLeague,
	type Spec,
	USER_TID,
} from "./valuationLeague.ts";

// ---------------------------------------------------------------------------
// THE DECISION, IN EVERY SPORT.
//
// valuationProperties sweeps the raw dv the calculator produces and holds it to
// a direction. That found two real defects, and it runs under basketball only -
// deliberately, and this is the file that explains why.
//
// The calculator has two designed kinks in it: a player below average has his
// value divided by twenty ("really bad players will just get no PT, but don't
// count them as 0"), and a value above 1 is raised to a per-sport EXPONENT.
// That exponent is 7 in basketball and 3 in football and baseball. At 7 an
// ordinary trade produces dv in the tens or hundreds and the kinks sit far
// below anything that matters; at 3 an ordinary trade produces dv well under
// one, which is exactly where they live. Sweeping dv there measures the kinks
// rather than the direction: it wobbles by a few tenths, non-monotonically, in
// six of the nine properties.
//
// None of that wobble reaches a decision, which is the point of this file. The
// accept line - the thing the AI actually does - was measured at every rating
// from 44 to 64 in all three sports and came out identical: refuse to 50,
// accept from 52, no hole anywhere. So the portable claim is not about dv, it
// is about the answer, and that is what is asserted here, everywhere.
// ---------------------------------------------------------------------------

const TARGET: Spec = { ovr: 52, age: 29 };

const offer = async (give: number[], get: number[]) => {
	await idb.cache.trade.clear();
	await idb.cache.trade.add({
		rid: 0,
		teams: [
			{
				tid: USER_TID,
				pids: give,
				pidsExcluded: [],
				dpids: [],
				dpidsExcluded: [],
			},
			{
				tid: AI_TID,
				pids: get,
				pidsExcluded: [],
				dpids: [],
				dpidsExcluded: [],
			},
		],
	} as any);
	return trade.propose(false);
};

const decideFor = async (give: Spec[], get: Spec[] = [TARGET]) => {
	const { userExtra, aiExtra } = await buildValuationLeague({
		user: give,
		ai: get,
	});
	const [ok] = await offer(
		userExtra.map((p) => p.pid),
		aiExtra.map((p) => p.pid),
	);
	return ok;
};

export const runDecisionProperties = () => {
	describe("what the AI decides, whatever the sport", () => {
		beforeEach(() => {
			changeTracker.disable();
			changeTracker.reset();
		});

		// The control, first, because everything below asserts a refusal or an
		// ordering and a league where nothing is ever accepted satisfies most of
		// that while proving nothing.
		test("a clearly generous offer is taken", async () => {
			assert.isTrue(await decideFor([{ ovr: 70, age: 27 }]));
		});

		test("and a clear downgrade is not", async () => {
			assert.isFalse(await decideFor([{ ovr: 40, age: 30 }]));
		});

		// NO HOLE IN THE LINE. Once the AI starts saying yes it must keep saying
		// yes: a rating it accepts and a better one it refuses would mean a
		// person could be turned down for offering too much, which is the most
		// obviously broken thing a trade AI can do.
		test("the accept line has no hole in it", async () => {
			const results: boolean[] = [];
			const ovrs = [44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64];
			for (const ovr of ovrs) {
				results.push(await decideFor([{ ovr, age: 28 }]));
			}
			const shown = ovrs
				.map((o, i) => `${o}:${results[i] ? "Y" : "n"}`)
				.join(" ");
			const firstYes = results.indexOf(true);
			assert.notStrictEqual(
				firstYes,
				-1,
				`nothing was ever accepted: ${shown}`,
			);
			assert.isFalse(results[0], `even a downgrade was accepted: ${shown}`);
			for (let i = firstYes; i < results.length; i++) {
				assert.isTrue(results[i], `refused a better player: ${shown}`);
			}
		});

		// An injury may not be what tips a deal INTO being accepted.
		test("being hurt never turns a refusal into an acceptance", async () => {
			for (const ovr of [54, 60, 66]) {
				const healthy = await decideFor([{ ovr, age: 28 }]);
				const hurt = await decideFor([{ ovr, age: 28, injuredGames: 60 }]);
				assert.isFalse(
					hurt && !healthy,
					`${ovr} ovr: the AI took him only once he was injured`,
				);
			}
		});

		// And neither may a bigger contract.
		test("a fatter contract never turns a refusal into an acceptance", async () => {
			for (const ovr of [54, 60, 66]) {
				const cheap = await decideFor([
					{ ovr, age: 28, amount: 1500, exp: 3000 },
				]);
				const dear = await decideFor([
					{ ovr, age: 28, amount: 30_000, exp: 3000 },
				]);
				assert.isFalse(
					dear && !cheap,
					`${ovr} ovr: the AI took him only once he got expensive`,
				);
			}
		});

		// THE MONEY PUMP. A front office that takes a swap and takes it back can
		// be run in a loop until the roster is whatever a person wants, and it
		// needs no insight into the game at all.
		test("a swap the AI takes is not one it will take back", async () => {
			const { userExtra, aiExtra } = await buildValuationLeague({
				user: [{ ovr: 70, age: 27 }],
				ai: [TARGET],
			});
			const star = userExtra[0]!.pid;
			const scrub = aiExtra[0]!.pid;
			const [forward] = await offer([star], [scrub]);
			assert.isTrue(forward, "fixture: the gift was refused");
			const [back] = await offer([scrub], [star]);
			assert.isFalse(back, "the AI handed back a player it was just given");
		});
	});
};
