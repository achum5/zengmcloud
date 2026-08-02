import { assert, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import getBest from "./getBest.ts";

// ---------------------------------------------------------------------------
// getBest carries a precondition that is stated only in a comment, and that
// only bites in some sports:
//
//   "playersAvailable is sorted by value. So if we hit a player at a minimum
//    contract at a position, no player with lower value needs to be considered"
//
// In the DRAFT_BY_TEAM_OVR sports - baseball, football, hockey - it acts on that
// belief and PRUNES every later player at a position once it has seen a
// minimum-contract player there. Sound on a value-sorted list; not sound on any
// other order.
//
// Posture-driven free agency hands getBest a list sorted by FIT instead, which
// can legitimately put a cheap young player at a position of need ahead of a
// far better one. Basketball never runs the pruning branch, so no basketball
// test in this suite can see the hazard - which is exactly why this one is here
// and why it is a football test.
// ---------------------------------------------------------------------------

const mk = (pid: number, ovr: number, pos: string, amount: number) =>
	({
		pid,
		ratings: [{ ovr, pos, ovrs: { [pos]: ovr } }],
		contract: { amount, exp: g.get("season") + 2 },
		injury: { type: "Healthy", gamesRemaining: 0 },
		value: ovr,
		valueNoPot: ovr,
		valueFuzz: ovr,
		valueNoPotFuzz: ovr,
		tid: -1,
		born: { year: g.get("season") - 25, loc: "" },
	}) as any;

describe("getBest and the order of the list it is given", () => {
	test("a cheap player listed first must not hide a better one at his position", () => {
		resetG();
		const minContract = g.get("minContract");

		// Both are wide receivers - deliberately not one of football's KEY_POSITIONS
		// so that the "we have nobody at this position" escape hatch cannot be what
		// rescues the result. The scrub is on a minimum deal and is listed FIRST.
		const scrub = mk(1, 30, "WR", minContract);
		const star = mk(2, 80, "WR", minContract * 6);

		const chosen = getBest([] as any, [scrub, star], 0);

		assert.ok(
			chosen !== undefined,
			"getBest returned nobody from a two-player pool it could comfortably afford",
		);
		assert.strictEqual(
			(chosen as any).pid,
			star.pid,
			"the far better player at the same position was pruned away purely for being listed behind a minimum-contract player - getBest's pruning assumes the list is in value order, so callers must not hand it any other order",
		);
	});
});
