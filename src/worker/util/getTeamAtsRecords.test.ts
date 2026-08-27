import { assert, beforeEach, describe, test } from "vitest";
import { resetG } from "../../test/helpers.ts";
import type { Game } from "../../common/types.ts";
import { getTeamAtsRecords } from "./getTeamAtsRecords.ts";

// A finished regular-season game: home won by `margin`, on a stored line of
// `spread` (positive = home favored).
const game = (margin: number, spread: number | undefined): Game =>
	({
		gid: 1,
		season: 2026,
		playoffs: false,
		teams: [
			{ tid: 0, pts: 100 + margin, ovr: 50 },
			{ tid: 1, pts: 100, ovr: 50 },
		],
		spread,
	}) as any;

const recordsFor = async (games: Game[]) => getTeamAtsRecords(2026, games);

describe("grading against the spread", () => {
	beforeEach(() => {
		resetG();
	});

	test("the favorite covers by beating its number", async () => {
		const records = await recordsFor([game(10, 5)]);
		assert.deepStrictEqual(records.get(0), { won: 1, lost: 0, pushed: 0 });
		assert.deepStrictEqual(records.get(1), { won: 0, lost: 1, pushed: 0 });
	});

	test("and fails to cover by falling short of it", async () => {
		const records = await recordsFor([game(2, 5)]);
		assert.deepStrictEqual(records.get(0), { won: 0, lost: 1, pushed: 0 });
		assert.deepStrictEqual(records.get(1), { won: 1, lost: 0, pushed: 0 });
	});

	test("landing exactly on the number is a push for both", async () => {
		const records = await recordsFor([game(5, 5)]);
		assert.deepStrictEqual(records.get(0), { won: 0, lost: 0, pushed: 1 });
		assert.deepStrictEqual(records.get(1), { won: 0, lost: 0, pushed: 1 });
	});

	// A bet is graded against the QUOTED line, and a quoted line is a half
	// point. Games simmed while play.ts stored the raw sum of the model and its
	// bias correction carry a float, and a push is an exact equality - so
	// without rounding those games could never push, whatever the score.
	test("a game stored with a raw float still pushes on its quoted number", async () => {
		const records = await recordsFor([game(5, 5.002_134_872_915_3)]);
		assert.deepStrictEqual(records.get(0), { won: 0, lost: 0, pushed: 1 });
		assert.deepStrictEqual(records.get(1), { won: 0, lost: 0, pushed: 1 });
	});

	test("a float that rounds to the other half point grades there", async () => {
		// 5.3 is quoted 5.5, so a 5-point win does not cover it.
		const records = await recordsFor([game(5, 5.3)]);
		assert.deepStrictEqual(records.get(0), { won: 0, lost: 1, pushed: 0 });
		// ...while 5.2 is quoted 5, which the same result pushes.
		const pushed = await recordsFor([game(5, 5.2)]);
		assert.deepStrictEqual(pushed.get(0), { won: 0, lost: 0, pushed: 1 });
	});

	test("playoff games are left out of the record", async () => {
		const playoff = { ...game(10, 5), playoffs: true } as Game;
		assert.strictEqual((await recordsFor([playoff])).size, 0);
	});
});
