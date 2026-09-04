import { assert, describe, test } from "vitest";
import { playerImpactFromGames } from "./getPlayerImpact.ts";
import { encodeShifts } from "./gameShifts.ts";
import type { Game } from "../../common/types.ts";

// A game of one repeated matchup, so the arithmetic is checkable by hand.
const game = ({
	lineups,
	poss,
	pts,
	tids = [3, 7],
	playoffs = false,
	repeat = 1,
}: {
	lineups: [number[], number[]];
	poss: [number, number];
	pts: [number, number];
	tids?: [number, number];
	playoffs?: boolean;
	repeat?: number;
}) => {
	const shifts = [];
	for (let i = 0; i < repeat; i++) {
		// Distinct lineups so they are separate rows rather than one merged one.
		shifts.push({
			lineups: [lineups[0], lineups[1].map((pid) => pid + i * 100)] as [
				number[],
				number[],
			],
			poss,
			pts,
		});
	}
	return {
		season: 2025,
		playoffs,
		numPlayersOnCourt: 5,
		shifts: encodeShifts(shifts, 5),
		teams: [{ tid: tids[0] }, { tid: tids[1] }],
	} as unknown as Game;
};

const starters: [number[], number[]] = [
	[1, 2, 3, 4, 5],
	[11, 12, 13, 14, 15],
];

describe("playerImpactFromGames", () => {
	// 60 possessions a side, plus 6 a game: 100 * 6 / 120 = +5 per 100.
	test("net rating is the margin over the possessions both sides had", () => {
		const games = Array.from({ length: 10 }, () =>
			game({ lineups: starters, poss: [60, 60], pts: [66, 60] }),
		);
		const impact = playerImpactFromGames(games, 1, 3)!;

		assert.strictEqual(impact.poss, 1200);
		assert.closeTo(impact.net, 5, 1e-9);
		assert.strictEqual(impact.partners.length, 4);
		for (const partner of impact.partners) {
			assert.strictEqual(partner.poss, 1200);
			assert.closeTo(partner.together, 5, 1e-9);
			// He never played a possession without them.
			assert.isUndefined(partner.apart);
		}
	});

	// The same man, seen with two different teammates: one lineup wins big and
	// the other loses, and each partner is credited with the one he was in.
	test("together and apart are the two halves of his own minutes", () => {
		const good = Array.from({ length: 10 }, () =>
			game({
				lineups: [
					[1, 2, 3, 4, 5],
					[11, 12, 13, 14, 15],
				],
				poss: [50, 50],
				pts: [60, 50],
			}),
		);
		const bad = Array.from({ length: 10 }, () =>
			game({
				lineups: [
					[1, 6, 7, 8, 9],
					[11, 12, 13, 14, 15],
				],
				poss: [50, 50],
				pts: [45, 50],
			}),
		);

		const impact = playerImpactFromGames([...good, ...bad], 1, 3)!;
		assert.strictEqual(impact.poss, 2000);
		// +100 over the good half, -50 over the bad: +50 over 2000.
		assert.closeTo(impact.net, 2.5, 1e-9);

		const two = impact.partners.find((p) => p.pid === 2)!;
		assert.strictEqual(two.poss, 1000);
		assert.closeTo(two.together, 10, 1e-9);
		assert.closeTo(two.apart, -5, 1e-9);

		const six = impact.partners.find((p) => p.pid === 6)!;
		assert.closeTo(six.together, -5, 1e-9);
		assert.closeTo(six.apart, 10, 1e-9);
	});

	// The scoreboard runs the other way for the away side.
	test("the margin is from his own team's side", () => {
		const games = Array.from({ length: 10 }, () =>
			game({ lineups: starters, poss: [60, 60], pts: [66, 60] }),
		);
		assert.closeTo(playerImpactFromGames(games, 11, 7)!.net, -5, 1e-9);
	});

	test("the playoffs are not part of it", () => {
		const games = Array.from({ length: 10 }, () =>
			game({
				lineups: starters,
				poss: [60, 60],
				pts: [66, 60],
				playoffs: true,
			}),
		);
		assert.isUndefined(playerImpactFromGames(games, 1, 3));
	});

	test("a game his team did not play is skipped", () => {
		const games = Array.from({ length: 10 }, () =>
			game({ lineups: starters, poss: [60, 60], pts: [66, 60], tids: [4, 7] }),
		);
		assert.isUndefined(playerImpactFromGames(games, 1, 3));
	});

	// Too few possessions is not a small number, it is no number.
	test("a player with almost no floor time gets nothing", () => {
		const games = [game({ lineups: starters, poss: [20, 20], pts: [22, 20] })];
		assert.isUndefined(playerImpactFromGames(games, 1, 3));
	});

	test("a pairing too thin to mean anything is left out", () => {
		const many = Array.from({ length: 10 }, () =>
			game({
				lineups: [
					[1, 2, 3, 4, 5],
					[11, 12, 13, 14, 15],
				],
				poss: [60, 60],
				pts: [66, 60],
			}),
		);
		const one = game({
			lineups: [
				[1, 2, 3, 4, 99],
				[11, 12, 13, 14, 15],
			],
			poss: [10, 10],
			pts: [10, 10],
		});

		const impact = playerImpactFromGames([...many, one], 1, 3)!;
		assert.isUndefined(impact.partners.find((p) => p.pid === 99));
		assert.isDefined(impact.partners.find((p) => p.pid === 2));
	});
});
