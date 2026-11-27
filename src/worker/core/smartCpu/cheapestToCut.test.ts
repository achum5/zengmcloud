import { assert, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { g } from "../../util/index.ts";
import { cheapestToCut, type Dead } from "./worth.ts";

// Dallas, one over the limit in the 2025 preseason (the owner's league): a
// 36-year-old with three seasons left at $24M, a spare player at $4.5M
// whose deal ends this season, one at $1.4M for three seasons, and a rookie
// just drafted (owed nothing).
const players: [string, Dead][] = [
	["Evans", [{ amount: 24010, exp: 2027 }]],
	["Colson", [{ amount: 4540, exp: 2025 }]],
	["Bouknight", [{ amount: 1400, exp: 2027 }]],
	["rookie", []],
];
const owed = (x: [string, Dead]) => x[1];
const names = (xs: [string, Dead][]) => xs.map((x) => x[0]);

test("a team cuts among the players it owes least after this season", () => {
	g.setWithoutSavingToDB("season", 2025);
	g.setWithoutSavingToDB("phase", PHASE.PRESEASON);
	g.setWithoutSavingToDB("minContract", 1150);
	assert.deepEqual(names(cheapestToCut(players, owed)), ["Colson", "rookie"]);
	// Within a season at the minimum of the least, owed alike.
	assert.deepEqual(names(cheapestToCut(players.slice(0, 1), owed)), ["Evans"]);
	assert.deepEqual(names(cheapestToCut([players[0]!, players[2]!], owed)), [
		"Bouknight",
	]);
});

test("in the offseason, after the season to come", () => {
	g.setWithoutSavingToDB("season", 2024);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("minContract", 1150);
	// 2025 is the season to come: Colson owes nothing after it, Bouknight
	// $2.8M - more than a season at the minimum past the least.
	assert.deepEqual(names(cheapestToCut(players.slice(0, 3), owed)), ["Colson"]);
});
