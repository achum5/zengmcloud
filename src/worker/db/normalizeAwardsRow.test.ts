import { assert, describe, test } from "vitest";
import { normalizeAwardsRow } from "./normalizeAwardsRow.ts";

const player = (pid: number, tid: number) => ({
	pid,
	tid,
	name: `Player ${pid}`,
	pts: 20,
	trb: 10,
	ast: 5,
});

const team = (tid: number) => ({
	tid,
	abbrev: "ABC",
	region: "Region",
	name: "Name",
	won: 60,
	lost: 22,
	tied: undefined,
	otl: undefined,
});

// The shape a league-mate on a build from before the custom-awards upgrade
// still writes, and the changesets and snapshots still carry.
const oldRow = () => ({
	season: 2020,
	bestRecord: team(3),
	bestRecordConfs: [team(3), team(7)],
	mvp: player(1, 3),
	dpoy: player(2, 7),
	roy: player(4, 5),
	smoy: player(6, 5),
	mip: player(8, 9),
	finalsMvp: player(1, 3),
	allLeague: [
		{ title: "First Team", players: [player(1, 3)] },
		{ title: "Second Team", players: [player(2, 7)] },
		{ title: "Third Team", players: [player(4, 5)] },
	],
	allDefensive: [
		{ title: "First Team", players: [player(2, 7)] },
		{ title: "Second Team", players: [player(1, 3)] },
		{ title: "Third Team", players: [player(4, 5)] },
	],
	allRookie: [player(4, 5)],
});

describe("normalizeAwardsRow", () => {
	test("an old row becomes a list of awards", () => {
		const row = normalizeAwardsRow(oldRow());

		assert.strictEqual(row.season, 2020);
		assert.strictEqual(row.bestRecord, 3);
		assert.deepStrictEqual(row.bestRecordConfs, { 0: 3, 1: 7 });

		// The whole point: every page that reads award history walks this.
		assert.isArray(row.awards);
		assert.isAbove(row.awards.length, 0);

		const mvp = row.awards.find((award) => award.actAs === "mvp");
		assert.strictEqual(mvp?.winner[0]?.pid, 1);
	});

	test("a row already in the new format is handed back untouched", () => {
		const row = {
			season: 2021,
			bestRecord: 3,
			bestRecordConfs: {},
			bestRecordDivs: {},
			awards: [],
		};
		assert.strictEqual(normalizeAwardsRow(row), row);
	});

	// It has awards, so it is new-format even though it has none of them.
	test("a new-format row with an empty award list is not converted", () => {
		const row = { ...oldRow(), awards: [] };
		assert.strictEqual(normalizeAwardsRow(row), row);
	});

	// A blank year in the history beats a page that will not open.
	test("a row too damaged to convert becomes a season with no awards", () => {
		const damaged = { season: 2019 };
		const row = normalizeAwardsRow(damaged);
		assert.strictEqual(row.season, 2019);
		assert.deepStrictEqual(row.awards, []);
	});

	test("anything that is not an awards row is left alone", () => {
		assert.strictEqual(normalizeAwardsRow(undefined as any), undefined);
	});
});
