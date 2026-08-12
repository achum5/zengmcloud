import { assert, describe, test } from "vitest";
import {
	buildChanges,
	planUnsyncedPush,
	type LeagueRows,
} from "./republishUnsyncedDays.ts";

// Pushing out a day that was simmed here and never reached the room. The
// incident these cover: the simmer lost the compare-and-swap to another
// device's trading-card edit, the advance was discarded, the room had no
// checkpoint to snap back to, and this device carried a day of games nobody
// else had. Both halves of the repair - deciding whether to push, and deciding
// what to push - are pure, so both can be asked directly.

const rows = (overrides: Partial<LeagueRows> = {}): LeagueRows => ({
	games: [],
	schedule: [],
	teamSeasons: [],
	teamStats: [],
	players: [],
	gameAttributes: [],
	...overrides,
});

const game = (gid: number, day: number, season = 2006) => ({
	gid,
	day,
	season,
	teams: [{ tid: 0 }, { tid: 1 }],
});

describe("planUnsyncedPush", () => {
	const local = { season: 2006, phase: 1, day: 12 };

	test("refuses on a device that is not in charge of simming", () => {
		const plan = planUnsyncedPush({
			room: { season: 2006, phase: 1, day: 11 },
			local,
			isAuthority: false,
		});
		assert.strictEqual(plan.ok, false);
	});

	test("refuses when the room has not recorded a position", () => {
		const plan = planUnsyncedPush({
			room: undefined,
			local,
			isAuthority: true,
		});
		assert.strictEqual(plan.ok, false);
	});

	// A season or phase apart is not a missing day, it is a divergence, and
	// publishing a day of games across one would bury the real problem under
	// records that do not belong to the room's world.
	test("refuses across a season gap", () => {
		const plan = planUnsyncedPush({
			room: { season: 2005, phase: 1, day: 11 },
			local,
			isAuthority: true,
		});
		assert.strictEqual(plan.ok, false);
	});

	test("refuses across a phase gap", () => {
		const plan = planUnsyncedPush({
			room: { season: 2006, phase: 3, day: 11 },
			local,
			isAuthority: true,
		});
		assert.strictEqual(plan.ok, false);
	});

	test("refuses when the room is level", () => {
		const plan = planUnsyncedPush({
			room: { season: 2006, phase: 1, day: 12 },
			local,
			isAuthority: true,
		});
		assert.strictEqual(plan.ok, false);
	});

	test("refuses when the room is ahead", () => {
		const plan = planUnsyncedPush({
			room: { season: 2006, phase: 1, day: 13 },
			local,
			isAuthority: true,
		});
		assert.strictEqual(plan.ok, false);
	});

	test("pushes from the room's day when this device is ahead", () => {
		const plan = planUnsyncedPush({
			room: { season: 2006, phase: 1, day: 11 },
			local,
			isAuthority: true,
		});
		assert.strictEqual(plan.ok, true);
		if (plan.ok) {
			assert.strictEqual(plan.season, 2006);
			assert.strictEqual(plan.roomDay, 11);
		}
	});
});

describe("buildChanges", () => {
	const idsFor = (changes: { store: string; id: any }[], store: string) =>
		changes
			.filter((change) => change.store === store)
			.map((change) => change.id);

	test("carries only games past the room's day, in this season", () => {
		const built = buildChanges(
			rows({
				games: [
					game(1, 10),
					game(2, 11),
					game(3, 12),
					game(4, 13),
					game(5, 12, 2005),
				],
			}),
			2006,
			{ kind: "after", day: 11 },
		);

		assert.deepStrictEqual(idsFor(built.changes, "games"), [3, 4]);
		assert.deepStrictEqual(built.days, [12, 13]);
		assert.strictEqual(built.games, 2);
	});

	// The room still holds a schedule row for every game this device played, and
	// a put-only changeset cannot remove it: every other device would show a
	// game as still to be played that has already been played here.
	test("deletes the schedule rows for exactly those games", () => {
		const built = buildChanges(
			rows({
				games: [game(1, 11), game(2, 12), game(3, 12)],
				schedule: [{ gid: 2 }, { gid: 3 }, { gid: 9 }],
			}),
			2006,
			{ kind: "after", day: 11 },
		);

		const deletes = built.changes.filter(
			(change) => change.store === "schedule",
		);
		assert.deepStrictEqual(
			deletes.map((change) => change.id),
			[2, 3],
		);
		assert.ok(deletes.every((change) => change.type === "delete"));
	});

	test("rewrites the whole current season's standings and team stats", () => {
		const built = buildChanges(
			rows({
				games: [game(1, 12)],
				teamSeasons: [
					{ rid: 1, season: 2006 },
					{ rid: 2, season: 2006 },
					{ rid: 3, season: 2005 },
				],
				teamStats: [
					{ rid: 10, season: 2006 },
					{ rid: 11, season: 2005 },
				],
			}),
			2006,
			{ kind: "after", day: 11 },
		);

		assert.deepStrictEqual(idsFor(built.changes, "teamSeasons"), [1, 2]);
		assert.deepStrictEqual(idsFor(built.changes, "teamStats"), [10]);
	});

	// Every rostered player, not just the ones who played: an injury that ticked
	// down on the bench is as much a change as a triple-double.
	test("carries rostered players and leaves free agents and retirees alone", () => {
		const built = buildChanges(
			rows({
				games: [game(1, 12)],
				players: [
					{ pid: 1, tid: 0 },
					{ pid: 2, tid: 4 },
					{ pid: 3, tid: -1 },
					{ pid: 4, tid: -2 },
				],
			}),
			2006,
			{ kind: "after", day: 11 },
		);

		assert.deepStrictEqual(idsFor(built.changes, "players"), [1, 2]);
	});

	test("carries the game attributes, keyed by name", () => {
		const built = buildChanges(
			rows({
				games: [game(1, 12)],
				gameAttributes: [
					{ key: "phase", value: 1 },
					{ key: "season", value: 2006 },
				],
			}),
			2006,
			{ kind: "after", day: 11 },
		);

		assert.deepStrictEqual(idsFor(built.changes, "gameAttributes"), [
			"phase",
			"season",
		]);
	});

	test("carries the playoff bracket when there is one", () => {
		const withSeries = buildChanges(
			rows({
				games: [game(1, 12)],
				playoffSeries: { season: 2006, series: [] },
			}),
			2006,
			{ kind: "after", day: 11 },
		);
		assert.deepStrictEqual(idsFor(withSeries.changes, "playoffSeries"), [2006]);

		const without = buildChanges(rows({ games: [game(1, 12)] }), 2006, {
			kind: "after",
			day: 11,
		});
		assert.strictEqual(idsFor(without.changes, "playoffSeries").length, 0);
	});

	// Nothing to push must produce nothing at all - not "everything except the
	// games". A caller that skipped the games === 0 check would otherwise
	// republish the entire league over an already-correct room.
	test("finds nothing when the room already has every day", () => {
		const built = buildChanges(
			rows({
				games: [game(1, 10), game(2, 11)],
				players: [{ pid: 1, tid: 0 }],
				teamSeasons: [{ rid: 1, season: 2006 }],
			}),
			2006,
			{ kind: "after", day: 11 },
		);

		assert.strictEqual(built.games, 0);
		assert.deepStrictEqual(built.days, []);
	});

	// An event's eid is a per-device autoincrement, so publishing rows keyed by
	// it would land on unrelated events elsewhere. The news is cosmetic; the
	// standings are not.
	test("never carries events", () => {
		const built = buildChanges(rows({ games: [game(1, 12)] }), 2006, {
			kind: "after",
			day: 11,
		});
		assert.ok(built.changes.every((change) => change.store !== "events"));
	});

	// NAMING THE DAY BY HAND. A room that never stamped a position gives the
	// automatic comparison nothing to work with, so the person at the keyboard
	// says which day did not go out. Only that day's games go, and only that
	// day's schedule rows are cleared.
	test("carries exactly the named day and nothing either side of it", () => {
		const built = buildChanges(
			rows({
				games: [game(1, 4), game(2, 5), game(3, 5), game(4, 6)],
				schedule: [{ gid: 1 }, { gid: 2 }, { gid: 3 }, { gid: 4 }],
			}),
			2006,
			{ kind: "only", days: [5] },
		);

		assert.deepStrictEqual(idsFor(built.changes, "games"), [2, 3]);
		assert.deepStrictEqual(idsFor(built.changes, "schedule"), [2, 3]);
		assert.deepStrictEqual(built.days, [5]);
		assert.strictEqual(built.games, 2);
	});

	test("a named day with no games here comes back empty", () => {
		const built = buildChanges(rows({ games: [game(1, 4)] }), 2006, {
			kind: "only",
			days: [5],
		});
		assert.strictEqual(built.games, 0);
	});

	test("a named day is scoped to its season", () => {
		const built = buildChanges(
			rows({ games: [game(1, 5, 2005), game(2, 5, 2006)] }),
			2006,
			{ kind: "only", days: [5] },
		);
		assert.deepStrictEqual(idsFor(built.changes, "games"), [2]);
	});
});
