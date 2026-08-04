import { afterEach, assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { idb } from "../../db/index.ts";
import { guardDayContiguity } from "./changeset.ts";
import { setSyncEngine } from "./engineHolder.ts";

// The day-contiguity guard: a changeset that plays day D must not apply while
// an earlier day of the same season sits locally unplayed. The incident: a
// device's engine skipped day 11's batch, day 12 applied right over the gap,
// and the device spent the evening with day 11 still showing spreads and Sim
// buttons while the rest of the room was two days further on. Every position
// check passed the whole time - only the DATA knew. This guard asks the data.

const gameFor = (gid: number, day: number, season = 2006) => ({
	store: "games" as any,
	id: gid,
	type: "put" as const,
	value: { gid, day, season, teams: [{ tid: 0 }, { tid: 1 }] },
});

const scheduleRow = (gid: number, day: number) => ({
	gid,
	day,
	homeTid: 0,
	awayTid: 1,
});

describe("guardDayContiguity", () => {
	let resyncMarks = 0;

	beforeEach(async () => {
		resetG();
		g.setWithoutSavingToDB("season", 2006);
		g.setWithoutSavingToDB("phase", 1);
		resyncMarks = 0;
		setSyncEngine({
			markResyncNeeded: () => {
				resyncMarks += 1;
			},
		} as any);
	});

	afterEach(() => {
		setSyncEngine(undefined);
	});

	// THE INCIDENT, exactly: day 11 skipped, day 12 arriving.
	test("day 12 is refused while day 11 sits unplayed", async () => {
		await resetCache({});
		await idb.cache.schedule.add(scheduleRow(110, 11) as any);
		await idb.cache.schedule.add(scheduleRow(111, 11) as any);

		let message = "";
		try {
			await guardDayContiguity({ changes: [gameFor(120, 12)] });
		} catch (error) {
			message = (error as Error).message;
		}
		assert.match(message, /day 11 of this season has not arrived/);
		assert.strictEqual(
			resyncMarks,
			1,
			"the refusal must arm the durable repair marker, or the device just pins forever",
		);
	});

	test("in-order play passes: day 11's own games while day 11 is scheduled", async () => {
		await resetCache({});
		await idb.cache.schedule.add(scheduleRow(110, 11) as any);
		await idb.cache.schedule.add(scheduleRow(111, 11) as any);
		await guardDayContiguity({
			changes: [gameFor(110, 11), gameFor(111, 11)],
		});
		assert.strictEqual(resyncMarks, 0);
	});

	// A multi-day sim ships several days in one changeset - the earlier day is
	// satisfied by the changeset itself.
	test("a multi-day changeset carries its own earlier days", async () => {
		await resetCache({});
		await idb.cache.schedule.add(scheduleRow(110, 11) as any);
		await idb.cache.schedule.add(scheduleRow(120, 12) as any);
		await guardDayContiguity({
			changes: [gameFor(110, 11), gameFor(120, 12)],
		});
		assert.strictEqual(resyncMarks, 0);
	});

	// A schedule row whose game this device already HAS is a phantom row (the
	// played-game sweep cleans those) - wrong, but not missing data. Refusing
	// on it would pin healthy devices on a cosmetic leftover.
	test("a phantom row for an already-played game does not refuse", async () => {
		await resetCache({});
		await idb.cache.schedule.add(scheduleRow(110, 11) as any);
		await idb.cache.games.add({
			gid: 110,
			day: 11,
			season: 2006,
			teams: [{ tid: 0 }, { tid: 1 }] as any,
		} as any);

		await guardDayContiguity({ changes: [gameFor(120, 12)] });
		assert.strictEqual(resyncMarks, 0);
	});

	// A replay walking LAST season's history is judged against the season the
	// device is in at that moment, not against this season's schedule.
	test("old-season games are never judged against this season's schedule", async () => {
		await resetCache({});
		await idb.cache.schedule.add(scheduleRow(110, 11) as any);
		await guardDayContiguity({
			changes: [gameFor(9000, 50, 2005)],
		});
		assert.strictEqual(resyncMarks, 0);
	});

	test("a changeset with no games is never judged", async () => {
		await resetCache({});
		await idb.cache.schedule.add(scheduleRow(110, 11) as any);
		await guardDayContiguity({
			changes: [
				{
					store: "players" as any,
					id: 1,
					type: "put" as const,
					value: { pid: 1, tid: 0 },
				},
			],
		});
		assert.strictEqual(resyncMarks, 0);
	});

	test("an empty schedule never refuses anything", async () => {
		await resetCache({});
		await guardDayContiguity({ changes: [gameFor(120, 12)] });
		assert.strictEqual(resyncMarks, 0);
	});
});
