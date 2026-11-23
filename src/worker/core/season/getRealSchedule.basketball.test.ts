import { assert, beforeEach, describe, test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g, helpers } from "../../util/index.ts";
import {
	getRealSchedule,
	mapTeamsToRealSlots,
	realSeasonsFor,
} from "./getRealSchedule.basketball.ts";
import realSchedules from "./realSchedules.basketball.json";
import { weekdayOf } from "../../../common/calendarDates.ts";

const defaultTeams = () =>
	helpers.getTeamsDefault().map((t) => ({
		tid: t.tid,
		cid: t.cid,
		did: t.did,
		abbrev: t.abbrev,
		region: t.region,
	}));

describe("getRealSchedule (basketball)", () => {
	beforeEach(() => {
		resetG();
		g.setWithoutSavingToDB("numGames", 82);
		g.setWithoutSavingToDB("numGamesDiv", 16);
		g.setWithoutSavingToDB("numGamesConf", 36);
	});

	test("every real season is a valid BBGM schedule", () => {
		const div = (slot: number) => Math.floor(slot / 5);
		const conf = (slot: number) => Math.floor(slot / 15);
		const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcd";
		for (const real of realSchedules.seasons) {
			const counts = Array.from({ length: 30 }, () => ({
				div: 0,
				conf: 0,
				other: 0,
			}));
			for (const night of real.nights) {
				const seen = new Set<string>();
				for (const letter of night) {
					assert.isFalse(seen.has(letter), `${real.season}: twice a night`);
					seen.add(letter);
				}
				for (let j = 0; j < night.length; j += 2) {
					const h = letters.indexOf(night[j]!);
					const a = letters.indexOf(night[j + 1]!);
					const level =
						div(h) === div(a) ? "div" : conf(h) === conf(a) ? "conf" : "other";
					counts[h]![level] += 1;
					counts[a]![level] += 1;
				}
			}
			for (const count of counts) {
				assert.deepStrictEqual(
					count,
					{ div: 16, conf: 36, other: 30 },
					real.season,
				);
			}
		}
	});

	test("there is a real season for every weekday the league can start on", () => {
		for (let season = 2027; season < 2034; season++) {
			assert.isNotEmpty(realSeasonsFor(season), String(season));
		}
	});

	test("default teams map to the slots, keeping their divisions", () => {
		const teams = defaultTeams();
		const tidBySlot = mapTeamsToRealSlots(teams)!;
		assert.strictEqual(new Set(tidBySlot).size, 30);
		const byTid = new Map(teams.map((t) => [t.tid, t]));
		for (let slot = 0; slot < 30; slot += 5) {
			const dids = new Set(
				tidBySlot.slice(slot, slot + 5).map((tid) => byTid.get(tid)!.did),
			);
			assert.strictEqual(
				dids.size,
				1,
				"a real division is one league division",
			);
		}
	});

	test("real teams get their own schedules", () => {
		const abbrevs = [
			"BOS",
			"BKN",
			"NYC",
			"PHI",
			"TOR",
			"CHI",
			"CLE",
			"DET",
			"IND",
			"MIL",
			"ATL",
			"CHA",
			"MIA",
			"ORL",
			"WAS",
			"DEN",
			"MIN",
			"OKC",
			"POR",
			"UTA",
			"GS",
			"LAC",
			"LAL",
			"PHO",
			"SAC",
			"DAL",
			"HOU",
			"MEM",
			"NOL",
			"SA",
		];
		// Shuffled tids and divisions numbered differently from the data.
		const teams = abbrevs.map((abbrev, i) => ({
			tid: (i * 7) % 30,
			cid: i < 15 ? 1 : 0,
			did: 5 - Math.floor(i / 5),
			abbrev,
		}));
		const tidBySlot = mapTeamsToRealSlots(teams)!;
		tidBySlot.forEach((tid, slot) => {
			assert.strictEqual(tid, teams[slot]!.tid, abbrevs[slot]);
		});
	});

	test("a league not shaped like the NBA gets nothing", () => {
		const teams = defaultTeams().slice(0, 28);
		assert.isUndefined(mapTeamsToRealSlots(teams));
		g.setWithoutSavingToDB("numGames", 72);
		assert.isUndefined(getRealSchedule(defaultTeams(), 2027));
	});

	test("the season lands on the league's dates", () => {
		for (let season = 2027; season < 2034; season++) {
			const games = getRealSchedule(defaultTeams(), season)!;
			assert.strictEqual(games.length, 1230);
			const christmas = `${season - 1}-12-25`;
			assert.strictEqual(
				games.filter((game) => game.date === christmas).length,
				5,
				`${season}: Christmas`,
			);
			const first = games.map((game) => game.date).sort()[0]!;
			assert.strictEqual(weekdayOf(first), 2, `${season}: opens on a Tuesday`);
			assert.strictEqual(first.slice(0, 7), `${season - 1}-10`);
		}
	});
});
