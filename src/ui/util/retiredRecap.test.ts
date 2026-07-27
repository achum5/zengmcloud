import { assert, describe, test } from "vitest";
import { FICTIONAL_LEAGUE_NOTICE } from "./fictionalLeagueNotice.ts";
import { buildRetiredRecapPrompt, parseRetiredRecaps } from "./retiredRecap.ts";
import type { RetiredPlayersData } from "../../worker/util/getRetiredPlayersForRecap.ts";

const data: RetiredPlayersData = {
	season: 2026,
	players: [
		{
			pid: 1,
			name: "Legend Guy",
			pos: "SF",
			hof: true,
			ageAtRetirement: 39,
			country: "USA",
			college: "Duke",
			heightIn: 80,
			weightLbs: 220,
			draft: { undrafted: false, round: 1, pick: 1, year: 2005 },
			firstSeason: 2006,
			lastSeason: 2026,
			seasonsPlayed: 21,
			totalGP: 1500,
			neverPlayed: false,
			peakOvr: 95,
			career: {
				gp: 1500,
				min: 36,
				pts: 27.4,
				trb: 7,
				ast: 6,
				stl: 1.5,
				blk: 0.6,
				fg: 10,
				fga: 20,
				fgp: 50,
				tp: 2,
				tpa: 5.3,
				tpp: 38,
				ft: 5.4,
				fta: 6.4,
				ftp: 85,
				per: 25,
				tsp: 60,
				usgp: 31,
				ws: 12.5,
				bpm: 6.2,
				vorp: 5.5,
			},
			playoffs: { gp: 200, pts: 29, trb: 8, ast: 6.5, per: 26 },
			teams: [
				{ abbrev: "LAL", from: 2006, to: 2020, gp: 1100 },
				{ abbrev: "BOS", from: 2021, to: 2026, gp: 400 },
			],
			bySeason: [
				{
					season: 2006,
					age: 19,
					stats: { gp: 78, min: 30, pts: 18, trb: 5, ast: 4, per: 16, ws: 4.1 },
					teams: [{ abbrev: "LAL", result: "made conf finals" }],
				},
			],
			awards: [
				{
					type: "Won Championship",
					count: 4,
					seasons: [2010, 2012, 2018, 2020],
				},
				{ type: "Most Valuable Player", count: 2, seasons: [2011, 2013] },
			],
			rings: 4,
		},
		{
			pid: 2,
			name: "Never Played",
			pos: "PG",
			hof: false,
			ageAtRetirement: 24,
			draft: { undrafted: true, round: 0, pick: 0, year: 2022 },
			seasonsPlayed: 0,
			totalGP: 0,
			neverPlayed: true,
			teams: [],
			bySeason: [],
			awards: [],
			rings: 0,
		},
	],
};

describe("buildRetiredRecapPrompt", () => {
	test("bakes markers, HoF flag, career, teams, awards; scales-length instruction present", () => {
		const prompt = buildRetiredRecapPrompt(data);
		assert.ok(prompt.includes("PLAYER 1"), prompt);
		assert.ok(prompt.includes("PLAYER 2"), prompt);
		assert.ok(prompt.includes("HALL OF FAMER"), prompt);
		// Career + playoffs lines.
		assert.ok(prompt.includes("Career per game: 27.4/7/6"), prompt);
		assert.ok(prompt.includes("Playoffs per game: 29/8/6.5"), prompt);
		// Teams with spans.
		assert.ok(prompt.includes("LAL (2006–2020)"), prompt);
		// Award tally with counts.
		assert.ok(prompt.includes("Won Championship ×4"), prompt);
		// 4 rings summarized.
		assert.ok(prompt.includes("4 championships"), prompt);
		// Height formatting.
		assert.ok(prompt.includes(`6'8"`), prompt);
		// The length-scaling instruction is in the brief.
		assert.ok(prompt.includes("scale the length to the career"), prompt);
		// Full box + advanced stats in the career line.
		assert.ok(prompt.includes("FG 10-20 (50%)"), prompt);
		assert.ok(prompt.includes("TS% 60"), prompt);
		assert.ok(prompt.includes("USG% 31"), prompt);
		assert.ok(prompt.includes("WS 12.5"), prompt);
		assert.ok(prompt.includes("VORP 5.5"), prompt);
		// Per-season line carries the team's result and advanced stats.
		assert.ok(prompt.includes("2006 LAL (made conf finals)"), prompt);
		assert.ok(prompt.includes("PER 16"), prompt);
	});

	test("undrafted, never-played player is marked as such (short writeup expected)", () => {
		const prompt = buildRetiredRecapPrompt(data);
		assert.ok(prompt.includes("undrafted (2022)"), prompt);
		assert.ok(prompt.includes("Never played a game in the league."), prompt);
	});
});

describe("parseRetiredRecaps", () => {
	test("files each writeup to its player id", () => {
		const text = `<!--player:1-->
**A legend walks away**

Two decades of greatness.

<!--player:2-->
**A cup of coffee**

Never suited up.`;
		const map = parseRetiredRecaps(text);
		assert.strictEqual(map.size, 2);
		assert.ok(map.get(1)!.startsWith("**A legend walks away**"));
		assert.ok(map.get(2)!.includes("Never suited up."));
	});
});

describe("fictional league notice", () => {
	// Names collide with real people by design, and without this the AI writes
	// about the real person - a college, a hometown, a championship that never
	// happened here. Every prompt must carry it.
	test("the prompt says nothing may come from real-world knowledge", () => {
		const prompt = buildRetiredRecapPrompt(data);
		assert.ok(prompt.includes(FICTIONAL_LEAGUE_NOTICE), prompt.slice(0, 400));
	});
});
