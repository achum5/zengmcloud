import { assert, describe, test } from "vitest";
import { FICTIONAL_LEAGUE_NOTICE } from "./fictionalLeagueNotice.ts";
import { buildSeasonRecapPrompt, parseSeasonRecaps } from "./seasonRecap.ts";
import type { RecapSeasonData } from "../../worker/util/getSeasonRecapData.ts";

const data: RecapSeasonData = {
	season: 2026,
	champ: { tid: 0, region: "LA", name: "Lakers", abbrev: "LAL" },
	runnerUp: { tid: 1, region: "Boston", name: "Celtics", abbrev: "BOS" },
	awards: [{ label: "MVP", player: "Star Guy", abbrev: "LAL" }],
	alreadyWrittenTotal: 0,
	teams: [
		{
			tid: 0,
			region: "LA",
			name: "Lakers",
			abbrev: "LAL",
			won: 60,
			lost: 22,
			ptsPerGame: 118.4,
			oppPtsPerGame: 110.1,
			seed: 1,
			madePlayoffs: true,
			playoffResult: "league champs",
			playoffSeriesResults: [
				{ round: 1, opp: "BKN", won: 4, lost: 1, win: true },
				{ round: 4, opp: "BOS", won: 4, lost: 2, win: true },
			],
			players: [
				{
					name: "Star Guy",
					pid: 5,
					pos: "PG",
					age: 27,
					ovr: 90,
					pot: 90,
					gp: 80,
					min: 36,
					pts: 30.1,
					trb: 8,
					ast: 9,
					stl: 1.5,
					blk: 0.4,
					tov: 3,
					fgp: 49,
					tpp: 40,
					ftp: 88,
					per: 28.5,
					playoff: { gp: 20, pts: 32, trb: 8, ast: 10 },
					awards: ["Most Valuable Player"],
					transactions: ["Lakers re-signed Star Guy to a 4 yr, $180M contract"],
					majorInjuries: [{ type: "Torn ACL", games: 62, season: 2023 }],
				},
			],
			franchise: {
				championships: 18,
				lastChampionship: 2026,
				playoffAppearances: 60,
				finalsAppearances: 32,
				totalWon: 3500,
				totalLost: 2600,
				recent: [
					{ season: 2025, won: 52, lost: 30, result: "made conf finals" },
				],
			},
			offseasonMoves: ["Lakers signed Star Guy to a 4 yr contract"],
			inSeasonMoves: ["Lakers traded for a role player"],
		},
		{
			tid: 1,
			region: "Boston",
			name: "Celtics",
			abbrev: "BOS",
			won: 58,
			lost: 24,
			madePlayoffs: true,
			playoffResult: "made finals",
			playoffSeriesResults: [],
			players: [],
			franchise: {
				championships: 17,
				playoffAppearances: 58,
				finalsAppearances: 21,
				totalWon: 3400,
				totalLost: 2700,
				recent: [],
			},
			offseasonMoves: [],
			inSeasonMoves: [],
		},
	],
};

describe("buildSeasonRecapPrompt", () => {
	test("bakes in team markers, league context, records, franchise, and moves", () => {
		const prompt = buildSeasonRecapPrompt(data);
		// Per-team marker id the AI must echo back.
		assert.ok(prompt.includes("TEAM 0"), prompt);
		assert.ok(prompt.includes("TEAM 1"), prompt);
		// League context.
		assert.ok(prompt.includes("Champion: LA Lakers"), prompt);
		assert.ok(prompt.includes("MVP — Star Guy"), prompt);
		// Record + playoff result.
		assert.ok(prompt.includes("60-22"), prompt);
		assert.ok(prompt.includes("league champs"), prompt);
		// Franchise history.
		assert.ok(prompt.includes("18 titles"), prompt);
		// Player line + playoffs + awards.
		assert.ok(prompt.includes("Star Guy"), prompt);
		assert.ok(prompt.includes("Playoffs: 32/8/10"), prompt);
		// The season-flip framing is present in the labels.
		assert.ok(
			prompt.includes("Offseason moves that built this season's roster"),
			prompt,
		);
		assert.ok(prompt.includes("signed Star Guy"), prompt);
		assert.ok(prompt.includes("In-season moves:"), prompt);
	});

	test("lays the data out chronologically: prior offseason before the season", () => {
		const prompt = buildSeasonRecapPrompt(data);
		const offseasonIdx = prompt.indexOf("Offseason moves that built");
		const seasonIdx = prompt.indexOf("The season:");
		const inSeasonIdx = prompt.indexOf("In-season moves:");
		assert.ok(offseasonIdx >= 0 && seasonIdx >= 0 && inSeasonIdx >= 0, prompt);
		// Prior-offseason build comes before the season, which comes before the
		// in-season moves — the flow the recap should follow.
		assert.ok(offseasonIdx < seasonIdx, prompt);
		assert.ok(seasonIdx < inSeasonIdx, prompt);
	});

	test("includes exact playoff series results so the AI can't guess series length", () => {
		const prompt = buildSeasonRecapPrompt(data);
		assert.ok(prompt.includes("Playoff series:"), prompt);
		assert.ok(prompt.includes("beat BKN 4-1"), prompt);
		assert.ok(prompt.includes("beat BOS 4-2"), prompt);
	});

	test("lists each player's own transactions in their block", () => {
		const prompt = buildSeasonRecapPrompt(data);
		assert.ok(prompt.includes("Move: Lakers re-signed Star Guy"), prompt);
	});

	test("lists a player's major (50+ game) injuries with when they happened", () => {
		const prompt = buildSeasonRecapPrompt(data);
		assert.ok(
			prompt.includes("Injury history: Torn ACL, missed 62 games (2023)"),
			prompt,
		);
	});
});

describe("parseSeasonRecaps", () => {
	test("files each recap to its team id, ignoring preamble", () => {
		const text = `Here are the recaps!

<!--team:0-->
**Lakers cap a title run**

Dominant from start to finish.

<!--team:1-->
**Celtics fall just short**

A finals loss.`;
		const map = parseSeasonRecaps(text);
		assert.strictEqual(map.size, 2);
		assert.ok(map.get(0)!.startsWith("**Lakers cap a title run**"), map.get(0));
		assert.ok(map.get(1)!.includes("A finals loss."));
		assert.ok(!map.get(0)!.includes("Here are the recaps"));
	});

	test("no markers → empty map (so the UI can warn)", () => {
		const map = parseSeasonRecaps("The AI forgot the markers.");
		assert.strictEqual(map.size, 0);
	});
});

describe("fictional league notice", () => {
	// Names collide with real people by design, and without this the AI writes
	// about the real person - a college, a hometown, a championship that never
	// happened here. Every prompt must carry it.
	test("the prompt says nothing may come from real-world knowledge", () => {
		const prompt = buildSeasonRecapPrompt(data);
		assert.ok(prompt.includes(FICTIONAL_LEAGUE_NOTICE), prompt.slice(0, 400));
	});
});
