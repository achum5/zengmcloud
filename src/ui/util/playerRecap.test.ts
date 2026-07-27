import { assert, describe, test } from "vitest";
import { buildPlayerRecapPrompt, parsePlayerRecaps } from "./playerRecap.ts";
import type { RecapPlayerBatch } from "../../worker/util/getPlayerRecapData.ts";

const RATING_KEYS = [
	"hgt",
	"stre",
	"spd",
	"jmp",
	"endu",
	"ins",
	"dnk",
	"ft",
	"fg",
	"tp",
	"oiq",
	"diq",
	"drb",
	"pss",
	"reb",
];

const player = (pid: number, seasons: number) => ({
	pid,
	name: `Player ${pid}`,
	pos: "SF",
	age: 25,
	born: { year: 1980, loc: "USA" },
	hgt: 79,
	weight: 220,
	draft: {
		year: 2001,
		round: 1,
		pick: 5,
		originalTid: 0,
		abbrev: "CHI",
	},
	teamAbbrevs: ["BOS"],
	retiredYear: undefined,
	hof: false,
	contract: { amount: 12000, exp: 2009 },
	injury: undefined,
	stats: Array.from({ length: seasons }, (_, i) => ({
		season: 2001 + i,
		age: 21 + i,
		abbrev: "BOS",
		playoffs: false,
		gp: 82,
		min: 2800,
		pts: 1600,
		trb: 500,
		ast: 400,
		stl: 100,
		blk: 50,
		tov: 200,
		fg: 600,
		fga: 1300,
		tp: 100,
		tpa: 280,
		ft: 300,
		fta: 380,
		per: 21.4,
	})),
	ratings: Array.from({ length: seasons }, (_, i) => ({
		season: 2001 + i,
		age: 21 + i,
		pos: "SF",
		ovr: 50 + i,
		pot: 75,
		ratings: Object.fromEntries(RATING_KEYS.map((k) => [k, 50 + i])),
	})),
	awards: [{ season: 2005, type: "All-Star" }],
	transactions: ["2001 draft: drafted by CHI (pick 5)"],
	injuries: [{ season: 2004, type: "Sprained ankle", games: 12 }],
	feats: [{ season: 2005, text: "52 pts, 11 reb, 4 ast (win)" }],
	alreadyWritten: false,
});

const batch = (players: any[]): RecapPlayerBatch => ({
	season: 2005,
	batchIndex: 0,
	batchCount: 3,
	batchSize: 40,
	totalPlayers: 100,
	alreadyWrittenTotal: 0,
	players,
});

describe("buildPlayerRecapPrompt", () => {
	test("every player gets an addressable marker instruction", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5), player(9, 5)]));
		assert.ok(prompt.includes("PLAYER <7>"));
		assert.ok(prompt.includes("PLAYER <9>"));
		assert.ok(prompt.includes("<!--player:ID-->"));
		assert.ok(prompt.includes("HEADING"));
	});

	test("carries the career up to the listed season", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 6)]));
		// Ratings for the first season AND the last must both be present - the
		// career arc is the entire point of the feature.
		assert.ok(prompt.includes("2001 age21"));
		assert.ok(prompt.includes("2006 age26"));
		assert.ok(prompt.includes("RATINGS BY SEASON:"));
		assert.ok(prompt.includes("TRANSACTIONS:"));
		assert.ok(prompt.includes("AWARDS:"));
		assert.ok(prompt.includes("FEATS:"));
		assert.ok(prompt.includes("INJURY HISTORY:"));
	});

	test("states the season and the batch position", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(1, 2)]));
		assert.ok(prompt.includes("LISTED SEASON: 2005"));
		assert.ok(prompt.includes("batch 1 of 3"));
	});

	test("a player who didn't play is marked as such rather than omitted", () => {
		const p = player(3, 0);
		const prompt = buildPlayerRecapPrompt(batch([p]));
		assert.ok(prompt.includes("PLAYER <3>"));
		assert.ok(prompt.includes("THIS SEASON: did not play"));
	});
});

describe("parsePlayerRecaps", () => {
	test("splits a reply into a headline and body per player", () => {
		const reply = [
			"```markdown",
			"<!--player:7-->",
			"A quiet year on the bench",
			"",
			"He barely played.",
			"",
			"<!--player:9-->",
			"The leap",
			"",
			"First paragraph.",
			"",
			"Second paragraph.",
			"```",
		].join("\n");
		const recaps = parsePlayerRecaps(reply);
		assert.strictEqual(recaps.size, 2);
		assert.deepStrictEqual(recaps.get(7), {
			headline: "A quiet year on the bench",
			body: "He barely played.",
		});
		assert.deepStrictEqual(recaps.get(9), {
			headline: "The leap",
			body: "First paragraph.\n\nSecond paragraph.",
		});
	});

	test("the year is stripped from the heading and re-added when filing", () => {
		// The AI writes "[2004] A leap". The year is dropped here and supplied
		// from the season being written, so a wrong year in the reply - very easy
		// when backfilling old seasons - can never reach the note.
		const recaps = parsePlayerRecaps(
			"<!--player:1-->\n[1997] A leap in Sacramento\n\nBody text.",
		);
		assert.deepStrictEqual(recaps.get(1), {
			headline: "A leap in Sacramento",
			body: "Body text.",
		});
	});

	test("strips decoration the AI adds to a headline anyway", () => {
		// Told "no bold, no heading marks" - but they show up regardless, and a
		// stray "**" in the note header would look broken.
		const recaps = parsePlayerRecaps(
			"<!--player:1-->\n## **The leap.**\n\nBody text.",
		);
		assert.strictEqual(recaps.get(1)!.headline, "The leap");
	});

	test("a recap with no headline keeps its whole text as the body", () => {
		// Rather than silently eating the first sentence as a headline.
		const recaps = parsePlayerRecaps("<!--player:4-->\nSolid rotation year.");
		assert.deepStrictEqual(recaps.get(4), {
			headline: "",
			body: "Solid rotation year.",
		});
	});

	test("a long first line is treated as prose, not a headline", () => {
		const long =
			"He came into the season with something to prove after a difficult year, and by the All-Star break he had proved it.";
		const recaps = parsePlayerRecaps(`<!--player:5-->\n${long}\n\nMore text.`);
		assert.strictEqual(recaps.get(5)!.headline, "");
		assert.ok(recaps.get(5)!.body.startsWith("He came into"));
	});

	test("tolerates whitespace inside the marker", () => {
		const recaps = parsePlayerRecaps("<!-- player: 12 -->\nHeadline\n\nText.");
		assert.strictEqual(recaps.get(12)!.body, "Text.");
	});

	test("a marker with no body is dropped rather than filed empty", () => {
		// Otherwise a truncated reply would wipe that player's section.
		const recaps = parsePlayerRecaps(
			"<!--player:1-->\nHeadline\n\nReal text.\n\n<!--player:2-->\n",
		);
		assert.strictEqual(recaps.size, 1);
		assert.strictEqual(recaps.has(2), false);
	});

	test("returns nothing for a reply with no markers", () => {
		assert.strictEqual(parsePlayerRecaps("Sorry, I can't help.").size, 0);
	});
});

describe("prompt size", () => {
	test("a full 40-player batch of long careers stays workable", () => {
		// Guards the density of the packing. 40 players is the default batch and
		// the user picked full ratings for every season, so this is the realistic
		// worst case: 40 fifteen-year veterans.
		const players = Array.from({ length: 40 }, (_, i) => player(i + 1, 15));
		const prompt = buildPlayerRecapPrompt(batch(players));
		const kb = prompt.length / 1024;
		// ~4 chars/token, so this should be well inside a large context window.
		assert.ok(kb < 400, `prompt is ${Math.round(kb)}KB`);
		assert.ok(kb > 20, "suspiciously small - is the data actually included?");
		// Measured: 141KB, about 36k tokens. Input is not the constraint here;
		// the AI's REPLY room is, which is why the batch size is a setting.
	});
});

describe("no future knowledge", () => {
	// Backfilling an old season with the full record in hand produced recaps
	// written with hindsight ("he'd hang on one more year in Vancouver"). The
	// data is truncated at the season in the worker, so the prompt cannot
	// contain a later year at all - this asserts the prompt honors that rather
	// than reintroducing it.
	test("a prompt built from truncated data mentions no later season", () => {
		const p = player(7, 15);
		// What the worker hands over for the 2005 recap: nothing after 2005.
		const truncated = {
			...p,
			stats: p.stats.filter((s) => s.season <= 2005),
			ratings: p.ratings.filter((r) => r.season <= 2005),
			awards: p.awards.filter((a) => a.season <= 2005),
			injuries: p.injuries.filter((i) => i.season <= 2005),
			feats: p.feats.filter((f) => f.season <= 2005),
		};
		const prompt = buildPlayerRecapPrompt(batch([truncated]));
		const body = prompt.slice(prompt.indexOf("=== PLAYERS ==="));
		for (const year of [2006, 2007, 2010, 2015]) {
			assert.ok(
				!body.includes(String(year)),
				`prompt leaked ${year} into a 2005 recap`,
			);
		}
		// ...while the seasons up to and including 2005 are all still there.
		for (const year of [2001, 2003, 2005]) {
			assert.ok(body.includes(String(year)), `missing ${year}`);
		}
	});

	test("a player's team comes from that season, not from today", () => {
		// p.teamAbbrevs is built from the season's stat rows, so a player traded
		// years later is not shown on his eventual team.
		const p = { ...player(7, 5), teamAbbrevs: ["BOS", "CHI"] };
		const prompt = buildPlayerRecapPrompt(batch([p]));
		assert.ok(prompt.includes("BOS / CHI"));
	});

	test("a player with no games that season has no team listed", () => {
		const p = { ...player(7, 0), teamAbbrevs: [] };
		const prompt = buildPlayerRecapPrompt(batch([p]));
		assert.ok(prompt.includes("no team"));
	});
});
