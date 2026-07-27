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
	teamAbbrev: "BOS",
	tid: 0,
	retiredYear: Infinity,
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
	});

	test("carries the whole career, not just the listed season", () => {
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
	test("splits a reply into one recap per player", () => {
		const reply = [
			"```markdown",
			"<!--player:7-->",
			"A quiet year off the bench.",
			"",
			"<!--player:9-->",
			"First paragraph.",
			"",
			"Second paragraph.",
			"```",
		].join("\n");
		const recaps = parsePlayerRecaps(reply);
		assert.strictEqual(recaps.size, 2);
		assert.strictEqual(recaps.get(7), "A quiet year off the bench.");
		assert.strictEqual(recaps.get(9), "First paragraph.\n\nSecond paragraph.");
	});

	test("works without a code fence", () => {
		const recaps = parsePlayerRecaps("<!--player:4-->\nSolid rotation year.");
		assert.strictEqual(recaps.get(4), "Solid rotation year.");
	});

	test("tolerates whitespace inside the marker", () => {
		const recaps = parsePlayerRecaps("<!-- player: 12 -->\nText.");
		assert.strictEqual(recaps.get(12), "Text.");
	});

	test("a marker with no body is dropped rather than filed empty", () => {
		// Otherwise a truncated reply would wipe that player's season section.
		const recaps = parsePlayerRecaps(
			"<!--player:1-->\nReal text.\n\n<!--player:2-->\n",
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
