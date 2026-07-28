import { assert, describe, test } from "vitest";
import { FICTIONAL_LEAGUE_NOTICE } from "./fictionalLeagueNotice.ts";
import {
	buildPlayerRecapPrompt,
	parsePlayerRecaps,
	parseRecapSeason,
	seasonStamp,
} from "./playerRecap.ts";
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
		teamResult: `${40 + i}-${42 - i}, lost in the first round`,
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
	seasonHighs: { pts: 52, trb: 14, ast: 9 },
	awardFinishes: [{ name: "Most Valuable Player", rank: 4 }],
	alreadyWritten: false,
});

const batch = (
	players: any[],
	extra: Partial<RecapPlayerBatch> = {},
): RecapPlayerBatch => ({
	season: 2005,
	leagueTeams: [
		{
			abbrev: "BOS",
			region: "Boston",
			name: "Celtics",
			won: 62,
			lost: 20,
			result: "won championship",
			conf: "Eastern Conference",
			roster: [
				{
					name: "Player 7",
					pos: "SF",
					age: 25,
					gp: 82,
					min: 34.1,
					pts: 19.5,
					trb: 6.1,
					ast: 4.9,
				},
			],
		},
		{
			abbrev: "CHI",
			region: "Chicago",
			name: "Bulls",
			won: 19,
			lost: 63,
			result: "missed playoffs",
			conf: "Eastern Conference",
			roster: [],
		},
	],
	champion: "BOS",
	leaders: [
		{
			stat: "pts",
			label: "points",
			leagueAvg: 11.2,
			players: [{ name: "Player 7", abbrev: "BOS", value: 19.5 }],
		},
	],
	awardRaces: [
		{
			name: "Most Valuable Player",
			players: [
				{ name: "Someone Else", abbrev: "CHI" },
				{ name: "Player 7", abbrev: "BOS" },
			],
		},
	],
	batchIndex: 0,
	batchCount: 3,
	batchSize: 40,
	totalPlayers: 100,
	alreadyWrittenTotal: 0,
	players,
	...extra,
});

describe("buildPlayerRecapPrompt", () => {
	test("every player gets an addressable marker instruction", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5), player(9, 5)]));
		assert.ok(prompt.includes("PLAYER <7>"));
		assert.ok(prompt.includes("PLAYER <9>"));
		assert.ok(prompt.includes("<!--player:ID-->"));
		assert.ok(prompt.includes("NO headline"));
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

	test("the season's standings and champion are sent once, not per player", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(1, 2), player(2, 2)]));
		assert.ok(prompt.includes("=== LEAGUE 2005 ==="));
		assert.ok(prompt.includes("Champion: BOS"));
		assert.ok(prompt.includes("BOS = Boston Celtics 62-20, won championship"));
		assert.ok(prompt.includes("CHI = Chicago Bulls 19-63, missed playoffs"));
		assert.ok(prompt.includes("Eastern Conference"));
		// One standings table for the batch, however many players are in it.
		assert.strictEqual(prompt.split("=== LEAGUE 2005 ===").length - 1, 1);
	});

	test("a league with no team data just omits the standings", () => {
		// Older leagues, or a season with no teamSeasons rows, must not produce an
		// empty heading.
		const prompt = buildPlayerRecapPrompt(
			batch([player(1, 2)], { leagueTeams: [], champion: undefined }),
		);
		assert.ok(!prompt.includes("=== LEAGUE"));
		assert.ok(prompt.includes("=== PLAYERS ==="));
	});

	test("each season's stat line carries what that team did", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("[40-42, lost in the first round]"));
		assert.ok(prompt.includes("[44-38, lost in the first round]"));
	});

	test("a player drafted this season gets his landing spot and roster", () => {
		const p = {
			...player(11, 1),
			draftInfo: {
				round: 1,
				pick: 3,
				overall: 3,
				abbrev: "CHI",
				teamResult: "19-63, missed playoffs",
				roster: [
					{ name: "Vet Guard", pos: "PG", age: 31, ovr: 62, pot: 62 },
					{ name: "Young Big", pos: "C", age: 23, ovr: 55, pot: 70 },
				],
			},
		};
		const prompt = buildPlayerRecapPrompt(batch([p]));
		assert.ok(prompt.includes("DRAFTED: rd1 pk3 (#3 overall) by CHI"));
		assert.ok(prompt.includes("CHI were 19-63, missed playoffs"));
		assert.ok(prompt.includes("Vet Guard PG age31 ovr62 pot62"));
		assert.ok(prompt.includes("Young Big C age23 ovr55 pot70"));
	});

	test("a veteran gets no draft block", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(!prompt.includes("DRAFTED:"));
	});

	test("a player retiring after this season gets his career totals", () => {
		const p = {
			...player(7, 16),
			retiring: {
				ageAtRetirement: 38,
				seasonsPlayed: 16,
				firstSeason: 1990,
				lastSeason: 2005,
				totalGP: 1204,
				peakOvr: 79,
				career: {
					gp: 1204,
					min: 34.2,
					pts: 21.4,
					trb: 6.1,
					ast: 4.8,
					stl: 1.2,
					blk: 0.4,
					fgp: 46.1,
					tpp: 35.2,
					ftp: 83.4,
				},
				playoffs: { gp: 122, pts: 23.9, trb: 6.6, ast: 5.1, fgp: 45 },
				teams: [
					{ abbrev: "BOS", from: 1990, to: 2001, gp: 900 },
					{ abbrev: "CHI", from: 2002, to: 2005, gp: 304 },
				],
				rings: 2,
			},
		};
		const prompt = buildPlayerRecapPrompt(batch([p]));
		assert.ok(prompt.includes("RETIRING AFTER THIS SEASON"));
		assert.ok(prompt.includes("16 seasons (1990-2005)"));
		assert.ok(prompt.includes("2 championships"));
		assert.ok(prompt.includes("Career per game: 21.4p"));
		assert.ok(prompt.includes("Playoffs per game: 23.9p"));
		assert.ok(prompt.includes("BOS (1990-2001, 900g)"));
		// And the instruction to actually write the second piece.
		assert.ok(prompt.includes("<!--retired:ID-->"));
		assert.ok(prompt.includes("RETIRING PLAYERS GET TWO PIECES"));
	});

	test("the listed season's injuries are called out, not just buried in the history", () => {
		// A fifteen-year veteran's INJURY HISTORY is thirty entries long; the three
		// that shaped THIS season have to be findable.
		const p = {
			...player(7, 5),
			injuries: [
				{ season: 2002, type: "Sprained ankle", games: 4 },
				{ season: 2005, type: "Torn meniscus", games: 31 },
				{ season: 2005, type: "Sore back", games: 6 },
			],
		};
		const prompt = buildPlayerRecapPrompt(batch([p]));
		assert.ok(
			prompt.includes(
				"INJURIES THIS SEASON: Torn meniscus (31g); Sore back (6g) — 37 games missed",
			),
		);
		// Still in the full history too.
		assert.ok(prompt.includes("2002 Sprained ankle (4g)"));
	});

	test("a player healthy all season gets no injury callout", () => {
		const p = {
			...player(7, 5),
			injuries: [{ season: 2002, type: "Sprained ankle", games: 4 }],
		};
		const prompt = buildPlayerRecapPrompt(batch([p]));
		// The instructions describe the label, so only the data half counts.
		const body = prompt.slice(prompt.indexOf("=== PLAYERS ==="));
		assert.ok(!body.includes("INJURIES THIS SEASON"));
		assert.ok(body.includes("INJURY HISTORY"));
	});

	test("a player who isn't retiring gets no retirement block", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		// The instructions describe the marker, so only the data half counts.
		const body = prompt.slice(prompt.indexOf("=== PLAYERS ==="));
		assert.ok(!body.includes("RETIRING AFTER THIS SEASON"));
		assert.ok(!body.includes("Career per game:"));
	});

	test("the instructions ask for team context and rookie fit", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("Keep the focus on the PLAYER"));
		assert.ok(prompt.includes("do not invent teammates"));
	});

	test("a player who didn't play is marked as such rather than omitted", () => {
		const p = player(3, 0);
		const prompt = buildPlayerRecapPrompt(batch([p]));
		assert.ok(prompt.includes("PLAYER <3>"));
		assert.ok(prompt.includes("THIS SEASON: did not play"));
	});
});

const byPid = (
	recaps: ReturnType<typeof parsePlayerRecaps>,
	pid: number,
	kind: "season" | "retirement" = "season",
) => recaps.find((x) => x.pid === pid && x.kind === kind);

describe("parsePlayerRecaps", () => {
	test("splits a reply into one piece of prose per player", () => {
		const reply = [
			"```markdown",
			"<!--player:7-->",
			"He barely played.",
			"",
			"<!--player:9-->",
			"First paragraph.",
			"",
			"Second paragraph.",
			"```",
		].join("\n");
		const recaps = parsePlayerRecaps(reply);
		assert.strictEqual(recaps.length, 2);
		assert.deepStrictEqual(byPid(recaps, 7), {
			pid: 7,
			kind: "season",
			headline: "",
			body: "He barely played.",
		});
		assert.deepStrictEqual(byPid(recaps, 9), {
			pid: 9,
			kind: "season",
			headline: "",
			body: "First paragraph.\n\nSecond paragraph.",
		});
	});

	test("a retiring player's two pieces are kept apart", () => {
		// The whole point of the second marker: a season recap and a retirement
		// writeup for the SAME player and the SAME year are different sections of
		// the note, and must never overwrite each other.
		const reply = [
			"<!--player:7-->",
			"He gave them 11 a night.",
			"",
			"<!--retired:7-->",
			"Sixteen years, one team",
			"",
			"He never played anywhere else.",
		].join("\n");
		const recaps = parsePlayerRecaps(reply);
		assert.strictEqual(recaps.length, 2);
		// The season recap is headed by its year alone...
		assert.deepStrictEqual(byPid(recaps, 7), {
			pid: 7,
			kind: "season",
			headline: "",
			body: "He gave them 11 a night.",
		});
		// ...while the career retrospective keeps a title.
		assert.deepStrictEqual(byPid(recaps, 7, "retirement"), {
			pid: 7,
			kind: "retirement",
			headline: "Sixteen years, one team",
			body: "He never played anywhere else.",
		});
	});

	test("a heading the AI wrote anyway is dropped, not left in the prose", () => {
		// Told not to write one, it still does sometimes. The note is headed by its
		// year alone, so a stray title must not become the opening line.
		for (const heading of [
			"[2004] A leap in Sacramento",
			"## The leap",
			"**The leap**",
		]) {
			const recaps = parsePlayerRecaps(
				`<!--player:1-->\n${heading}\n\nBody text.`,
			);
			assert.deepStrictEqual(
				byPid(recaps, 1),
				{ pid: 1, kind: "season", headline: "", body: "Body text." },
				heading,
			);
		}
	});

	test("prose that merely starts with bold is left alone", () => {
		// Only a whole line of bold is a heading; a bolded name opening a sentence
		// is the writing itself.
		const recaps = parsePlayerRecaps(
			"<!--player:1-->\n**Marcus Bell** had the best year of his career.",
		);
		assert.strictEqual(
			byPid(recaps, 1)!.body,
			"**Marcus Bell** had the best year of his career.",
		);
	});

	test("a single-line recap is kept whole", () => {
		const recaps = parsePlayerRecaps("<!--player:4-->\nSolid rotation year.");
		assert.deepStrictEqual(byPid(recaps, 4), {
			pid: 4,
			kind: "season",
			headline: "",
			body: "Solid rotation year.",
		});
	});

	test("tolerates whitespace inside the marker", () => {
		const recaps = parsePlayerRecaps("<!-- player: 12 -->\nText.");
		assert.strictEqual(byPid(recaps, 12)!.body, "Text.");
	});

	test("a marker with no body is dropped rather than filed empty", () => {
		// Otherwise a truncated reply would wipe that player's section.
		const recaps = parsePlayerRecaps(
			"<!--player:1-->\nReal text.\n\n<!--player:2-->\n",
		);
		assert.strictEqual(recaps.length, 1);
		assert.strictEqual(byPid(recaps, 2), undefined);
	});

	test("returns nothing for a reply with no markers", () => {
		assert.strictEqual(parsePlayerRecaps("Sorry, I can't help.").length, 0);
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

describe("fictional league notice", () => {
	// Names collide with real people by design, and without this the AI writes
	// about the real person - a college, a hometown, a championship that never
	// happened here. Every prompt must carry it.
	test("the prompt says nothing may come from real-world knowledge", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes(FICTIONAL_LEAGUE_NOTICE), prompt.slice(0, 400));
	});
});

describe("league context", () => {
	// The batch is one slice of the league, so on its own a player's block gives
	// no way to know whether his 19.5 led the league or was thirtieth. Without
	// this the AI either skips the strongest sentence available or invents it.
	test("the leaders board and the league average are in the prompt", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("LEAGUE LEADERS"));
		assert.ok(prompt.includes("points (avg 11.2)"));
		assert.ok(prompt.includes("1. Player 7 BOS 19.5"));
	});

	test("award races arrive in finishing order, and a player carries his own", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("AWARD RACES"));
		assert.ok(prompt.includes("1. Someone Else CHI, 2. Player 7 BOS"));
		assert.ok(prompt.includes("AWARD FINISH: Most Valuable Player 4th"));
	});

	test("each team carries its rotation, so a player has teammates", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("BOS = Boston Celtics 62-20, won championship"));
		assert.ok(
			prompt.includes("Player 7 SF age25 82g 34.1m 19.5p 6.1r 4.9a"),
			prompt.slice(
				prompt.indexOf("=== LEAGUE"),
				prompt.indexOf("LEAGUE LEADERS"),
			),
		);
	});

	test("season highs, height and weight, and games started all make it in", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("SEASON HIGHS (single game): 52pts 14trb 9ast"));
		assert.ok(prompt.includes(`6'7", 220 lbs`));
		// gs is optional - a season row without it still reads as plain games.
		assert.ok(prompt.includes("82g "));
	});
});

describe("season stamp", () => {
	// A reply written for one season looks completely normal when pasted into
	// another: every player keeps the same pid, so forty recaps quietly attach to
	// the wrong year and there is no way to tell afterward.
	test("the prompt asks for the stamp and says why it matters", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes(seasonStamp(2005)));
		assert.ok(prompt.includes("season stamp"));
	});

	test("the season is read back out of a reply", () => {
		const reply = [
			"```markdown",
			seasonStamp(2005),
			"<!--player:7-->",
			"x",
			"```",
		].join("\n");
		assert.strictEqual(parseRecapSeason(reply), 2005);
		// And the recaps still parse with the stamp sitting above them.
		assert.strictEqual(parsePlayerRecaps(reply).length, 1);
	});

	test("a reply for another season is identifiable as such", () => {
		const reply = [
			"```markdown",
			seasonStamp(2000),
			"<!--player:7-->",
			"x",
			"```",
		].join("\n");
		assert.strictEqual(parseRecapSeason(reply), 2000);
	});

	test("an unstamped reply reads as unknown rather than as this season", () => {
		assert.strictEqual(
			parseRecapSeason("```markdown\n<!--player:7-->\nx\n```"),
			undefined,
		);
	});
});

describe("the draft class", () => {
	// The draft is held after the season ends, so a player in that class has
	// never played a game. Left to "did not play this season" the AI wrote it up
	// as though the year had gone wrong for him.
	const rookie = () => ({
		...player(11, 0),
		draftInfo: {
			round: 1,
			pick: 2,
			overall: 2,
			abbrev: "TOR",
			teamResult: "48-34, lost in the first round",
			roster: [{ name: "Elton Brand", pos: "PF", age: 22, ovr: 62, pot: 75 }],
		},
	});

	test("a draftee isn't described as having missed the season", () => {
		const prompt = buildPlayerRecapPrompt(batch([rookie()]));
		assert.ok(!prompt.includes("THIS SEASON: did not play"));
		assert.ok(
			prompt.includes(
				"THIS SEASON: not in the league yet — drafted at the end of 2005, first season is 2006",
			),
		);
	});

	test("the block spells out when he actually starts", () => {
		const prompt = buildPlayerRecapPrompt(batch([rookie()]));
		assert.ok(
			prompt.includes(
				"the 2005 draft is held after the 2005 season ends, so his first season is 2006",
			),
		);
	});

	test("the instructions say the same thing", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("THE DRAFT IS HELD AFTER THE SEASON ENDS"));
		assert.ok(prompt.includes("his first season is the one AFTER it"));
	});

	test("someone who simply didn't play still reads that way", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(3, 0)]));
		assert.ok(prompt.includes("THIS SEASON: did not play"));
	});
});

describe("offseason timing in the prompt", () => {
	test("the instructions explain the (for YYYY) mark on a transaction", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("EVERY TRANSACTION IS DATED THE SAME WAY"));
		assert.ok(prompt.includes('"(for YYYY)"'));
		assert.ok(
			prompt.includes("was made in the offseason and takes effect that year"),
		);
	});

	// A recap that hedges about when a player changed teams reads like someone
	// working it out from a spreadsheet, not someone who watched the season.
	test("the instructions ask for settled fact, not guesswork", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(
			prompt.includes("Write like someone who watched these seasons happen"),
		);
		assert.ok(prompt.includes("Never hedge"));
	});
});

describe("markdown and linkable names", () => {
	// Notes render as markdown, and team names in the text are turned into links
	// to that season's page - which only works if the AI writes the name rather
	// than the three-letter abbreviation the stat lines use.
	test("the league block maps each abbreviation to a full team name", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("BOS = Boston Celtics"));
		assert.ok(prompt.includes("CHI = Chicago Bulls"));
	});

	test("the instructions ask for markdown and for full team names", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("rendered as Markdown"));
		assert.ok(prompt.includes("never the abbreviation"));
	});
});

describe("next season's draft class", () => {
	// Prospects are in the batch but have never played, so they get a scouting
	// report rather than a recap - filed under the season being written, since
	// that is when it's being scouted.
	const prospect = () => ({
		...player(21, 0),
		// The pick he actually landed is in the database once the draft has been
		// played, which is exactly the case when catching up on old seasons.
		draft: {
			year: 2006,
			round: 2,
			pick: 2,
			originalTid: 0,
			abbrev: "DEN",
		},
		prospect: {
			draftYear: 2006,
			pos: "C",
			college: "State",
			ovr: 44,
			pot: 71,
			ratings: { hgt: 90, stre: 55, ins: 60, tp: 20, oiq: 35 },
		},
	});

	test("the scouting profile is in the block", () => {
		const prompt = buildPlayerRecapPrompt(batch([prospect()]));
		assert.ok(
			prompt.includes(
				"PROSPECT — eligible for the 2006 draft, which is held at the end of the 2006 season",
			),
			prompt,
		);
		assert.ok(prompt.includes("ovr44 pot71 | hgt90 stre55 ins60 tp20 oiq35"));
	});

	// Backfilling an old season, the draft has already been played and the result
	// is sitting in the database. Printing it turns a scouting report into a
	// summary of what happened.
	test("where he actually got drafted is nowhere in the prompt", () => {
		const prompt = buildPlayerRecapPrompt(batch([prospect()]));
		const body = prompt.slice(prompt.indexOf("PLAYER <21>"));
		assert.ok(!body.includes("DEN"), body);
		assert.ok(!body.includes("rd2"), body);
		assert.ok(!body.includes("undrafted"), body);
		assert.ok(body.includes("nobody knows yet where he will go"));
	});

	test("he is headed by his class and position, not as a man with no team", () => {
		const prompt = buildPlayerRecapPrompt(batch([prospect()]));
		assert.ok(
			prompt.includes("Player 21 — C, age 25 in 2005, 2006 draft class"),
		);
		assert.ok(!prompt.includes("Player 21 — , "));
		const body = prompt.slice(prompt.indexOf("PLAYER <21>"));
		assert.ok(!body.includes("no team"), body);
	});

	test("a prospect isn't written up as having missed the season", () => {
		const prompt = buildPlayerRecapPrompt(batch([prospect()]));
		assert.ok(!prompt.includes("THIS SEASON: did not play"));
		assert.ok(
			prompt.includes(
				"THIS SEASON: not in the league — eligible for the 2006 draft",
			),
		);
	});

	test("the instructions demand a long report built only from the ratings", () => {
		const prompt = buildPlayerRecapPrompt(batch([player(7, 5)]));
		assert.ok(prompt.includes("PROSPECTS GET A FULL SCOUTING REPORT"));
		assert.ok(prompt.includes("A short report is a failure here"));
		assert.ok(
			prompt.includes("Never print or refer to a rating in the report"),
		);
	});
});
