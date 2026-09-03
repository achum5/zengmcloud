import { assert, test } from "vitest";
import { resetCache, resetG } from "../../test/helpers.ts";
import { idb } from "../db/index.ts";
import { g, helpers } from "./index.ts";
import { PHASE } from "../../common/constants.ts";
import { player, team } from "../core/index.ts";
import GameSim from "../core/GameSim.ts";
import { processTeam } from "../core/game/loadTeams.ts";
import createRandomPlayers from "../core/league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../common/budgetLevels.ts";
import {
	getAutoRecapsForDay,
	recapGamesForDay,
	type RecapGame,
} from "./getDayGamesForRecap.ts";
import type { Game } from "../../common/types.ts";
import { verifyRecap } from "./recapAccuracy.ts";

// A CORPUS OF REAL RECAPS.
//
// getAutoRecap writes every recap the game shows - one per game, one per day -
// and the only honest way to judge it is to read a lot of its output at once.
// A weakness a hand-built fixture never shows will show up nine times across a
// season. So: build a league, play it with the real engine, store the games the
// way the game stores them, and run the real recap path over every day.
//
//   SPORT=basketball RECAP_DAYS=30 RECAP_SEED=1 \
//     RECAP_LOG=/tmp/recaps.txt npx vitest --run src/worker/util/recapCorpus.test.ts
//
// Writes the corpus to RECAP_LOG and a metrics summary to RECAP_LOG.stats.txt.
// Skipped entirely unless RECAP_LOG is set, so it costs CI nothing.
const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const LOG = nodeEnv.RECAP_LOG;
const NUM_TEAMS = 30;
const DAYS = Number(nodeEnv.RECAP_DAYS ?? 30);
const SEED = Number(nodeEnv.RECAP_SEED ?? 1);

const rngFromSeed = (seed: number): (() => number) => {
	let a = seed >>> 0;
	return () => {
		a += 0x6d2b79f5;
		let t = a;
		t = Math.imul(t ^ (t >>> 15), t | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

const stubLeagueDb = () => {
	const store = {
		index: () => store,
		getAll: async () => [],
		get: async () => undefined,
		async *iterate() {},
	};
	(idb as any).league = {
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
		getAll: async () => [],
		get: async () => undefined,
	};
};

const STAT_KEYS = [
	"min",
	"fg",
	"fga",
	"tp",
	"tpa",
	"ft",
	"fta",
	"orb",
	"drb",
	"ast",
	"stl",
	"blk",
	"tov",
	"pf",
	"pts",
] as const;

// Distinct, plausible names. The generator's name data isn't loaded under test
// (every player comes out "FirstName LastName"), and a corpus where everyone
// shares a name can't show a recap naming the same man twice.
const FIRST = [
	"Marcus",
	"Devin",
	"Tyrese",
	"Jalen",
	"Cade",
	"Anthony",
	"Darius",
	"Evan",
	"Franz",
	"Grant",
	"Herb",
	"Isaiah",
	"Jaden",
	"Keegan",
	"Lonnie",
	"Miles",
	"Naji",
	"Obi",
	"Payton",
	"Quentin",
	"Reggie",
	"Scoot",
	"Trey",
	"Ausar",
	"Vince",
	"Walker",
	"Xavier",
	"Yuta",
	"Zeke",
	"Amen",
	"Bennedict",
	"Corey",
];
const LAST = [
	"Brooks",
	"Carter",
	"Dunn",
	"Ellis",
	"Foster",
	"Green",
	"Hayes",
	"Ingram",
	"Jackson",
	"King",
	"Lowry",
	"Mathis",
	"Nowell",
	"Oakley",
	"Porter",
	"Quinn",
	"Reed",
	"Sharpe",
	"Turner",
	"Underwood",
	"Vaughn",
	"Wallace",
	"York",
	"Zeller",
	"Bishop",
	"Crowder",
	"Dawkins",
	"Everett",
	"Flagg",
	"Gilmore",
];

test("recap corpus", { timeout: 3_600_000 }, async () => {
	if (!LOG) {
		return;
	}
	// Same dodge decadesSim uses: the typecheck project has no node types.
	const { writeFileSync } = await import(("node" + ":fs") as any);

	// SEED THE WHOLE RUN. common/random.ts calls Math.random directly, so player
	// generation and GameSim are otherwise a different league every time - which
	// makes a before/after on a phrasing change a comparison of two different
	// seasons. Rates over 900 games still move honestly that way, but nothing
	// small does, and no individual recap can be diffed at all. Overriding
	// Math.random for the duration is the only lever the module offers.
	const realRandom = Math.random;
	const seeded = rngFromSeed(SEED * 7919 + 13);
	Math.random = seeded;
	try {
		await runCorpus(writeFileSync);
	} finally {
		Math.random = realRandom;
	}
});

const runCorpus = async (writeFileSync: (p: string, d: string) => void) => {
	const LOG = nodeEnv.RECAP_LOG!;

	resetG();
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("userTids", []);
	g.setWithoutSavingToDB("userTid", 0);
	g.setWithoutSavingToDB("realisticFaces", false);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

	const teams: any[] = [];
	const NAMES = [
		["Atlanta", "Hawks", "ATL"],
		["Boston", "Celtics", "BOS"],
		["Brooklyn", "Nets", "BKN"],
		["Charlotte", "Hornets", "CHA"],
		["Chicago", "Bulls", "CHI"],
		["Cleveland", "Cavaliers", "CLE"],
		["Dallas", "Mavericks", "DAL"],
		["Denver", "Nuggets", "DEN"],
		["Detroit", "Pistons", "DET"],
		["Golden State", "Warriors", "GSW"],
		["Houston", "Rockets", "HOU"],
		["Indiana", "Pacers", "IND"],
		["Los Angeles", "Clippers", "LAC"],
		["Los Angeles", "Lakers", "LAL"],
		["Memphis", "Grizzlies", "MEM"],
		["Miami", "Heat", "MIA"],
		["Milwaukee", "Bucks", "MIL"],
		["Minnesota", "Timberwolves", "MIN"],
		["New Orleans", "Pelicans", "NOP"],
		["New York", "Knicks", "NYK"],
		["Oklahoma City", "Thunder", "OKC"],
		["Orlando", "Magic", "ORL"],
		["Philadelphia", "76ers", "PHI"],
		["Phoenix", "Suns", "PHX"],
		["Portland", "Trail Blazers", "POR"],
		["Sacramento", "Kings", "SAC"],
		["San Antonio", "Spurs", "SAS"],
		["Toronto", "Raptors", "TOR"],
		["Utah", "Jazz", "UTA"],
		["Washington", "Wizards", "WAS"],
	];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const [region, name, abbrev] = NAMES[tid]!;
		teams.push(
			team.generate({
				tid,
				cid: tid % 2,
				did: tid % 2,
				region,
				name,
				abbrev,
				pop: 18 ** (tid / Math.max(1, NUM_TEAMS - 1)),
				imgURL: "",
			} as any),
		);
	}
	const players = await createRandomPlayers({
		activeTids: teams.map((t) => t.tid),
		onlyFreeAgents: false,
		scoutingLevel: DEFAULT_LEVEL,
		teams,
	});
	await resetCache({ players, teams, draftPicks: [] });
	stubLeagueDb();
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const t = (await idb.cache.teams.get(tid))!;
		await idb.cache.teamSeasons.add(team.genSeasonRow(t) as any);
	}

	// valueNoPot drives GameSim's rotation; freshly generated players have none,
	// and without it every team plays five men all forty-eight minutes.
	let n = 0;
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	])) {
		await player.updateValues(p);
		p.firstName = FIRST[n % FIRST.length]!;
		p.lastName = `${LAST[Math.floor(n / FIRST.length) % LAST.length]!}`;
		n += 1;
		await idb.cache.players.put(p);
	}

	const season = g.get("season");
	const rng = rngFromSeed(SEED);
	let gid = 1;

	const loadSide = async (tid: number) => {
		const [t, teamSeason, ps] = await Promise.all([
			idb.cache.teams.get(tid),
			idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [season, tid]),
			idb.getCopies.players({ tid }, "noCopyCache"),
		]);
		if (!t || !teamSeason) {
			return undefined;
		}
		return processTeam(t, teamSeason, ps);
	};

	for (let day = 1; day <= DAYS; day++) {
		for (const p of await idb.cache.players.indexGetAll("playersByTid", [
			0,
			Infinity,
		])) {
			if (p.injury.gamesRemaining > 0) {
				p.injury.gamesRemaining -= 1;
				if (p.injury.gamesRemaining <= 0) {
					p.injury = { type: "Healthy", gamesRemaining: 0 };
				}
				await idb.cache.players.put(p);
			}
		}

		const tids = Array.from({ length: NUM_TEAMS }, (_, i) => i);
		for (let i = tids.length - 1; i > 0; i--) {
			const j = Math.floor(rng() * (i + 1));
			[tids[i], tids[j]] = [tids[j]!, tids[i]!];
		}

		for (let i = 0; i + 1 < tids.length; i += 2) {
			const home = await loadSide(tids[i]!);
			const away = await loadSide(tids[i + 1]!);
			if (!home || !away) {
				continue;
			}
			const result: any = new GameSim({
				gid,
				day,
				teams: helpers.deepCopy([home, away]) as any,
				doPlayByPlay: false,
				homeCourtFactor: 1,
				neutralSite: false,
				allStarGame: false,
				baseInjuryRate: g.get("injuryRate"),
			} as any).run();

			const w = result.team[0].stat.pts > result.team[1].stat.pts ? 0 : 1;
			const row: any = {
				gid,
				day,
				season,
				att: 18000,
				clutchPlays: (result.clutchPlays ?? []).map((c: any) => `${c.text}.`),
				numPlayersOnCourt: result.numPlayersOnCourt ?? 5,
				numPeriods: g.get("numPeriods"),
				overtimes: result.overtimes ?? 0,
				playoffs: false,
				scoringSummary: result.scoringSummary ?? [],
				won: { tid: result.team[w].id, pts: result.team[w].stat.pts },
				lost: { tid: result.team[1 - w].id, pts: result.team[1 - w].stat.pts },
				teams: [0, 1].map((j) => {
					const t = result.team[j];
					const out: any = {
						tid: t.id,
						ovr: t.ovr,
						pts: t.stat.pts,
						ptsQtrs: t.stat.ptsQtrs,
						players: [],
					};
					for (const k of STAT_KEYS) {
						out[k] = t.stat[k] ?? 0;
					}
					for (const sp of t.player) {
						const line: any = {
							pid: sp.id,
							name: sp.name,
							pos: sp.pos,
							injury: sp.injury,
						};
						for (const k of STAT_KEYS) {
							line[k] = sp.stat[k] ?? 0;
						}
						line.pm = sp.stat.pm ?? 0;
						line.gs = sp.stat.gs ?? 0;
						out.players.push(line);
					}
					return out;
				}),
			};
			await idb.cache.games.add(row as Game);

			for (const j of [0, 1] as const) {
				const ts = await idb.cache.teamSeasons.indexGet(
					"teamSeasonsBySeasonTid",
					[season, result.team[j].id],
				);
				if (ts) {
					if (j === w) {
						ts.won += 1;
					} else {
						ts.lost += 1;
					}
					(ts as any).gp = ts.won + ts.lost;
					await idb.cache.teamSeasons.put(ts);
				}
				for (const sp of result.team[j].player) {
					if (sp.newInjury) {
						const p = await idb.cache.players.get(sp.id);
						if (p && p.injury.gamesRemaining === 0) {
							p.injury = player.injury(DEFAULT_LEVEL);
							await idb.cache.players.put(p);
						}
					}
				}
			}
			gid += 1;
		}
	}

	// The real recap path, day by day. Every recap is dumped WITH the RecapGame
	// it was written from, so an accuracy checker can hold each number in the
	// prose against the box score it came out of.
	const out: string[] = [];
	const gameRecaps: string[] = [];
	const dayRecaps: string[] = [];
	const pairs: { gid: number; recap: string; game: RecapGame }[] = [];
	for (let day = 1; day <= DAYS; day++) {
		const { notes, dayRecap } = await getAutoRecapsForDay({ season, day });
		for (const rg of await recapGamesForDay({ season, day })) {
			const note = notes[rg.gid];
			if (note) {
				pairs.push({ gid: rg.gid, recap: note, game: rg });
			}
		}
		out.push(
			`\n${"=".repeat(78)}\nDAY ${day}\n${"=".repeat(78)}\n`,
			`--- DAY RECAP ---\n${dayRecap}\n`,
		);
		dayRecaps.push(dayRecap);
		for (const [id, note] of Object.entries(notes)) {
			gameRecaps.push(note);
			out.push(`--- GAME ${id} ---\n${note}\n`);
		}
	}
	writeFileSync(LOG, out.join("\n"));
	writeFileSync(`${LOG}.jsonl`, pairs.map((x) => JSON.stringify(x)).join("\n"));

	// ACCURACY. Every number in the finished prose, held against the box score
	// it was written from. A corpus run is the only place this can be asked at
	// scale, so it is asked here and it is fatal - a recap that states something
	// the game does not contain is the one defect no amount of good phrasing
	// makes up for.
	const violations: string[] = [];
	for (const { gid, recap, game } of pairs) {
		for (const v of verifyRecap(recap, game)) {
			violations.push(`gid ${gid} [${v.kind}] ${v.detail}\n    ${v.sentence}`);
		}
	}
	writeFileSync(
		`${LOG}.accuracy.txt`,
		violations.length === 0
			? `${pairs.length} recaps checked, no violations`
			: violations.join("\n"),
	);

	// Metrics. Repetition is the failure mode a corpus is FOR: a phrase that
	// reads fine once reads like a template the ninth time on the same page.
	const headline = (r: string) => r.split("\n")[0] ?? "";
	const sentences = (r: string) =>
		r
			.split("\n")
			.slice(1)
			.join(" ")
			.split(/(?<=[!.?])\s+/)
			.map((s) => s.trim())
			.filter(Boolean);

	// A sentence's SHAPE: names and numbers blanked out, so two sentences built
	// from one template collide even though they describe different games.
	const shape = (s: string) =>
		s
			.replace(/\*\*[^*]+\*\*/g, "N")
			.replace(/\b[A-Z][a-z]+\b/g, "N")
			.replace(/\d+(\.\d+)?%?/g, "#")
			.replace(/\s+/g, " ")
			.trim();

	const tally = (items: string[]) => {
		const m = new Map<string, number>();
		for (const i of items) {
			m.set(i, (m.get(i) ?? 0) + 1);
		}
		return [...m.entries()].sort((a, b) => b[1] - a[1]);
	};

	const allSentences = gameRecaps.flatMap(sentences);
	const shapes = tally(allSentences.map(shape));
	const headlineShapes = tally(gameRecaps.map((r) => shape(headline(r))));
	const dupSentences = tally(allSentences).filter(([, c]) => c > 1);

	const stats = [
		`games=${gameRecaps.length} days=${dayRecaps.length}`,
		`game recap: mean chars=${Math.round(gameRecaps.reduce((a, b) => a + b.length, 0) / gameRecaps.length)}`,
		`sentences=${allSentences.length} distinct shapes=${shapes.length} ` +
			`(top shape used ${shapes[0]?.[1]}x = ${((100 * (shapes[0]?.[1] ?? 0)) / allSentences.length).toFixed(1)}%)`,
		`headline shapes: ${headlineShapes.length} distinct over ${gameRecaps.length} games ` +
			`(top ${headlineShapes[0]?.[1]}x)`,
		`verbatim repeated sentences: ${dupSentences.length}`,
		"",
		"TOP 25 SENTENCE SHAPES:",
		...shapes.slice(0, 25).map(([s, c]) => `${String(c).padStart(4)}  ${s}`),
		"",
		"TOP 15 HEADLINE SHAPES:",
		...headlineShapes
			.slice(0, 15)
			.map(([s, c]) => `${String(c).padStart(4)}  ${s}`),
		"",
		"TOP 20 VERBATIM REPEATS:",
		...dupSentences
			.slice(0, 20)
			.map(([s, c]) => `${String(c).padStart(4)}  ${s}`),
	];
	writeFileSync(`${LOG}.stats.txt`, stats.join("\n"));

	assert.ok(gameRecaps.length > 0, "produced recaps");
	assert.strictEqual(
		violations.length,
		0,
		`${violations.length} inaccurate claims across ${pairs.length} recaps:\n${violations
			.slice(0, 25)
			.join("\n")}`,
	);
};
