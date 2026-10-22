import { test } from "vitest";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { makeSeededRandom } from "../../common/random.ts";
import type { DraftPick, Player, TradeTeams } from "../../common/types.ts";
import { last } from "../../common/utils.ts";
import { league, player, team } from "../../worker/core/index.ts";
import { dropPlayers } from "../../worker/core/team/checkRosterSizes.ts";
import processTrade from "../../worker/core/trade/processTrade.ts";
import summary from "../../worker/core/trade/summary.ts";
import {
	humanTradeSettings,
	judgeHumanOffer,
} from "../../worker/core/realisticFrontOffice/humanTrades.ts";
import {
	aiTids,
	count,
	currentOutlook,
	loadTeam,
	marginOf,
	MIN_GAIN,
	priceFor,
	salariesWork,
	type Asset,
	type TeamState,
} from "../../worker/core/realisticFrontOffice/market.ts";
import {
	franchiseValue,
	OUTLOOK_YEARS,
	retentionKey,
	retentionOf,
	seasonLine,
	teamStrengths,
	type LeagueOutlook,
} from "../../worker/core/realisticFrontOffice/outlook.ts";
import {
	playerLine,
	rosterPoints,
	type StrengthPlayer,
	type ValueRatings,
} from "../../worker/core/realisticFrontOffice/strength.ts";
import { stayChance } from "../../worker/core/realisticFrontOffice/offers.ts";
import { idb } from "../../worker/db/index.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";
import { play, setOrder, toSimTeam, type SimTeam } from "../impact/simTools.ts";

// THE SHARK: an expert human hunting for trades the CPU's model misprices,
// with the game's own sim as ground truth - what tunes how much more the
// margin of error counts in a deal with a human (humanTradeSettings).
//
// A random-players arena league with Realistic Front Offices on, every team
// run by the CPU but one: the shark, a user team, so the CPU market leaves it
// alone and the judge treats it as a human. In the preseason and a third and
// two thirds of the way to the deadline it hunts:
//
//   1. Candidates, with every CPU team: 1-for-1, 2-for-1 and 1-for-2 among
//      each side's best healthy players, legal under the trade screen's
//      salary rule and roster limit. One the CPU would turn down outright
//      gets the shark's cheapest pick added, if that is enough.
//   2. The judge's own first test, exactly (its gain against marginFactor
//      times its margin of error), screens them; the rest are ranked by the
//      model's read of what each does for the shark this season.
//   3. The best few are played out in a quick sim, both sides; the best of
//      those for the shark go to the full judge (wait, better offer).
//   4. Each yes is played out again on fresh random numbers; the shark makes
//      the one that helps it most, if by more than twice the sim's noise.
//   5. That deal is graded once more on fresh random numbers, both sides:
//      what it did to each team this season, next to what the model expected.
//
// Every sim is the trade checker's (arena.test.ts): healthy rosters in their
// best order, against every other team as it stands, game i of the roster
// before and after on the same random numbers. Those barely help (a changed
// roster draws them differently from the first possession), so the noise is
// about 18 points over root games of the per-game difference.
//
//   SHARK_RUN=1 SHARK_SEED=1 SHARK_SEASONS=3 SHARK_FACTOR=1.5 SHARK_OUT=/tmp/shark \
//     npx vitest --run --project basketball src/test/shark/shark.test.ts
//   SHARK_REPORT=/tmp/shark npx vitest --run --project basketball src/test/shark/shark.test.ts
//
// SHARK_PASSIVE=1 plays the same league with the shark never trading - the
// baseline for its record. Games per sim: SHARK_SCREEN_GAMES (150) quick,
// SHARK_DECIDE_GAMES (400) deciding, SHARK_GAMES (600) grading.

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

const importNode = (name: string) => import(/* @vite-ignore */ name);

// Players a side, best first, that deals are built from.
const TOP_PLAYERS = Number(env.SHARK_TOP ?? 8);
// Candidates played out in the quick sim, per hunt, and at most per team.
const QUICK_SIMS = Number(env.SHARK_QUICK ?? 24);
const QUICK_PER_TEAM = 2;
// The full judge, in order of the quick sim, until this many say yes.
const JUDGE_MAX = 8;
const YES_MAX = 3;
// Shark picks tried as a sweetener, cheapest first.
const PICKS_TRIED = 4;
const PICK_DRAFTS = 3;
// Deals the shark may make in one hunt.
const DEALS_PER_HUNT = Number(env.SHARK_DEALS ?? 3);
// Margin factors the candidate counts are reported at.
const FACTORS = [0, 1, 1.5, 2, 3, 4];

type Stat = { mean: number; se: number; n: number };

const pairedStat = (after: number[], before: number[]): Stat => {
	const n = after.length;
	const d = after.map((x, i) => x - before[i]!);
	const mean = d.reduce((a, x) => a + x, 0) / n;
	const variance = d.reduce((a, x) => a + (x - mean) ** 2, 0) / (n - 1);
	return { mean, se: Math.sqrt(variance / n), n };
};

const round = (x: number, digits = 3) =>
	Math.round(x * 10 ** digits) / 10 ** digits;

const healthy = (players: readonly Player[]) =>
	players.map((p) => {
		const copy = helpers.deepCopy(p);
		copy.injury = { type: "Healthy", gamesRemaining: 0 };
		return copy;
	});

const simTeamOf = async (tid: number, players: readonly Player[]) => {
	const roster = healthy(players).sort((a, b) => b.valueNoPot - a.valueNoPot);
	setOrder(roster);
	return toSimTeam(tid, roster);
};

// Points per game the model gives a healthy roster this season, as the trade
// checker reads it.
const lineCache = new Map<string, StrengthPlayer>();
const lineOf = (p: Player): StrengthPlayer => {
	const r = last(p.ratings);
	const key = `${p.pid}:${r.season}:${r.ovr}:${p.valueNoPot}`;
	let line = lineCache.get(key);
	if (!line) {
		line = {
			...playerLine(r as unknown as ValueRatings),
			valueNoPot: p.valueNoPot,
		};
		lineCache.set(key, line);
	}
	return line;
};
const modelPoints = (players: readonly Player[]) =>
	rosterPoints(players.map(lineOf));

const playersOf = (assets: readonly Asset[]) =>
	assets.flatMap((x) => (x.kind === "player" ? [x.p] : []));
const picksOf = (assets: readonly Asset[]) =>
	assets.flatMap((x) => (x.kind === "pick" ? [x.dp] : []));

const applyTrade = (
	t: TeamState,
	outgoing: readonly Asset[],
	incoming: readonly Asset[],
) => {
	const outP = new Set(playersOf(outgoing).map((p) => p.pid));
	const outD = new Set(picksOf(outgoing).map((dp) => dp.dpid));
	return {
		players: [
			...t.players.filter((p) => !outP.has(p.pid)),
			...playersOf(incoming),
		],
		picks: [
			...t.picks.filter((dp) => !outD.has(dp.dpid)),
			...picksOf(incoming),
		],
	};
};

const describePlayer = (p: Player) => {
	const r = last(p.ratings);
	return {
		pid: p.pid,
		age: g.get("season") - p.born.year,
		ovr: r.ovr,
		pot: r.pot,
		pos: r.pos,
		valueNoPot: round(p.valueNoPot, 1),
		// As a share of the cap, and seasons after this one.
		amount: round(p.contract.amount / g.get("salaryCap")),
		years: p.contract.exp - g.get("season"),
		rookie: !!p.contract.rookie,
		injury: p.injury.gamesRemaining,
	};
};

const describePick = (dp: DraftPick) => ({
	dpid: dp.dpid,
	season: dp.season,
	round: dp.round,
	originalTid: dp.originalTid,
});

type Candidate = {
	cpuTid: number;
	// What the shark gives and gets.
	give: Asset[];
	get: Asset[];
	// The CPU's gain and margin of error by its own model, in titles, and the
	// margin factor up to which it would pass the judge's first test.
	gain: number;
	margin: number;
	factorLimit: number;
	// The model's read of each side this season, healthy, points per game.
	modelShark: number;
	modelCpu: number;
	quick?: { shark: Stat; cpu: Stat };
};

test.runIf(env.SHARK_RUN === "1")(
	"shark: hunt for mispriced trades",
	{ timeout: 12 * 60 * 60 * 1000 },
	async () => {
		const seed = Number(env.SHARK_SEED ?? 1);
		const numSeasons = Number(env.SHARK_SEASONS ?? 2);
		const factor = Number(env.SHARK_FACTOR ?? humanTradeSettings.marginFactor);
		const passive = env.SHARK_PASSIVE === "1";
		// SHARK_OBJECTIVE=harm: hunt for the deals the CPU misreads worst, not
		// the ones that help the shark most.
		const harm = env.SHARK_OBJECTIVE === "harm";
		const games = Number(env.SHARK_GAMES ?? 600);
		const decideGames = Number(env.SHARK_DECIDE_GAMES ?? 400);
		const screenGames = Number(env.SHARK_SCREEN_GAMES ?? 150);
		const sharkTid = Number(env.SHARK_TID ?? 0);
		const out = env.SHARK_OUT ?? ".";
		const tag = passive
			? `passive-${seed}`
			: `${harm ? "h" : "f"}${factor}-${seed}`;
		const fs = await importNode("node:fs/promises");
		await fs.mkdir(out, { recursive: true });
		const logLine = async (line: string) => {
			await fs.appendFile(`${out}/progress-${tag}.log`, `${line}\n`);
		};

		await importNode("fake-indexeddb/auto");
		const realRandom = Math.random;
		Math.random = makeSeededRandom(seed);
		const started = Date.now();
		try {
			await createArenaLeague(seed);
			await league.setGameAttributes({
				realisticFrontOffices: true,
				spectator: false,
				userTid: sharkTid,
				userTids: [sharkTid],
				// Nobody gets fired.
				gracePeriodEnd: 9999,
			} as any);
			humanTradeSettings.marginFactor = factor;

			const hunts: Record<string, unknown>[] = [];
			const deals: Record<string, unknown>[] = [];
			const pool: Record<string, unknown>[] = [];
			let huntNo = 0;

			// The shark's roster as a human keeps it: within the limits, the
			// worst cut, minimum players signed when short.
			const fixSharkRoster = async () => {
				const players = await idb.cache.players.indexGetAll(
					"playersByTid",
					sharkTid,
				);
				if (players.length > g.get("maxRosterSize")) {
					await dropPlayers(players, players.length - g.get("maxRosterSize"));
				}
				let size = players.length;
				if (size < g.get("minRosterSize")) {
					const free = (
						await idb.cache.players.indexGetAll(
							"playersByTid",
							PLAYER.FREE_AGENT,
						)
					)
						.filter((p) => p.contract.amount === g.get("minContract"))
						.sort((a, b) => b.value - a.value);
					while (size < g.get("minRosterSize")) {
						const p = free.shift() ?? (await player.genRandomFreeAgent());
						await player.sign(p, sharkTid, p.contract, g.get("phase"));
						await idb.cache.players.put(p);
						size += 1;
					}
				}
				await team.rosterAutoSort(sharkTid);
			};

			// Every team's healthy roster as it stands, for the sims.
			const opponentsNow = async () => {
				const list: SimTeam[] = [];
				for (let tid = 0; tid < g.get("numTeams"); tid++) {
					list.push(
						await simTeamOf(
							tid,
							await idb.cache.players.indexGetAll("playersByTid", tid),
						),
					);
				}
				return list;
			};

			const hunt = async (label: string) => {
				huntNo += 1;
				const clock = [Date.now()];
				const seedBase = (seed * 1000 + huntNo) * 10;
				const outlook = await currentOutlook();
				const shark = await loadTeam(outlook, sharkTid);
				const opponents = await opponentsNow();
				const numTeams = opponents.length;

				const leagueRandom = Math.random;
				const margins = async (
					tid: number,
					players: readonly Player[],
					stage: number,
					n: number,
				) => {
					const subject = await simTeamOf(tid, players);
					const out: number[] = [];
					try {
						for (let i = 0; i < n; i++) {
							const oppTid = (tid + 1 + (i % (numTeams - 1))) % numTeams;
							Math.random = makeSeededRandom(
								(seedBase + stage) * 1_000_003 + i,
							);
							const home = i % 2 === 0;
							const result = home
								? play(
										helpers.deepCopy(subject),
										helpers.deepCopy(opponents[oppTid]!),
									)
								: play(
										helpers.deepCopy(opponents[oppTid]!),
										helpers.deepCopy(subject),
									);
							const margin = result.team[0].stat.pts - result.team[1].stat.pts;
							out.push(home ? margin : -margin);
						}
					} finally {
						Math.random = leagueRandom;
					}
					return out;
				};
				const beforeCache = new Map<string, number[]>();
				const before = async (
					tid: number,
					players: readonly Player[],
					stage: number,
					n: number,
				) => {
					const key = `${tid}:${stage}:${n}`;
					let xs = beforeCache.get(key);
					if (!xs) {
						xs = await margins(tid, players, stage, n);
						beforeCache.set(key, xs);
					}
					return xs;
				};
				const simDeal = async (
					c: Candidate,
					cpu: TeamState,
					stage: number,
					n: number,
					sides: "both" | "shark",
				) => {
					const sharkAfter = applyTrade(shark, c.give, c.get).players;
					const sharkStat = pairedStat(
						await margins(sharkTid, sharkAfter, stage, n),
						await before(sharkTid, shark.players, stage, n),
					);
					if (sides === "shark") {
						return { shark: sharkStat };
					}
					const cpuAfter = applyTrade(cpu, c.get, c.give).players;
					const cpuStat = pairedStat(
						await margins(cpu.tid, cpuAfter, stage, n),
						await before(cpu.tid, cpu.players, stage, n),
					);
					return { shark: sharkStat, cpu: cpuStat };
				};

				const sharkModelBefore = modelPoints(shark.players);
				const sharkPlayers = [...shark.tradablePlayers]
					.filter((p) => p.injury.gamesRemaining === 0)
					.sort((a, b) => b.valueNoPot - a.valueNoPot)
					.slice(0, TOP_PLAYERS);
				const sharkPicks = shark.picks
					.filter(
						(dp) =>
							typeof dp.season === "number" &&
							dp.season <= g.get("season") + PICK_DRAFTS,
					)
					.map((dp) => ({
						asset: { kind: "pick" as const, dp },
						cost: priceFor(outlook, shark, [{ kind: "pick", dp }], []),
					}))
					.sort((a, b) => b.cost - a.cost)
					.slice(0, PICKS_TRIED)
					.map((x) => x.asset);
				const inSeason = g.get("phase") === PHASE.REGULAR_SEASON;
				const maxRoster = g.get("maxRosterSize");

				const candidates: Candidate[] = [];
				const states = new Map<number, TeamState>();
				let built = 0;
				const passAt = FACTORS.map(() => 0);
				let sweetened = 0;
				for (const cpuTid of await aiTids()) {
					const cpu = await loadTeam(outlook, cpuTid);
					states.set(cpuTid, cpu);
					const cpuModelBefore = modelPoints(cpu.players);
					const cpuPlayers = [...cpu.tradablePlayers]
						.filter((p) => p.injury.gamesRemaining === 0)
						.sort((a, b) => b.valueNoPot - a.valueNoPot)
						.slice(0, TOP_PLAYERS);
					const asAssets = (ps: Player[]) =>
						ps.map((p) => ({ kind: "player" as const, p }));
					const shapes: [Asset[], Asset[]][] = [];
					const singlesS = asAssets(sharkPlayers);
					const singlesC = asAssets(cpuPlayers);
					const pairs = (xs: Asset[]) =>
						xs.flatMap((x, i) => xs.slice(i + 1).map((y) => [x, y]));
					for (const a of singlesS) {
						for (const b of singlesC) {
							shapes.push([[a], [b]]);
						}
						for (const bs of pairs(singlesC)) {
							shapes.push([[a], bs]);
						}
					}
					for (const as of pairs(singlesS)) {
						for (const b of singlesC) {
							shapes.push([as, [b]]);
						}
					}
					const failing: Candidate[] = [];
					const price = (give: Asset[], get: Asset[]) => {
						const gain = priceFor(outlook, cpu, get, give);
						const margin = marginOf(outlook, cpu, get, give);
						return {
							gain,
							margin,
							factorLimit:
								margin > 0
									? (gain - MIN_GAIN) / margin
									: gain >= MIN_GAIN
										? Infinity
										: -Infinity,
						};
					};
					for (const [give, get] of shapes) {
						const sharkSize = shark.players.length - give.length + get.length;
						const cpuSize = cpu.players.length - get.length + give.length;
						if (
							sharkSize > maxRoster ||
							(inSeason && cpuSize > maxRoster) ||
							!salariesWork(shark, give, get) ||
							!salariesWork(cpu, get, give)
						) {
							continue;
						}
						built += 1;
						const priced = price(give, get);
						for (const [i, k] of FACTORS.entries()) {
							if (priced.factorLimit >= k) {
								passAt[i]! += 1;
							}
						}
						const c: Candidate = {
							cpuTid,
							give,
							get,
							...priced,
							modelShark:
								modelPoints(applyTrade(shark, give, get).players) -
								sharkModelBefore,
							modelCpu:
								modelPoints(applyTrade(cpu, get, give).players) -
								cpuModelBefore,
						};
						if (c.factorLimit >= factor) {
							candidates.push(c);
						} else {
							failing.push(c);
						}
					}
					// A pick to get over the line, for the deals best for the shark
					// that fall short.
					failing.sort((a, b) => b.modelShark - a.modelShark);
					for (const c of failing.slice(0, 2 * QUICK_PER_TEAM)) {
						if (c.modelShark <= 0) {
							break;
						}
						for (const pick of sharkPicks) {
							const give = [...c.give, pick];
							const priced = price(give, c.get);
							if (priced.factorLimit >= factor) {
								candidates.push({ ...c, give, ...priced });
								sweetened += 1;
								break;
							}
						}
					}
				}

				clock.push(Date.now());
				// The model's read first, then the quick sim.
				const perTeam = new Map<number, number>();
				const toSim = candidates
					.filter((c) => c.modelShark > 0)
					.sort((a, b) => b.modelShark - a.modelShark)
					.filter((c) => {
						const k = perTeam.get(c.cpuTid) ?? 0;
						perTeam.set(c.cpuTid, k + 1);
						return k < QUICK_PER_TEAM;
					})
					.slice(0, QUICK_SIMS);
				for (const c of toSim) {
					c.quick = (await simDeal(
						c,
						states.get(c.cpuTid)!,
						1,
						screenGames,
						"both",
					)) as Candidate["quick"];
					pool.push({
						hunt: huntNo,
						label,
						season: g.get("season"),
						factor,
						...record(outlook, c, states.get(c.cpuTid)!),
						quick: {
							shark: roundStat(c.quick!.shark),
							cpu: roundStat(c.quick!.cpu),
						},
					});
				}

				clock.push(Date.now());
				// The full judge, best for the shark first - or, hunting for the
				// CPU's misreads, worst for the CPU against its model first, among
				// deals that help the shark.
				const misreadOf = (c: Candidate, cpu: Stat) => cpu.mean - c.modelCpu;
				const verdicts: Record<string, number> = {};
				const yes: Candidate[] = [];
				let judged = 0;
				const order = harm
					? toSim
							.filter((c) => c.quick!.shark.mean > 0)
							.sort(
								(a, b) =>
									misreadOf(a, a.quick!.cpu) - misreadOf(b, b.quick!.cpu),
							)
					: [...toSim].sort(
							(a, b) => b.quick!.shark.mean - a.quick!.shark.mean,
						);
				for (const c of order) {
					if (judged >= JUDGE_MAX || yes.length >= YES_MAX) {
						break;
					}
					if (
						harm ? misreadOf(c, c.quick!.cpu) >= 0 : c.quick!.shark.mean <= 0
					) {
						break;
					}
					judged += 1;
					const verdict = await judgeHumanOffer(
						{
							tid: c.cpuTid,
							pids: playersOf(c.get).map((p) => p.pid),
							dpids: picksOf(c.get).map((dp) => dp.dpid),
						},
						{
							tid: sharkTid,
							pids: playersOf(c.give).map((p) => p.pid),
							dpids: picksOf(c.give).map((dp) => dp.dpid),
						},
					);
					verdicts[verdict.kind] = (verdicts[verdict.kind] ?? 0) + 1;
					if (verdict.kind === "yes") {
						yes.push(c);
					}
				}

				clock.push(Date.now());
				// Each yes on fresh random numbers; the best for the shark, if
				// clearly a gain - or the CPU's worst misread, if beyond the noise,
				// among those that help the shark.
				let best: { c: Candidate; stat: Stat; cpu?: Stat } | undefined;
				for (const c of yes) {
					const result = await simDeal(
						c,
						states.get(c.cpuTid)!,
						2,
						decideGames,
						harm ? "both" : "shark",
					);
					const stat = result.shark;
					if (harm) {
						const cpu = (result as { cpu: Stat }).cpu;
						if (
							stat.mean > 0 &&
							(!best || misreadOf(c, cpu) < misreadOf(best.c, best.cpu!))
						) {
							best = { c, stat, cpu };
						}
					} else if (!best || stat.mean > best.stat.mean) {
						best = { c, stat };
					}
				}
				clock.push(Date.now());
				const seconds = clock
					.slice(1)
					.map((x, i) => Math.round((x - clock[i]!) / 1000));
				const row: Record<string, unknown> = {
					hunt: huntNo,
					seconds,
					label,
					season: g.get("season"),
					factor,
					candidates: built,
					passAt: Object.fromEntries(
						FACTORS.map((k, i) => [String(k), passAt[i]]),
					),
					sweetened,
					passed: candidates.length,
					quickSims: toSim.length,
					judged,
					verdicts,
					decision: best
						? {
								gain: roundStat(best.stat),
								cpu: best.cpu ? roundStat(best.cpu) : undefined,
								cpuTid: best.c.cpuTid,
							}
						: undefined,
				};
				hunts.push(row);
				if (
					!best ||
					(harm
						? misreadOf(best.c, best.cpu!) >= -best.cpu!.se
						: best.stat.mean <= 2 * best.stat.se)
				) {
					await logLine(
						`${label} hunt ${huntNo}: ${built} built, ${candidates.length} pass, ${judged} judged ${JSON.stringify(verdicts)}, no deal [${seconds.join("/")} s] (${((Date.now() - started) / 60000).toFixed(1)} min)`,
					);
					return false;
				}

				// Graded on fresh random numbers, both sides.
				const c = best.c;
				const cpu = states.get(c.cpuTid)!;
				const graded = (await simDeal(c, cpu, 3, games, "both")) as {
					shark: Stat;
					cpu: Stat;
				};
				const misread = graded.cpu.mean - c.modelCpu;
				// What the CPU's misread this season costs it in titles: this season
				// only, and carried into later seasons while the players it moved
				// would have been there.
				const afterCpu = applyTrade(cpu, c.get, c.give);
				const path = teamStrengths(
					outlook,
					cpu.tid,
					afterCpu.players,
					afterCpu.picks,
					{ tradedAway: cpu.tradedAway, extra: count(c.get) },
				);
				const moved = [...playersOf(c.get), ...playersOf(c.give)];
				const stays = Array.from({ length: OUTLOOK_YEARS + 1 }, (_, y) =>
					y === 0
						? 1
						: moved.reduce(
								(a, p) =>
									a +
									seasonLine(
										outlook.lines,
										p,
										outlook.season,
										outlook.offset,
										y,
										outlook.remaining.get(cpu.tid) ?? 0,
										retentionOf(outlook, p.pid, cpu.tid),
									).availability,
								0,
							) / Math.max(1, moved.length),
				);
				const valueOfMisread = (x: number, carried: boolean) =>
					franchiseValue(
						outlook,
						path.map((s, y) => s + x * (carried ? stays[y]! : y === 0 ? 1 : 0)),
					) - franchiseValue(outlook, path);
				const sharkGain = priceFor(outlook, shark, c.give, c.get);
				// The chance the judge gave each player it takes of re-signing with
				// it, next to the mood model's.
				const retention = [];
				for (const p of playersOf(c.give)) {
					retention.push({
						pid: p.pid,
						years: p.contract.exp - g.get("season"),
						used: retentionOf(outlook, p.pid, cpu.tid),
						actual: round(await stayChance(p, cpu.tid)),
					});
				}
				// And the CPU's gain had it used the mood model's, put back after.
				const unset = retention.filter(
					(x) => !outlook.retention.has(retentionKey(x.pid, cpu.tid)),
				);
				for (const x of unset) {
					outlook.retention.set(retentionKey(x.pid, cpu.tid), x.actual);
				}
				const gainTrueRetention = priceFor(outlook, cpu, c.get, c.give);
				for (const x of unset) {
					outlook.retention.delete(retentionKey(x.pid, cpu.tid));
				}

				const teams: TradeTeams = [
					{
						tid: sharkTid,
						pids: playersOf(c.give).map((p) => p.pid),
						dpids: picksOf(c.give).map((dp) => dp.dpid),
						pidsExcluded: [],
						dpidsExcluded: [],
					},
					{
						tid: c.cpuTid,
						pids: playersOf(c.get).map((p) => p.pid),
						dpids: picksOf(c.get).map((dp) => dp.dpid),
						pidsExcluded: [],
						dpidsExcluded: [],
					},
				];
				const tradeSummary = await summary(teams);
				const o = outlook.teams.get(c.cpuTid);
				const os = outlook.teams.get(sharkTid);
				deals.push({
					hunt: huntNo,
					label,
					season: g.get("season"),
					factor,
					...record(outlook, c, cpu),
					decision: roundStat(best.stat),
					graded: {
						shark: roundStat(graded.shark),
						cpu: roundStat(graded.cpu),
					},
					misread: round(misread),
					misreadValue: {
						now: round(valueOfMisread(misread, false), 5),
						carried: round(valueOfMisread(misread, true), 5),
						// The sim's noise, in the same units.
						seNow: round(Math.abs(valueOfMisread(graded.cpu.se, false)), 5),
						seCarried: round(Math.abs(valueOfMisread(graded.cpu.se, true)), 5),
					},
					sharkModelGain: round(sharkGain, 5),
					retention,
					gainTrueRetention: round(gainTrueRetention, 5),
					cpuStanding: o
						? { now: round(o.strength[0]!, 2), title: round(o.title[0]!, 4) }
						: undefined,
					sharkStanding: os
						? { now: round(os.strength[0]!, 2), title: round(os.title[0]!, 4) }
						: undefined,
					warning: tradeSummary.warning,
				});
				if (tradeSummary.warning) {
					await logLine(`${label}: blocked by the trade screen`);
					return false;
				}
				await processTrade(teams, undefined);
				await team.rosterAutoSort(c.cpuTid);
				await fixSharkRoster();
				await logLine(
					`${label} hunt ${huntNo}: ${built} built, ${candidates.length} pass, ${judged} judged ${JSON.stringify(verdicts)}; DEAL with ${c.cpuTid}: shark ${graded.shark.mean.toFixed(2)}±${graded.shark.se.toFixed(2)}, cpu ${graded.cpu.mean.toFixed(2)}±${graded.cpu.se.toFixed(2)} (model ${c.modelCpu.toFixed(2)}) [${seconds.join("/")} s] (${((Date.now() - started) / 60000).toFixed(1)} min)`,
				);
				return true;
			};

			const huntUpTo = async (label: string) => {
				if (passive) {
					return;
				}
				for (let i = 0; i < DEALS_PER_HUNT; i++) {
					if (!(await hunt(label))) {
						break;
					}
				}
			};

			await advanceSeasons(numSeasons, {
				onPreseason: async () => {
					await fixSharkRoster();
					await huntUpTo("preseason");
					await fixSharkRoster();
				},
				surveyAt: (daysToDeadline) => [
					Math.round((daysToDeadline * 2) / 3),
					Math.round(daysToDeadline / 3),
				],
				onSurvey: async (daysToDeadline) => {
					await huntUpTo(`${daysToDeadline} days to deadline`);
				},
			});

			const seasons = [];
			for (let s = g.get("season") - numSeasons; s < g.get("season"); s++) {
				// Old seasons are only in the database.
				const [ts] = await idb.merged.teamSeasons.getAll({
					tid: sharkTid,
					season: s,
				});
				seasons.push({ season: s, won: ts?.won ?? 0, lost: ts?.lost ?? 0 });
			}
			await fs.writeFile(
				`${out}/shark-${tag}.json`,
				JSON.stringify({
					seed,
					factor,
					passive,
					objective: harm ? "harm" : "gain",
					games,
					decideGames,
					screenGames,
					sharkTid,
					minutes: (Date.now() - started) / 60000,
					seasons,
					hunts,
					deals,
					pool,
				}),
			);
		} finally {
			Math.random = realRandom;
		}
	},
);

const roundStat = (s: Stat) => ({ mean: round(s.mean), se: round(s.se) });

const record = (outlook: LeagueOutlook, c: Candidate, cpu: TeamState) => ({
	cpuTid: c.cpuTid,
	gives: playersOf(c.give).map(describePlayer),
	givesPicks: picksOf(c.give).map(describePick),
	gets: playersOf(c.get).map(describePlayer),
	gain: round(c.gain, 5),
	margin: round(c.margin, 5),
	factorLimit: round(c.factorLimit, 2),
	modelShark: round(c.modelShark),
	modelCpu: round(c.modelCpu),
	cpuStrength: round(outlook.teams.get(cpu.tid)?.strength[0] ?? 0, 2),
});

type RunFile = {
	seed: number;
	factor: number;
	passive: boolean;
	objective?: "gain" | "harm";
	games: number;
	seasons: { season: number; won: number; lost: number }[];
	hunts: {
		candidates: number;
		passed: number;
		passAt: Record<string, number>;
		judged: number;
		verdicts: Record<string, number>;
	}[];
	deals: (ReturnType<typeof record> & {
		label: string;
		season: number;
		warning: string | null;
		graded: { shark: Stat; cpu: Stat };
		misread: number;
		gainTrueRetention?: number;
		misreadValue: {
			now: number;
			carried: number;
			seNow: number;
			seCarried: number;
		};
		givesPicks: ReturnType<typeof describePick>[];
	})[];
	pool: (ReturnType<typeof record> & {
		hunt: number;
		quick: { shark: Stat; cpu: Stat };
	})[];
};

const meanOf = (xs: number[]) =>
	xs.length === 0 ? Number.NaN : xs.reduce((a, x) => a + x, 0) / xs.length;
const seOf = (xs: number[]) => {
	const m = meanOf(xs);
	return xs.length < 2
		? Number.NaN
		: Math.sqrt(
				xs.reduce((a, x) => a + (x - m) ** 2, 0) / (xs.length - 1) / xs.length,
			);
};
const fmt = (x: number, digits = 2) =>
	Number.isFinite(x) ? x.toFixed(digits) : "-";
const pct = (x: number) =>
	Number.isFinite(x) ? `${Math.round(x * 100)}%` : "-";
const pm = (xs: number[], digits = 2) =>
	`${fmt(meanOf(xs), digits)} ± ${fmt(seOf(xs), digits)}`;

test.runIf(env.SHARK_REPORT !== undefined)("shark: report", async () => {
	const fs = await importNode("node:fs/promises");
	const dir = env.SHARK_REPORT!;
	const runs: RunFile[] = [];
	for (const f of await fs.readdir(dir)) {
		if (/^shark-.*\.json$/.test(f)) {
			runs.push(JSON.parse(await fs.readFile(`${dir}/${f}`, "utf8")));
		}
	}
	const passive = new Map(
		runs.filter((r) => r.passive).map((r) => [r.seed, r]),
	);
	// A column per objective and margin factor.
	const columnOf = (r: RunFile) =>
		`${r.objective === "harm" ? "harm " : ""}${r.factor}`;
	const columns = [
		...new Set(
			runs
				.filter((r) => !r.passive)
				.sort(
					(a, b) =>
						Number(a.objective === "harm") - Number(b.objective === "harm") ||
						a.factor - b.factor,
				)
				.map(columnOf),
		),
	];

	const lines = [
		"# Shark report",
		"",
		"Per accepted deal, graded on fresh random numbers (healthy, against the rest of the league, before and after on the same numbers). Points per game this season. Misread: the CPU's change in the sim minus what its model expected (the trade checker's modelNow). Truly worse off: its model gain plus its misread in titles, carried while the players moved would have been there, below zero. ± is the standard error across deals; noise is the per-deal standard error of the sim.",
		"",
		"Columns: the margin factor, for the shark after its own gain (default) and the one after the CPU's misreads (harm).",
		"",
		`| marginFactor | ${columns.join(" | ")} |`,
		`|---|${columns.map(() => "---|").join("")}`,
	];
	const rows: [string, (rs: RunFile[]) => string][] = [
		[
			"leagues x seasons",
			(rs) => `${rs.length} x ${meanOf(rs.map((r) => r.seasons.length))}`,
		],
		["hunts", (rs) => String(rs.reduce((a, r) => a + r.hunts.length, 0))],
		[
			"candidates a hunt",
			(rs) =>
				fmt(meanOf(rs.flatMap((r) => r.hunts.map((h) => h.candidates))), 0),
		],
		[
			"pass the judge's first test, a hunt",
			(rs) => fmt(meanOf(rs.flatMap((r) => r.hunts.map((h) => h.passed))), 0),
		],
		[
			"judged in full: yes / better / wait / no",
			(rs) => {
				const n = (k: string) =>
					rs.reduce(
						(a, r) => a + r.hunts.reduce((b, h) => b + (h.verdicts[k] ?? 0), 0),
						0,
					);
				return `${n("yes")} / ${n("better")} / ${n("wait")} / ${n("no")}`;
			},
		],
		[
			"deals made (a league-season)",
			(rs) => {
				const made = rs.flatMap((r) => r.deals.filter((d) => !d.warning));
				const seasons = rs.reduce((a, r) => a + r.seasons.length, 0);
				return `${made.length} (${fmt(made.length / seasons, 1)})`;
			},
		],
		[
			"shark's gain a deal",
			(rs) => pm(made(rs).map((d) => d.graded.shark.mean)),
		],
		[
			"shark's gain, model",
			(rs) => fmt(meanOf(made(rs).map((d) => d.modelShark))),
		],
		["CPU's change a deal", (rs) => pm(made(rs).map((d) => d.graded.cpu.mean))],
		[
			"CPU's change, model",
			(rs) => fmt(meanOf(made(rs).map((d) => d.modelCpu))),
		],
		["CPU's misread a deal", (rs) => pm(made(rs).map((d) => d.misread))],
		[
			"CPU lost beyond noise",
			(rs) =>
				pct(
					meanOf(
						made(rs).map((d) =>
							d.graded.cpu.mean < -2 * d.graded.cpu.se ? 1 : 0,
						),
					),
				),
		],
		[
			"CPU misread beyond noise",
			(rs) =>
				pct(
					meanOf(
						made(rs).map((d) => (d.misread < -2 * d.graded.cpu.se ? 1 : 0)),
					),
				),
		],
		[
			"CPU's model gain a deal (titles x100)",
			(rs) => fmt(100 * meanOf(made(rs).map((d) => d.gain))),
		],
		[
			"misread in titles x100, this season / carried",
			(rs) =>
				`${fmt(100 * meanOf(made(rs).map((d) => d.misreadValue.now)))} / ${fmt(100 * meanOf(made(rs).map((d) => d.misreadValue.carried)))}`,
		],
		[
			"CPU's gain after its misread (titles x100, carried)",
			(rs) => pm(made(rs).map((d) => 100 * (d.gain + d.misreadValue.carried))),
		],
		[
			"CPU's gain at the mood model's re-signing odds (titles x100)",
			(rs) => {
				const xs = made(rs).filter((d) => d.gainTrueRetention !== undefined);
				return `${pm(xs.map((d) => 100 * d.gainTrueRetention!))} (${xs.length} deals)`;
			},
		],
		[
			"CPU truly worse off (carried)",
			(rs) =>
				pct(
					meanOf(
						made(rs).map((d) => (d.gain + d.misreadValue.carried < 0 ? 1 : 0)),
					),
				),
		],
		[
			"sim noise a deal, shark / CPU",
			(rs) =>
				`${fmt(meanOf(made(rs).map((d) => d.graded.shark.se)))} / ${fmt(meanOf(made(rs).map((d) => d.graded.cpu.se)))}`,
		],
		[
			"shark win% vs passive",
			(rs) => {
				const diffs: number[] = [];
				for (const r of rs) {
					const base = passive.get(r.seed);
					if (!base) {
						continue;
					}
					for (const [i, s] of r.seasons.entries()) {
						const b = base.seasons[i];
						if (b) {
							diffs.push(
								s.won / Math.max(1, s.won + s.lost) -
									b.won / Math.max(1, b.won + b.lost),
							);
						}
					}
				}
				return `${diffs.length > 0 ? (meanOf(diffs) >= 0 ? "+" : "") : ""}${fmt(100 * meanOf(diffs), 1)} ± ${fmt(100 * seOf(diffs), 1)} pts`;
			},
		],
	];
	for (const [label, cell] of rows) {
		lines.push(
			`| ${label} | ${columns.map((k) => cell(runs.filter((r) => !r.passive && columnOf(r) === k))).join(" | ")} |`,
		);
	}

	// The cost of a higher bar: every candidate the shark built, in every hunt
	// of every run, and the factors up to which the CPU would take it by the
	// judge's first test.
	const allHunts = runs.filter((r) => !r.passive).flatMap((r) => r.hunts);
	lines.push(
		"",
		`Candidates passing the judge's first test, a hunt (of ${fmt(meanOf(allHunts.map((h) => h.candidates)), 0)} built, ${allHunts.length} hunts):`,
		"",
		`| factor | ${FACTORS.join(" | ")} |`,
		`|---|${FACTORS.map(() => "---|").join("")}`,
		`| passing | ${FACTORS.map((k) => fmt(meanOf(allHunts.map((h) => h.passAt[String(k)] ?? 0)), 0)).join(" | ")} |`,
	);

	// Blind spots: the players the shark took and gave in the deals it made,
	// and the quick sims bucketed by the best player it would get.
	const allMade = made(runs.filter((r) => !r.passive));
	const traits = (ps: ReturnType<typeof describePlayer>[]) =>
		`age ${fmt(meanOf(ps.map((p) => p.age)), 1)}, ovr ${fmt(meanOf(ps.map((p) => p.ovr)), 1)}, pot ${fmt(meanOf(ps.map((p) => p.pot)), 1)}, ${fmt(meanOf(ps.map((p) => p.years)), 1)} more seasons at ${fmt(100 * meanOf(ps.map((p) => p.amount)), 1)}% of the cap, rookie deals ${pct(meanOf(ps.map((p) => (p.rookie ? 1 : 0))))}`;
	lines.push(
		"",
		"Players in the deals the shark made:",
		"",
		`- it got: ${traits(allMade.flatMap((d) => d.gets))}`,
		`- it gave: ${traits(allMade.flatMap((d) => d.gives))}; picks in ${pct(meanOf(allMade.map((d) => (d.givesPicks.length > 0 ? 1 : 0))))} of deals`,
		"",
		"Quick sims by the best player the shark would get (CPU misread, points per game; shark's edge: its sim gain over the model's):",
		"",
		"| bucket | candidates | CPU misread | shark's edge |",
		"|---|---|---|---|",
	);
	const pool = runs.filter((r) => !r.passive).flatMap((r) => r.pool);
	const buckets: [string, (p: ReturnType<typeof describePlayer>) => string][] =
		[
			[
				"age",
				(p) =>
					p.age < 23
						? "<23"
						: p.age < 27
							? "23-26"
							: p.age < 31
								? "27-30"
								: "31+",
			],
			[
				"ovr",
				(p) =>
					p.ovr < 50
						? "<50"
						: p.ovr < 55
							? "50-54"
							: p.ovr < 60
								? "55-59"
								: "60+",
			],
			["pos", (p) => p.pos],
			["seasons left", (p) => (p.years >= 2 ? "2+" : String(p.years))],
		];
	for (const [name, key] of buckets) {
		const groups = new Map<string, RunFile["pool"]>();
		for (const c of pool) {
			const main = [...c.gets].sort((a, b) => b.valueNoPot - a.valueNoPot)[0];
			if (!main) {
				continue;
			}
			const k = key(main);
			groups.set(k, [...(groups.get(k) ?? []), c]);
		}
		for (const [k, list] of [...groups].sort()) {
			lines.push(
				`| ${name} ${k} | ${list.length} | ${pm(list.map((c) => c.quick.cpu.mean - c.modelCpu))} | ${pm(list.map((c) => c.quick.shark.mean - c.modelShark))} |`,
			);
		}
	}
	lines.push("", "Deals the CPU misread worst:", "");
	for (const d of [...allMade]
		.sort((a, b) => a.misread - b.misread)
		.slice(0, 6)) {
		const show = (ps: ReturnType<typeof describePlayer>[]) =>
			ps
				.map(
					(p) =>
						`${p.pos} ${p.age}y ${p.ovr}/${p.pot} (${p.years + 1}y at ${fmt(100 * p.amount, 0)}%${p.rookie ? ", rookie" : ""})`,
				)
				.join(" + ");
		lines.push(
			`- f${(d as { factor?: number }).factor ?? ""} ${d.season} ${d.label}: shark gets ${show(d.gets)} for ${show(d.gives)}${d.givesPicks.length > 0 ? ` + ${d.givesPicks.map((dp) => `${dp.season} R${dp.round}`).join(", ")}` : ""}; CPU ${fmt(d.graded.cpu.mean)} ± ${fmt(d.graded.cpu.se)} in the sim, model ${fmt(d.modelCpu)}; shark ${fmt(d.graded.shark.mean)}; CPU model gain ${fmt(100 * d.gain)} vs margin ${fmt(100 * d.margin)} (passes up to factor ${fmt(d.factorLimit, 1)})`,
		);
	}
	await fs.writeFile(`${dir}/report.md`, lines.join("\n"));
});

const made = (rs: RunFile[]) =>
	rs.flatMap((r) => r.deals.filter((d) => !d.warning));
