import { test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { game, phase, season, team } from "../index.ts";
import { getLines } from "./getLines.ts";

// DOES THE FUTURES BOARD MATCH THE ENGINE? Not against its own ratings (the EV
// harness, futuresCalibration, can only ever prove the board consistent with
// itself) but against what actually happens: random leagues, each priced by
// the real getLines preseason, at each quarter of the season and as the
// playoffs start, then played to a champion by the real sim - injuries and
// all - and every board scored against the outcome. Never part of the normal
// run.
//
// 1. Record (one process per seed range; a process holds ~30 leagues before
//    it runs out of memory):
//
//      ENGINE_CAL_OUT=/tmp/cal.jsonl ENGINE_CAL_SEEDS=1,2,3 \
//        npx vitest run --project basketball src/worker/core/sportsbook/engineCalibration.test.ts
//
// 2. Score: ENGINE_CAL_SCORE=/tmp/cal.jsonl[,more.jsonl] ENGINE_CAL_REPORT=/tmp/report.json
//    - each market's log loss, and how the board's favourite was priced
//    against how often it won; the win totals' miss, and the slope of actual
//    wins on the line (1 when the line is on the right scale).
//
// What it found, on 133 leagues (see FUTURES_OVR_SLOPE in getLines.ts): the
// board then priced the preseason title favourite at 23% and it won 47%, and
// its win totals sat a third too far from .500.

const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);
const conditions = {} as any;

const snapshot = async (tag: string) => {
	const teams = await team.getFormattedAll({
		attrs: ["tid", "cid", "did"],
		seasonAttrs: ["won", "lost"],
		season: g.get("season"),
		showNoStats: true,
	});
	const lines = await getLines();
	return {
		tag,
		numGames: g.get("numGames"),
		teams: teams.map((t) => ({
			tid: t.tid,
			cid: t.cid,
			did: t.did,
			won: t.seasonAttrs.won,
			lost: t.seasonAttrs.lost,
		})),
		title: lines.championship.map((r) => [r.tid, r.americanOdds]),
		conf: lines.conferences.map((c) => [
			c.cid,
			c.teams.map((r) => [r.tid, r.americanOdds]),
		]),
		div: lines.divisions.map((d) => [
			d.did,
			d.teams.map((r) => [r.tid, r.americanOdds]),
		]),
		wins: lines.winTotals.map((r) => [r.tid, r.line, r.over, r.under]),
	};
};

test.skipIf(!env.ENGINE_CAL_OUT)(
	"engine calibration: record",
	{ timeout: 86_400_000 },
	async () => {
		const fs = await importNode("node:fs");
		await importNode("fake-indexeddb/auto");
		for (const seed of (env.ENGINE_CAL_SEEDS ?? "1").split(",").map(Number)) {
			const realRandom = Math.random;
			Math.random = makeSeededRandom(seed);
			(idb as any).league = undefined;
			await createArenaLeague(20_000 + seed);
			await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
			const snaps = [await snapshot("pre")];
			const total = await season.getDaysLeftSchedule();
			let left = total;
			for (const frac of [0.25, 0.5, 0.75]) {
				const target = Math.round(total * (1 - frac));
				await game.play(left - target, conditions);
				left = target;
				snaps.push(await snapshot(`r${frac}`));
			}
			while (g.get("phase") < PHASE.PLAYOFFS) {
				const days = await season.getDaysLeftSchedule();
				if (days > 0) {
					await game.play(days, conditions);
				} else {
					await phase.newPhase(PHASE.PLAYOFFS, conditions);
				}
			}
			snaps.push(await snapshot("po"));
			await game.play(100, conditions);
			const ts = await team.getFormattedAll({
				attrs: ["tid", "cid", "did"],
				seasonAttrs: ["won", "winp", "playoffRoundsWon"],
				season: g.get("season"),
			});
			fs.appendFileSync(
				env.ENGINE_CAL_OUT,
				JSON.stringify({
					seed,
					rounds: g.get("numGamesPlayoffSeries").length,
					snaps,
					outcome: ts.map((t) => ({
						tid: t.tid,
						cid: t.cid,
						did: t.did,
						won: t.seasonAttrs.won,
						winp: t.seasonAttrs.winp,
						prw: t.seasonAttrs.playoffRoundsWon,
					})),
				}) + "\n",
			);
			Math.random = realRandom;
		}
	},
);

const implied = (a: number) => (a > 0 ? 100 / (a + 100) : -a / (-a + 100));
const mean = (x: number[]) => x.reduce((a, b) => a + b, 0) / (x.length || 1);

test.skipIf(!env.ENGINE_CAL_SCORE)(
	"engine calibration: score",
	{ timeout: 600_000 },
	async () => {
		const fs = await importNode("node:fs");
		const seasons = env
			.ENGINE_CAL_SCORE!.split(",")
			.flatMap((f) => fs.readFileSync(f, "utf8").split("\n"))
			.filter(Boolean)
			.map((l: string) => JSON.parse(l));
		const report: Record<string, any> = { seasons: seasons.length };
		for (const tag of ["pre", "r0.25", "r0.5", "r0.75", "po"]) {
			const title: number[] = [];
			const fav: [number, number][] = [];
			const conf: number[] = [];
			const div: number[] = [];
			const wins: [number, number][] = [];
			for (const S of seasons) {
				const snap = S.snaps.find((s: any) => s.tag === tag);
				if (!snap) {
					continue;
				}
				const out = new Map<number, any>(S.outcome.map((o: any) => [o.tid, o]));
				const champ = S.outcome.find((o: any) => o.prw === S.rounds)?.tid;
				// The board de-vigged: each price's implied chance over the field's.
				const lp = (list: [number, number][], winner: number) => {
					const sum = list.reduce((s, [, a]) => s + implied(a), 0);
					const row = list.find(([tid]) => tid === winner);
					return -Math.log(Math.max(1e-4, row ? implied(row[1]) / sum : 0));
				};
				if (snap.title.length > 0) {
					title.push(lp(snap.title, champ));
					const sum = snap.title.reduce(
						(s: number, [, a]: [number, number]) => s + implied(a),
						0,
					);
					const [tid, a] = snap.title[0];
					fav.push([implied(a) / sum, tid === champ ? 1 : 0]);
				}
				for (const [cid, list] of snap.conf) {
					const w = S.outcome.find(
						(o: any) => o.cid === cid && o.prw >= S.rounds - 1,
					)?.tid;
					conf.push(lp(list, w));
				}
				for (const [did, list] of snap.div) {
					const inDiv = S.outcome.filter((o: any) => o.did === did);
					const w = inDiv.reduce((b: any, o: any) =>
						o.winp > b.winp ? o : b,
					).tid;
					div.push(lp(list, w));
				}
				for (const [tid, line] of snap.wins) {
					wins.push([line, out.get(tid).won]);
				}
			}
			const ml = mean(wins.map(([l]) => l));
			const ma = mean(wins.map(([, a]) => a));
			const cov = mean(wins.map(([l, a]) => (l - ml) * (a - ma)));
			const v = mean(wins.map(([l]) => (l - ml) ** 2));
			report[tag] = {
				titleLogLoss: mean(title),
				favouritePriced: mean(fav.map(([p]) => p)),
				favouriteWon: mean(fav.map(([, w]) => w)),
				confLogLoss: mean(conf),
				divLogLoss: mean(div),
				winTotalsMiss: Math.sqrt(mean(wins.map(([l, a]) => (a - l) ** 2))),
				winTotalsSlope: v > 0 ? cov / v : undefined,
			};
		}
		fs.writeFileSync(
			env.ENGINE_CAL_REPORT ?? "/dev/stdout",
			JSON.stringify(report, null, 2),
		);
	},
);
