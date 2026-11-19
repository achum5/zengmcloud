import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import type { Player } from "../../common/types.ts";
import { g, helpers } from "../../worker/util/index.ts";
import { inGameOrder, play, toSimTeam } from "../impact/simTools.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE TRADE AUDIT: every trade the smart CPU makes over a stretch of a real
// league, played out in the game's own sim. As each deal is made the league's
// rosters are kept; afterwards each side plays every other team as the
// league stood then, with and without the deal, on the same random numbers -
// what the deal did on the floor this season, against what the roster model
// said it would (its points this season, before and after).
//   TA_LEAGUE=<league.json> TA_OUT=<file> [TA_DAYS=100] [TA_GAMES=30] \
//     npx vitest --run --project basketball src/test/smart/tradeAuditProbe.test.ts
// Rows to TA_OUT.json; a summary to TA_OUT.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.TA_LEAGUE)(
	"trade audit probe",
	{ timeout: 14_400_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.TA_OUT!, lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.TA_LEAGUE!);
		const { league, game, phase } = await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const { smartDealWatchers } =
			await import("../../worker/core/smartCpu/market.ts");
		const { playerLine, rosterPoints } =
			await import("../../worker/core/realisticFrontOffice/strength.ts");
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const healthy = (p: Player): Player => ({
			...helpers.deepCopy(p),
			injury: { type: "Healthy", gamesRemaining: 0 },
		});
		const lineOf = (p: Player) => ({
			...playerLine(p.ratings.at(-1) as any, p.ratings.at(-1)!.pos),
			valueNoPot: p.valueNoPot,
		});
		type Snapshot = {
			label: string;
			seller: number;
			buyer: number;
			out: number[];
			offer: number[];
			picks: string;
			title: Record<number, number>;
			rosters: Map<number, Player[]>;
		};
		const snaps: Snapshot[] = [];
		smartDealWatchers.push((deal: any, view: any) => {
			const label = `${g.get("season")} ph${g.get("phase")}`;
			const title = {
				[deal.seller]: view.outlook.teams.get(deal.seller)?.title?.[0] ?? 0,
				[deal.buyer]: view.outlook.teams.get(deal.buyer)?.title?.[0] ?? 0,
			};
			const pickName = (dp: any) =>
				`${dp.season} R${dp.round} ${abbrev.get(dp.originalTid)}`;
			const picks = `${abbrev.get(deal.seller)} gets [${deal.offer.picks.map(pickName).join(", ")}], ${abbrev.get(deal.buyer)} gets [${deal.out.picks.map(pickName).join(", ")}]`;
			const out = deal.out.players.map((p: Player) => p.pid);
			const offer = deal.offer.players.map((p: Player) => p.pid);
			// The league as the market sees it, before the deal is made.
			const rosters = new Map<number, Player[]>();
			for (const t of teams) {
				rosters.set(
					t.tid,
					(view.heldNow(t.tid).players as Player[]).map(healthy),
				);
			}
			snaps.push({
				label,
				seller: deal.seller,
				buyer: deal.buyer,
				out,
				offer,
				picks,
				title,
				rosters,
			});
		});
		if (g.get("phase") < PHASE.REGULAR_SEASON) {
			await phase.newPhase(PHASE.REGULAR_SEASON, {});
		}
		const days = Number(env.TA_DAYS ?? 100);
		await game.play(days, {});
		log(`${snaps.length} trades in ${days} days`);
		const games = Number(env.TA_GAMES ?? 30);
		const realRandom = Math.random;
		const rows: any[] = [];
		try {
			for (const s of snaps) {
				const named = (pid: number) => {
					for (const ps of s.rosters.values()) {
						const p = ps.find((x) => x.pid === pid);
						if (p) {
							return `${p.firstName} ${p.lastName} ${g.get("season") - p.born.year}y ${p.ratings.at(-1)!.ovr}`;
						}
					}
					return `${pid}`;
				};
				const sides: any[] = [];
				for (const [tid, gives, gets] of [
					[s.seller, s.out, s.offer],
					[s.buyer, s.offer, s.out],
				] as const) {
					const from = tid === s.seller ? s.buyer : s.seller;
					const before = s.rosters.get(tid)!;
					// Read before the deal: only if what it gives is still there.
					if (!gives.every((pid) => before.some((p) => p.pid === pid))) {
						continue;
					}
					const incoming = s.rosters
						.get(from)!
						.filter((p) => gets.includes(p.pid));
					const after = [
						...before.filter((p) => !gives.includes(p.pid)),
						...incoming,
					];
					const model =
						rosterPoints(after.map(lineOf)) - rosterPoints(before.map(lineOf));
					// The league as it was, and with the deal: the partner too.
					const opponentsOf = async (made: boolean) => {
						const opponents = new Map<number, any>();
						for (const [oppTid, ps] of s.rosters) {
							if (oppTid === tid) {
								continue;
							}
							const opp =
								oppTid === from && made
									? [
											...ps.filter((p) => !gets.includes(p.pid)),
											...before.filter((p) => gives.includes(p.pid)),
										]
									: ps;
							opponents.set(oppTid, await toSimTeam(oppTid, inGameOrder(opp)));
						}
						return opponents;
					};
					const margins = async (roster: Player[], made: boolean) => {
						const opponents = await opponentsOf(made);
						const team = await toSimTeam(tid, inGameOrder(roster));
						const out: number[] = [];
						for (const [oppTid, opp] of opponents) {
							for (let i = 0; i < games; i++) {
								Math.random = makeSeededRandom(oppTid * 1_000_003 + i);
								const home = i % 2 === 0;
								const result = home
									? play(helpers.deepCopy(team), helpers.deepCopy(opp))
									: play(helpers.deepCopy(opp), helpers.deepCopy(team));
								const k = home ? 0 : 1;
								out.push(result.team[k].stat.pts - result.team[1 - k].stat.pts);
							}
						}
						return out;
					};
					const a = await margins(before, false);
					const b = await margins(after, true);
					const diffs = b.map((x, i) => x - a[i]!);
					const mean = diffs.reduce((t, x) => t + x, 0) / diffs.length;
					const sd = Math.sqrt(
						diffs.reduce((t, x) => t + (x - mean) ** 2, 0) / (diffs.length - 1),
					);
					sides.push({
						tid,
						team: abbrev.get(tid),
						title: s.title[tid],
						gives: gives.map(named),
						gets: gets.map(named),
						model,
						sim: mean,
						se: sd / Math.sqrt(diffs.length),
					});
				}
				rows.push({ label: s.label, picks: s.picks, sides });
				for (const x of sides) {
					log(
						`${s.label} ${x.team} (${(100 * x.title).toFixed(1)}%) gives [${x.gives.join("; ")}] gets [${x.gets.join("; ")}]: model ${x.model >= 0 ? "+" : ""}${x.model.toFixed(2)}, sim ${x.sim >= 0 ? "+" : ""}${x.sim.toFixed(2)} +- ${x.se.toFixed(2)}`,
					);
				}
				log(`  picks: ${s.picks}`);
				fs.writeFileSync(`${env.TA_OUT}.json`, JSON.stringify(rows));
			}
		} finally {
			Math.random = realRandom;
		}
	},
);
