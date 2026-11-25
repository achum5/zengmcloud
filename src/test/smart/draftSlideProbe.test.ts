import { test } from "vitest";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { makeSeededRandom } from "../../common/random.ts";
import type { Conditions, Player } from "../../common/types.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import {
	createArenaLeague,
	exportArenaLeague,
	importArenaLeague,
} from "../arena/runLeague.ts";

// Why a prospect slides in the smart CPU's draft: each first-round pick's
// worth to the team of the board's best prospects, in parts - arena leagues
// on vanilla's rules. DS_SEED=<seed> DS_OUT=<file>; DS_SAVE=<file> keeps the
// league at the draft, DS_LEAGUE=<file> replays the draft from one.
// DS_CAREERS=<n> (outlookSettings.youngCareers) and DS_BOARD=0|1
// (smartDraftSettings.scoutedBoard) for the draft as it was: 24 and 0.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);
const conditions = {} as Conditions;

test.skipIf(!env.DS_OUT)("draft slide", { timeout: 7_200_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const { smartDraftWatchers, smartDraftSettings } =
		await import("../../worker/core/smartCpu/draft.ts");
	const { outlookSettings } =
		await import("../../worker/core/realisticFrontOffice/outlook.ts");
	if (env.DS_CAREERS) {
		outlookSettings.youngCareers = Number(env.DS_CAREERS);
	}
	if (env.DS_BOARD) {
		smartDraftSettings.scoutedBoard = env.DS_BOARD === "1";
	}
	const { draft, game, phase, season, league } =
		await import("../../worker/core/index.ts");
	const seed = Number(env.DS_SEED ?? 9177);
	Math.random = makeSeededRandom(seed);
	(idb as any).league = undefined;
	await createArenaLeague(seed);
	if (env.DS_LEAGUE) {
		await importArenaLeague(env.DS_LEAGUE);
		await league.loadGameAttributes();
		Math.random = makeSeededRandom(seed + 1);
	} else {
		await league.setGameAttributes({
			realisticFrontOffices: false,
			smartCpu: true,
		});
		while (g.get("phase") !== PHASE.DRAFT) {
			const ph = g.get("phase");
			if (ph === PHASE.PRESEASON) {
				await phase.newPhase(PHASE.REGULAR_SEASON, conditions);
			} else if (
				ph === PHASE.REGULAR_SEASON ||
				ph === PHASE.AFTER_TRADE_DEADLINE
			) {
				await game.play(await season.getDaysLeftSchedule(), conditions);
			} else if (ph === PHASE.PLAYOFFS) {
				await game.play(100, conditions);
			} else if (ph === PHASE.DRAFT_LOTTERY) {
				await phase.newPhase(PHASE.DRAFT, conditions);
			} else {
				throw new Error(`phase ${ph}`);
			}
		}
		if (env.DS_SAVE) {
			await exportArenaLeague(env.DS_SAVE);
		}
		Math.random = makeSeededRandom(seed + 1);
	}

	const rows: unknown[] = [];
	const none = { players: [] as Player[], picks: [] };
	const age = (p: Player) => g.get("season") - p.born.year;
	const r = (p: Player) => p.ratings.at(-1)!;
	smartDraftWatchers.push((dp, worths, view) => {
		const board = [...worths].sort(
			(a, b) => b.p.value - a.p.value || a.p.pid - b.p.pid,
		);
		const choice = worths[0]!;
		const held = view.heldNow(dp.tid);
		const base = {
			players: held.players,
			picks: held.picks.filter((x) => x.dpid !== dp.dpid),
		};
		const title = view.titleChancesNow(dp.tid);
		const look = (x: (typeof worths)[number], bids: boolean) => {
			const one = { players: [x.drafted], picks: [] };
			const parts = view.dealPartsNow(
				dp.tid,
				PLAYER.UNDRAFTED,
				none,
				one,
				base,
			);
			const row: Record<string, unknown> = {
				pid: x.p.pid,
				rank: board.indexOf(x) + 1,
				age: age(x.p),
				ovr: r(x.p).ovr,
				pot: r(x.p).pot,
				value: Math.round(x.p.value * 10) / 10,
				worth: x.worth,
				goal: parts.goal,
				got: parts.got,
				kept: view.keptChanceNow(dp.tid, x.drafted),
				use: view.useOfNow(dp.tid, one, base),
				price: view.piecePriceNow(one, [dp.tid, PLAYER.UNDRAFTED]),
			};
			if (bids) {
				const all: [number, number][] = [];
				for (let tid = 0; tid < g.get("numActiveTeams"); tid++) {
					if (tid !== dp.tid) {
						all.push([tid, view.bidNow(tid, one)]);
					}
				}
				all.sort((a, b) => b[1] - a[1]);
				row.bidders = all.slice(0, 3).map(([tid, bid]) => ({
					tid,
					bid,
					title0: view.titleChancesNow(tid)[0],
				}));
			}
			return row;
		};
		const first = dp.round === 1;
		rows.push({
			seed,
			season: g.get("season"),
			round: dp.round,
			pick: dp.pick,
			tid: dp.tid,
			title: title.slice(0, 5).map((x) => Math.round(x * 1e4) / 1e4),
			chosen: look(choice, first),
			margin: view.marginNow(dp.tid, none, {
				players: [worths.reduce((a, b) => (b.worth > a.worth ? b : a)).drafted],
				picks: [],
			}),
			board: board.map((x) => look(x, first && x === board[0])),
		});
		fs.writeFileSync(
			env.DS_OUT!,
			rows.map((x) => JSON.stringify(x)).join("\n") + "\n",
		);
	});
	await draft.runPicks({ type: "untilEnd" }, conditions);
});
