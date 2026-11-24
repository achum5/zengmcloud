import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";

// How far CPU teams reach in the draft with the smart CPU on vanilla's rules:
// each pick's place on the board (the prospects left, by value) - arena
// leagues, DR_SEEDS=<seed,...> DR_SEASONS=<n> DR_OUT=<file>.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.DR_OUT)("draft reach", { timeout: 7_200_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const { smartDraftWatchers } =
		await import("../../worker/core/smartCpu/draft.ts");
	const rows: unknown[] = [];
	let seed = 0;
	smartDraftWatchers.push((dp, worths) => {
		const choice = worths[0]!;
		const board = [...worths].sort(
			(a, b) => b.p.value - a.p.value || a.p.pid - b.p.pid,
		);
		const rank = board.indexOf(choice) + 1;
		const r = (p: any) => p.ratings.at(-1);
		rows.push({
			seed,
			season: g.get("season"),
			round: dp.round,
			pick: dp.pick,
			tid: dp.tid,
			rank,
			chosen: {
				age: g.get("season") - choice.p.born.year,
				ovr: r(choice.p).ovr,
				pot: r(choice.p).pot,
				value: choice.p.value,
			},
			top: {
				age: g.get("season") - board[0]!.p.born.year,
				ovr: r(board[0]!.p).ovr,
				pot: r(board[0]!.p).pot,
				value: board[0]!.p.value,
			},
		});
		fs.writeFileSync(
			env.DR_OUT!,
			rows.map((x) => JSON.stringify(x)).join("\n") + "\n",
		);
	});
	for (const s of (env.DR_SEEDS ?? "9177").split(",").map(Number)) {
		seed = s;
		Math.random = makeSeededRandom(s);
		(idb as any).league = undefined;
		await createArenaLeague(s);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			realisticFrontOffices: false,
			smartCpu: true,
		});
		await advanceSeasons(Number(env.DR_SEASONS ?? 2));
	}
});
