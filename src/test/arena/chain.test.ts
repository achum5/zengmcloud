import { afterAll, assert, beforeAll, test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { league } from "../../worker/core/index.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import {
	createArenaLeague,
	exportArenaLeague,
	importArenaLeague,
} from "./runLeague.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(5);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
});

afterAll(() => {
	Math.random = realRandom;
});

test(
	"an exported arena league carries on in another as it was",
	{ timeout: 120_000 },
	async () => {
		const fs = await importNode("node:fs/promises");
		const os = await importNode("node:os");
		const dir = await fs.mkdtemp(`${os.tmpdir()}/arena-chain-`);
		const file = `${dir}/league.jsonl`;
		try {
			await createArenaLeague(9101);
			const players = await idb.cache.players.getAll();
			const p = players[0]!;
			p.contract.amount = 4321;
			await idb.cache.players.put(p);
			const season = g.get("season");
			// Read from the league carried on, not the one replaced.
			await league.setGameAttributes({ salaryCap: 123_456 });
			await exportArenaLeague(file);

			(idb as any).league = undefined;
			await createArenaLeague(9102);
			await importArenaLeague(file);
			assert.strictEqual(g.get("season"), season);
			assert.strictEqual(g.get("salaryCap"), 123_456);
			const after = await idb.cache.players.getAll();
			assert.strictEqual(after.length, players.length);
			assert.strictEqual(
				(await idb.cache.players.get(p.pid))!.contract.amount,
				4321,
			);
		} finally {
			await fs.rm(dir, { recursive: true, force: true });
		}
	},
);
