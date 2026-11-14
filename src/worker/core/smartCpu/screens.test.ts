import { afterAll, assert, beforeAll, test } from "vitest";
import { makeSeededRandom } from "../../../common/random.ts";
import type { Player, TradeTeams } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import type { LookingFor } from "../trade/makeItWork.ts";
import { askOf, smartTradingBlock } from "./screens.ts";

// The trading block with the smart CPU on: what the user says it is after
// shapes each CPU team's offer, as on the game's own trading block.

const importNode = (name: string) => import(/* @vite-ignore */ name);
const realRandom = Math.random;
let star: Player;

const nothing = (): LookingFor => ({
	positions: new Set(),
	skills: new Set(),
	draftPicks: false,
	prospects: false,
	bestCurrentPlayers: false,
});

const theirPlayers = async (offer: TradeTeams) =>
	(await Promise.all(offer[1].pids.map((pid) => idb.cache.players.get(pid))))
		.filter((p) => p !== undefined)
		.map((p) => p);

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(5);
	(idb as any).league = undefined;
	await createArenaLeague(9177);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("smartCpu", true);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", 0);
	g.setWithoutSavingToDB("userTids", [0]);
	star = (await idb.cache.players.indexGetAll("playersByTid", 0)).sort(
		(a, b) => b.value - a.value,
	)[0]!;
}, 900_000);

afterAll(() => {
	Math.random = realRandom;
});

test("nothing asked for: each team's own best offer, as before", () => {
	assert.isUndefined(askOf(undefined));
	assert.isUndefined(askOf(nothing()));
});

test("draft picks: every offer has one", { timeout: 900_000 }, async () => {
	const offers = await smartTradingBlock(0, [star.pid], [], {
		...nothing(),
		draftPicks: true,
	});
	assert.isAbove(offers.length, 0);
	for (const offer of offers) {
		assert.isAbove(offer[1].dpids.length, 0, `team ${offer[1].tid}`);
	}
});

test("a center: every offer has one", { timeout: 900_000 }, async () => {
	const offers = await smartTradingBlock(0, [star.pid], [], {
		...nothing(),
		positions: new Set(["C"]),
	});
	assert.isAbove(offers.length, 0);
	for (const offer of offers) {
		const players = await theirPlayers(offer);
		assert.isTrue(
			players.some((p) => p.ratings.at(-1)!.pos.includes("C")),
			`team ${offer[1].tid}`,
		);
	}
});

test(
	"prospects: every offer has a player 23 or younger with room to grow",
	{ timeout: 900_000 },
	async () => {
		const offers = await smartTradingBlock(0, [star.pid], [], {
			...nothing(),
			prospects: true,
		});
		assert.isAbove(offers.length, 0);
		for (const offer of offers) {
			assert.isTrue(
				(await theirPlayers(offer)).some((p) => {
					const r = p.ratings.at(-1)!;
					return g.get("season") - p.born.year <= 23 && r.pot - r.ovr >= 5;
				}),
				`team ${offer[1].tid}`,
			);
		}
	},
);

test(
	"salary at most: nobody paid more comes back",
	{ timeout: 900_000 },
	async () => {
		const most = 8000;
		const offers = await smartTradingBlock(0, [star.pid], [], {
			...nothing(),
			player: { salaryMost: most },
		});
		assert.isAbove(offers.length, 0);
		for (const offer of offers) {
			for (const p of await theirPlayers(offer)) {
				assert.isAtMost(contractOf(p).amount, most, `${p.pid}`);
			}
		}
	},
);
