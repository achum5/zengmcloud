import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { player } from "../index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import {
	frontOfficeCuts,
	frontOfficeFill,
	worthSigningAtLimit,
} from "./cuts.ts";
import { currentOutlook, loadTeam } from "./market.ts";
import {
	franchiseValue,
	outlookSettings,
	retentionKey,
	teamStrengths,
} from "./outlook.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;
const tid = 4;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(8);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9012);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

test(
	"a team over the limit cuts the players it can best do without",
	{ timeout: 120_000 },
	async () => {
		const freeAgents = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		).sort((a, b) => a.value - b.value);
		// The worst free agents join, two over the limit.
		const size = (await idb.cache.players.indexGetAll("playersByTid", tid))
			.length;
		for (const p of freeAgents.slice(0, g.get("maxRosterSize") - size + 2)) {
			await player.sign(p, tid, p.contract, g.get("phase"));
			await idb.cache.players.put(p);
		}
		const players = await idb.cache.players.indexGetAll("playersByTid", tid);
		const best = [...players]
			.sort((a, b) => b.valueNoPot - a.valueNoPot)
			.slice(0, 5)
			.map((p) => p.pid);
		const over = players.length - g.get("maxRosterSize");
		assert.isAbove(over, 0);

		const cut = await frontOfficeCuts(players, over);
		assert.strictEqual(cut.length, over);
		const left = await idb.cache.players.indexGetAll("playersByTid", tid);
		assert.strictEqual(left.length, g.get("maxRosterSize"));
		for (const pid of best) {
			assert.notInclude(cut, pid);
		}
	},
);

test(
	"a team short of players and of guards signs a guard",
	{ timeout: 120_000 },
	async () => {
		const other = 6;
		const players = await idb.cache.players.indexGetAll("playersByTid", other);
		// Its guards leave.
		const guards = players.filter((p) => p.ratings.at(-1)!.pos.includes("G"));
		for (const p of guards) {
			p.tid = PLAYER.FREE_AGENT;
			await idb.cache.players.put(p);
		}
		const candidates = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		)
			.filter((p) => !guards.includes(p))
			.sort((a, b) => b.value - a.value);
		assert.isTrue(
			candidates.slice(0, 15).some((p) => p.ratings.at(-1)!.pos.includes("G")),
		);
		const p = await frontOfficeFill(other, candidates);
		assert.include(p!.ratings.at(-1)!.pos, "G");
	},
);

test(
	"a team at the limit does not sign a player it would release",
	{ timeout: 120_000 },
	async () => {
		// The first test left the team at the limit.
		const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		assert.strictEqual(roster.length, g.get("maxRosterSize"));
		const worst = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		).sort((a, b) => a.value - b.value)[0]!;
		const signed = {
			...worst,
			tid,
			contract: { ...worst.contract, amount: g.get("minContract") },
		};
		assert.isFalse(await worthSigningAtLimit(tid, signed));
	},
);

test(
	"a player worse than a minimum free agent costs his team nothing once his contract is up",
	{ timeout: 120_000 },
	async () => {
		const other = 9;
		const season = g.get("season");
		// A veteran scrub on a deal that runs out this season.
		const scrub = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		)
			.filter((p) => season - p.born.year >= 28)
			.sort((a, b) => a.valueNoPot - b.valueNoPot)[0]!;
		const outlook = await currentOutlook();
		const t = await loadTeam(outlook, other);
		const players = [
			...t.players,
			{
				...scrub,
				tid: other,
				contract: {
					...scrub.contract,
					amount: g.get("minContract"),
					exp: season,
				},
			},
		];
		const valueAt = (stay: number) => {
			outlook.retention.set(retentionKey(scrub.pid, other), stay);
			return franchiseValue(
				outlook,
				teamStrengths(outlook, other, players, t.picks),
				other,
			);
		};
		const was = outlookSettings.wantedOnly;
		try {
			// Re-signed or not, he is let go: whatever his odds of staying,
			// the team is worth the same.
			outlookSettings.wantedOnly = true;
			assert.closeTo(valueAt(0.9), valueAt(0), 1e-9);
			// Not so when a team keeps everyone at his odds.
			outlookSettings.wantedOnly = false;
			assert.isBelow(valueAt(0.9), valueAt(0));
		} finally {
			outlookSettings.wantedOnly = was;
		}
	},
);
