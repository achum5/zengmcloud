import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { setTestContract } from "../../../test/helpers.ts";
import { carryOut, roomPlans, sheddingFor } from "./clearRoom.ts";
import { contractOf } from "./contractOf.ts";
import { currentOutlook, loadTeam, MIN_GAIN } from "./market.ts";
import { mostFor } from "./stages.ts";
import getPayroll from "../team/getPayroll.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(41);
	(idb as any).league = undefined;
	await createArenaLeague(9023);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

// Each player on the team paid his share of `share` of the cap, through at
// least `exp`.
const setPayroll = async (tid: number, share: number, exp: number) => {
	const cap = g.get("salaryCap");
	const roster = await idb.cache.players.indexGetAll("playersByTid", tid);
	const total = roster.reduce((a, p) => a + contractOf(p).amount, 0);
	for (const p of roster) {
		setTestContract(p, {
			amount: Math.max(
				g.get("minContract"),
				(contractOf(p).amount * share * cap) / total,
			),
			exp: Math.max(contractOf(p).exp, exp),
		});
		await idb.cache.players.put(p);
	}
};

test(
	"a team over the cap can trade salary to a team with room to sign a free agent",
	{ timeout: 600_000 },
	async () => {
		const season = g.get("season");
		const cap = g.get("salaryCap");
		const over = 5;
		const room = 12;
		await setPayroll(over, 1.05, season + 2);
		await setPayroll(room, 0.5, season + 1);
		const p = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		).sort((a, b) => b.value - a.value)[0]!;
		const amount = Math.round(0.2 * cap);

		const outlook = await currentOutlook();
		const team = (await idb.cache.teams.get(over))!;
		const t = await loadTeam(outlook, over);
		const x = await loadTeam(outlook, room);

		// The least it must shed: enough, and no more.
		const need = sheddingFor(team, t, p, amount)!;
		assert.isAbove(need, 0);
		const most = (shed: number) =>
			mostFor({
				t: team,
				p,
				payroll: t.payroll - shed,
				rosterSize: t.players.length,
			}) ?? 0;
		assert.isAtLeast(most(need), amount);
		assert.isBelow(most(need - 1000), amount);

		const plans = roomPlans(outlook, team, t, p, amount, [x]);
		assert.isAbove(plans.length, 0, "no trade clears the room");
		for (const plan of plans) {
			assert.isAtLeast(plan.freed, need);
			assert.strictEqual(plan.partner, room);
			assert.isAtLeast(plan.partnerGain - plan.partnerMargin, MIN_GAIN);
			assert.isAtLeast(
				mostFor({
					t: team,
					p,
					payroll: plan.state.payroll,
					rosterSize: plan.state.players.length,
				}) ?? 0,
				amount,
			);
		}

		// Carried out: the players go, and the room is there.
		const plan = plans[0]!;
		const before = await getPayroll(over);
		assert.isTrue(await carryOut(outlook, plan, p, amount, "him"));
		assert.closeTo(await getPayroll(over), before - plan.freed, 1);
		for (const a of plan.give) {
			if (a.kind === "player") {
				assert.strictEqual((await idb.cache.players.get(a.p.pid))!.tid, room);
			}
		}
		const events = await idb.cache.events.getAll();
		const trade = events.findLast((e) => e.type === "trade") as any;
		assert.include(
			trade.aiTrade?.frontOffice?.sides[0]?.reason ?? "",
			"Clears cap room to sign him",
		);
	},
);
