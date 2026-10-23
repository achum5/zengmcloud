import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import {
	currentOutlook,
	loadTeam,
	marginOf,
	priceFor,
	type Asset,
	type TeamState,
} from "./market.ts";
import type { Player } from "../../../common/types.ts";
import { explainDeal, type ExplainSide } from "./explain.ts";
import { priceSigning } from "./freeAgency.ts";
import {
	buildOutlook,
	outlookSettings,
	teamStrengths,
	type LeagueOutlook,
} from "./outlook.ts";

// Moves no good front office makes, checked for every team of a league: a
// deal that clears a team's margin of error by the minimum gain is one it
// would make.

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(37);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9019);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

const wouldMake = async (
	t: TeamState,
	give: Asset[],
	get: Asset[],
	partner: TeamState,
) => {
	const outlook = await currentOutlook();
	return (
		priceFor(outlook, t, give, get, partner) - marginOf(outlook, t, give, get) >
		0.002
	);
};

const teams = async () => {
	const outlook = await currentOutlook();
	const out: { t: TeamState; partner: TeamState; strength: number }[] = [];
	for (let tid = 0; tid < g.get("numTeams"); tid++) {
		out.push({
			t: await loadTeam(outlook, tid),
			partner: await loadTeam(outlook, (tid + 7) % g.get("numTeams")),
			strength: outlook.teams.get(tid)!.strength[0]!,
		});
	}
	return out;
};

test(
	"no team gives a young, cheap, good player away for nothing",
	{ timeout: 300_000 },
	async () => {
		const season = g.get("season");
		let checked = 0;
		for (const { t, partner } of await teams()) {
			for (const p of t.players) {
				const r = p.ratings.at(-1)!;
				if (
					season - p.born.year <= 23 &&
					r.ovr >= 52 &&
					r.pot >= 60 &&
					p.contract.amount <= 6000
				) {
					checked += 1;
					assert.isFalse(
						await wouldMake(t, [{ kind: "player", p }], [], partner),
						`team ${t.tid} would give away ${p.pid}`,
					);
				}
			}
		}
		assert.isAbove(checked, 10);
	},
);

test(
	"no contender trades its best player for two firsts",
	{ timeout: 300_000 },
	async () => {
		let checked = 0;
		for (const { t, partner, strength } of await teams()) {
			if (strength <= 3) {
				continue;
			}
			const best = [...t.players].sort(
				(a, b) => b.valueNoPot - a.valueNoPot,
			)[0]!;
			const firsts = partner.picks
				.filter((dp) => dp.round === 1)
				.slice(0, 2)
				.map((dp) => ({ kind: "pick" as const, dp }));
			checked += 1;
			assert.isFalse(
				await wouldMake(t, [{ kind: "player", p: best }], firsts, partner),
				`team ${t.tid} would sell its best player`,
			);
		}
		assert.isAbove(checked, 2);
	},
);

test(
	"no team gives its best player away for nothing when he is out for the season but back next",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
		for (const { t, partner } of await teams()) {
			const best = [...t.players].sort(
				(a, b) => b.valueNoPot - a.valueNoPot,
			)[0]!;
			const hurt = {
				...helpers.deepCopy(best),
				injury: { type: "Torn ACL", gamesRemaining: 60 },
			};
			const players = t.players.map((p) => (p === best ? hurt : p));
			const now = { ...t, players, tradablePlayers: players };
			// The team as it stands with him hurt.
			const before = priceFor(outlook, now, [], []);
			const give: Asset[] = [{ kind: "player", p: hurt }];
			const gain = priceFor(outlook, now, give, [], partner) - before;
			assert.isAtMost(
				gain - marginOf(outlook, now, give, []),
				0.002,
				`team ${t.tid} would give away its injured best player`,
			);
		}
	},
);

test(
	"the worst teams are never worse off for more first-round picks for being able to let the players go after their rookie deals",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
		const all = await teams();
		const worst = [...all].sort((a, b) => a.strength - b.strength).slice(0, 5);
		for (const { t, partner } of worst) {
			const firsts = partner.picks
				.filter((dp) => dp.round === 1 && dp.originalTid === partner.tid)
				.slice(0, 4)
				.map((dp) => ({ kind: "pick" as const, dp }));
			// Decided afresh on the roster the deal leaves, not as on the
			// roster as it stands (as priceFor does, to be quick).
			const value = (letGo: boolean) => {
				outlookSettings.letGo = letGo;
				try {
					return priceFor(
						outlook,
						{ ...t, letGo: undefined },
						[],
						firsts,
						partner,
					);
				} finally {
					outlookSettings.letGo = true;
				}
			};
			const withLetGo = value(true);
			assert.isAtLeast(withLetGo, value(false) - 1e-9, `team ${t.tid}`);
			assert.isAbove(withLetGo, 0, `team ${t.tid}`);
		}
	},
);

test(
	"no team is better off giving one of its picks away for nothing",
	{ timeout: 300_000 },
	async () => {
		const outlook = await currentOutlook();
		let checked = 0;
		for (const { t } of await teams()) {
			for (const dp of t.picks) {
				checked += 1;
				// Within the margin's smallest gain: a pick's player who is not
				// worth his place is cut straight after the draft, for nothing.
				assert.isAtMost(
					priceFor(outlook, t, [{ kind: "pick", dp }], []),
					0.001,
					`team ${t.tid}, ${dp.season} round ${dp.round}`,
				);
			}
		}
		assert.isAbove(checked, 100);
	},
);

test(
	"what a deal is worth does not hang on what was priced before it",
	{ timeout: 300_000 },
	async () => {
		// The same deals on two outlooks of the same league, priced in opposite
		// orders: picks for players, so where the picks land moves with each.
		const dealsOn = async (outlook: LeagueOutlook) => {
			const deals: {
				t: TeamState;
				partner: TeamState;
				give: Asset[];
				get: Asset[];
			}[] = [];
			for (let tid = 0; tid < g.get("numTeams"); tid += 3) {
				const t = await loadTeam(outlook, tid);
				const partner = await loadTeam(outlook, (tid + 7) % g.get("numTeams"));
				for (const dp of t.picks.filter((x) => x.round === 1).slice(0, 2)) {
					deals.push({
						t,
						partner,
						give: [{ kind: "pick", dp }],
						get: [{ kind: "player", p: partner.players[3]! }],
					});
				}
			}
			return { outlook, deals };
		};
		const price = ({ outlook, deals }: Awaited<ReturnType<typeof dealsOn>>) =>
			deals.map((d) => priceFor(outlook, d.t, d.give, d.get, d.partner));
		const first = await dealsOn(await buildOutlook());
		const second = await dealsOn(await buildOutlook());
		second.deals.reverse();
		const forward = price(first);
		const backward = price(second).reverse();
		assert.isAbove(forward.length, 10);
		assert.deepEqual(backward, forward);
	},
);

test("a deal's breakdown shows where it moves each team's picks", async () => {
	const outlook = await currentOutlook();
	const all = await teams();
	const { t, partner } = [...all].sort((a, b) => a.strength - b.strength)[
		Math.floor(all.length / 2)
	]!;
	const star = [...t.players].sort((a, b) => b.valueNoPot - a.valueNoPot)[0]!;
	const side = (
		s: TeamState,
		gives: Player[],
		gets: Player[],
	): ExplainSide => ({
		tid: s.tid,
		players: s.players,
		picks: s.picks,
		tradedAway: s.tradedAway,
		gain: 0,
		margin: 0,
		gives: { players: gives, picks: [] },
		gets: { players: gets, picks: [] },
	});
	const { sides } = explainDeal(outlook, [
		side(t, [star], []),
		side(partner, [], [star]),
	]);
	// Without its best player, its own first comes earlier; with him, the
	// other team's comes later.
	const own = (k: 0 | 1, tid: number) =>
		sides[k].picksMoved!.find((x) => x.originalTid === tid && x.round === 1)!;
	assert.isBelow(own(0, t.tid).after, own(0, t.tid).before - 1);
	assert.isAbove(own(1, partner.tid).after, own(1, partner.tid).before);
});

test(
	"no team signs a free agent for making it worse",
	{ timeout: 300_000 },
	async () => {
		// In free agency, when a poor player the rotation would play ahead of
		// better ones could make a team worse, and so its own pick better.
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		const outlook = await currentOutlook();
		const pool = (
			await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
		)
			.sort((a, b) => b.value - a.value)
			.slice(0, 5);
		let checked = 0;
		for (let tid = 0; tid < g.get("numTeams"); tid++) {
			const t = await loadTeam(outlook, tid);
			for (const p of pool) {
				const contract = {
					amount: g.get("minContract"),
					exp: g.get("season") + 2,
				};
				const signed = { ...p, tid, contract: { ...p.contract, ...contract } };
				const path = teamStrengths(
					outlook,
					tid,
					[...t.players, signed],
					t.picks,
				);
				if (path.every((x, y) => x <= t.path[y]!)) {
					checked += 1;
					assert.isAtMost(
						priceSigning(outlook, t, p, contract).gain,
						0,
						`team ${tid}, free agent ${p.pid}`,
					);
				}
			}
		}
		assert.isAbove(checked, 0);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	},
);

test(
	"a team with a full roster gains nothing from a free agent worse than anyone on it",
	{ timeout: 300_000 },
	async () => {
		// In free agency, when rosters may run over the limit until the season.
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		const tid = 3;
		// Others' bench players, by value: every other one fills its roster to the
		// limit, and the rest are free agents about as good as its last men.
		const bench = (
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
		)
			.filter((p) => p.tid !== tid && p.value < 50 && p.value > 44)
			.sort((a, b) => b.value - a.value);
		let roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		for (const p of roster.filter((x) => x.value <= 44)) {
			p.tid = PLAYER.FREE_AGENT;
			await idb.cache.players.put(p);
		}
		roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		const fill = bench
			.filter((_, i) => i % 2 === 0)
			.slice(0, g.get("maxRosterSize") - roster.length);
		for (const p of bench) {
			p.tid = fill.includes(p) ? tid : PLAYER.FREE_AGENT;
			// The roster's contracts all run out after next season.
			p.contract.exp = Math.min(p.contract.exp, g.get("season") + 1);
			await idb.cache.players.put(p);
		}
		for (const p of roster) {
			p.contract.exp = Math.min(p.contract.exp, g.get("season") + 1);
			await idb.cache.players.put(p);
		}
		roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		assert.equal(roster.length, g.get("maxRosterSize"));
		const outlook = await currentOutlook();
		const t = await loadTeam(outlook, tid);
		const worst = Math.min(...t.players.map((p) => p.value));
		let checked = 0;
		for (const p of bench.filter((x) => !fill.includes(x) && x.value < worst)) {
			if (checked === 5) {
				break;
			}
			checked += 1;
			const { gain } = priceSigning(outlook, t, p, {
				amount: g.get("minContract"),
				exp: g.get("season") + 3,
			});
			// He would be the one cut, his salary still owed.
			assert.isBelow(gain, 0, `free agent ${p.pid}, value ${p.value}`);
		}
		assert.isAbove(checked, 0);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	},
);

test(
	"an offseason trade taking a team past the roster limit pays for the player it must cut",
	{ timeout: 300_000 },
	async () => {
		g.setWithoutSavingToDB("phase", PHASE.DRAFT);
		const tid = 5;
		// Filled to the limit with others' bench players; the rest of them are
		// the players it could be handed for nothing.
		const bench = (
			await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
		)
			.filter((p) => p.tid !== tid && p.value < 56 && p.value > 44)
			.sort((a, b) => b.value - a.value);
		let roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		for (const p of roster.filter((x) => x.value <= 44)) {
			p.tid = PLAYER.FREE_AGENT;
			await idb.cache.players.put(p);
		}
		roster = await idb.cache.players.indexGetAll("playersByTid", tid);
		const fill = bench
			.filter((_, i) => i % 2 === 0)
			.slice(0, g.get("maxRosterSize") - roster.length);
		for (const p of fill) {
			p.tid = tid;
			await idb.cache.players.put(p);
		}
		// Everyone under contract next season, when the roster has to fit.
		for (const p of [...roster, ...bench]) {
			p.contract.exp = Math.max(p.contract.exp, g.get("season") + 1);
			await idb.cache.players.put(p);
		}
		const outlook = await currentOutlook();
		const t = await loadTeam(outlook, tid);
		assert.equal(t.players.length, g.get("maxRosterSize"));
		const worst = Math.min(...t.players.map((p) => p.value));
		let checked = 0;
		for (const p of bench.filter((x) => !fill.includes(x) && x.value < worst)) {
			if (checked === 5) {
				break;
			}
			const partner = await loadTeam(outlook, p.tid);
			checked += 1;
			assert.isBelow(
				priceFor(outlook, t, [], [{ kind: "player", p }], partner),
				0,
				`player ${p.pid}, value ${p.value}`,
			);
		}
		assert.isAbove(checked, 0);
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	},
);
