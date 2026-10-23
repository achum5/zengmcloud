import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
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
import { outlookSettings } from "./outlook.ts";

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
