import { test } from "vitest";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import type { DraftPick, Player } from "../../common/types.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// SMART_LEAGUE=<league.json>: the goal worked out (docs/smart-cpu.md) on a
// real league. Each team's use for every player and pick (what it does to the
// team's discounted title chances), the market price (the second-highest use
// among the teams that don't own it), each team's exchange rate (its best
// buys' use per unit of price), and what that says about some trades.
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.SMART_LEAGUE)(
	"prices on a real league",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { buildOutlook, ensureRetention, franchiseValue, teamStrengths } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		await loadLeagueFile(env.SMART_LEAGUE!);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const out: string[] = [];
		const say = (s: string) => out.push(s);
		const outlook = await buildOutlook({ iterations: 1000 });
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const season = g.get("season");
		const rosters = new Map<number, Player[]>();
		const picks = new Map<number, DraftPick[]>();
		for (const t of teams) {
			rosters.set(
				t.tid,
				await idb.cache.players.indexGetAll("playersByTid", t.tid),
			);
			picks.set(
				t.tid,
				await idb.cache.draftPicks.indexGetAll("draftPicksByTid", t.tid),
			);
		}
		const G = (
			tid: number,
			players: readonly Player[],
			dps: readonly DraftPick[],
		) => franchiseValue(outlook, teamStrengths(outlook, tid, players, dps), tid);
		const base = new Map(
			teams.map((t) => [
				t.tid,
				G(t.tid, rosters.get(t.tid)!, picks.get(t.tid)!),
			]),
		);

		type Asset = {
			key: string;
			owner: number;
			label: string;
			p?: Player;
			dp?: DraftPick;
		};
		const assets: Asset[] = [];
		const top = Number(env.SMART_TOP ?? 12);
		const named = new Set(
			(env.SMART_NAMED ?? "").split(",").filter((x) => x.length > 0),
		);
		for (const t of teams) {
			const players = rosters.get(t.tid)!;
			const best = [...players].sort((a, b) => b.value - a.value).slice(0, top);
			for (const p of players) {
				if (
					!best.includes(p) &&
					!named.has(`${p.firstName} ${p.lastName}`)
				) {
					continue;
				}
				const r = p.ratings.at(-1)!;
				assets.push({
					key: `p${p.pid}`,
					owner: t.tid,
					p,
					label: `${p.firstName} ${p.lastName} (${abbrev.get(t.tid)}, ${season - p.born.year}y ${r.ovr}/${r.pot}, $${((p.contract?.amount ?? 0) / 1000).toFixed(1)}M->${p.contract?.exp})`,
				});
			}
			if (env.SMART_PICKS !== "0") {
				for (const dp of picks.get(t.tid)!) {
					assets.push({
						key: `d${dp.dpid}`,
						owner: t.tid,
						dp,
						label: `${dp.season} R${dp.round} pick of ${abbrev.get(dp.originalTid)} (owned by ${abbrev.get(t.tid)})`,
					});
				}
			}
		}
		// Every team's chance of keeping each of these players past his
		// contract, as it would be on that team (SMART_RETENTION=0: the
		// outlook's unknown 50% for players on other teams, as before).
		const tr = performance.now();
		if (env.SMART_RETENTION !== "0") {
			const ps = assets.filter((a) => a.p).map((a) => a.p!);
			for (const t of teams) {
				await ensureRetention(outlook, ps, t.tid);
			}
		}
		say(`re-sign odds for every team: ${((performance.now() - tr) / 1000).toFixed(0)} s`);
		const t0 = performance.now();
		const use = new Map<string, Map<number, number>>();
		for (const a of assets) {
			const byTeam = new Map<number, number>();
			for (const t of teams) {
				const players = rosters.get(t.tid)!;
				const dps = picks.get(t.tid)!;
				let v: number;
				if (a.owner === t.tid) {
					v =
						base.get(t.tid)! -
						G(
							t.tid,
							a.p ? players.filter((x) => x.pid !== a.p!.pid) : players,
							a.dp ? dps.filter((x) => x.dpid !== a.dp!.dpid) : dps,
						);
				} else {
					v =
						G(
							t.tid,
							a.p ? [...players, a.p] : players,
							a.dp ? [...dps, a.dp] : dps,
						) - base.get(t.tid)!;
				}
				byTeam.set(t.tid, v);
			}
			use.set(a.key, byTeam);
		}
		say(
			`${assets.length} assets x ${teams.length} teams: ${((performance.now() - t0) / 1000).toFixed(0)} s`,
		);
		// Market price: the second-highest use among the teams that don't own it.
		const price = new Map<string, number>();
		for (const a of assets) {
			const bids = [...use.get(a.key)!]
				.filter(([tid]) => tid !== a.owner)
				.map(([, v]) => v)
				.sort((x, y) => y - x);
			price.set(a.key, bids[1] ?? bids[0] ?? 0);
		}
		// Exchange rate: a team's best buys, use per unit of price.
		const rate = new Map<number, number>();
		for (const t of teams) {
			const ratios = assets
				.filter((a) => a.owner !== t.tid && price.get(a.key)! > 0.002)
				.map((a) => use.get(a.key)!.get(t.tid)! / price.get(a.key)!)
				.sort((x, y) => y - x)
				.slice(0, 5);
			rate.set(
				t.tid,
				ratios.reduce((s, x) => s + x, 0) / Math.max(1, ratios.length),
			);
		}
		say(
			`exchange rate (own titles per title of market price, best 5 buys): ${teams
				.map((t) => `${t.abbrev} ${rate.get(t.tid)!.toFixed(2)}`)
				.join(", ")}`,
		);
		const worth = (tid: number, key: string) =>
			Math.max(use.get(key)!.get(tid)!, rate.get(tid)! * price.get(key)!);
		const m = (x: number) => (x * 1000).toFixed(1);
		const show = (a: Asset) => {
			const u = use.get(a.key)!;
			const best = [...u].sort((x, y) => y[1] - x[1]).slice(0, 3);
			return `${a.label}: own use ${m(u.get(a.owner)!)}, price ${m(price.get(a.key)!)}, worth to owner ${m(worth(a.owner, a.key))} | keenest: ${best.map(([tid, v]) => `${abbrev.get(tid)} ${m(v)}`).join(", ")}`;
		};
		say(
			"\n(milli-titles: thousandths of a title, discounted over ten seasons)",
		);
		say("\nMost valuable by market price:");
		for (const a of [...assets]
			.sort((x, y) => price.get(y.key)! - price.get(x.key)!)
			.slice(0, 15)) {
			say("  " + show(a));
		}
		const firsts = assets
			.filter((a) => a.dp?.round === 1)
			.sort((x, y) => price.get(y.key)! - price.get(x.key)!);
		if (firsts.length > 0) {
			say("\nFirst-round picks by market price (top and bottom):");
			for (const a of [...firsts.slice(0, 6), ...firsts.slice(-4)]) {
				say("  " + show(a));
			}
		}
		say("\nPlayers with the lowest price (contracts nobody wants):");
		for (const a of assets
			.filter((x) => x.p)
			.sort((x, y) => price.get(x.key)! - price.get(y.key)!)
			.slice(0, 8)) {
			say("  " + show(a));
		}
		say("\nNamed:");
		for (const a of assets.filter(
			(x) => x.p && named.has(`${x.p.firstName} ${x.p.lastName}`),
		)) {
			say("  " + show(a));
		}
		await fs.writeFile(
			`${env.SMART_OUT ?? "."}/prices-probe.txt`,
			out.join("\n"),
		);
		await fs.writeFile(
			`${env.SMART_OUT ?? "."}/prices-probe.json`,
			JSON.stringify({
				assets: assets.map((a) => ({
					key: a.key,
					owner: a.owner,
					label: a.label,
				})),
				use: Object.fromEntries(
					[...use].map(([k, mm]) => [k, Object.fromEntries(mm)]),
				),
				price: Object.fromEntries(price),
				rate: Object.fromEntries(rate),
			}),
		);
	},
);
