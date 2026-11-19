import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// A trade already made, undone and judged again: the players and picks put
// back where they were, then each side's worth of the trade split into its
// goal and the price premiums, each piece's use and price to each side,
// the buyer's strength and title chances season by season with and without
// it, and the market's best real offer for what the seller gave.
//   TR_LEAGUE=<league.json> TR_OUT=<file> TR_SELLER=<tid> TR_BUYER=<tid> \
//     TR_SOLD=<pids> TR_PAID=<pids> TR_PICKS=<dpids> npx vitest --run \
//     --project basketball src/test/smart/tradeReplayProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);
const ids = (x: string | undefined) =>
	(x ?? "")
		.split(",")
		.filter((s) => s !== "")
		.map(Number);

test.skipIf(!env.TR_LEAGUE)(
	"trade replay probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.TR_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.TR_LEAGUE!);
		const { league } = await import("../../worker/core/index.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { idb } = await import("../../worker/db/index.ts");
		const seller = Number(env.TR_SELLER);
		const buyer = Number(env.TR_BUYER);
		// Undo it.
		for (const pid of ids(env.TR_SOLD)) {
			const p = await idb.cache.players.get(pid);
			await idb.cache.players.put({ ...p!, tid: seller });
		}
		for (const pid of ids(env.TR_PAID)) {
			const p = await idb.cache.players.get(pid);
			await idb.cache.players.put({ ...p!, tid: buyer });
		}
		for (const dpid of ids(env.TR_PICKS)) {
			const dp = await idb.cache.draftPicks.get(dpid);
			await idb.cache.draftPicks.put({ ...dp!, tid: buyer });
		}
		const { buildOutlook, teamStrengths, curveAt, franchiseValue } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { SmartView } = await import("../../worker/core/smartCpu/worth.ts");
		const outlook: any = await buildOutlook();
		const view: any = new SmartView(outlook, []);
		await view.prime();
		const teams = await idb.cache.teams.getAll();
		const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
		const season = g.get("season");
		const players = new Map(
			(await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])).map(
				(p) => [p.pid, p],
			),
		);
		const picks = new Map(
			(await idb.cache.draftPicks.getAll()).map((dp) => [dp.dpid, dp]),
		);
		const out = {
			players: ids(env.TR_SOLD).map((pid) => players.get(pid)!),
			picks: [],
		};
		const paid = {
			players: ids(env.TR_PAID).map((pid) => players.get(pid)!),
			picks: ids(env.TR_PICKS).map((dpid) => picks.get(dpid)!),
		};
		const name = (x: any) =>
			"pid" in x
				? `${x.firstName} ${x.lastName} ${season - x.born.year}y ${x.ratings.at(-1).ovr}/${x.ratings.at(-1).pot}`
				: `${x.season} R${x.round} ${abbrev.get(x.originalTid)}`;
		const title = (tid: number) =>
			(100 * (outlook.teams.get(tid)?.title?.[0] ?? 0)).toFixed(1);
		log(
			`${season} phase ${g.get("phase")}: ${abbrev.get(seller)} (${title(seller)}%) sells [${out.players.map(name).join("; ")}] to ${abbrev.get(buyer)} (${title(buyer)}%) for [${[...paid.players, ...paid.picks].map(name).join("; ")}]`,
		);
		for (const tid of [seller, ...view.keenestCache.tids]) {
			await (
				await import("../../worker/core/realisticFrontOffice/outlook.ts")
			).ensureRetention(outlook, [...out.players, ...paid.players], tid);
		}
		const split = (tid: number, other: number, give: any, get: any) => {
			const w = view.dealWorthNow(tid, other, give, get);
			const h = view.heldNow(tid);
			const pin = view.premiumNow(
				tid,
				{ players: [], picks: get.picks },
				[tid, other],
				h,
			);
			const pout = view.premiumNow(tid, give, [tid, other], h);
			return `${w.toFixed(4)} (goal ${(w - pin + pout).toFixed(4)}, premium got ${pin.toFixed(4)}, given ${pout.toFixed(4)})`;
		};
		log(`seller's worth: ${split(seller, buyer, out, paid)}`);
		log(`buyer's worth: ${split(buyer, seller, paid, out)}`);
		const one = (x: any) =>
			"pid" in x ? { players: [x], picks: [] } : { players: [], picks: [x] };
		for (const x of [...out.players, ...paid.players, ...paid.picks]) {
			const a = one(x);
			log(
				`  ${name(x)}: use to ${abbrev.get(buyer)} ${view.useOfNow(buyer, a).toFixed(4)}, to ${abbrev.get(seller)} ${view.useOfNow(seller, a).toFixed(4)}, price ${view.priceNow(a, [seller, buyer]).toFixed(4)}`,
			);
		}
		log(
			`  all it paid: use to ${abbrev.get(buyer)} ${view.useOfNow(buyer, paid).toFixed(4)}, price ${view.priceNow(paid, [seller, buyer]).toFixed(4)}`,
		);
		// The buyer, season by season, as it was and with the trade.
		const held = view.heldNow(buyer);
		const after = view.swappedFrom(held, paid, out);
		const path = (h: any) => teamStrengths(outlook, buyer, h.players, h.picks);
		const before = path(held);
		const withIt = path(after);
		const fmt = (xs: number[]) => xs.map((x) => x.toFixed(2)).join(" ");
		log(`${abbrev.get(buyer)} strength by season, as it was: ${fmt(before)}`);
		log(
			`${abbrev.get(buyer)} strength by season, with the trade: ${fmt(withIt)}`,
		);
		const titles = (xs: number[]) =>
			xs
				.map((s, y) => (100 * curveAt(outlook.titleCurves[y], s)).toFixed(1))
				.join(" ");
		log(`title % as it was: ${titles(before)}`);
		log(`title % with it:   ${titles(withIt)}`);
		log(
			`franchise value ${franchiseValue(outlook, before, buyer).toFixed(4)} -> ${franchiseValue(outlook, withIt, buyer).toFixed(4)}`,
		);
		// Without each piece it paid, one at a time.
		for (const x of [...paid.players, ...paid.picks]) {
			const less = view.swappedFrom(held, one(x), { players: [], picks: [] });
			log(`  without ${name(x)}: ${fmt(path(less))}`);
		}
		const withMerrill = view.swappedFrom(held, { players: [], picks: [] }, out);
		log(
			`  with ${out.players.map(name).join("; ")} added, nothing paid: ${fmt(path(withMerrill))}`,
		);
		// The buyer's lines this season as the roster model sees them, with and
		// without what it bought (TR_LINES).
		if (env.TR_LINES) {
			const { teamPoints } =
				await import("../../worker/core/realisticFrontOffice/outlook.ts");
			const byValue = new Map(
				[...players.values()].map((p: any) => [
					Math.round(p.valueNoPot * 1000),
					`${p.firstName} ${p.lastName} ${p.ratings.at(-1).pos} ${p.ratings.at(-1).ovr}`,
				]),
			);
			for (const [label, h] of [
				["as it was", held],
				["with the trade", after],
			] as const) {
				log(`\n${abbrev.get(buyer)} this season's lines, ${label}:`);
				teamPoints(
					outlook,
					buyer,
					h.players,
					h.picks,
					undefined,
					undefined,
					(y: number, lines: readonly any[]) => {
						if (y !== 0) {
							return;
						}
						for (const l of [...lines].sort(
							(a, b) => b.valueNoPot - a.valueNoPot,
						)) {
							log(
								`  ${(byValue.get(Math.round(l.valueNoPot * 1000)) ?? "(fill)").padEnd(32)} valueNoPot ${l.valueNoPot.toFixed(1)} avail ${l.availability.toFixed(2)} roleFree ${l.roleFree.toFixed(2)} offense ${l.offense.toFixed(2)} usage ${l.usage.toFixed(2)} guard ${l.guard} forward ${l.forward} synergy ${JSON.stringify(l.synergy, (k, v) => (typeof v === "number" ? Math.round(v * 100) / 100 : v))}`,
							);
						}
					},
				);
			}
		}
		// A season's roster as the outlook has it (TR_SEASON_LINES=y,...), as
		// it was and without the players in TR_DROP: everyone with his chance
		// of being there, and the room and what it buys.
		if (env.TR_SEASON_LINES) {
			const ys = ids(env.TR_SEASON_LINES);
			const drop = ids(env.TR_DROP);
			for (const [label, ps] of [
				["as it was", held.players],
				[
					"without TR_DROP",
					held.players.filter((p: any) => !drop.includes(p.pid)),
				],
			] as const) {
				const rooms: string[] = [];
				const named = (i: number) =>
					i < ps.length
						? `${ps[i].firstName} ${ps[i].lastName} ${season - ps[i].born.year}y exp ${ps[i].contract?.exp ?? ps[i].contracts?.at?.(-1)?.exp ?? "?"}`
						: `#${i}`;
				const strengths = teamStrengths(
					outlook,
					buyer,
					ps,
					held.picks,
					undefined,
					(y: number, room: number, signings: readonly any[]) => {
						if (ys.includes(y)) {
							rooms[y] =
								`room ${room.toFixed(2)} buys ${signings.map((x) => `${x.valueNoPot.toFixed(1)}@${x.availability.toFixed(2)}`).join(" ")}`;
						}
					},
					undefined,
					undefined,
					(y: number, lines: readonly any[]) => {
						if (!ys.includes(y)) {
							return;
						}
						log(`\n${abbrev.get(buyer)} season ${y} lines, ${label}:`);
						for (const [i, l] of lines.entries()) {
							if (l.availability <= 0.001) {
								continue;
							}
							const halves = l.halves
								? ` halves ${l.halves.map((h: any) => `${h.valueNoPot.toFixed(1)}@${h.availability.toFixed(2)}`).join("/")}`
								: "";
							log(
								`  ${named(i).padEnd(40)} value ${l.valueNoPot.toFixed(1)} avail ${l.availability.toFixed(2)}${halves}`,
							);
						}
					},
				);
				for (const y of ys) {
					log(
						`  ${rooms[y] ?? "no room"}; strength ${strengths[y]!.toFixed(2)}`,
					);
				}
			}
		}
		const real = await view.bestRealOffer(seller, out, -1);
		log(
			`market's best real offer now: ${real ? `${abbrev.get(real.buyer)} [${[...real.offer.players, ...real.offer.picks].map(name).join("; ")}] worth ${real.worth.toFixed(4)} to the seller` : "none"}`,
		);
	},
);
