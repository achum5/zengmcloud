import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE ROOM PROBE: at free agency's opening, what signing a free agent rated
// RP_OVR or so is worth to each team with the room for him, at his ask and
// at bigger salaries - his gain, and season by season the team's room with
// and without him, what that room buys, its strength and the titles.
//   RP_LEAGUE=<league.json saved before free agency> RP_OUT=<file> \
//     npx vitest --run --project basketball src/test/smart/roomProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.RP_LEAGUE)("room probe", { timeout: 3_600_000 }, async () => {
	await importNode("fake-indexeddb/auto");
	const fs = await importNode("node:fs");
	const out = env.RP_OUT ?? "/tmp/roomProbe.txt";
	const lines: string[] = [];
	const log = (x: string) => {
		lines.push(x);
		fs.writeFileSync(out, lines.join("\n") + "\n");
	};
	await loadLeagueFile(env.RP_LEAGUE!);
	const { league, phase } = await import("../../worker/core/index.ts");
	const { PHASE, PLAYER } = await import("../../common/constants.ts");
	const { idb } = await import("../../worker/db/index.ts");
	await league.setGameAttributes({
		spectator: true,
		smartCpu: true,
		realisticFrontOffices: false,
	});
	while (g.get("phase") !== PHASE.FREE_AGENCY) {
		await phase.newPhase((g.get("phase") + 1) as any, {});
	}
	const o = await import("../../worker/core/realisticFrontOffice/outlook.ts");
	const { currentOutlook } =
		await import("../../worker/core/realisticFrontOffice/market.ts");
	const { viewFor, prepareSignings, smartSigningPrice, signedOn } =
		await import("../../worker/core/smartCpu/signing.ts");
	const { noteVanillaPool } =
		await import("../../worker/core/smartCpu/vanilla.ts");
	const { freeAgentPoolShare } =
		await import("../../worker/core/realisticFrontOffice/payroll.ts");
	const { team } = await import("../../worker/core/index.ts");
	await noteVanillaPool();
	const cap = g.get("salaryCap");
	const teams = await idb.cache.teams.getAll();
	const abbrev = new Map(teams.map((t) => [t.tid, t.abbrev]));
	const fas = (await idb.cache.players.indexGetAll(
		"playersByTid",
		PLAYER.FREE_AGENT,
	)) as any[];
	const want = Number(env.RP_OVR ?? 49);
	const pool = [...fas].sort(
		(a, b) => b.ratings.at(-1).ovr - a.ratings.at(-1).ovr,
	);
	log(
		`${g.get("season")} free agency opens, pool left ${freeAgentPoolShare()?.toFixed(2)}; best: ${pool
			.slice(0, 12)
			.map(
				(p) =>
					`${p.ratings.at(-1).ovr}/${p.ratings.at(-1).pot} ${g.get("season") - p.born.year}y @${p.contractDemand.amount}`,
			)
			.join(", ")}`,
	);
	const picked = pool
		.filter(
			(p) =>
				Math.abs(p.ratings.at(-1).ovr - want) <= 1 &&
				g.get("season") - p.born.year <= 26,
		)
		.slice(0, Number(env.RP_PLAYERS ?? 2));
	const outlook = await currentOutlook();
	await prepareSignings(outlook, picked);
	const view = viewFor(outlook);
	const shares = (env.RP_SHARES ?? "0.05,0.15,0.28").split(",").map(Number);
	for (const p of picked) {
		const r = p.ratings.at(-1);
		log(
			`\n== ${p.firstName} ${p.lastName} ${g.get("season") - p.born.year}y ${r.ovr}/${r.pot} v${p.value.toFixed(1)} asks ${p.contractDemand.amount} thru ${p.contractDemand.exp}`,
		);
		for (const t of teams) {
			const payroll = await team.getPayroll(t.tid);
			const room = cap - payroll;
			if (room < 0.1 * cap) {
				continue;
			}
			await o.ensureRetention(outlook, [p], t.tid);
			const h = await view.holdingsOf(t.tid);
			const rows: string[] = [];
			const ask = p.contractDemand.amount / cap;
			const all = [...shares, ask].sort((x, y) => x - y);
			for (const share of all) {
				const amount = Math.round(share * cap);
				if (amount > room) {
					continue;
				}
				const contract = { amount, exp: p.contractDemand.exp };
				const { net } = smartSigningPrice(
					outlook,
					{ tid: t.tid, players: h.players, picks: h.picks },
					p,
					contract,
				);
				const margin = view.marginNow(
					t.tid,
					{ players: [], picks: [] },
					{
						players: [signedOn(p, t.tid, contract)],
						picks: [],
					},
				);
				rows.push(
					`${share === ask ? "ask " : ""}${(100 * share).toFixed(0)}%: ${net.toFixed(4)} (margin ${margin.toFixed(4)})`,
				);
			}
			// Season by season at the biggest salary that fits.
			const share = all.filter((s) => s * cap <= room).at(-1)!;
			const signed = signedOn(p, t.tid, {
				amount: Math.round(share * cap),
				exp: p.contractDemand.exp,
			});
			const detail = (players: any[]) => {
				const rooms: number[] = [];
				const hauls: string[] = [];
				const s = o.teamStrengths(
					outlook,
					t.tid,
					players,
					h.picks,
					undefined,
					(y: number, roomShare: number, signings: readonly any[]) => {
						rooms[y] = roomShare;
						hauls[y] = signings
							.map(
								(x) =>
									`${x.valueNoPot.toFixed(0)}x${x.availability.toFixed(2)}`,
							)
							.join("+");
					},
				);
				return { s, rooms, hauls };
			};
			const a = detail(h.players);
			const b = detail([...h.players, signed]);
			const fv = (s: number[], k: number) =>
				o.franchiseValue(outlook, s.slice(0, k), t.tid);
			const years: string[] = [];
			for (let k = 1; k <= 5; k++) {
				years.push(
					`y${k - 1} room ${(100 * (a.rooms[k - 1] ?? Number.NaN)).toFixed(0)}->${(100 * (b.rooms[k - 1] ?? Number.NaN)).toFixed(0)}% haul ${a.hauls[k - 1] ?? "-"} -> ${b.hauls[k - 1] ?? "-"} mov ${a.s[k - 1]!.toFixed(2)}${b.s[k - 1]! >= a.s[k - 1]! ? "+" : ""}${(b.s[k - 1]! - a.s[k - 1]!).toFixed(2)} title ${(fv(b.s, k) - fv(a.s, k) - (fv(b.s, k - 1) - fv(a.s, k - 1))).toFixed(5)}`,
				);
			}
			log(
				`${abbrev.get(t.tid)} (title ${(100 * (outlook.teams.get(t.tid)?.title?.[0] ?? 0)).toFixed(1)}%, winp ${(outlook.winShare?.get(t.tid)?.[0] ?? Number.NaN).toFixed(2)}, room ${((100 * room) / cap).toFixed(0)}%, ${h.players.length} players) ${rows.join(" ")}\n    ${years.join("\n    ")}`,
			);
		}
	}
});
