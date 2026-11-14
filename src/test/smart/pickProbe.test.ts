import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";
import { contractOf } from "../../worker/core/realisticFrontOffice/contractOf.ts";

// What GM teams think draft picks are worth, by how far off they are: each
// team's use of its own firsts and their market price, against a veteran's.
//   PICK_PROBE=1 PICK_OUT=<dir> [PICK_FUTURE=7] npx vitest --run \
//     --project basketball src/test/smart/pickProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.PICK_PROBE === "1")(
	"pick probe",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { currentOutlook } =
			await import("../../worker/core/realisticFrontOffice/market.ts");
		const { viewFor } = await import("../../worker/core/smartCpu/signing.ts");
		Math.random = makeSeededRandom(Number(env.PICK_SEED ?? 5));
		(idb as any).league = undefined;
		await createArenaLeague(Number(env.PICK_LEAGUE ?? 9177));
		if (env.PICK_FUTURE) {
			g.setWithoutSavingToDB(
				"numSeasonsFutureDraftPicks",
				Number(env.PICK_FUTURE),
			);
		}
		await advanceSeasons(Number(env.PICK_SEASONS ?? 1));
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		g.setWithoutSavingToDB("smartCpu", true);
		const t0 = performance.now();
		const view = viewFor(await currentOutlook());
		const t1 = performance.now();
		await view.prime();
		const t2 = performance.now();
		const out: string[] = [];
		const say = (s: string) => out.push(s);
		const teams = (await idb.cache.teams.getAll()).filter((t) => !t.disabled);
		const strength = new Map<number, number>();
		for (const t of teams) {
			strength.set(t.tid, view.value(t.tid, await view.holdingsOf(t.tid)));
		}
		const ranked = teams
			.map((t) => t.tid)
			.sort((a, b) => strength.get(b)! - strength.get(a)!);
		const picked = [
			...ranked.slice(0, 2),
			...ranked.slice(14, 16),
			...ranked.slice(-3),
		];
		// Veterans 33 and over rated 55 or better, on other teams.
		const vets = (await idb.cache.players.getAll())
			.filter(
				(p) =>
					p.tid >= 0 &&
					g.get("season") - p.born.year >= 33 &&
					p.ratings.at(-1)!.ovr >= 55,
			)
			.sort((a, b) => b.ratings.at(-1)!.ovr - a.ratings.at(-1)!.ovr)
			.slice(0, 6);
		const f = (x: number) => (100 * x).toFixed(2);
		say(
			`season ${g.get("season")} phase ${g.get("phase")}, future drafts ${g.get("numSeasonsFutureDraftPicks")}; worth in hundredths of a title`,
		);
		for (const tid of picked) {
			const h = await view.holdingsOf(tid);
			say(
				`\n== tid ${tid}: franchise value ${f(strength.get(tid)!)}, rank ${ranked.indexOf(tid) + 1}`,
			);
			for (const dp of [...h.picks].sort(
				(a, b) =>
					(a.season as number) - (b.season as number) || a.round - b.round,
			)) {
				const one = { players: [], picks: [dp] };
				const use = await view.useOf(tid, one);
				const price = await view.price(one, [tid]);
				say(
					`  ${dp.season} R${dp.round} of ${dp.originalTid}: use ${f(use)}, price ${f(price)}`,
				);
			}
			for (const p of vets.filter((p) => p.tid !== tid).slice(0, 3)) {
				const one = { players: [p], picks: [] };
				const use = await view.useOf(tid, one);
				const price = await view.price(one, [p.tid]);
				const c = contractOf(p);
				say(
					`  vet ${p.pid} ${g.get("season") - p.born.year}y ${p.ratings.at(-1)!.ovr} $${(c.amount / 1000).toFixed(1)}M->${c.exp} of ${p.tid}: use ${f(use)}, price ${f(price)}`,
				);
			}
			// Its furthest first for the best of those veterans, by its worth.
			const far = [...h.picks]
				.filter((dp) => dp.round === 1)
				.sort((a, b) => (b.season as number) - (a.season as number))[0];
			const vet = vets.find((p) => p.tid !== tid);
			if (far && vet) {
				const worth = await view.dealWorth(
					tid,
					vet.tid,
					{ players: [], picks: [far] },
					{ players: [vet], picks: [] },
				);
				say(
					`  its ${far.season} R1 for vet ${vet.pid}: worth ${f(worth)} to it`,
				);
			}
		}
		say(
			`\noutlook ${((t1 - t0) / 1000).toFixed(1)} s, prime ${((t2 - t1) / 1000).toFixed(1)} s, probe ${((performance.now() - t2) / 1000).toFixed(1)} s`,
		);
		await fs.writeFile(`${env.PICK_OUT ?? "."}/picks.txt`, out.join("\n"));
	},
);
