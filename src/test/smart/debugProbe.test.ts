import { test } from "vitest";
import { makeSeededRandom } from "../../common/random.ts";
import { idb } from "../../worker/db/index.ts";
import { g } from "../../worker/util/index.ts";
import { advanceSeasons, createArenaLeague } from "../arena/runLeague.ts";

// A deal's worth taken apart, for finding why a rule fails.
//   DEBUG_PROBE=1 DEBUG_OUT=<dir> npx vitest --run --project basketball \
//     src/test/smart/debugProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.runIf(env.DEBUG_PROBE === "1")(
	"debug probe",
	{ timeout: 3_600_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs/promises");
		const { buildOutlook } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		const { contractOf } =
			await import("../../worker/core/realisticFrontOffice/contractOf.ts");
		const { SmartView } = await import("../../worker/core/smartCpu/worth.ts");
		Math.random = makeSeededRandom(77);
		(idb as any).league = undefined;
		await createArenaLeague(9177);
		await advanceSeasons(1);
		g.setWithoutSavingToDB("realisticFrontOffices", true);
		const view = new SmartView(await buildOutlook({ iterations: 1000 }), []);
		await view.prime();
		const lines: string[] = [];
		const say = (s: string) => lines.push(s);
		say(
			`phase ${g.get("phase")} season ${g.get("season")} max ${g.get("maxRosterSize")} cap ${g.get("salaryCap")}`,
		);
		say(`keenest ${(await view.keenest()).join(",")}`);
		for (const tid of [24, 7]) {
			const h = view.heldNow(tid);
			const best = [...h.players].sort((a, b) => b.value - a.value)[0]!;
			const out = { players: [best], picks: [] };
			const none = { players: [], picks: [] };
			const signed = h.players.filter(
				(p) => contractOf(p).exp >= g.get("season"),
			).length;
			say(
				`team ${tid}: ${h.players.length} players (${signed} signed), best ${best.pid} ${best.ratings.at(-1)!.ovr} $${contractOf(best).amount} to ${contractOf(best).exp}`,
			);
			const v = view.value(tid, h);
			const vWithout = view.value(tid, view.swappedFrom(h, out, none));
			say(
				`  value ${v.toFixed(4)} without him ${vWithout.toFixed(4)} use ${(v - vWithout).toFixed(4)}`,
			);
			const [cut, dead] = view.afterCuts(tid, h);
			say(
				`  after cuts: ${cut.players.length} players, dead ${JSON.stringify(dead)}`,
			);
			for (const k of await view.keenest()) {
				say(
					`  bid from ${k}: ${view.bidNow(k, out).toFixed(4)} (use ${view.useOfNow(k, out).toFixed(4)})`,
				);
			}
			say(
				`  price (not ${tid}, ${tid === 24 ? 0 : 1}): ${view.priceNow(out, [tid, tid === 24 ? 0 : 1]).toFixed(4)}`,
			);
			say(
				`  dealWorth: ${view.dealWorthNow(tid, tid === 24 ? 0 : 1, out, none).toFixed(4)}`,
			);
		}
		await fs.writeFile(`${env.DEBUG_OUT ?? "."}/debug.txt`, lines.join("\n"));
	},
);
