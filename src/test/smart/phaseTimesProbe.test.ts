import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// PHASE TIMES: a league file played through one season the way the game
// advances it, each step timed - every phase change, the season's days in
// weeks, the draft, free agency's days - with where the smart CPU spent it
// (smartClock, by kind).
//   PT_LEAGUE=<league.json> PT_OUT=<file> [PT_SEASONS=1] \
//     npx vitest --run --project basketball src/test/smart/phaseTimesProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.PT_LEAGUE)(
	"phase times probe",
	{ timeout: 14_400_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(env.PT_OUT ?? "/dev/stdout", lines.join("\n") + "\n");
		};
		await loadLeagueFile(env.PT_LEAGUE!);
		const { league, game, phase, draft, freeAgents } =
			await import("../../worker/core/index.ts");
		const { PHASE, PHASE_TEXT } = await import("../../common/constants.ts");
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		const { smartClock } = await import("../../worker/core/smartCpu/clock.ts");
		const snapshot = () =>
			Object.fromEntries(
				Object.entries(smartClock)
					.filter(([k]) => !k.includes("@"))
					.map(([k, v]) => [k, { ...v }]),
			);
		const step = async (label: string, f: () => Promise<unknown>) => {
			const before = snapshot();
			const t = performance.now();
			await f();
			const s = (performance.now() - t) / 1000;
			const parts = Object.entries(smartClock)
				.filter(([k]) => !k.includes("@"))
				.map(([k, v]) => ({
					k,
					ms: v.ms - (before[k]?.ms ?? 0),
					n: v.n - (before[k]?.n ?? 0),
				}))
				.filter((x) => x.ms > 500)
				.sort((a, b) => b.ms - a.ms)
				.slice(0, 6)
				.map((x) => `${x.k} ${(x.ms / 1000).toFixed(1)}s (${x.n})`)
				.join(", ");
			log(`${label.padEnd(44)} ${s.toFixed(1).padStart(7)} s  ${parts}`);
		};
		// PT_CLEAR_PRICE=0: clearing room counts players sent at their use alone.
		if (env.PT_CLEAR_PRICE === "0") {
			const { clearRoomSettings } =
				await import("../../worker/core/smartCpu/clearRoom.ts");
			clearRoomSettings.atPrice = false;
		}
		// PT_TO_PRESEASON=1: stop at the next preseason.
		const seasons = env.PT_TO_PRESEASON ? 0 : Number(env.PT_SEASONS ?? 1);
		const start = g.get("season");
		log(`${start} ${PHASE_TEXT[g.get("phase")]}, ${seasons} season(s)`);
		let guard = 0;
		while (
			!(
				(g.get("season") >= start + seasons || env.PT_TO_PRESEASON) &&
				g.get("phase") === PHASE.PRESEASON &&
				guard > 0
			) &&
			guard++ < 10000
		) {
			const ph = g.get("phase");
			const at = `${g.get("season")} ${PHASE_TEXT[ph]}`;
			if (ph === PHASE.REGULAR_SEASON || ph === PHASE.AFTER_TRADE_DEADLINE) {
				await step(`${at}: 7 days`, () => game.play(7, {}));
			} else if (ph === PHASE.PLAYOFFS) {
				await step(`${at}: playoffs`, () => game.play(100, {}));
			} else if (ph === PHASE.DRAFT) {
				await step(`${at}: the draft`, () =>
					draft.runPicks({ type: "untilEnd" }, {}),
				);
				if (g.get("phase") === PHASE.DRAFT) {
					await step(`${at} -> ${PHASE_TEXT[PHASE.AFTER_DRAFT]}`, () =>
						phase.newPhase(PHASE.AFTER_DRAFT, {}),
					);
				}
			} else if (ph === PHASE.FREE_AGENCY) {
				await step(`${at}: ${g.get("daysLeft")} days`, () =>
					freeAgents.play(g.get("daysLeft"), {}),
				);
				if (g.get("phase") === PHASE.FREE_AGENCY) {
					await step(`${at} -> ${PHASE_TEXT[PHASE.PRESEASON]}`, () =>
						phase.newPhase(PHASE.PRESEASON, {}),
					);
				}
			} else {
				await step(
					`${at} -> ${(PHASE_TEXT as Record<number, string>)[ph + 1]}`,
					() => phase.newPhase((ph + 1) as any, {}),
				);
			}
		}
		const total = Object.entries(smartClock)
			.filter(([k]) => !k.includes("@"))
			.sort((a, b) => b[1].ms - a[1].ms)
			.slice(0, 15)
			.map(([k, v]) => `  ${k}: ${(v.ms / 1000).toFixed(1)}s in ${v.n}`);
		log(`smart CPU time by kind:\n${total.join("\n")}`);
	},
);
