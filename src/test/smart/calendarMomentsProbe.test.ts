import { test } from "vitest";
import { g } from "../../worker/util/index.ts";
import { loadLeagueFile } from "./loadLeague.ts";

// THE MARKET ON A CALENDAR: a random-players league played night by night to
// the trade deadline with the smart CPU, writing each night's games, the
// nights left to the deadline and the market moment the league last met at -
// to check the market meets on each of the deadline's last nights when every
// sim day is one night of an NBA calendar (calendarTicks.ts).
//   CM_LEAGUE=<lid> [CM_CALENDAR=0] CM_OUT=<file> \
//     npx vitest --run --project basketball src/test/smart/calendarMomentsProbe.test.ts
const env: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const importNode = (name: string) => import(/* @vite-ignore */ name);

test.skipIf(!env.CM_LEAGUE)(
	"market moments on a calendar",
	{ timeout: 7_200_000 },
	async () => {
		await importNode("fake-indexeddb/auto");
		const fs = await importNode("node:fs");
		const out = env.CM_OUT ?? "/tmp/calendarMoments.txt";
		const lines: string[] = [];
		const log = (x: string) => {
			lines.push(x);
			fs.writeFileSync(out, lines.join("\n") + "\n");
		};
		(globalThis as any).process.env.ARENA_LEAGUE = JSON.stringify({
			...JSON.parse(env.ARENA_LEAGUE ?? "{}"),
			nbaCalendar: env.CM_CALENDAR !== "0",
		});
		await loadLeagueFile(`arena:${env.CM_LEAGUE}`);
		const { league, game, phase } = await import("../../worker/core/index.ts");
		const { PHASE } = await import("../../common/constants.ts");
		const { idb } = await import("../../worker/db/index.ts");
		const getDaysLeftSchedule = (
			await import("../../worker/core/season/getDaysLeftSchedule.ts")
		).default;
		await league.setGameAttributes({
			spectator: true,
			smartCpu: true,
			realisticFrontOffices: false,
		});
		if (g.get("phase") < PHASE.REGULAR_SEASON) {
			await phase.newPhase(PHASE.REGULAR_SEASON, {});
		}
		const schedule = await idb.cache.schedule.getAll();
		log(
			`calendar ${schedule.some((x) => x.date !== undefined)}, ${schedule.length} schedule rows`,
		);
		const moments: string[] = [];
		while (g.get("phase") === PHASE.REGULAR_SEASON) {
			const today = (await idb.cache.schedule.getAll())[0];
			let daysLeft: number | string = "-";
			try {
				daysLeft = await getDaysLeftSchedule("tradeDeadline");
			} catch {}
			const tonight = (await idb.cache.schedule.getAll()).filter(
				(x) => x.day === today?.day && x.homeTid >= 0,
			).length;
			const before = g.get("smartMarketMoment");
			await game.play(1, {});
			const after = g.get("smartMarketMoment");
			if (after !== before) {
				moments.push(String(after));
			}
			log(
				`day ${today?.day} ${today?.date ?? ""} games ${tonight} to deadline ${daysLeft} moment ${after !== before ? after : ""}`,
			);
		}
		log(`moments ${moments.length}: ${moments.join(", ")}`);
	},
);
