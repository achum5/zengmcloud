// Is the game sim bit-for-bit what it was? For auditing a sim change that is
// meant to be behavior-preserving - a performance pull from upstream, a
// refactor. Skipped unless SIM_EQUIV_OUT is set.
//
// It seeds Math.random, generates two rosters, sims N games through GameSim
// (half with play-by-play on), and writes a hash of everything - rosters,
// processed teams, box scores, play-by-play - to that path. Run it at the two
// commits and compare:
//
//   SIM_EQUIV_OUT=/tmp/before.json npx vitest run --project basketball \
//     src/worker/core/game/simEquivalence.audit.test.ts
//   ... check out the other commit, write /tmp/after.json the same way ...
//   diff /tmp/before.json /tmp/after.json
//
// rosterHash and sidesHash equal but resultsHash different means the
// divergence is inside GameSim; the perGame list says which games. The first
// time this ran it caught a per-player synergy memo being filled before home
// court advantage rescaled the ratings (see synergyCache.test.ts) - a drift no
// other test noticed, because every box score still looked plausible.
import { test } from "vitest";
import { resetG } from "../../../test/helpers.ts";
import { g } from "../../util/index.ts";
import { player } from "../index.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "./loadTeams.ts";
import { DEFAULT_PLAY_THROUGH_INJURIES } from "../../../common/constants.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";

const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};
const OUT = nodeEnv.SIM_EQUIV_OUT;
const N = Number(nodeEnv.SIM_EQUIV_GAMES ?? 60);

const mulberry32 = (a: number) => () => {
	a |= 0;
	a = (a + 0x6d2b79f5) | 0;
	let t = Math.imul(a ^ (a >>> 15), 1 | a);
	t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
	return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};

const stable = (x: any): string => {
	if (x === undefined) {
		return "undefined";
	}
	if (x === null || typeof x !== "object") {
		return JSON.stringify(x);
	}
	if (Array.isArray(x)) {
		return `[${x.map(stable).join(",")}]`;
	}
	return `{${Object.keys(x)
		.sort()
		.map((k) => `${JSON.stringify(k)}:${stable(x[k])}`)
		.join(",")}}`;
};

const fnv = (s: string) => {
	let h = 0x811c9dc5;
	for (let i = 0; i < s.length; i++) {
		h ^= s.charCodeAt(i);
		h = Math.imul(h, 0x01000193) >>> 0;
	}
	return h.toString(16).padStart(8, "0");
};

test.skipIf(!OUT)("sim equivalence audit", { timeout: 1_200_000 }, async () => {
	resetG();
	g.setWithoutSavingToDB("userTids", []);
	g.setWithoutSavingToDB("userTid", 0);

	const realRandom = Math.random;
	Math.random = mulberry32(12345);
	try {
		const rosters: any[][] = [[], []];
		for (const tid of [0, 1]) {
			for (let i = 0; i < 13; i++) {
				const p = player.generate(
					tid,
					19 + (i % 15),
					2016,
					true,
					DEFAULT_LEVEL,
				);
				p.pid = tid * 100 + i;
				p.stats = [];
				p.injuries = [];
				rosters[tid]!.push(p);
			}
		}
		const rosterHash = fnv(stable(rosters));

		const sides: any[] = [];
		for (const tid of [0, 1]) {
			const t = {
				tid,
				playThroughInjuries: DEFAULT_PLAY_THROUGH_INJURIES,
				depth: undefined,
			};
			const teamSeason = { won: 0, lost: 0, tied: 0, otl: 0, cid: 0, did: 0 };
			sides.push(await processTeam(t as any, teamSeason as any, rosters[tid]!));
		}
		const sidesHash = fnv(stable(sides));

		const perGame: string[] = [];
		let all = "";
		for (let k = 0; k < N; k++) {
			Math.random = mulberry32(1000 + k);
			const result: any = new GameSim({
				gid: k + 1,
				day: 1,
				teams: structuredClone(sides),
				doPlayByPlay: k % 2 === 0,
				homeCourtFactor: 1,
				neutralSite: false,
				allStarGame: false,
				baseInjuryRate: 0.01,
			} as any).run();
			const s = stable(result);
			if (k < 2) {
				const fs0 = await import(("node" + ":fs") as any);
				fs0.writeFileSync(`${OUT}.g${k}.json`, s);
			}
			perGame.push(fnv(s));
			all += s;
		}

		const fs = await import(("node" + ":fs") as any);
		fs.writeFileSync(
			OUT!,
			JSON.stringify(
				{ rosterHash, sidesHash, resultsHash: fnv(all), perGame, N },
				null,
				1,
			),
		);
	} finally {
		Math.random = realRandom;
	}
});
