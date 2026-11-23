import { league } from "../../worker/core/index.ts";
import { idb } from "../../worker/db/index.ts";
import { helpers } from "../../worker/util/index.ts";
import { getDefaultSettings } from "../../worker/views/newLeague.ts";

// A BBGM export loaded into a test league (fake-indexeddb must be set up
// first), with its own settings - for the smart CPU's checks on the owner's
// league (docs/smart-cpu.md).
const importNode = (name: string) => import(/* @vite-ignore */ name);

const unwrap = (x: unknown) =>
	Array.isArray(x) &&
	x.length > 0 &&
	typeof x[0] === "object" &&
	x[0] &&
	"start" in x[0]
		? (x as { value: unknown }[]).at(-1)!.value
		: x;

export const loadLeagueFile = async (file: string) => {
	// OUTLOOK_SET: outlook settings to try, as JSON - for a probe to measure
	// one each way.
	const env: Record<string, string | undefined> =
		(globalThis as any).process?.env ?? {};
	if (env.OUTLOOK_SET) {
		const { outlookSettings } =
			await import("../../worker/core/realisticFrontOffice/outlook.ts");
		Object.assign(outlookSettings, JSON.parse(env.OUTLOOK_SET));
	}
	// MARKET_SET: the smart CPU market's settings, likewise.
	if (env.MARKET_SET) {
		const { marketSettings } =
			await import("../../worker/core/smartCpu/market.ts");
		Object.assign(marketSettings, JSON.parse(env.MARKET_SET));
	}
	// SMART_SET: the smart CPU's valuation settings (smartSettings), likewise;
	// MARGIN_SET: the margin of error's (marginSettings).
	if (env.SMART_SET) {
		const { smartSettings } =
			await import("../../worker/core/smartCpu/worth.ts");
		Object.assign(smartSettings, JSON.parse(env.SMART_SET));
	}
	// VANILLA_SET: the smart CPU's signings on vanilla's rules
	// (vanillaSigningSettings), likewise.
	if (env.VANILLA_SET) {
		const { vanillaSigningSettings } =
			await import("../../worker/core/smartCpu/vanilla.ts");
		Object.assign(vanillaSigningSettings, JSON.parse(env.VANILLA_SET));
	}
	if (env.MARGIN_SET) {
		const { marginSettings } =
			await import("../../worker/core/realisticFrontOffice/margin.ts");
		Object.assign(marginSettings, JSON.parse(env.MARGIN_SET));
	}
	// LL_SEED: another run of the same league, for a spread of outcomes.
	const { makeSeededRandom } = await import("../../common/random.ts");
	Math.random = makeSeededRandom(Number(env.LL_SEED ?? 1));
	// "arena:<lid>": a new random-players league instead of a file
	// (test/arena), for a run on a league nobody uploaded.
	if (file.startsWith("arena:")) {
		const { createArenaLeague } = await import("../arena/runLeague.ts");
		(idb as any).league = undefined;
		await createArenaLeague(Number(file.slice("arena:".length)));
		Math.random = makeSeededRandom(Number(env.LL_SEED ?? 1));
		return;
	}
	const fs = await importNode("node:fs/promises");
	const data = JSON.parse(await fs.readFile(file, "utf8"));
	const ga = data.gameAttributes;
	const settings: Record<string, unknown> =
		helpers.deepCopy(getDefaultSettings());
	for (const key of Object.keys(settings)) {
		if (Object.hasOwn(ga, key)) {
			settings[key] = unwrap(ga[key]);
		}
	}
	(idb as any).league = undefined;
	await league.createStream(data, {
		confs: unwrap(ga.confs),
		divs: unwrap(ga.divs),
		fromFile: {
			gameAttributes: ga,
			hasRookieContracts: true,
			maxGid: undefined,
			startingSeason: data.startingSeason ?? unwrap(ga.startingSeason),
			teams: data.teams,
			version: data.version,
		},
		getLeagueOptions: undefined,
		keptKeys: new Set([...Object.keys(data), "startingSeason", "version"]),
		lid: 79,
		name: "Owner",
		setLeagueCreationStatus: () => {},
		settings,
		shuffleRosters: false,
		startingSeasonFromInput: undefined,
		teamsFromInput: data.teams,
		tid: 0,
	} as any);
	await league.loadGameAttributes();
	// LL_UNDO: a trade to undo before the probe looks, as JSON -
	// {"players":{"<pid>":<tid>},"picks":{"<dpid>":<tid>}}, each back where it was.
	if (env.LL_UNDO) {
		const undo = JSON.parse(env.LL_UNDO);
		for (const [pid, tid] of Object.entries(undo.players ?? {})) {
			const p = await idb.cache.players.get(Number(pid));
			await idb.cache.players.put({ ...p!, tid: tid as number });
		}
		for (const [dpid, tid] of Object.entries(undo.picks ?? {})) {
			const dp = await idb.cache.draftPicks.get(Number(dpid));
			await idb.cache.draftPicks.put({ ...dp!, tid: tid as number });
		}
	}
};
