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
	const { makeSeededRandom } = await import("../../common/random.ts");
	Math.random = makeSeededRandom(1);
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
};
