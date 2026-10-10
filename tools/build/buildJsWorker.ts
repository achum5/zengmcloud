import { build } from "rolldown";
import { parentPort, workerData } from "node:worker_threads";
import { rolldownConfig, type BundleName } from "../lib/rolldownConfig.ts";
import type { Sport } from "../lib/getSport.ts";
import type { JsonHashes } from "../lib/jsonUrls.ts";

const LODASH_BLACKLIST = [/^lodash$/, /^lodash-es/, /^lodash\//];

const BLACKLIST = {
	ui: [...LODASH_BLACKLIST, /\/worker/],
	worker: [...LODASH_BLACKLIST, /\/ui/, /^react/],
	court: [...LODASH_BLACKLIST, /\/worker/, /^react/],
};

const buildFile = async (
	sport: Sport,
	name: BundleName,
	versionNumber: string,
	jsonHashes: JsonHashes,
) => {
	let modulepreloadFilenames: string[] | undefined;
	const config = rolldownConfig(sport, name, {
		nodeEnv: "production",
		blacklistOptions: BLACKLIST[name],
		jsonHashes,
		versionNumber,
		onModulepreloadFilenames: (filenames) => {
			modulepreloadFilenames = filenames;
		},
	});
	await build(config);

	if (modulepreloadFilenames === undefined) {
		throw new Error(`modulepreloadFilenames is undefined for ${name}`);
	}

	parentPort!.postMessage(modulepreloadFilenames);
};

const { name, sport, versionNumber, jsonHashes } = workerData;

await buildFile(sport, name, versionNumber, jsonHashes);
