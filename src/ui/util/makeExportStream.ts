import { openDB } from "@dumbmatter/idb";
import type { IDBPCursorWithValue, StoreNames } from "@dumbmatter/idb";
import { LEAGUE_DATABASE_VERSION } from "../../common/constants.ts";
import {
	gameAttributesCache,
	gameAttributesKeysOtherSports,
} from "../../common/defaultGameAttributes.ts";
import { local } from "./local.ts";
import { toWorker } from "./toWorker.ts";
import { pushSyncDebugEntry } from "./syncDebugStore.ts";
import type {
	LeagueDB,
	LeagueDBStoreNames,
} from "../../worker/db/connectLeague.ts";
import { gameAttributesArrayToObject } from "../../common/gameAttributesArrayToObject.ts";
import getAll from "../../common/getAll.ts";

// Otherwise it often pulls just one record per transaction, as it's hitting up against the high water mark
const TWENTY_MEGABYTES_IN_BYTES = 20 * 1024 * 1024;

// If we just let the normal highWaterMark mechanism work, it might pull only one record at a time, which is not ideal given the cost of starting a transaction. Make it too high, and the progress bar becomes unrealistic (especially when uploading to Dropbox which is slower than writing to disk) because it is measured from reading the database, not the end of the stream.
const highWaterMark = TWENTY_MEGABYTES_IN_BYTES;
const minSizePerPull = TWENTY_MEGABYTES_IN_BYTES;

const stringSizeInBytes = (str: string | undefined) => {
	if (!str) {
		return 0;
	}

	// https://stackoverflow.com/a/23329386/786644
	let s = str.length;
	for (let i = str.length - 1; i >= 0; i--) {
		const code = str.charCodeAt(i);
		if (code > 0x7f && code <= 0x7ff) {
			s++;
		} else if (code > 0x7ff && code <= 0xffff) {
			s += 2;
		}
		if (code >= 0xdc00 && code <= 0xdfff) {
			i--;
		}
	}
	return s;
};

const NUM_SPACES_IN_TAB = 2;

type ProcessStores<ReturnType> = Partial<{
	[K in LeagueDBStoreNames]?: (a: LeagueDB[K]["value"]) => ReturnType;
}>;

const makeExportStream = async (
	storesInput: LeagueDBStoreNames[],
	{
		abortSignal,
		compressed = false,
		filter = {},
		forEach = {},
		map = {},
		name,
		onPercentDone,
		onProcessingStore,
		signal,
	}: {
		abortSignal?: AbortSignal;
		compressed?: boolean;
		filter?: ProcessStores<boolean>;
		forEach?: ProcessStores<void>;
		map?: ProcessStores<any>;
		name?: string;
		onPercentDone?: (percentDone: number) => void;
		onProcessingStore?: (processingStore: string) => void;
		signal?: AbortSignal;
	},
) => {
	const lid = local.getState().lid;
	if (lid === undefined) {
		throw new Error("Missing lid");
	}

	// Don't worry about upgrades or anything, because this function will only be called if the league database already exists
	const leagueDB = await openDB<LeagueDB>(
		`league${lid}`,
		LEAGUE_DATABASE_VERSION,
		{
			blocking() {
				leagueDB.close();
			},
		},
	);

	// Always flush before export, so export is current!
	await toWorker("main", "idbCacheFlush", undefined);

	// A FULL export of a synced league gets a sync checkpoint in its meta: the
	// room fingerprint plus the change-log position this file's data already
	// includes. Re-importing it can then join the room and catch up from the
	// checkpoint instead of replaying the whole room history. Partial exports
	// must not claim it - their state is not what the checkpoint describes.
	let syncCheckpoint: { leagueId: string; watermark: number } | undefined;
	// Saved replays are exempt from the completeness test. A replay is a
	// rewatchable recording of a game, not league STATE - the checkpoint
	// describes where this file's data sits in the room's history, and leaving
	// the recordings out doesn't make that claim any less true. They are also
	// far and away the largest thing in a league and never pruned, so counting
	// them would force anyone making a handoff file to choose between a sync
	// checkpoint and a file small enough to import on a phone.
	const CHECKPOINT_OPTIONAL_STORES = new Set(["liveGamePlayByPlay"]);
	const missingStores = Array.from(leagueDB.objectStoreNames).filter(
		(store) =>
			!(storesInput as string[]).includes(store) &&
			!CHECKPOINT_OPTIONAL_STORES.has(store),
	);
	const isFullExport = missingStores.length === 0;
	// Mirrored to the sync debug overlay so a checkpoint-less export is never
	// silent again (a store missing from the export = re-imports replay the
	// whole room history from zero).
	pushSyncDebugEntry({
		at: new Date().toISOString(),
		event: "export:full-check",
		isFullExport,
		missingStores,
	});
	if (isFullExport) {
		try {
			syncCheckpoint = await toWorker("main", "getSyncCheckpoint", undefined);
		} catch {
			// Not synced / worker refused - just export without a checkpoint.
		}
	}

	const space = compressed ? "" : " ";
	const tab = compressed ? "" : " ".repeat(NUM_SPACES_IN_TAB);
	const newline = compressed ? "" : "\n";

	const jsonStringify = (object: any, indentationLevels: number) => {
		if (compressed) {
			return JSON.stringify(object);
		}

		const json = JSON.stringify(object, null, NUM_SPACES_IN_TAB);

		return json.replaceAll("\n", `\n${tab.repeat(indentationLevels)}`);
	};

	const stores = storesInput.filter(
		(store) => store !== "teamSeasons" && store !== "teamStats",
	);
	const includeTeamSeasonsAndStats = stores.length !== storesInput.length;

	const writeRootObject = (
		controller: ReadableStreamController<string>,
		name: string,
		object: any,
	) =>
		controller.enqueue(
			// @ts-expect-error Typescript 4.9 bug I think
			`,${newline}${tab}"${name}":${space}${jsonStringify(object, 1)}`,
		);

	let storeIndex = 0;
	let prevKey: string | number | undefined;
	let prevStore: string | undefined;
	let cancelCallback: (() => void) | undefined;
	const enqueuedFirstRecord = new Set();

	let numRecordsSeen = 0;
	let numRecordsTotal = 0;
	let prevPercentDone = 0;
	const incrementNumRecordsSeen = (count: number = 1) => {
		numRecordsSeen += count;
		if (onPercentDone) {
			const percentDone = Math.round((numRecordsSeen / numRecordsTotal) * 100);
			if (percentDone !== prevPercentDone) {
				onPercentDone(percentDone);
				prevPercentDone = percentDone;
			}
		}
	};

	return new ReadableStream<string>(
		{
			async start(controller) {
				signal?.addEventListener("abort", () => {
					controller.error(new DOMException("Aborted", "AbortError"));
				});

				const tx = leagueDB.transaction(storesInput);
				for (const store of storesInput) {
					if (store === "gameAttributes") {
						numRecordsTotal += 1;
					} else {
						numRecordsTotal += await tx.objectStore(store).count();
					}
				}

				await controller.enqueue(
					`{${newline}${tab}"version":${space}${LEAGUE_DATABASE_VERSION}`,
				);

				// The meta object is only read when importing: name sets the league
				// name, syncCheckpoint fast-forwards the room catch-up (see above).
				if (name || syncCheckpoint) {
					await writeRootObject(controller, "meta", {
						...(name ? { name } : {}),
						...(syncCheckpoint ? { syncCheckpoint } : {}),
					});
				}
			},

			async pull(controller) {
				// console.log("PULL", controller.desiredSize / 1024 / 1024);
				const done = () => {
					if (cancelCallback) {
						cancelCallback();
					}

					controller.close();

					leagueDB.close();
				};

				if (cancelCallback || abortSignal?.aborted) {
					done();
					return;
				}

				// let count = 0;
				let size = 0;

				const enqueue = (string: string) => {
					size += stringSizeInBytes(string);
					controller.enqueue(string);
				};

				const store = stores[storeIndex]!;
				if (onProcessingStore && store !== prevStore) {
					onProcessingStore(store);
				}

				// Define this up here so it is undefined for gameAttributes, triggering the "go to next store" logic at the bottom
				let cursor:
					| IDBPCursorWithValue<LeagueDB, any, any, unknown, "readonly">
					| null
					| undefined;

				if (store === "gameAttributes") {
					// gameAttributes is special because we need to convert it into an object
					let rows = (await leagueDB.getAll(store)).filter(
						(row) => !gameAttributesCache.includes(row.key),
					);

					if (filter[store]) {
						rows = rows.filter(filter[store]);
					}

					// No need to include settings that don't apply to this sport

					rows = rows.filter(
						(row) => !gameAttributesKeysOtherSports.has(row.key),
					);

					if (forEach[store]) {
						for (const row of rows) {
							forEach[store](row);
						}
					}

					if (map[store]) {
						rows = rows.map(map[store]);
					}

					const gameAttributesObject = gameAttributesArrayToObject(rows);

					await writeRootObject(
						controller,
						"gameAttributes",
						gameAttributesObject,
					);

					incrementNumRecordsSeen();
				} else {
					const txStores: StoreNames<LeagueDB>[] =
						store === "teams" ? ["teams", "teamSeasons", "teamStats"] : [store];

					const transaction = leagueDB.transaction(txStores);

					const range =
						prevKey !== undefined
							? IDBKeyRange.lowerBound(prevKey, true)
							: undefined;
					cursor = await transaction.objectStore(store).openCursor(range);
					while (cursor) {
						let value = cursor.value;

						if (!filter[store] || filter[store](value)) {
							// count += 1;

							const enqueuedFirst = enqueuedFirstRecord.has(store);
							const comma = enqueuedFirst ? "," : "";

							if (!enqueuedFirst) {
								enqueue(`,${newline}${tab}"${store}": [`);
								enqueuedFirstRecord.add(store);
							}

							if (forEach[store]) {
								forEach[store](value);
							}
							if (store === "players") {
								if (value.imgURL) {
									delete value.face;
								}
							}

							if (map[store]) {
								value = map[store](value);
							}

							if (store === "teams" && includeTeamSeasonsAndStats) {
								// This is a bit dangerous, since it will possibly read all teamStats/teamSeasons rows into memory, but that will very rarely exceed MIN_RECORDS_PER_PULL and we will just do one team per transaction, to be safe.

								const tid = value.tid;

								type Info<Store extends "teamSeasons" | "teamStats"> = {
									key: string;
									store: Store;
									index: keyof LeagueDB[Store]["indexes"];
									keyRange: IDBKeyRange;
								};
								const infos: (Info<"teamSeasons"> | Info<"teamStats">)[] = [
									{
										key: "seasons",
										store: "teamSeasons",
										index: "tid, season",
										keyRange: IDBKeyRange.bound([tid], [tid, ""]),
									},
									{
										key: "stats",
										store: "teamStats",
										index: "tid",
										keyRange: IDBKeyRange.only(tid),
									},
								];

								for (const info of infos) {
									const index = transaction
										.objectStore(info.store)
										.index(info.index as any);
									value[info.key] = await getAll(index, info.keyRange);
									incrementNumRecordsSeen(value[info.key].length);
								}
							}

							enqueue(
								`${comma}${newline}${tab.repeat(2)}${jsonStringify(value, 2)}`,
							);
						}

						incrementNumRecordsSeen();

						prevKey = cursor.key as any;

						const desiredSize = controller.desiredSize;
						if (
							desiredSize !== null &&
							(desiredSize > 0 || size < minSizePerPull) &&
							!cancelCallback &&
							!abortSignal?.aborted
						) {
							// Keep going if desiredSize or minSizePerPull want us to
							cursor = await cursor.continue();
						} else {
							break;
						}
					}
				}

				// console.log("PULLED", count, size / 1024 / 1024);
				if (!cursor) {
					// Actually done with this store - we didn't just stop due to desiredSize
					storeIndex += 1;
					prevKey = undefined;
					if (enqueuedFirstRecord.has(store)) {
						enqueue(`${newline}${tab}]`);
					} else {
						// Ensure we don't ever enqueue nothing, in which case the stream can get stuck
						enqueue("");
					}

					if (storeIndex >= stores.length) {
						// Done whole export!
						await controller.enqueue(`${newline}}${newline}`);

						done();
					}
				}
			},
			cancel() {
				return new Promise((resolve) => {
					cancelCallback = resolve;
				});
			},
		},
		{
			highWaterMark,
			size: stringSizeInBytes,
		},
	);
};

export default makeExportStream;
