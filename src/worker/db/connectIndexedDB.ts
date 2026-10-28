import { openDB } from "@dumbmatter/idb";
import type {
	IDBPDatabase,
	IDBPTransaction,
	StoreNames,
} from "@dumbmatter/idb";
import { WEBSITE_ROOT } from "../../common/constants.ts";
import { logEvent, toUI } from "../util/index.ts";

// A transient IndexedDB transaction hiccup: the transaction went INACTIVE
// before one of its requests could run. WebKit does this under write pressure,
// and iOS aggressively kills in-flight transactions when a PWA is backgrounded
// mid-write ("Attempt to get a record from database without an in-progress
// transaction"). The failed operation here is always best-effort and self-heals
// on the next attempt - the per-second league heartbeat, the phase-text cache,
// and the multiplayer sync watermark all re-run - so it must NOT be surfaced as
// a persistent, scary error toast. Worse, rethrowing it (below) forces the
// transaction to abort, which pops a SECOND toast. We detect these and keep them
// console-only: no toast, no rethrow.
export const isTransientTransactionError = (error: any): boolean => {
	if (!error) {
		return false;
	}
	if (
		error.name === "TransactionInactiveError" ||
		error.name === "InvalidStateError"
	) {
		return true;
	}
	const message = typeof error.message === "string" ? error.message : "";
	return /in-progress transaction|transaction (is )?(not active|inactive|has finished|is finished)/i.test(
		message,
	);
};

// An operation aborted is usually nothing wrong at all: a transaction the
// game cancelled itself (sync catching up, replacing stale work), or one the
// browser cut off as the app went into the background on a phone. Only when
// storage really is all but full is it worth telling anybody about.
export const isAbortError = (error: any): boolean =>
	!!error &&
	(error.name === "AbortError" ||
		(typeof error.message === "string" &&
			/operation was aborted/i.test(error.message)));
const storageNearlyFull = async (): Promise<boolean> => {
	try {
		const storage = (globalThis as any).navigator?.storage;
		if (!storage?.estimate) {
			return false;
		}
		const { usage, quota } = (await storage.estimate()) as {
			usage?: number;
			quota?: number;
		};
		return (
			usage !== undefined &&
			quota !== undefined &&
			quota > 0 &&
			usage / quota > 0.9
		);
	} catch {
		return false;
	}
};

// If duplicate message is sent multiple times in a row (like IndexedDB transaction abort with many open requests), only show one
// ReturnType rather than number because bbgm-rosters type checks this with Node types
const debounceMessagesStore = new Map<string, ReturnType<typeof setTimeout>>();
const stopBecauseDebounce = (text: string) => {
	const timeoutID = debounceMessagesStore.get(text);
	if (timeoutID === undefined) {
		const newTimeoutID = setTimeout(() => {
			debounceMessagesStore.delete(text);
		}, 1000);
		debounceMessagesStore.set(text, newTimeoutID);
		return false;
	}
	return true;
};

const connectIndexedDB = async <DBTypes>({
	name,
	version,
	create,
	migrate,
	lid,
}: {
	name: string;
	version: number;
	lid: number;
	create: (db: IDBPDatabase<DBTypes>) => void;
	migrate: (a: {
		db: IDBPDatabase<DBTypes>;
		lid: number;
		oldVersion: number;
		transaction: IDBPTransaction<
			DBTypes,
			StoreNames<DBTypes>[],
			"versionchange"
		>;
	}) => Promise<void>;
}) => {
	const db = await openDB<DBTypes>(name, version, {
		async upgrade(db, oldVersion, newVerison, transaction) {
			try {
				if (oldVersion === 0) {
					create(db);
				} else {
					await migrate({ db, lid, oldVersion, transaction });
				}
			} catch (error) {
				void toUI("bugsnagNotify", [error]);
				console.log(error);
				transaction.abort();
			}
		},
		blocked() {
			logEvent({
				type: "error",
				text: "Please close any other open tabs.",
				saveToDb: false,
			});
		},
		blocking() {
			db.close();
		},
		terminated() {
			logEvent({
				type: "error",
				text: "Something bad happened. Please try restarting your browser.",
				saveToDb: false,
				persistent: true,
			});
		},
	});

	const quotaErrorMessage = `browser isn't letting the game store any more data!<br><br>Try <a href="/">deleting some old leagues</a> or deleting old data (Tools > Delete Old Data within a league). Clearing space elsewhere on your hard drive might help too. <a href="https://${WEBSITE_ROOT}/manual/debugging/quota-errors/"><b>Read this for more info.</b></a>`;

	// (Aborted: a word only if storage really is all but full.)
	const warnIfFull = () => {
		void storageNearlyFull().then((full) => {
			const text = `Your ${quotaErrorMessage}`;
			if (full && !stopBecauseDebounce(text)) {
				logEvent({
					type: "error",
					text,
					saveToDb: false,
					persistent: true,
				});
			}
		});
	};

	db.addEventListener("abort", (event: any) => {
		console.log(`${name} database abort event`, event.target.error);

		// A transaction that aborted because it went inactive under load self-heals
		// on the next attempt; don't nag the user about it.
		if (isTransientTransactionError(event.target.error)) {
			return;
		}
		if (isAbortError(event.target.error)) {
			warnIfFull();
			return;
		}

		let text: string | undefined;
		if (
			event.target.error &&
			event.target.error.name === "QuotaExceededError"
		) {
			text = `Your ${quotaErrorMessage}`;
		} else if (event.target.error) {
			text = `${name} database abort event: ${event.target.error.message}<br><br>Maybe your ${quotaErrorMessage}`;
		}

		if (text && !stopBecauseDebounce(text)) {
			logEvent({
				type: "error",
				text,
				saveToDb: false,
				persistent: true,
			});
		}

		if (event.target.error) {
			throw event.target.error;
		}
	});
	db.addEventListener("error", (event: any) => {
		console.log(`${name} database error event`, event.target.error);

		// A transient inactive-transaction error self-heals on the next attempt.
		// Rethrowing it here (below) would force the transaction to abort and pop a
		// SECOND toast, so bail out entirely: console-only, no toast, no rethrow.
		if (isTransientTransactionError(event.target.error)) {
			return;
		}
		if (isAbortError(event.target.error)) {
			warnIfFull();
			return;
		}

		if (event.target.error) {
			let text: string;
			if (event.target.error.message.includes("abort")) {
				text = `${name} database error event: ${event.target.error.message}<br><br>Maybe your ${quotaErrorMessage}`;
			} else {
				text = `${name} database error event: ${event.target.error.message}`;
			}

			if (!stopBecauseDebounce(text)) {
				logEvent({
					type: "error",
					text,
					saveToDb: false,
					persistent: true,
				});
			}

			throw event.target.error;
		}
	});

	return db;
};

export default connectIndexedDB;
