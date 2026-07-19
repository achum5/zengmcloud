import { openDB } from "@dumbmatter/idb";
import type {
	IDBPDatabase,
	IDBPTransaction,
	StoreNames,
} from "@dumbmatter/idb";
import { WEBSITE_ROOT } from "../../common/constants.ts";
import { logEvent } from "../util/index.ts";

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

// If duplicate message is sent multiple times in a row (like IndexedDB transaction abort with many open requests), only show one
const debounceMessagesStore = new Map<string, number>();
const stopBecauseDebounce = (text: string) => {
	const timeoutID = debounceMessagesStore.get(text);
	if (timeoutID === undefined) {
		const newTimeoutID = self.setTimeout(() => {
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
			if (oldVersion === 0) {
				create(db);
			} else {
				await migrate({ db, lid, oldVersion, transaction });
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

	db.addEventListener("abort", (event: any) => {
		console.log(`${name} database abort event`, event.target.error);

		// A transaction that aborted because it went inactive under load self-heals
		// on the next attempt; don't nag the user about it.
		if (isTransientTransactionError(event.target.error)) {
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
