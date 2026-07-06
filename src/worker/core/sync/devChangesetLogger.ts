import { toUI } from "../../util/index.ts";
import type { Changeset } from "./changeset.ts";

// Dev-only: print the exact records an action changed. Forwarded to the UI
// thread because the game runs in a SharedWorker, whose console output does not
// appear in the page's DevTools console (see syncLog in ui/api). This is a pure
// function - it does NOT drain the tracker; afterAction does that once and
// passes the changeset in.
export const logChangeset = (label: string, changeset: Changeset) => {
	const n = changeset.changes.length;
	if (n === 0) {
		return;
	}

	const byStore: Record<string, number> = {};
	for (const change of changeset.changes) {
		byStore[change.store] = (byStore[change.store] ?? 0) + 1;
	}

	// Small enough to show every record; for bulk changes (e.g. a sim) just send
	// the per-store summary so we don't ship/flood thousands of records.
	const changes = n <= 40 ? changeset.changes : undefined;
	const text =
		changes === undefined
			? `${label} → ${n} changes (bulk, e.g. sim)`
			: `${label} → ${n} change(s)`;

	// Fire-and-forget; a stale UI without syncLog must never throw here.
	void toUI("syncLog", [{ label: text, byStore, changes }]).catch(() => {});
};
