// Opt-in flag for the "bring your own Firestore" multiplayer feature, persisted
// in localStorage and read on startup - mirroring the syncDebugLog flag. It is
// off by default, so the whole feature stays dormant (and multiplayer behaves
// exactly as before) unless a user turns it on. Purely a UI gate: the worker
// only ever points at a custom project when the UI actually passes a config.

const STORAGE_KEY = "byoFirestore";

export const byoFirestoreEnabled = (): boolean => {
	try {
		return localStorage.getItem(STORAGE_KEY) === "1";
	} catch {
		return false;
	}
};

export const setByoFirestoreEnabled = (enabled: boolean) => {
	try {
		if (enabled) {
			localStorage.setItem(STORAGE_KEY, "1");
		} else {
			localStorage.removeItem(STORAGE_KEY);
		}
	} catch {}
};
