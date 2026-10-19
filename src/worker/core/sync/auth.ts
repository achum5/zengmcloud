import { getAuth, signInAnonymously } from "firebase/auth";
import { getFirebaseApp } from "./firebaseApp.ts";

// Sign in anonymously and return this device's stable Firebase uid. We use the
// uid as the sync clientId, which (a) lets Firestore rules require that a change
// is authored by the account that wrote it, and (b) persists across refreshes
// via Firebase's auth persistence. If persistence falls back to in-memory (some
// worker contexts), a new uid per session is harmless - catch-up re-applies our
// own past changes idempotently.
// Keyed by Firebase app name so a bring-your-own-Firestore app and the default
// app never share (or clobber) each other's in-flight sign-in. With only the
// default app present this is a one-entry map - identical to the single variable
// it replaced.
const pendingByApp = new Map<string, Promise<string>>();
const SIGN_IN_MS = 20_000;

export const ensureAnonymousAuth = async (): Promise<string> => {
	const app = getFirebaseApp();
	const auth = getAuth(app);

	if (auth.currentUser) {
		return auth.currentUser.uid;
	}

	let pending = pendingByApp.get(app.name);
	if (!pending) {
		// A sign-in that never answers is let go of too, not kept as the one
		// every later connect waits on until the page is reloaded. (If it does
		// land late, auth.currentUser has it for the next try.)
		const attempt: Promise<string> = new Promise<string>((resolve, reject) => {
			const timer = setTimeout(() => {
				reject(new Error("Timed out signing in"));
			}, SIGN_IN_MS);
			signInAnonymously(auth).then(
				(cred) => {
					clearTimeout(timer);
					resolve(cred.user.uid);
				},
				(error) => {
					clearTimeout(timer);
					reject(error);
				},
			);
		}).catch((error) => {
			if (pendingByApp.get(app.name) === attempt) {
				pendingByApp.delete(app.name);
			}
			throw error;
		});
		pending = attempt;
		pendingByApp.set(app.name, pending);
	}

	return pending;
};
