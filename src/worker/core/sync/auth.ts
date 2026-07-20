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

export const ensureAnonymousAuth = async (): Promise<string> => {
	const app = getFirebaseApp();
	const auth = getAuth(app);

	if (auth.currentUser) {
		return auth.currentUser.uid;
	}

	let pending = pendingByApp.get(app.name);
	if (!pending) {
		pending = signInAnonymously(auth)
			.then((cred) => cred.user.uid)
			.catch((error) => {
				pendingByApp.delete(app.name);
				throw error;
			});
		pendingByApp.set(app.name, pending);
	}

	return pending;
};
