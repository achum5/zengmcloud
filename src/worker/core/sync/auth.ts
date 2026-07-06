import { getAuth, signInAnonymously } from "firebase/auth";
import { getFirebaseApp } from "./firebaseApp.ts";

// Sign in anonymously and return this device's stable Firebase uid. We use the
// uid as the sync clientId, which (a) lets Firestore rules require that a change
// is authored by the account that wrote it, and (b) persists across refreshes
// via Firebase's auth persistence. If persistence falls back to in-memory (some
// worker contexts), a new uid per session is harmless - catch-up re-applies our
// own past changes idempotently.
let pending: Promise<string> | undefined;

export const ensureAnonymousAuth = async (): Promise<string> => {
	const auth = getAuth(getFirebaseApp());

	if (auth.currentUser) {
		return auth.currentUser.uid;
	}

	if (!pending) {
		pending = signInAnonymously(auth)
			.then((cred) => cred.user.uid)
			.catch((error) => {
				pending = undefined;
				throw error;
			});
	}

	return pending;
};
