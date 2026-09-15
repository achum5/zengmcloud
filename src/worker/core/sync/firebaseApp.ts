import { initializeApp, getApp, type FirebaseApp } from "firebase/app";
import {
	firebaseAppNameFor,
	getActiveFirebaseConfig,
} from "./firebaseAppState.ts";

// Loaded only through loadSyncBackend.ts - see the note there before adding an
// importer. Which project it builds an app for is decided by firebaseAppState,
// which the connect path sets before this module ever loads.

// Single Firebase app instance per project, memoized by Firebase's own app
// registry. getApp() throws when the (named or default) app doesn't exist yet,
// which is our signal to initialize it.
export const getFirebaseApp = (): FirebaseApp => {
	const config = getActiveFirebaseConfig();
	const name = firebaseAppNameFor(config);

	if (name === undefined) {
		try {
			return getApp();
		} catch {
			return initializeApp(config);
		}
	}

	try {
		return getApp(name);
	} catch {
		return initializeApp(config, name);
	}
};
