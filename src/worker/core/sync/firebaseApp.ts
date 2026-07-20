import { initializeApp, getApp, type FirebaseApp } from "firebase/app";
import { firebaseConfig, type FirebaseConfig } from "./firebaseConfig.ts";

// The Firebase project this device's sync is currently pointed at. Defaults to
// the built-in project, so with no bring-your-own-Firestore config every path
// below behaves exactly as it did before this existed. A connect sets it (to a
// custom project or back to the default) BEFORE the first Firebase touch; see
// connect.ts.
let activeConfig: FirebaseConfig = firebaseConfig;

export const setActiveFirebaseConfig = (config?: FirebaseConfig | null) => {
	activeConfig = config ?? firebaseConfig;
};

export const getActiveFirebaseConfig = (): FirebaseConfig => activeConfig;

// The default project uses the unnamed default Firebase app (unchanged
// behavior). A bring-your-own project uses an app named after its projectId, so
// the two never collide and switching projects yields a distinct app + auth
// context (Auth and Firestore must share one app).
const appNameFor = (config: FirebaseConfig): string | undefined =>
	config.projectId === firebaseConfig.projectId
		? undefined
		: `byo-${config.projectId}`;

// Single Firebase app instance per project, memoized by Firebase's own app
// registry. getApp() throws when the (named or default) app doesn't exist yet,
// which is our signal to initialize it.
export const getFirebaseApp = (): FirebaseApp => {
	const config = activeConfig;
	const name = appNameFor(config);

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
