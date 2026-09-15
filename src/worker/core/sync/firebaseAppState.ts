import { firebaseConfig, type FirebaseConfig } from "./firebaseConfig.ts";

// WHICH Firebase project this device's sync is pointed at - deliberately kept in
// a module that does NOT import the Firebase SDK.
//
// The SDK is ~428 KB of the worker bundle and a single-player league never
// touches a byte of it, so everything that imports `firebase/*` sits behind one
// dynamic import (see loadSyncBackend.ts). The config, though, is read and
// written on the ordinary connect path before any of that loads - so it lives
// here, where importing it costs nothing.
//
// Defaults to the built-in project, so with no bring-your-own-Firestore config
// every path behaves exactly as it did before this existed. A connect sets it
// (to a custom project or back to the default) BEFORE the first Firebase touch.
let activeConfig: FirebaseConfig = firebaseConfig;

export const setActiveFirebaseConfig = (config?: FirebaseConfig | null) => {
	activeConfig = config ?? firebaseConfig;
};

export const getActiveFirebaseConfig = (): FirebaseConfig => activeConfig;

// The default project uses the unnamed default Firebase app (unchanged
// behavior). A bring-your-own project uses an app named after its projectId, so
// the two never collide and switching projects yields a distinct app + auth
// context (Auth and Firestore must share one app).
export const firebaseAppNameFor = (
	config: FirebaseConfig,
): string | undefined =>
	config.projectId === firebaseConfig.projectId
		? undefined
		: `byo-${config.projectId}`;
