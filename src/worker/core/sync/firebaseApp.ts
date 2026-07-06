import { initializeApp, getApps, type FirebaseApp } from "firebase/app";
import { firebaseConfig } from "./firebaseConfig.ts";

// Single shared Firebase app instance, so Auth and Firestore share the same
// auth context (they must be on the same app, in the same thread).
export const getFirebaseApp = (): FirebaseApp => {
	const apps = getApps();
	return apps.length > 0 ? apps[0]! : initializeApp(firebaseConfig);
};
