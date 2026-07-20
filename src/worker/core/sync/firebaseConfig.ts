// Re-exported from common/ so both the worker and UI threads share one source
// of truth (the UI thread needs it for Cloud Messaging - see pushNotifications).
export { firebaseConfig, vapidKey } from "../../../common/firebaseConfig.ts";
export type { FirebaseConfig } from "../../../common/firebaseConfig.ts";
