import { safeLocalStorage } from "./safeLocalStorage.ts";

// Lightweight localStorage keys for push notifications, split out so the
// bootstrap keep-fresh hook can read the "enabled" flag WITHOUT importing
// pushNotifications.ts (which statically pulls in the large firebase/messaging
// dependency). That dep must only load for devices that actually turned push on.
export const PUSH_ENABLED_KEY = "pushNotificationsEnabled";
export const PUSH_NAME_KEY = "pushNotificationsName";

// Whether this device has push turned on: the flag says so - or, with it gone
// (the app's storage cleared, say, by the phone), the browser still lets this
// site notify, which only ever comes of turning it on here (see
// enablePushNotifications). Read without the Firebase SDK.
export const pushWanted = (): boolean =>
	safeLocalStorage.getItem(PUSH_ENABLED_KEY) === "1" ||
	(typeof Notification !== "undefined" &&
		Notification.permission === "granted");
