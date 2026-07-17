import { initializeApp, getApps, type FirebaseApp } from "firebase/app";
import { getMessaging, getToken, isSupported } from "firebase/messaging";
import { firebaseConfig, vapidKey } from "../../common/firebaseConfig.ts";
import { safeLocalStorage } from "./safeLocalStorage.ts";
import { toWorker } from "./toWorker.ts";
import { PUSH_ENABLED_KEY, PUSH_NAME_KEY } from "./pushNotificationsShared.ts";

// Phone push notifications, driven from the UI thread. Firebase Cloud Messaging
// can only run in a window context (it needs a service worker and the
// Notification API), so token acquisition happens here - NOT in the game's
// SharedWorker. The token is then handed to the worker, which stores it in the
// league room for the Cloud Function to deliver to.
//
// This module is imported only by the Multiplayer Sync page, so firebase/messaging
// (a large dependency) is not pulled into the bundle for anyone who never opens
// that page.

// FCM registers its own service worker at this dedicated scope, so it never
// clobbers the app's main Workbox service worker (which controls "/").
const FCM_SW_URL = "/firebase-messaging-sw.js";
const FCM_SW_SCOPE = "/firebase-cloud-messaging-push-scope";

// Remember that the user turned push on (and under what name) so we can silently
// re-register after a refresh, which rotates nothing but re-asserts the token.
const ENABLED_KEY = PUSH_ENABLED_KEY;
const NAME_KEY = PUSH_NAME_KEY;

const getApp = (): FirebaseApp =>
	getApps().length > 0 ? getApps()[0]! : initializeApp(firebaseConfig);

// Whether this browser can do web push at all. On iPhone this is only true once
// the site has been added to the Home Screen (installed as a PWA).
export const pushSupported = async (): Promise<boolean> => {
	try {
		return (
			typeof Notification !== "undefined" &&
			"serviceWorker" in navigator &&
			(await isSupported())
		);
	} catch {
		return false;
	}
};

export const pushConfigured = (): boolean => vapidKey !== "";

export const getPushPermission = (): NotificationPermission =>
	typeof Notification !== "undefined" ? Notification.permission : "denied";

export const getStoredPushName = (): string =>
	safeLocalStorage.getItem(NAME_KEY) ?? "";

const registerToken = async (name: string): Promise<string> => {
	const registration = await navigator.serviceWorker.register(FCM_SW_URL, {
		scope: FCM_SW_SCOPE,
	});
	const messaging = getMessaging(getApp());
	const token = await getToken(messaging, {
		vapidKey,
		serviceWorkerRegistration: registration,
	});
	if (!token) {
		throw new Error("Could not obtain a push token from the browser.");
	}
	await toWorker("main", "registerPushToken", { token, name });
	return token;
};

// Turn on push for this device: ask permission, get a token, register it with
// the room. Throws a human-readable error the settings page can display.
export const enablePushNotifications = async (name = ""): Promise<void> => {
	if (!pushConfigured()) {
		throw new Error(
			"Push notifications aren't set up on the server yet (missing VAPID key). See docs/PUSH_NOTIFICATIONS_SETUP.md.",
		);
	}
	if (!(await pushSupported())) {
		throw new Error(
			"This browser can't do push notifications. On iPhone, first add this site to your Home Screen (Share → Add to Home Screen), then open it from there.",
		);
	}

	const permission = await Notification.requestPermission();
	if (permission !== "granted") {
		throw new Error(
			"Notification permission was blocked. Enable notifications for this site in your browser settings, then try again.",
		);
	}

	await registerToken(name);

	safeLocalStorage.setItem(ENABLED_KEY, "1");
	safeLocalStorage.setItem(NAME_KEY, name);
};

// Silently re-assert the token, if push was previously enabled and permission is
// still granted. Safe to call on every page load / foreground; does nothing
// unless everything is already in place. Returns true only when the token was
// actually re-registered with the room, so callers can retry (registration
// throws until the sync engine is connected to a shared league).
export const restorePushNotifications = async (): Promise<boolean> => {
	if (
		!pushConfigured() ||
		safeLocalStorage.getItem(ENABLED_KEY) !== "1" ||
		getPushPermission() !== "granted"
	) {
		return false;
	}
	if (!(await pushSupported())) {
		return false;
	}
	try {
		await registerToken(getStoredPushName() || "A league-mate");
		return true;
	} catch {
		// Best-effort; not connected to a shared league yet, or a transient
		// failure. The caller may retry, and the user can re-enable manually.
		return false;
	}
};
