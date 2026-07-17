import { safeLocalStorage } from "./safeLocalStorage.ts";
import { PUSH_ENABLED_KEY } from "./pushNotificationsShared.ts";

// FCM push tokens rotate on their own — notably for installed iOS PWAs that
// have been backgrounded for a while. When a token rotates, the server's copy
// goes dead, the delivery Cloud Function garbage-collects it, and the device
// then silently receives NOTHING until it re-registers a fresh token.
//
// Previously the token was only re-asserted when the user opened the
// Multiplayer Sync page, which almost never happens mid-season — so devices
// quietly fell off the notification list. This re-asserts the token far more
// aggressively: once on every app load, and again whenever the app returns to
// the foreground (the iOS PWA resume case, where there is often no page
// reload). registerToken/getToken is idempotent and returns the current token,
// so re-asserting is cheap and simply keeps the room's copy alive.
//
// Gated on the "enabled" flag read from localStorage directly, so the heavy
// firebase/messaging dependency is only dynamically imported for devices that
// actually turned push on — everyone else pays nothing.

// getToken hits the network + service worker, so throttle the foreground-resume
// path; the initial load always forces through.
const MIN_INTERVAL = 30 * 60 * 1000;
let lastRefresh = 0;

// Returns true if the token was actually re-registered (the sync engine was
// connected), false if it was skipped/throttled or the engine wasn't ready.
const refresh = async (force: boolean): Promise<boolean> => {
	if (safeLocalStorage.getItem(PUSH_ENABLED_KEY) !== "1") {
		return false;
	}
	const now = Date.now();
	if (!force && now - lastRefresh < MIN_INTERVAL) {
		return true; // recently refreshed; nothing to do
	}
	lastRefresh = now;
	try {
		const { restorePushNotifications } = await import("./pushNotifications.ts");
		return await restorePushNotifications();
	} catch {
		return false;
	}
};

export const keepPushTokenFresh = () => {
	// On load the sync engine may not be connected to the room yet (registration
	// throws until it is), so retry with backoff until it takes, then stop.
	void (async () => {
		const delays = [0, 8000, 20_000, 45_000, 90_000];
		for (const delay of delays) {
			if (safeLocalStorage.getItem(PUSH_ENABLED_KEY) !== "1") {
				return;
			}
			if (delay > 0) {
				await new Promise((resolve) => {
					setTimeout(resolve, delay);
				});
			}
			if (await refresh(true)) {
				return;
			}
		}
	})();

	// iOS PWAs are usually resumed from the background without a full reload, so
	// a load-time refresh alone would go stale. Re-assert on every foreground.
	document.addEventListener("visibilitychange", () => {
		if (document.visibilityState === "visible") {
			void refresh(false);
		}
	});
	window.addEventListener("focus", () => {
		void refresh(false);
	});
};
