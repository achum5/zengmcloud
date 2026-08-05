import { toWorker } from "./toWorker.ts";

// The moment a user looks at the app again - tab switch, window focus, a PWA
// resumed from the background - is exactly when the browser's parked network
// leaves the screen stale (Safari suspends Firestore's stream for hidden
// tabs, and the worker's timers may have been throttled too). Kick the sync
// engine RIGHT THEN instead of waiting for the next health tick, so what the
// user sees is current within a second of them looking.
//
// Throttled: the events overlap (visibilitychange + focus + pageshow often
// fire together), and one kick is plenty.
export const initSyncForegroundNudge = () => {
	let lastNudgeAt = 0;
	const nudge = () => {
		const now = Date.now();
		if (now - lastNudgeAt < 2000) {
			return;
		}
		lastNudgeAt = now;
		void toWorker("main", "syncNudge", undefined).catch(() => {
			// Not connected / worker busy - the health tick still covers it.
		});
	};

	document.addEventListener("visibilitychange", () => {
		if (document.visibilityState === "visible") {
			nudge();
		}
	});
	window.addEventListener("focus", nudge);
	// pageshow covers a restore from the back/forward cache, which is how iOS
	// often brings a suspended PWA back.
	window.addEventListener("pageshow", nudge);
};
