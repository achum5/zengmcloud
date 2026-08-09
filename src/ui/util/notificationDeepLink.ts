// Routing a tapped phone notification to the page it is about.
//
// WHY THE SERVICE WORKER CANNOT JUST NAVIGATE US. The obvious move - and what
// this used to do - is `client.navigate(url)` from the notificationclick
// handler. But navigate() rejects with a TypeError unless the window is
// controlled by the service worker calling it, and the push worker is
// deliberately registered at its own scope (/firebase-cloud-messaging-push-scope)
// so it can't collide with the app's Workbox worker at "/". Every app window is
// therefore uncontrolled from its point of view, navigate() always threw, the
// error was swallowed, and only the focus() survived - which is exactly the
// "tapping a notification just reopens whatever page I was already on" symptom.
//
// postMessage has no such requirement, so the worker hands the destination to
// the page and the page routes itself, in-app, with no reload.

export const NOTIFICATION_CLICK_MESSAGE = "zengm-notification-click";

export type NotificationClickMessage = {
	type: typeof NOTIFICATION_CLICK_MESSAGE;
	// League-relative, e.g. "game_log/ATL/2007/42". Never has a leading "/l/{lid}".
	path?: string;
	// The worker's own best guess at an absolute URL, built from the league id it
	// cached. Only used when this window can't work out a league id itself.
	url?: string;
};

// The league this window is looking at. Preferred over the worker's cached id:
// the cache is a fallback for a cold start and can name a league this window has
// since navigated away from.
const lidFromLocation = (pathname: string): string | undefined =>
	/^\/l\/(\d+)(\/|$)/.exec(pathname)?.[1];

// Where a click should actually take this window, or undefined for "nowhere
// useful" - in which case the tap just focuses the app, as before.
export const resolveDeepLink = (
	message: NotificationClickMessage,
	pathname: string,
): string | undefined => {
	const path = message.path?.replace(/^\/+/, "");
	if (path) {
		const lid = lidFromLocation(pathname);
		if (lid !== undefined) {
			return `/l/${lid}/${path}`;
		}
	}
	// No path, or this window isn't in a league (the dashboard, say) - fall back
	// to whatever the worker resolved, which may still be a valid league URL.
	return message.url && message.url !== "/" ? message.url : undefined;
};

export const isNotificationClickMessage = (
	data: unknown,
): data is NotificationClickMessage =>
	typeof data === "object" &&
	data !== null &&
	(data as { type?: unknown }).type === NOTIFICATION_CLICK_MESSAGE;

export const initNotificationDeepLinks = () => {
	if (typeof navigator === "undefined" || !navigator.serviceWorker) {
		return;
	}

	navigator.serviceWorker.addEventListener("message", async (event) => {
		if (!isNotificationClickMessage(event.data)) {
			return;
		}

		const url = resolveDeepLink(event.data, window.location.pathname);
		if (url === undefined || url === window.location.pathname) {
			return;
		}

		// A full load would throw away the loaded league and take seconds on a
		// phone, so route in-app. If this window is on a different league than the
		// link (or outside one entirely), resolveDeepLink hands back the worker's
		// absolute URL and a real navigation is the only correct thing.
		const sameLeague =
			lidFromLocation(url) !== undefined &&
			lidFromLocation(url) === lidFromLocation(window.location.pathname);
		if (sameLeague) {
			// Imported here rather than at the top so this module stays free of the
			// view layer and its helpers can be unit-tested on their own.
			const { realtimeUpdate } = await import("./realtimeUpdate.ts");
			await realtimeUpdate([], url);
		} else {
			window.location.href = url;
		}
	});
};
