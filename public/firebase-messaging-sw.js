/*
 * Firebase Cloud Messaging service worker for ZenGM phone notifications.
 *
 * Deliberately dependency-free: it does NOT import the Firebase SDK. The Cloud
 * Function sends data-only messages, which arrive here as a raw "push" event, so
 * all we need is to show the notification and handle clicks. This keeps the file
 * tiny, avoids loading Firebase from a CDN inside the worker, and can't conflict
 * with the app's main Workbox service worker (this one is registered at the
 * dedicated /firebase-cloud-messaging-push-scope scope).
 *
 * It must live at the site root (public/ is served from /) so it can be
 * registered as /firebase-messaging-sw.js.
 */

// Where the app stashes the current league id so a deep link can be resolved
// even when the app is fully closed. Kept in sync with src/ui/util/pushLid.ts.
const LID_CACHE = "zengm-push";
const LID_KEY = "/__push_lid";

self.addEventListener("push", (event) => {
	let payload = {};
	try {
		payload = event.data ? event.data.json() : {};
	} catch {
		payload = {};
	}

	// Support both data-only messages (what our Cloud Function sends) and the
	// FCM "notification" envelope, just in case.
	const data = payload.data || {};
	const notification = payload.notification || {};

	const title = data.title || notification.title || "ZenGM";
	const body = data.body || notification.body || "";
	// League-relative path (no leading /l/{lid}); resolved at click time.
	const path = data.path || "";

	event.waitUntil(
		self.registration.showNotification(title, {
			body,
			// Collapse repeats of the same kind so a phone doesn't stack duplicates.
			tag: data.tag || "zengm-sync",
			renotify: true,
			data: { path },
		}),
	);
});

// The recipient's own league id: prefer an already-open ZenGM tab's URL, then
// fall back to the value the app remembered in the cache.
async function resolveLid(clientsArr) {
	for (const client of clientsArr) {
		const match = /\/l\/(\d+)(\/|$)/.exec(client.url || "");
		if (match) {
			return match[1];
		}
	}
	try {
		const cache = await caches.open(LID_CACHE);
		const res = await cache.match(LID_KEY);
		if (res) {
			const lid = (await res.text()).trim();
			if (lid) {
				return lid;
			}
		}
	} catch {
		// No remembered lid; fall through to the app root.
	}
	return undefined;
}

// Kept in sync with NOTIFICATION_CLICK_MESSAGE in src/ui/util/notificationDeepLink.ts.
const CLICK_MESSAGE = "zengm-notification-click";

self.addEventListener("notificationclick", (event) => {
	event.notification.close();
	const path = (event.notification.data && event.notification.data.path) || "";

	event.waitUntil(
		(async () => {
			const clientsArr = await self.clients.matchAll({
				type: "window",
				includeUncontrolled: true,
			});
			const lid = await resolveLid(clientsArr);
			const url = lid && path ? `/l/${lid}/${path}` : "/";

			// Focus an existing ZenGM tab and let IT do the routing.
			//
			// NOT client.navigate(): that rejects with a TypeError unless the window
			// is controlled by the worker calling it, and this worker lives at its
			// own scope (see the header comment) so it controls none of them. It
			// threw on every tap, the catch swallowed it, and the focus alone is why
			// tapping a notification only ever reopened the current page.
			// postMessage carries no such restriction.
			for (const client of clientsArr) {
				if ("focus" in client) {
					await client.focus();
					if (path) {
						client.postMessage({ type: CLICK_MESSAGE, path, url });
					}
					return;
				}
			}
			// Cold start: no window to talk to, so the URL has to carry the
			// destination. This is the one case navigate()'s replacement can't cover.
			if (self.clients.openWindow) {
				await self.clients.openWindow(url);
			}
		})(),
	);
});
