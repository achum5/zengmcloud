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
	const url = data.url || "/";

	event.waitUntil(
		self.registration.showNotification(title, {
			body,
			// Collapse repeats of the same kind so a phone doesn't stack duplicates.
			tag: data.tag || "zengm-sync",
			renotify: true,
			data: { url },
		}),
	);
});

self.addEventListener("notificationclick", (event) => {
	event.notification.close();
	const url = (event.notification.data && event.notification.data.url) || "/";

	event.waitUntil(
		self.clients
			.matchAll({ type: "window", includeUncontrolled: true })
			.then((clientsArr) => {
				// Focus an existing ZenGM tab if one is open; otherwise open one.
				for (const client of clientsArr) {
					if ("focus" in client) {
						client.focus();
						if ("navigate" in client && url !== "/") {
							client.navigate(url).catch(() => {});
						}
						return undefined;
					}
				}
				if (self.clients.openWindow) {
					return self.clients.openWindow(url);
				}
				return undefined;
			}),
	);
});
