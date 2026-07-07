// Persist the active league id where the push service worker can read it, so a
// tapped notification can deep-link into the right league even when the app is
// fully closed (no open tab to read the lid from). Kept in sync with the
// LID_CACHE / LID_KEY constants in public/firebase-messaging-sw.js.
//
// Deliberately dependency-free (no Firebase import) so Controller can call it
// without pulling firebase/messaging into the main bundle.

const LID_CACHE = "zengm-push";
const LID_KEY = "/__push_lid";

let remembered: number | undefined;

export const rememberLidForPush = (lid: number) => {
	if (remembered === lid || typeof caches === "undefined") {
		return;
	}
	remembered = lid;
	caches
		.open(LID_CACHE)
		.then((cache) => cache.put(LID_KEY, new Response(String(lid))))
		.catch(() => {
			// Best-effort; deep links just fall back to the app root without it.
		});
};
