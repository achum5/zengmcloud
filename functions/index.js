// Cloud Function: deliver ZenGM league push notifications to phones.
//
// It triggers whenever a client writes a doc to leagues/{code}/notifications
// (the acting device enqueues one after a trade, roster move, sim, or phase
// change - see src/worker/core/sync/notifications.ts). It looks up the room's
// registered devices, skips the author, optionally filters by target team, and
// sends a data-only FCM push to the rest. Because the push is delivered by
// Google's servers, it reaches phones even when ZenGM is completely closed.
//
// Deploy with: firebase deploy --only functions
// (Requires the Blaze plan; at a few users the cost is effectively $0.)

const { onDocumentCreated } = require("firebase-functions/v2/firestore");
const { initializeApp } = require("firebase-admin/app");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");
const { getMessaging } = require("firebase-admin/messaging");

initializeApp();

// FCM error codes that mean a token is permanently dead and should be dropped.
const DEAD_TOKEN_ERRORS = new Set([
	"messaging/registration-token-not-registered",
	"messaging/invalid-registration-token",
	"messaging/invalid-argument",
]);

exports.sendLeagueNotification = onDocumentCreated(
	"leagues/{code}/notifications/{notificationId}",
	async (event) => {
		const snapshot = event.data;
		if (!snapshot) {
			return;
		}
		const notification = snapshot.data();
		const { code } = event.params;

		const db = getFirestore();
		const membersSnapshot = await db
			.collection("leagues")
			.doc(code)
			.collection("members")
			.get();

		// null targetTids means "everyone in the room".
		const targetTids = Array.isArray(notification.targetTids)
			? notification.targetTids
			: null;

		const tokens = [];
		const tokenToRef = new Map();
		membersSnapshot.forEach((doc) => {
			const member = doc.data();
			if (!member.fcmToken) {
				return;
			}
			// Don't notify the person who made the change.
			if (doc.id === notification.authorId) {
				return;
			}
			// Team targeting (unused in v1, but supported): only ping the managers
			// of the affected teams.
			if (targetTids && !targetTids.includes(member.tid)) {
				return;
			}
			tokens.push(member.fcmToken);
			tokenToRef.set(member.fcmToken, doc.ref);
		});

		if (tokens.length === 0) {
			return;
		}

		// Data-only payload: the service worker (public/firebase-messaging-sw.js)
		// reads this and shows the notification. Sending it as `data` (not
		// `notification`) keeps display fully in our control and avoids duplicates.
		// `path` is a league-relative deep link the SW resolves against the
		// recipient's own lid.
		const message = {
			data: {
				title: String(notification.title || "ZenGM"),
				body: String(notification.body || ""),
				path: String(notification.path || ""),
			},
			tokens,
		};

		const response = await getMessaging().sendEachForMulticast(message);

		// Garbage-collect tokens FCM reports as permanently invalid, so dead
		// devices don't pile up in the room.
		const staleUpdates = [];
		response.responses.forEach((result, i) => {
			if (
				!result.success &&
				result.error &&
				DEAD_TOKEN_ERRORS.has(result.error.code)
			) {
				const ref = tokenToRef.get(tokens[i]);
				if (ref) {
					staleUpdates.push(
						ref
							.update({ fcmToken: FieldValue.delete() })
							.catch(() => undefined),
					);
				}
			}
		});
		await Promise.all(staleUpdates);
	},
);
