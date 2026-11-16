// Cloud Function: deliver ZenGM league push notifications to phones.
//
// It triggers whenever a client writes a doc to leagues/{code}/notifications
// (the acting device enqueues one after a trade, roster move, sim, or phase
// change - see src/worker/core/sync/notifications.ts). It looks up the room's
// registered devices, skips the author, optionally filters by target team, and
// sends a data-only FCM push to the rest. Because the push is delivered by
// Google's servers, it reaches phones even when ZenGM is completely closed.
//
// A device in the middle of a live game has its pushes held until the game
// is over, then sent as one (see hold.js, and releaseHeldNotifications
// below).
//
// Deploy with: npx firebase-tools deploy --only functions --project zengmcloud-4a454
// (Requires the Blaze plan; at a few users the cost is effectively $0.)

const {
	onDocumentCreated,
	onDocumentUpdated,
} = require("firebase-functions/v2/firestore");
const { initializeApp } = require("firebase-admin/app");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");
const { getMessaging } = require("firebase-admin/messaging");
const hold = require("./hold.js");

initializeApp();

// FCM error codes that mean a token is permanently dead and should be dropped.
const DEAD_TOKEN_ERRORS = new Set([
	"messaging/registration-token-not-registered",
	"messaging/invalid-registration-token",
	"messaging/invalid-argument",
]);

const errorCode = (error) => error?.code ?? error?.errorInfo?.code;

// One push to one device, dropping its token if FCM says it is dead.
const sendOne = async (ref, token, data) => {
	try {
		await getMessaging().send({ token, data });
	} catch (error) {
		if (DEAD_TOKEN_ERRORS.has(errorCode(error))) {
			await ref
				.update({ fcmToken: FieldValue.delete() })
				.catch(() => undefined);
		}
	}
};

// A push for a device that is holding its pushes, or has some held from a
// hold that ran out without being lifted (a phone that went dark mid game):
// read again in a transaction, so a hold lifted at this very moment either
// sees this one in its list or this sees the hold gone. Kept with the rest
// while the hold lasts; otherwise sent now, with whatever was held.
const sendOrHold = async (db, ref, entry) => {
	const out = await db.runTransaction(async (tx) => {
		const member = (await tx.get(ref)).data();
		if (!member) {
			return undefined;
		}
		const now = Date.now();
		if (hold.isHeld(member, now)) {
			tx.update(ref, { heldPushes: hold.withHeld(member.heldPushes, entry) });
			return undefined;
		}
		const list = hold.heldList(member.heldPushes);
		if (list.length > 0) {
			tx.update(ref, { heldPushes: FieldValue.delete() });
		}
		const data = hold.combine([...list, entry], now);
		return data && member.fcmToken
			? { token: member.fcmToken, data }
			: undefined;
	});
	if (out) {
		await sendOne(ref, out.token, out.data);
	}
};

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

		// ONE MEMBER DOC PER TOKEN.
		//
		// A member doc is keyed by the device's anonymous Firebase uid, but the
		// FCM token belongs to the browser's push subscription, and the two have
		// nothing to do with each other. Lose the auth persistence - clear site
		// data, reinstall the home-screen app, or just let iOS evict the storage
		// of a site you have not opened in a week - and the next registration
		// writes the SAME token under a NEW uid while the old doc keeps it.
		//
		// Nothing ever cleaned that up, so the token went into the send list
		// twice and the phone got every notification twice. The same stale doc
		// also defeats the author skip below: the acting device is only one of
		// its two uids, so it gets pushed its own change.
		//
		// Group by token, keep the most recently registered doc, and clear the
		// token off the rest. Only when every doc in the group can be ordered -
		// a missing updatedAt means we cannot tell which is current, and a wrong
		// guess silently turns someone's notifications off.
		const byToken = new Map();
		membersSnapshot.forEach((doc) => {
			const member = doc.data();
			if (!member.fcmToken) {
				return;
			}
			const group = byToken.get(member.fcmToken) ?? [];
			group.push({ doc, member });
			byToken.set(member.fcmToken, group);
		});

		const supersededRefs = new Set();
		const dedupeUpdates = [];
		for (const group of byToken.values()) {
			if (group.length < 2) {
				continue;
			}
			const stamped = group.filter(
				(entry) => typeof entry.member.updatedAt?.toMillis === "function",
			);
			if (stamped.length !== group.length) {
				continue;
			}
			stamped.sort(
				(a, b) => b.member.updatedAt.toMillis() - a.member.updatedAt.toMillis(),
			);
			for (const entry of stamped.slice(1)) {
				supersededRefs.add(entry.doc.ref.path);
				dedupeUpdates.push(
					entry.doc.ref
						.update({ fcmToken: FieldValue.delete() })
						.catch(() => undefined),
				);
			}
		}
		if (dedupeUpdates.length > 0) {
			await Promise.all(dedupeUpdates);
		}

		const tokens = [];
		const tokenToRef = new Map();
		// Devices in a live game, or with pushes held from one: one at a time,
		// below.
		const holding = [];
		const now = Date.now();
		membersSnapshot.forEach((doc) => {
			const member = doc.data();
			if (!member.fcmToken) {
				return;
			}
			// Just had its token cleared above as a stale duplicate.
			if (supersededRefs.has(doc.ref.path)) {
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
			// Belt and braces: even if the cleanup above declined to pick a winner,
			// never put the same token in one send twice.
			if (tokenToRef.has(member.fcmToken)) {
				return;
			}
			tokenToRef.set(member.fcmToken, doc.ref);
			if (
				hold.isHeld(member, now) ||
				hold.heldList(member.heldPushes).length > 0
			) {
				holding.push(doc.ref);
				return;
			}
			tokens.push(member.fcmToken);
		});

		if (holding.length > 0) {
			const entry = hold.toHeld(notification, now);
			await Promise.all(
				holding.map((ref) =>
					sendOrHold(db, ref, entry).catch((error) => {
						console.error("Held push failed", error);
					}),
				),
			);
		}

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

// A device's hold lifted - its live game over here, it walked out, or it came
// back after going dark mid game: what was held for it goes out now, as one
// push. Every write to a member doc lands here, so most return at once.
exports.releaseHeldNotifications = onDocumentUpdated(
	"leagues/{code}/members/{uid}",
	async (event) => {
		const after = event.data?.after;
		const member = after?.data();
		if (
			!after ||
			!member ||
			member.liveHold === true ||
			hold.heldList(member.heldPushes).length === 0
		) {
			return;
		}
		const db = getFirestore();
		const out = await db.runTransaction(async (tx) => {
			const now = Date.now();
			const current = (await tx.get(after.ref)).data();
			if (!current || hold.isHeld(current, now)) {
				return undefined;
			}
			const list = hold.heldList(current.heldPushes);
			if (list.length === 0) {
				return undefined;
			}
			tx.update(after.ref, { heldPushes: FieldValue.delete() });
			const data = hold.combine(list, now);
			return data && current.fcmToken
				? { token: current.fcmToken, data }
				: undefined;
		});
		if (out) {
			await sendOne(after.ref, out.token, out.data);
		}
	},
);
