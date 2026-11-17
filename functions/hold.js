// THE PUSH HOLD, the function's half (the device's is
// src/worker/core/sync/liveHoldLease.ts). A device in the middle of a live
// game stamps gameHold on its member doc; pushes for it wait there, in
// heldPushes, until it stops - and then go out as one.

// How long a stamp holds. The device writes it again every two minutes
// while it lasts; one not written again in this long is a device that went
// dark - not just a phone locked for a bit mid game. (What was held for it
// then goes out on its own: see releaseLapsedHolds in index.js.)
const LIVE_HOLD_LEASE_MS = 10 * 60 * 1000;
// The longest a hold lasts from when it began, however often it is written
// again: a live game is over by then. (A device that never says when it
// began - an old build - holds nothing.)
const HOLD_MOST_MS = 45 * 60 * 1000;
// The most a device keeps waiting (the latest ones).
const MAX_HELD = 8;
// A push older than this is no news any more: dropped, not sent.
const HELD_MAX_AGE_MS = 6 * 60 * 60 * 1000;
// What a push shows of them all, at most.
const MAX_TITLE = 80;
const MAX_BODY = 1500;

// (Under its own names - gameHold, not the liveHold the first of these
// went by - so a device on a newer build than the function it reaches, or
// the other way about, never holds what nothing is there to let go.)
const isHeld = (member, now) =>
	member?.gameHold === true &&
	typeof member.gameHoldAt?.toMillis === "function" &&
	now - member.gameHoldAt.toMillis() < LIVE_HOLD_LEASE_MS &&
	typeof member.gameHoldSince?.toMillis === "function" &&
	now - member.gameHoldSince.toMillis() < HOLD_MOST_MS;

// Where the function keeps note of the devices it has pushes held for, so a
// hold that lapses without the device saying so still lets them go (see
// releaseLapsedHolds): one doc, no client can read or write it.
const REGISTRY = "pushHolds/active";
const holdKey = (code, uid) => `${code}|${uid}`;
const fromKey = (key) => {
	const i = key.indexOf("|");
	return i > 0 ? { code: key.slice(0, i), uid: key.slice(i + 1) } : undefined;
};

const heldList = (value) =>
	Array.isArray(value)
		? value.filter(
				(entry) =>
					entry !== null &&
					typeof entry === "object" &&
					typeof entry.title === "string" &&
					typeof entry.body === "string",
			)
		: [];

const toHeld = (data, now) => ({
	title: String(data.title || "ZenGM"),
	body: String(data.body || ""),
	path: String(data.path || ""),
	at: now,
});

const withHeld = (list, entry) => [...heldList(list), entry].slice(-MAX_HELD);

const clip = (text, most) =>
	text.length > most ? `${text.slice(0, most - 1)}…` : text;

// The one push that stands for them all: oldest first, each title once.
// undefined when none is still worth sending.
const combine = (list, now) => {
	const fresh = heldList(list).filter(
		(entry) => typeof entry.at !== "number" || now - entry.at < HELD_MAX_AGE_MS,
	);
	if (fresh.length === 0) {
		return undefined;
	}
	const last = fresh.at(-1);
	if (fresh.length === 1) {
		return { title: last.title, body: last.body, path: last.path ?? "" };
	}
	const titles = [...new Set(fresh.map((entry) => entry.title))];
	return {
		title: clip(titles.join(" · "), MAX_TITLE),
		body: clip(
			fresh
				.map((entry) => entry.body)
				.filter((body) => body !== "")
				.join("\n"),
			MAX_BODY,
		),
		path: typeof last.path === "string" ? last.path : "",
	};
};

module.exports = {
	LIVE_HOLD_LEASE_MS,
	HOLD_MOST_MS,
	REGISTRY,
	holdKey,
	fromKey,
	MAX_HELD,
	HELD_MAX_AGE_MS,
	isHeld,
	heldList,
	toHeld,
	withHeld,
	combine,
};
