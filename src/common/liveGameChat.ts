// Chat during a live-simmed game, and the record of it afterwards.
//
// THE ANCHOR IS THE PLAY INDEX, not a wall clock. A live broadcast already
// syncs `cursor` - how many play-by-play events the broadcaster has consumed -
// and every follower is stepped to exactly that position. Stamping a message
// with that number instead of a timestamp buys three things at once:
//
//   - Replay alignment is exact and free. Re-watching the game shows each
//     message at the moment of the game it was sent at, with no drift.
//   - No clock-skew problem between devices.
//   - Spoiler safety falls out. A message is shown only once the VIEWER has
//     reached its anchor, so someone who joined late or paused behind the
//     action never sees "WHAT A SHOT" before the shot.
//
// The scoreboard fields are a snapshot taken when the message was sent, rather
// than derived from the anchor later, so the replay line can say exactly what
// the score and clock read at that moment without re-simulating anything.

export type LiveGameChatMessage = {
	// Unique per message. Concurrent senders merge into one document keyed by
	// this, so two people typing at once can never clobber each other, and a
	// re-delivered message is deduped rather than doubled.
	id: string;
	// Play-by-play events consumed when this was sent - see above.
	cursor: number;
	// Wall clock, only for ordering messages that share an anchor.
	at: number;
	// The team the sending device had selected. Snapshotted (not looked up
	// later) so a relocated or renamed franchise still reads correctly years on.
	tid: number;
	abbrev: string;
	text: string;
	// What the scoreboard read when this was sent.
	quarter?: string;
	clock?: string;
	score?: string;
	// Which broadcast this belongs to, so a room doc left over from the
	// previous game can never leak into this one.
	startedAt?: number;
};

export const MAX_CHAT_MESSAGE_LENGTH = 280;

// Trim and bound what a device is allowed to send. Returns undefined for
// anything not worth publishing.
export const sanitizeChatText = (raw: string): string | undefined => {
	const text = raw.replaceAll(/\s+/g, " ").trim();
	if (text === "") {
		return undefined;
	}
	return text.slice(0, MAX_CHAT_MESSAGE_LENGTH);
};

// The messages a viewer at `cursor` is allowed to see, in the order they were
// sent. This one function serves both the live game and the replay - which is
// the point of anchoring to the play index.
export const visibleChatMessages = (
	messages: LiveGameChatMessage[],
	cursor: number,
): LiveGameChatMessage[] =>
	messages
		.filter((m) => m.cursor <= cursor)
		.sort((a, b) => a.cursor - b.cursor || a.at - b.at);

// Merge incoming messages into what we already have, deduped by id and left in
// a stable order. Used both by the live subscription and when folding a
// finished game's chat into the saved replay.
export const mergeChatMessages = (
	existing: LiveGameChatMessage[],
	incoming: LiveGameChatMessage[],
): LiveGameChatMessage[] => {
	const byId = new Map<string, LiveGameChatMessage>();
	for (const m of [...existing, ...incoming]) {
		if (m && typeof m.id === "string" && typeof m.cursor === "number") {
			byId.set(m.id, m);
		}
	}
	return [...byId.values()].sort((a, b) => a.cursor - b.cursor || a.at - b.at);
};
