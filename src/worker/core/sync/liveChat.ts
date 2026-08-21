import { idb } from "../../db/index.ts";
import { g, toUI } from "../../util/index.ts";
import { syncDebugLog } from "./debugLog.ts";
import {
	mergeChatMessages,
	sanitizeChatText,
	type LiveGameChatMessage,
} from "../../../common/liveGameChat.ts";
import type { SyncTransport } from "./types.ts";

// Live game chat: the room side of it. The pure rules (anchoring, ordering,
// spoiler filtering) live in common/liveGameChat.ts; this module owns the
// subscription, the send path, and - the part that outlives the game - folding
// the finished conversation into the saved replay so it travels with the
// league forever, exports included.

let unsubscribe: (() => void) | undefined;
let transport: SyncTransport | undefined;
let messages: LiveGameChatMessage[] = [];

// The broadcast these messages belong to. A room's chat doc is reused game
// after game, so without this the previous game's conversation would appear
// over the new one for anyone who joined before it was cleared.
let currentStartedAt: number | undefined;
let currentGid: number | undefined;

const pushToUI = () => {
	void toUI("updateLocal", [{ mpLiveChat: messages }]);
};

// The live-game chat has no UI any more - its drawer is position:fixed and
// kept landing in the middle of the screen instead of against the game, so it
// was removed from the Live Game page. Nothing subscribes here as a result:
// the room's chat doc would otherwise be read on every live sim, on every
// device, for a panel nobody can see. The rest of the module is left intact so
// saved replays that already carry a chat log still read back.
const CHAT_UI_REMOVED = true;

export const setupLiveChat = (t: SyncTransport) => {
	teardownLiveChat();
	transport = t;
	if (CHAT_UI_REMOVED) {
		return;
	}
	unsubscribe = t.subscribeLiveChat?.((incoming) => {
		// Only this broadcast's messages. Anything stamped for a different one
		// is last game's, still sitting in the room doc.
		const relevant =
			currentStartedAt === undefined
				? incoming
				: incoming.filter(
						(m) =>
							m.startedAt === undefined || m.startedAt === currentStartedAt,
					);
		messages = mergeChatMessages([], relevant);
		pushToUI();
	});
};

export const teardownLiveChat = () => {
	unsubscribe?.();
	unsubscribe = undefined;
	transport = undefined;
	messages = [];
	currentStartedAt = undefined;
	currentGid = undefined;
	pushToUI();
};

// Called when a broadcast begins on ANY device (the broadcaster clears the
// room doc; everyone else just re-scopes what they will accept).
export const beginLiveChat = async (
	gid: number,
	startedAt: number,
	isBroadcaster: boolean,
) => {
	currentGid = gid;
	currentStartedAt = startedAt;
	messages = [];
	pushToUI();
	if (isBroadcaster && transport?.clearLiveChat) {
		try {
			await transport.clearLiveChat();
		} catch (error) {
			syncDebugLog("liveChat:clear-failed", { error: String(error) });
		}
	}
};

export const sendLiveChatMessage = async ({
	text,
	cursor,
	quarter,
	clock,
	score,
}: {
	text: string;
	cursor: number;
	quarter?: string;
	clock?: string;
	score?: string;
}) => {
	const clean = sanitizeChatText(text);
	if (clean === undefined || !transport?.publishLiveChatMessage) {
		return;
	}

	// Attributed to whichever team THIS device currently has selected, which is
	// what makes the log read like a broadcast booth rather than a list of
	// usernames.
	const tid = g.get("userTid");
	let abbrev = "";
	try {
		abbrev = (await idb.cache.teams.get(tid))?.abbrev ?? "";
	} catch {
		// A missing team just means an unlabelled message, never a lost one.
	}

	const message: LiveGameChatMessage = {
		id:
			typeof crypto !== "undefined" && crypto.randomUUID
				? crypto.randomUUID()
				: `${Date.now()}-${Math.floor(Math.random() * 1e9)}`,
		cursor,
		at: Date.now(),
		tid,
		abbrev,
		text: clean,
		quarter,
		clock,
		score,
		startedAt: currentStartedAt,
	};

	// Show it locally at once rather than waiting for the round trip - the
	// subscription will deliver the same message back and dedupe by id.
	messages = mergeChatMessages(messages, [message]);
	pushToUI();

	try {
		await transport.publishLiveChatMessage(message);
	} catch (error) {
		syncDebugLog("liveChat:publish-failed", { error: String(error) });
	}
};

// Fold the conversation into the saved replay, so re-watching the game shows
// the chat at the moments it happened - for everyone, forever, and in exports.
// Only the BROADCASTER writes: liveGamePlayByPlay is a synced store, so having
// every watcher write their own copy would put several devices in a pointless
// publish race over the same row.
export const persistLiveChatToReplay = async (isBroadcaster: boolean) => {
	if (!isBroadcaster || currentGid === undefined || messages.length === 0) {
		return;
	}
	const gid = currentGid;
	try {
		const row = await idb.cache.liveGamePlayByPlay.get(gid);
		if (!row) {
			// No saved replay for this game (replays can be off) - nothing to
			// attach the conversation to.
			return;
		}
		const merged = mergeChatMessages(row.chat ?? [], messages);
		await idb.cache.liveGamePlayByPlay.put({ ...row, chat: merged });
		syncDebugLog("liveChat:saved-to-replay", { gid, count: merged.length });
	} catch (error) {
		syncDebugLog("liveChat:save-failed", { gid, error: String(error) });
	}
};
