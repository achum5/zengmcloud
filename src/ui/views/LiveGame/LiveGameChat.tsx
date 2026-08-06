import { useEffect, useMemo, useRef, useState } from "react";
import {
	MAX_CHAT_MESSAGE_LENGTH,
	visibleChatMessages,
	type LiveGameChatMessage,
} from "../../../common/liveGameChat.ts";
import { toWorker } from "../../util/toWorker.ts";

// Chat alongside a live game, and the record of it on a replay.
//
// Collapsible on purpose: on a phone the court and box score are already
// fighting for room, so chat opens over the bottom of the screen and can be
// shut with one tap - but while it IS open the game stays visible above it,
// because watching together is the whole point. Desktop gets the same panel,
// just taller.

const Message = ({ message }: { message: LiveGameChatMessage }) => (
	<div className="mb-2">
		<div className="d-flex align-items-baseline gap-1 small">
			<span className="fw-bold">{message.abbrev || "—"}</span>
			<span className="text-body-secondary">
				{[message.quarter, message.clock].filter(Boolean).join(" ")}
				{message.score ? ` · ${message.score}` : ""}
			</span>
		</div>
		<div style={{ overflowWrap: "anywhere" }}>{message.text}</div>
	</div>
);

export const LiveGameChat = ({
	messages,
	cursor,
	canSend,
	quarter,
	clock,
	score,
}: {
	messages: LiveGameChatMessage[];
	// How far THIS viewer has watched. Messages anchored past it stay hidden,
	// which is what keeps a replay in step and a late joiner unspoiled.
	cursor: number;
	// False on a replay: the log is a record of what was said live.
	canSend: boolean;
	quarter?: string;
	clock?: string;
	score?: string;
}) => {
	const [open, setOpen] = useState(false);
	const [text, setText] = useState("");
	const [sending, setSending] = useState(false);
	const listRef = useRef<HTMLDivElement>(null);
	const seenCount = useRef(0);

	// The cursor when the user STARTED typing. Anchoring to the moment they
	// began reacting - rather than the moment they hit send - is what makes a
	// replay read naturally: the message lands with the play that prompted it,
	// not several plays later once they finished typing.
	const anchorAtTypingStart = useRef<number | undefined>(undefined);

	const visible = useMemo(
		() => visibleChatMessages(messages, cursor),
		[messages, cursor],
	);

	// Stick to the newest message while open.
	useEffect(() => {
		if (open && listRef.current) {
			listRef.current.scrollTop = listRef.current.scrollHeight;
		}
	}, [open, visible.length]);

	useEffect(() => {
		if (open) {
			seenCount.current = visible.length;
		}
	}, [open, visible.length]);

	const unread = open ? 0 : Math.max(0, visible.length - seenCount.current);

	const send = async () => {
		const toSend = text.trim();
		if (toSend === "" || sending) {
			return;
		}
		setSending(true);
		try {
			await toWorker("main", "sendLiveChatMessage", {
				text: toSend,
				cursor: anchorAtTypingStart.current ?? cursor,
				quarter,
				clock,
				score,
			});
			setText("");
			anchorAtTypingStart.current = undefined;
		} finally {
			setSending(false);
		}
	};

	return (
		<div className="mt-2">
			<button
				type="button"
				className="btn btn-secondary btn-sm"
				onClick={() => setOpen(!open)}
			>
				{open ? "Hide chat" : "Chat"}
				{visible.length > 0 ? ` (${visible.length})` : ""}
				{unread > 0 ? (
					<span className="badge bg-danger ms-1">{unread}</span>
				) : null}
			</button>

			{open ? (
				<div className="border rounded mt-2 p-2">
					<div
						ref={listRef}
						style={{ maxHeight: 220, overflowY: "auto" }}
						className="mb-2"
					>
						{visible.length === 0 ? (
							<div className="text-body-secondary small">No messages yet.</div>
						) : (
							visible.map((m) => <Message key={m.id} message={m} />)
						)}
					</div>

					{canSend ? (
						<form
							className="input-group input-group-sm"
							onSubmit={(event) => {
								event.preventDefault();
								void send();
							}}
						>
							<input
								type="text"
								className="form-control"
								placeholder="Message"
								maxLength={MAX_CHAT_MESSAGE_LENGTH}
								value={text}
								onChange={(event) => {
									if (text === "" && event.target.value !== "") {
										anchorAtTypingStart.current = cursor;
									}
									setText(event.target.value);
								}}
							/>
							<button
								type="submit"
								className="btn btn-primary"
								disabled={sending || text.trim() === ""}
							>
								Send
							</button>
						</form>
					) : null}
				</div>
			) : null}
		</div>
	);
};
