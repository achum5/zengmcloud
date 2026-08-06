import { useEffect, useMemo, useRef, useState } from "react";
import {
	MAX_CHAT_MESSAGE_LENGTH,
	visibleChatMessages,
	type LiveGameChatMessage,
} from "../../../common/liveGameChat.ts";
import { toWorker } from "../../util/toWorker.ts";

// Chat alongside a live game, and the record of it on a replay.
//
// Collapsible on purpose: the court and box score are already fighting for room,
// so chat opens as a drawer along the bottom of the screen and shuts with one
// tap - but while it IS open the game stays visible above it, because watching
// together is the whole point. Same drawer on desktop and mobile; on a bigger
// screen there is simply more space under the court for it to use.

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
	boundaryEl,
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
	// The sticky block holding the score and the court. The drawer is capped to
	// the space BELOW it, so opening the chat never hides the game.
	boundaryEl?: HTMLElement | null;
}) => {
	const [open, setOpen] = useState(false);
	const [text, setText] = useState("");
	const [sending, setSending] = useState(false);
	const listRef = useRef<HTMLDivElement>(null);
	const seenCount = useRef(0);

	// How tall the whole drawer may be: everything from the bottom of the court
	// down. Capping the DRAWER rather than the message list means the toggle,
	// the padding and the input row are accounted for by the flex layout instead
	// of by a guessed constant, so the list gets exactly the leftover space.
	const [maxHeight, setMaxHeight] = useState<number | undefined>(undefined);
	useEffect(() => {
		if (!open) {
			return;
		}
		const measure = () => {
			// visualViewport is the part actually on screen - on iOS innerHeight
			// includes the area behind the browser chrome, which would size the
			// drawer taller than the space it has.
			const viewport = window.visualViewport?.height ?? window.innerHeight;
			const bottom = boundaryEl?.getBoundingClientRect().bottom;
			// No court to measure means no idea where it ends, so take half the
			// screen rather than assuming the whole thing is free.
			const floor =
				bottom === undefined || !Number.isFinite(bottom)
					? viewport / 2
					: Math.max(0, bottom);
			setMaxHeight(Math.max(120, Math.floor(viewport - floor)));
		};
		measure();

		// The court sizes itself AFTER the first paint, so a one-shot measurement
		// reads the sticky block as just the score row and hands the drawer the
		// court's own space. Watching the element is what keeps the two honest.
		const observer = new ResizeObserver(measure);
		if (boundaryEl) {
			observer.observe(boundaryEl);
		}
		window.addEventListener("resize", measure);
		window.addEventListener("scroll", measure, { passive: true });
		window.visualViewport?.addEventListener("resize", measure);
		return () => {
			observer.disconnect();
			window.removeEventListener("resize", measure);
			window.removeEventListener("scroll", measure);
			window.visualViewport?.removeEventListener("resize", measure);
		};
	}, [open, boundaryEl]);

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

	// A replay with nothing ever said is not worth a control - there is nothing
	// to open and nothing to add. Live, the panel always shows, because an
	// empty chat is exactly when you want to start one.
	if (!canSend && messages.length === 0) {
		return null;
	}

	return (
		<div className="live-chat-dock" style={open ? { maxHeight } : undefined}>
			<button
				type="button"
				className="btn btn-secondary btn-sm align-self-start"
				onClick={() => setOpen(!open)}
			>
				{open ? "Hide chat" : "Chat"}
				{visible.length > 0 ? ` (${visible.length})` : ""}
				{unread > 0 ? (
					<span className="badge bg-danger ms-1">{unread}</span>
				) : null}
			</button>

			{open ? (
				<div className="live-chat-panel border rounded mt-2 p-2">
					<div ref={listRef} className="live-chat-messages mb-2">
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
