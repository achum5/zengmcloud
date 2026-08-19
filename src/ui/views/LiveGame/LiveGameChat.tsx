import { useEffect, useMemo, useRef, useState } from "react";
import {
	MAX_CHAT_MESSAGE_LENGTH,
	visibleChatMessages,
	type LiveGameChatMessage,
} from "../../../common/liveGameChat.ts";
import { toWorker } from "../../util/toWorker.ts";
import { OnScreenKeyboard } from "../../components/OnScreenKeyboard.tsx";
import clsx from "clsx";

// Type with the in-page keyboard rather than the device's own. Phones only: a
// desktop keyboard is hardware, costs no screen space and moves no viewport, so
// there is nothing there to fix and a replica would only be in the way.
const OWN_KEYBOARD = typeof window !== "undefined" && window.mobile === true;

// Enough drawer for the keyboard, the message box and a line or two of what has
// already been said. Below this the keyboard is not usable, so the drawer takes
// the room even if that means covering the court.
const TYPING_MIN_HEIGHT = 380;

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
	boundaryEls = [],
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
	// Everything the drawer must stay clear of: the sticky score block and the
	// court, which are siblings rather than one element. The drawer starts below
	// the LOWEST of them, so opening the chat never hides the game.
	boundaryEls?: (HTMLElement | null)[];
}) => {
	const [open, setOpen] = useState(false);
	const [text, setText] = useState("");
	const [sending, setSending] = useState(false);
	// On a phone the message box is not an <input> and typing is done with the
	// keyboard below - see OnScreenKeyboard for why. This is whether that
	// keyboard is up; on desktop it is never used and the real field is shown.
	const [typing, setTyping] = useState(false);
	const listRef = useRef<HTMLDivElement>(null);
	const seenCount = useRef(0);

	// Where the drawer sits, both measured off the court:
	//
	// maxHeight - everything from the bottom of the court down. Capping the
	// DRAWER rather than the message list means the toggle, the padding and the
	// input row are accounted for by the flex layout instead of by a guessed
	// constant, so the list gets exactly the leftover space.
	//
	// left - the content area's left edge. The nav sidebar is also fixed, and
	// sits ABOVE this (z-index 1040), so a drawer spanning the full width simply
	// hid its own toggle button behind the nav on desktop. Following the game's
	// column keeps the drawer beside the sidebar instead of under it, and tracks
	// the sidebar being toggled for free (that changes the column's width, which
	// the observer below is already watching).
	// bottom - how far up from the layout viewport's bottom edge to sit. Zero
	// almost always; on a phone with the keyboard open it is the keyboard's
	// height, because `position: fixed; bottom: 0` pins to the LAYOUT viewport
	// and the keyboard covers the bottom of it - which put the message box the
	// user just tapped underneath the keyboard they opened by tapping it.
	const [dock, setDock] = useState<{
		maxHeight?: number;
		// The whole visible area, which is the most the drawer may ever take -
		// used when the on-screen keyboard is up and needs more room than the
		// space under the court.
		viewport?: number;
		left: number;
		bottom: number;
	}>({ left: 0, bottom: 0 });
	const [scoreEl, courtEl] = boundaryEls;
	const elements = useMemo(
		() => [scoreEl, courtEl].filter((el) => el != null),
		[scoreEl, courtEl],
	);
	useEffect(() => {
		const measure = () => {
			// visualViewport is the part actually on screen - on iOS innerHeight
			// includes the area behind the browser chrome AND behind the keyboard,
			// either of which would size the drawer taller than the space it has.
			const vv = window.visualViewport;
			const viewport = vv?.height ?? window.innerHeight;
			// How far the visible area is scrolled WITHIN the layout viewport, so
			// element positions can be expressed in what the user can actually see.
			// Pinch-zoom and the keyboard both move this.
			const offsetTop = vv?.offsetTop ?? 0;
			const bottom = vv
				? Math.max(0, Math.round(window.innerHeight - vv.height - offsetTop))
				: 0;
			const rects = elements.map((el) => el.getBoundingClientRect());
			const bottoms = rects
				.map((rect) => rect.bottom - offsetTop)
				.filter((value) => Number.isFinite(value));
			// Nothing to measure means no idea where the game ends, so take half
			// the screen rather than assuming the whole thing is free. Scrolled
			// past everything, the bottoms go negative and the drawer is welcome
			// to the lot.
			const floor =
				bottoms.length === 0 ? viewport / 2 : Math.max(0, ...bottoms);
			setDock({
				// The 120px floor can push the drawer over the court, but only
				// when there is genuinely no room below it - in practice, with the
				// keyboard open. That is the one moment the person is looking at
				// what they are typing rather than at the game, and the
				// alternative is a message box too short to use.
				maxHeight: Math.max(120, Math.floor(viewport - floor)),
				viewport: Math.floor(viewport),
				left: Math.max(0, Math.floor(rects[0]?.left ?? 0)),
				bottom,
			});
		};
		measure();

		// The court sizes itself AFTER the first paint, so a one-shot measurement
		// reads the game as just the score row and hands the drawer the court's
		// own space. Watching the elements is what keeps them honest.
		const observer = new ResizeObserver(measure);
		for (const el of elements) {
			observer.observe(el);
		}
		window.addEventListener("resize", measure);
		window.addEventListener("scroll", measure, { passive: true });
		// The visual viewport moves and resizes independently of the page - the
		// keyboard opening is a resize, and scrolling under it is a scroll.
		window.visualViewport?.addEventListener("resize", measure);
		window.visualViewport?.addEventListener("scroll", measure);
		return () => {
			observer.disconnect();
			window.removeEventListener("resize", measure);
			window.removeEventListener("scroll", measure);
			window.visualViewport?.removeEventListener("resize", measure);
			window.visualViewport?.removeEventListener("scroll", measure);
		};
	}, [elements]);

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

	// Every edit to the message goes through here, from either keyboard, so the
	// typing anchor and the length cap cannot be honored on one path and missed
	// on the other.
	const changeText = (next: string) => {
		if (text === "" && next !== "") {
			anchorAtTypingStart.current = cursor;
		}
		setText(next.slice(0, MAX_CHAT_MESSAGE_LENGTH));
	};

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

	// While the on-screen keyboard is up the drawer needs room for it, which the
	// space under the court often is not. Take the smaller of "as much as the
	// screen has" and "enough for the keyboard plus a line or two", so a game
	// with plenty of room below it is still never covered - the drawer only
	// climbs over the court when it genuinely has nowhere else to go.
	const maxHeight = open
		? typing
			? Math.min(
					dock.viewport ?? TYPING_MIN_HEIGHT,
					Math.max(dock.maxHeight ?? 0, TYPING_MIN_HEIGHT),
				)
			: dock.maxHeight
		: undefined;

	return (
		<div
			className="live-chat-dock"
			style={{
				left: dock.left,
				bottom: dock.bottom,
				maxHeight,
			}}
		>
			<button
				type="button"
				className="btn btn-secondary btn-sm align-self-start"
				onClick={() => {
					if (open) {
						setTyping(false);
					}
					setOpen(!open);
				}}
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
						OWN_KEYBOARD ? (
							<>
								<div className="input-group input-group-sm">
									{/* Deliberately not an <input>: nothing here can take
									    focus, so nothing can summon the native keyboard.
									    That is the entire point - see OnScreenKeyboard. */}
									<div
										className={clsx("form-control live-chat-field", {
											typing,
										})}
										role="textbox"
										aria-label="Message"
										onPointerDown={(event) => {
											event.preventDefault();
											setTyping(true);
										}}
									>
										{text === "" && !typing ? (
											<span className="text-body-secondary">Message</span>
										) : (
											<>
												{text}
												{typing ? <span className="live-chat-caret" /> : null}
											</>
										)}
									</div>
									{/* While the keyboard is up its own Send key does this,
									    the way the return key does on a phone - two of them
									    side by side is just clutter. */}
									{typing ? null : (
										<button
											type="button"
											className="btn btn-primary"
											disabled={sending || text.trim() === ""}
											onClick={() => {
												void send();
											}}
										>
											Send
										</button>
									)}
								</div>
								{typing ? (
									<OnScreenKeyboard
										onKey={(key) => changeText(text + key)}
										onBackspace={() => changeText(text.slice(0, -1))}
										onSubmit={() => {
											void send();
										}}
										onDismiss={() => setTyping(false)}
										submitDisabled={sending || text.trim() === ""}
									/>
								) : null}
							</>
						) : (
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
										changeText(event.target.value);
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
						)
					) : null}
				</div>
			) : null}
		</div>
	);
};
