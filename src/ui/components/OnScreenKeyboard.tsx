import clsx from "clsx";
import { useEffect, useRef, useState } from "react";
import { shouldAutoShift } from "../util/textSuggestions.ts";

// A KEYBOARD THAT IS PART OF THE PAGE.
//
// The native keyboard on iOS is not a panel over the app, it is a change to the
// viewport: it shrinks visualViewport.height without moving the layout viewport,
// which moves everything anchored to either of them. In a live game that is the
// whole screen - the sticky court, the affixed play controls, the ticker and the
// chat drawer all resize or jump the moment the message box is tapped, and iOS
// then zooms the page in on the focused field for good measure. You lose sight
// of the game to type one line about it.
//
// This never asks for the native keyboard at all. The chat field is not an
// <input>, so nothing can be focused, so nothing can summon one - and these keys
// are ordinary elements inside the drawer, which means the layout that already
// accounts for the drawer accounts for them too. Nothing about the viewport
// changes, so nothing about the game moves.
//
// Layout and behavior follow iOS deliberately, down to measured proportions -
// muscle memory is the point of a replica. The geometry was taken off a
// side-by-side screenshot of the real keyboard on a 440pt-wide iPhone and is
// expressed in container-width units (see the .osk styles), so it holds on any
// screen: key faces about 7% of the width and 9.5% tall, the airy iOS gaps,
// the half-key stagger of the middle row (which falls out of centering
// fixed-width keys), shift and backspace pushed to the edges with the extra
// inset before Z and after M, and a 123/space/send row split roughly
// 22/46/22. Above the keys sits the QuickType-style suggestion strip; shift
// sticks for one letter, locks on a double tap, and arms itself at the start
// of a message and after a sentence ends.

export type ShiftState = "off" | "once" | "lock";

export type KeyLayer = "letters" | "numbers" | "symbols";

export const KEY_LAYERS: Record<KeyLayer, readonly (readonly string[])[]> = {
	letters: [
		["q", "w", "e", "r", "t", "y", "u", "i", "o", "p"],
		["a", "s", "d", "f", "g", "h", "j", "k", "l"],
		["z", "x", "c", "v", "b", "n", "m"],
	],
	numbers: [
		["1", "2", "3", "4", "5", "6", "7", "8", "9", "0"],
		["-", "/", ":", ";", "(", ")", "$", "&", "@", '"'],
		[".", ",", "?", "!", "'"],
	],
	symbols: [
		["[", "]", "{", "}", "#", "%", "^", "*", "+", "="],
		["_", "\\", "|", "~", "<", ">", "€", "£", "¥", "•"],
		[".", ",", "?", "!", "'"],
	],
};

// Two taps this close together lock shift, the way they do on a phone.
export const DOUBLE_TAP_MS = 400;

// Tapping shift: on, off, or locked when it was a double tap. Locked only ever
// unlocks by being tapped again, which is why a double tap cannot land on
// "once" - there would be no way back to lock from there without a third tap.
export const nextShiftState = (
	current: ShiftState,
	doubleTap: boolean,
): ShiftState => {
	if (current === "lock") {
		return "off";
	}
	if (doubleTap) {
		return "lock";
	}
	return current === "off" ? "once" : "off";
};

// Shift is spent by typing one character, unless it is locked.
export const afterTypingShift = (current: ShiftState): ShiftState =>
	current === "once" ? "off" : current;

// Only letters have a shifted form here. The number and symbol layers reach
// their alternates by switching layer, exactly like the phone, so shift must not
// silently uppercase a "?" into itself and spend itself doing nothing.
export const shiftedKey = (key: string, shift: ShiftState): string =>
	shift === "off" ? key : key.toUpperCase();

const Key = ({
	label,
	onPress,
	className,
	ariaLabel,
	disabled,
}: {
	label: string;
	onPress: () => void;
	className?: string;
	ariaLabel?: string;
	disabled?: boolean;
}) => (
	<button
		type="button"
		className={clsx("osk-key", className)}
		aria-label={ariaLabel ?? label}
		disabled={disabled}
		// On pointerdown, not click: a real key registers when it goes down, and
		// preventDefault is what stops the press from moving focus or starting a
		// scroll. It also swallows the click that would otherwise follow, so the
		// key cannot fire twice.
		onPointerDown={(event) => {
			event.preventDefault();
			if (!disabled) {
				onPress();
			}
		}}
	>
		{label}
	</button>
);

export const OnScreenKeyboard = ({
	text = "",
	suggestions = [],
	onKey,
	onBackspace,
	onSuggestion,
	onSubmit,
	onDismiss,
	submitLabel = "Send",
	submitDisabled,
}: {
	// What has been typed so far - read only for the auto-shift rule.
	text?: string;
	// QuickType strip contents, provided by the owner of the text.
	suggestions?: readonly string[];
	onKey: (key: string) => void;
	onBackspace: () => void;
	onSuggestion?: (suggestion: string) => void;
	onSubmit: () => void;
	onDismiss: () => void;
	submitLabel?: string;
	submitDisabled?: boolean;
}) => {
	const [layer, setLayer] = useState<KeyLayer>("letters");
	const [shift, setShift] = useState<ShiftState>(() =>
		shouldAutoShift(text) ? "once" : "off",
	);
	const lastShiftTap = useRef(0);

	// Arm shift when a sentence ends or the message empties - on the
	// transition, so tapping shift off mid-sentence stays off.
	const prevAutoShift = useRef(shouldAutoShift(text));
	useEffect(() => {
		const should = shouldAutoShift(text);
		if (should && !prevAutoShift.current) {
			setShift((current) => (current === "off" ? "once" : current));
		}
		prevAutoShift.current = should;
	}, [text]);

	const rows = KEY_LAYERS[layer];
	const letters = layer === "letters";

	const type = (key: string) => {
		onKey(letters ? shiftedKey(key, shift) : key);
		if (letters) {
			setShift(afterTypingShift(shift));
		}
	};

	const tapShift = () => {
		// performance.now rather than Date.now: it cannot jump backwards when the
		// clock is adjusted, which would make a double tap unrecognizable.
		const now = performance.now();
		const doubleTap = now - lastShiftTap.current < DOUBLE_TAP_MS;
		lastShiftTap.current = now;
		setShift(nextShiftState(shift, doubleTap));
	};

	return (
		<div className={clsx("osk", { "osk-letters": letters })}>
			{/* The QuickType strip. Suggestions are tapped, never auto-applied -
			    see textSuggestions.ts - and the keyboard-dismiss chevron lives at
			    its end, since iPhones have no dismiss key below. */}
			<div className="osk-suggest">
				{suggestions.slice(0, 3).map((suggestion, i) => (
					<button
						key={suggestion}
						type="button"
						className={clsx("osk-suggest-item", { first: i === 0 })}
						onPointerDown={(event) => {
							event.preventDefault();
							onSuggestion?.(suggestion);
						}}
					>
						{suggestion}
					</button>
				))}
				<button
					type="button"
					className="osk-suggest-hide"
					aria-label="Hide keyboard"
					onPointerDown={(event) => {
						event.preventDefault();
						onDismiss();
					}}
				>
					⌄
				</button>
			</div>

			{rows.map((row, i) => (
				<div key={i} className={clsx("osk-row", { "osk-row-edges": i === 2 })}>
					{i === 2 ? (
						letters ? (
							<Key
								label={shift === "lock" ? "⇪" : "⇧"}
								ariaLabel="Shift"
								className={clsx("osk-key-mod", {
									active: shift !== "off",
								})}
								onPress={tapShift}
							/>
						) : (
							<Key
								label={layer === "numbers" ? "#+=" : "123"}
								ariaLabel="More symbols"
								className="osk-key-mod"
								onPress={() =>
									setLayer(layer === "numbers" ? "symbols" : "numbers")
								}
							/>
						)
					) : null}
					{row.map((key) => (
						<Key
							key={key}
							label={letters ? shiftedKey(key, shift) : key}
							onPress={() => type(key)}
						/>
					))}
					{i === 2 ? (
						<Key
							label="⌫"
							ariaLabel="Backspace"
							className="osk-key-mod"
							onPress={onBackspace}
						/>
					) : null}
				</div>
			))}

			<div className="osk-row osk-row-bottom">
				<Key
					label={letters ? "123" : "ABC"}
					ariaLabel={letters ? "Numbers" : "Letters"}
					className="osk-key-side"
					onPress={() => setLayer(letters ? "numbers" : "letters")}
				/>
				<Key
					label="space"
					ariaLabel="Space"
					className="osk-key-space"
					onPress={() => type(" ")}
				/>
				<Key
					label={submitLabel}
					className="osk-key-side osk-key-send"
					disabled={submitDisabled}
					onPress={onSubmit}
				/>
			</div>
		</div>
	);
};
