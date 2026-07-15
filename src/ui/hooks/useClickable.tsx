import { useCallback, useRef, useState, type MouseEvent } from "react";

const IGNORED_ELEMENTS = new Set([
	"A",
	"BUTTON",
	"INPUT",
	"SELECT",
	"TEXTAREA",
]);
const IGNORED_ELEMENTS_SELECTOR = [
	...IGNORED_ELEMENTS,

	// data-no-row-highlight is a hack and ideally would be removed
	"[data-no-row-highlight]",
].join(",");

// When `controlled` is passed, the highlight is owned by the caller (selected +
// onToggle) instead of the ephemeral internal state - so the same click-to-
// highlight gesture can drive a real selection. All the click guards
// (links/buttons/data-no-row-highlight) are shared.
const useClickable = (controlled?: {
	selected: boolean;
	onToggle: () => void;
}) => {
	const [clickedInternal, setClicked] = useState(false);

	// Keep the latest controlled callbacks without rebuilding toggleClicked.
	const controlledRef = useRef(controlled);
	controlledRef.current = controlled;

	const clicked = controlled ? controlled.selected : clickedInternal;

	const toggleClicked = useCallback(
		(event: MouseEvent<HTMLTableRowElement>) => {
			// Purposely using event.target instead of event.currentTarget because we do want check what internal element was clicked on, not the row itself

			// I think this is not actually needed, just for TypeScript
			const target = event.target;
			if (!(target instanceof Element)) {
				return;
			}

			// Don't toggle the row if a link was clicked.
			if (target.nodeName && IGNORED_ELEMENTS.has(target.nodeName)) {
				return;
			}

			// This handles modals, where for some reason an event is triggered for any click on or outside the modal, even though the modal is not a child of the actual clickable element (event.currentTarget)
			if (!event.currentTarget.contains(target)) {
				return;
			}

			// Search up tree a bit, in case there was like a span inside a button or something
			if (target.closest(IGNORED_ELEMENTS_SELECTOR)) {
				// This means we found a parent element that is one of IGNORED_ELEMENTS, so ignore!
				return;
			}

			if (controlledRef.current) {
				controlledRef.current.onToggle();
			} else {
				setClicked((prevClicked) => !prevClicked);
			}
		},
		[],
	);

	return {
		clicked,
		toggleClicked,
	};
};

export default useClickable;
