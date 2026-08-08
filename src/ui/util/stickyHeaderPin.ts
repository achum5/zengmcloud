// The marker class a modal puts on the element it pins while it holds the page
// still (see Modal.tsx, and .ios-modal-pinned in light.scss).
//
// It lives in its own leaf module so the modal manager and the sticky-header
// watchdog can both name it without either importing the other: the watchdog is
// deliberately loaded on demand, and a static import from Modal.tsx would pull
// it back into the main chunk.
export const PINNED_CLASS = "ios-modal-pinned";
export const PINNED_SELECTOR = `.${PINNED_CLASS}`;
