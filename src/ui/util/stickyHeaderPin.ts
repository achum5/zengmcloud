// The marker class a modal puts on the element it pins while it holds the page
// still (see Modal.tsx, and .ios-modal-pinned in light.scss).
//
// It lives in its own leaf module so the modal manager and the sticky-header
// watchdog can both name it without either importing the other: the watchdog is
// deliberately loaded on demand, and a static import from Modal.tsx would pull
// it back into the main chunk.
export const PINNED_CLASS = "ios-modal-pinned";
export const PINNED_SELECTOR = `.${PINNED_CLASS}`;

// The marker class the sticky fallback puts on the header's parent when
// position:sticky has stopped working in the page altogether (see
// visualViewportHeader.ts, and .sticky-fallback-pinned in light.scss).
//
// A CLASS, not an inline style, for a specific reason: the watchdog's repair
// ladder clears inline `position` on the bars, so an inline fallback and the
// ladder take it in turns and the header flickers.
export const STICKY_FALLBACK_CLASS = "sticky-fallback-pinned";
export const STICKY_FALLBACK_HEIGHT_VAR = "--sticky-fallback-header-height";
