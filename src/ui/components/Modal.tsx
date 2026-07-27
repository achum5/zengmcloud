import type { ContainerState } from "@restart/ui/ModalManager";
import { Modal as BaseModal } from "react-bootstrap";
import BootstrapModalManager from "react-bootstrap/BootstrapModalManager";
import { createNanoEvents } from "nanoevents";

export const emitter = createNanoEvents<{
	keepScrollToRight: () => void;
}>();

// If animation is enabled, the modal gets stuck open on Android Chrome 91. This happens only when clicking Cancel/Save - the X and clicking outside the modal still works to close it. All my code is working - show does get set false, it does get rendered, just still displayed. Disabling ads makes no difference. It works when calling programmatically wtih ButtonElement.click() but not with an actual click. Disabling animation fixes it though. Also https://mail.google.com/mail/u/0/#inbox/FMfcgzGkZGhkhtPsGFPFxcKxhvZFkHpl
const animation = false;

// iOS Safari/WebKit (iPhone, iPod, and iPadOS - which reports as "Macintosh"
// but has a touch screen). react-bootstrap locks the background behind a modal
// with `overflow: hidden` on <body>, which iOS silently ignores; worse, on a
// page that's already scrolled, iOS then mis-renders every `position: fixed`
// element - the fixed top navbar, the modal itself, and its backdrop all detach
// and scroll away with the page content. So a fixed header appears to "unstick"
// and drift up the moment any modal (e.g. the ratings popover, which is a modal
// on mobile) opens. The robust cross-browser scroll lock - pin <body> in place -
// sidesteps it entirely: if the page can't scroll, nothing can detach.
// Exported + pure so the platform gate (including iPadOS, which reports a
// "Macintosh" UA and is only distinguishable by having a touch screen) is
// regression-tested.
export const isIOSUserAgent = (ua: string, maxTouchPoints: number): boolean =>
	/iP(hone|od|ad)/.test(ua) || (/Macintosh/.test(ua) && maxTouchPoints > 1);

const IS_IOS =
	typeof navigator !== "undefined" &&
	isIOSUserAgent(navigator.userAgent || "", navigator.maxTouchPoints ?? 0);

class MyModalManager extends BootstrapModalManager {
	// The scroll position captured when the body was pinned, so it can be
	// restored on unlock. Undefined = not currently pinned.
	private lockedScrollY: number | undefined;

	constructor() {
		super();
	}

	override setContainerStyle(containerState: ContainerState) {
		// Capture the scroll position BEFORE super mutates layout. setContainerStyle
		// only fires for the first modal in the stack, so this locks exactly once.
		const shouldLock = IS_IOS && this.lockedScrollY === undefined;
		const scrollY = shouldLock ? window.scrollY : 0;

		super.setContainerStyle(containerState);

		if (shouldLock) {
			this.lockedScrollY = scrollY;
			const { style } = document.body;
			style.position = "fixed";
			style.top = `-${scrollY}px`;
			style.left = "0";
			style.right = "0";
			style.width = "100%";
			// Pins the sticky header to the viewport top and restores its layout
			// space so the background doesn't jump (see .ios-modal-pinned in
			// light.scss).
			document.body.classList.add("ios-modal-pinned");
		}

		if (!containerState.scrollBarWidth) {
			return;
		}

		const element = document.querySelector(".league-top-bar-toggle");

		// Element only exists within league
		if (element instanceof HTMLElement) {
			element.style.right = `${containerState.scrollBarWidth}px`;
		}
	}
	override removeContainerStyle(containerState: ContainerState) {
		super.removeContainerStyle(containerState);

		// Unpin the body (fires only when the last modal closes) and restore the
		// scroll position it was locked at.
		if (this.lockedScrollY !== undefined) {
			const y = this.lockedScrollY;
			this.lockedScrollY = undefined;
			const { style } = document.body;
			style.position = "";
			style.top = "";
			style.left = "";
			style.right = "";
			style.width = "";
			document.body.classList.remove("ios-modal-pinned");
			window.scrollTo(0, y);
		}

		if (!containerState.scrollBarWidth) {
			return;
		}

		const element = document.querySelector(".league-top-bar-toggle");

		// Element only exists within league
		if (element instanceof HTMLElement) {
			element.style.right = "";
		}

		emitter.emit("keepScrollToRight");
	}
}

const manager = new MyModalManager();

export const Modal = ({
	children,
	...props
}: Parameters<typeof BaseModal>[0]) => {
	return (
		<BaseModal animation={animation} manager={manager} {...props}>
			{children}
		</BaseModal>
	);
};

Modal.Body = BaseModal.Body;
Modal.Footer = BaseModal.Footer;
Modal.Header = BaseModal.Header;
Modal.Title = BaseModal.Title;
