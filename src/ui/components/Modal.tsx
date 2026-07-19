import type { ContainerState } from "@restart/ui/ModalManager";
import { Modal as BaseModal } from "react-bootstrap";
import BootstrapModalManager from "react-bootstrap/BootstrapModalManager";
import { createNanoEvents } from "nanoevents";
import { getScrollEl } from "../util/scrollContainer.ts";

export const emitter = createNanoEvents<{
	keepScrollToRight: () => void;
}>();

// If animation is enabled, the modal gets stuck open on Android Chrome 91. This happens only when clicking Cancel/Save - the X and clicking outside the modal still works to close it. All my code is working - show does get set false, it does get rendered, just still displayed. Disabling ads makes no difference. It works when calling programmatically wtih ButtonElement.click() but not with an actual click. Disabling animation fixes it though. Also https://mail.google.com/mail/u/0/#inbox/FMfcgzGkZGhkhtPsGFPFxcKxhvZFkHpl
const animation = false;

class MyModalManager extends BootstrapModalManager {
	// True while the app scroll container is frozen for an open modal.
	private locked = false;

	override setContainerStyle(containerState: ContainerState) {
		super.setContainerStyle(containerState);

		// Freeze the app's scroll container (#content) at its current position
		// while a modal is open, instead of react-bootstrap's <body> overflow lock
		// (the app body never scrolls; #content does). Because #content simply
		// stops scrolling - no <body> position:fixed pin, no scroll math - the
		// sticky header can't jump, and this is robust on iOS where the old
		// body-fixed hack mis-rendered every fixed element. setContainerStyle only
		// fires for the FIRST modal in the stack, so this locks exactly once.
		if (!this.locked) {
			this.locked = true;
			const scrollEl = getScrollEl();
			scrollEl.style.overflowY = "hidden";
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

		// Unfreeze #content (fires only when the LAST modal closes); its scrollTop
		// was preserved throughout, so nothing needs restoring.
		if (this.locked) {
			this.locked = false;
			getScrollEl().style.overflowY = "";
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
