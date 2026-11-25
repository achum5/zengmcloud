// A DAY'S AUTO PLAY SIM TIME IN A BUBBLE over the day: on hover with a mouse,
// or press and hold on a touch screen, which has no hover. Any element with the
// class sim-time-day and the time in data-sim-time (see simTimes.ts) gets one.
// A held touch doesn't open the day; a plain tap still does.

const HOVER_MS = 120;
const HOLD_MS = 450;
const MOVE_PX = 10;
// How long a bubble from a touch stays up.
const SHOWN_MS = 2500;

let installed = false;
let bubble: HTMLDivElement | undefined;
let hideTimer: number | undefined;

const hide = () => {
	window.clearTimeout(hideTimer);
	bubble?.remove();
	bubble = undefined;
};

const show = (
	el: Element,
	text: string,
	{ below = false, forMs }: { below?: boolean; forMs?: number } = {},
) => {
	hide();
	if (!el.isConnected) {
		return;
	}
	const tip = document.createElement("div");
	tip.className = "sim-time-tip";
	tip.setAttribute("role", "tooltip");
	tip.textContent = text;
	document.body.append(tip);

	// Over the day (under it if asked, or if there's no room), kept on screen.
	const day = el.getBoundingClientRect();
	const size = tip.getBoundingClientRect();
	const margin = 8;
	const left = Math.max(
		margin,
		Math.min(
			day.left + day.width / 2 - size.width / 2,
			window.innerWidth - margin - size.width,
		),
	);
	let top = below ? day.bottom + 6 : day.top - size.height - 6;
	if (top < margin) {
		top = day.bottom + 6;
	} else if (top + size.height > window.innerHeight - margin) {
		top = day.top - size.height - 6;
	}
	tip.style.left = `${left}px`;
	tip.style.top = `${top}px`;

	bubble = tip;
	if (forMs !== undefined) {
		hideTimer = window.setTimeout(hide, forMs);
	}
};

const dayOf = (target: EventTarget | null) => {
	const el = target instanceof Element ? target.closest(".sim-time-day") : null;
	const text = el?.getAttribute("data-sim-time");
	return el && text ? { el, text } : undefined;
};

export const installSimTimeTips = () => {
	if (installed || typeof document === "undefined") {
		return;
	}
	installed = true;

	// HOVER, with a mouse (not the mouse events a tap makes).
	let hovered: Element | undefined;
	let hoverTimer: number | undefined;
	document.addEventListener(
		"pointerover",
		(event) => {
			if (event.pointerType !== "mouse") {
				return;
			}
			const day = dayOf(event.target);
			if (day?.el === hovered) {
				return;
			}
			window.clearTimeout(hoverTimer);
			hide();
			hovered = day?.el;
			if (day) {
				hoverTimer = window.setTimeout(() => {
					// (The time as it is now: it moves along as sims go by.)
					const text = day.el.getAttribute("data-sim-time");
					if (hovered === day.el && text) {
						// (Under the header's days, not over the menus. A finger
						// is under a held day, so that one stays over it.)
						show(day.el, text, {
							below: !!day.el.closest(".navbar, .tbd-bar"),
						});
					}
				}, HOVER_MS);
			}
		},
		true,
	);
	document.addEventListener(
		"pointerout",
		(event) => {
			if (event.pointerType !== "mouse" || !hovered) {
				return;
			}
			const to = event.relatedTarget;
			if (to instanceof Node && hovered.contains(to)) {
				return;
			}
			window.clearTimeout(hoverTimer);
			hovered = undefined;
			hide();
		},
		true,
	);
	document.addEventListener(
		"pointerdown",
		(event) => {
			if (event.pointerType === "mouse") {
				window.clearTimeout(hoverTimer);
				hide();
			}
		},
		true,
	);

	// PRESS AND HOLD, on a touch screen.
	let press:
		| { x: number; y: number; timer: number; el: Element; text: string }
		| undefined;
	// This touch turned into a hold, so it opens nothing.
	let held = false;
	let swallowClicksUntil = 0;

	const cancel = () => {
		if (press) {
			window.clearTimeout(press.timer);
			press = undefined;
		}
	};

	document.addEventListener(
		"touchstart",
		(event) => {
			hide();
			cancel();
			held = false;
			const touch = event.touches[0];
			const day = dayOf(event.target);
			if (event.touches.length !== 1 || !touch || !day) {
				return;
			}
			press = {
				x: touch.clientX,
				y: touch.clientY,
				...day,
				timer: window.setTimeout(() => {
					if (press) {
						held = true;
						show(press.el, press.text, { forMs: SHOWN_MS });
						press = undefined;
					}
				}, HOLD_MS),
			};
		},
		{ capture: true, passive: true },
	);

	document.addEventListener(
		"touchmove",
		(event) => {
			const touch = event.touches[0];
			if (
				press &&
				touch &&
				Math.hypot(touch.clientX - press.x, touch.clientY - press.y) > MOVE_PX
			) {
				cancel();
			}
		},
		{ capture: true, passive: true },
	);

	document.addEventListener(
		"touchend",
		(event) => {
			cancel();
			if (held) {
				held = false;
				// (No click from this touch: it was a look, not a tap.)
				if (event.cancelable) {
					event.preventDefault();
				}
				swallowClicksUntil = Date.now() + 600;
			}
		},
		{ capture: true, passive: false },
	);

	document.addEventListener(
		"touchcancel",
		() => {
			cancel();
		},
		{ capture: true, passive: true },
	);

	document.addEventListener(
		"click",
		(event) => {
			if (Date.now() < swallowClicksUntil) {
				event.preventDefault();
				event.stopPropagation();
			}
		},
		true,
	);

	// Android's long-press menu for a link (open in new tab...) would cover it.
	document.addEventListener(
		"contextmenu",
		(event) => {
			if (press || held) {
				event.preventDefault();
			}
		},
		true,
	);

	window.addEventListener("scroll", hide, { capture: true, passive: true });
};
