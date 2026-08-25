// A flight recorder for the sticky header.
//
// The header coming unstuck has never been reproducible anywhere but a real
// iOS device, which is why it has been fixed and un-fixed several times (see the
// history at the top of stickyHeaderWatchdog.ts). Guessing from a screenshot is
// what keeps failing, so the app now records what actually happened and hands it
// over on request.
//
// The log matters more than a live snapshot, because by the time anyone can tap
// the button the evidence is usually gone: a detached header is only detectable
// while the page is scrolled, and reaching a button in the header means
// scrolling back to the top, where a broken header and a healthy one sit in
// exactly the same place.

export type HeaderLogEntry = {
	// Milliseconds since the page loaded - relative time is what matters here,
	// and it avoids a wall clock in the report.
	at: number;
	// What happened: "detached", "repaired", "gave-up", "forced", ...
	kind: string;
	scrollY: number;
	headerTop: number;
	detail?: string;
};

// Enough to cover a resume plus several scrolls, small enough to paste.
const MAX_ENTRIES = 60;

const entries: HeaderLogEntry[] = [];

export const recordHeaderEvent = (entry: Omit<HeaderLogEntry, "at">) => {
	entries.push({ ...entry, at: Math.round(performance.now()) });
	if (entries.length > MAX_ENTRIES) {
		entries.shift();
	}
};

export const getHeaderLog = (): HeaderLogEntry[] => [...entries];

export type HeaderSnapshot = Record<string, unknown>;

// Pure so the formatting is testable without a DOM.
export const formatHeaderReport = (
	snapshot: HeaderSnapshot,
	log: HeaderLogEntry[],
): string => {
	const lines: string[] = ["ZenGM sticky header report", ""];

	for (const [key, value] of Object.entries(snapshot)) {
		lines.push(`${key}: ${value === undefined ? "-" : String(value)}`);
	}

	lines.push("", `log (${log.length} entries, ms since load):`);
	if (log.length === 0) {
		lines.push("  (empty - the header never reported a fault in this session)");
	} else {
		for (const e of log) {
			const detail = e.detail === undefined ? "" : ` ${e.detail}`;
			lines.push(
				`  ${e.at} ${e.kind} scrollY=${e.scrollY} headerTop=${e.headerTop}${detail}`,
			);
		}
	}

	return lines.join("\n");
};

const num = (value: number) => Math.round(value);

// tag#id.class, trimmed - enough to find the element in the source without
// making the report unreadable.
const describe = (el: Element): string => {
	const id = el.id ? `#${el.id}` : "";
	const cls =
		typeof el.className === "string" && el.className.trim()
			? `.${el.className.trim().split(/\s+/).slice(0, 3).join(".")}`
			: "";
	return `${el.tagName.toLowerCase()}${id}${cls}`;
};

// The sticky probe, as its own export so the watchdog can take one at the
// moment it declares a fault - which is the reading that matters, and is not
// the reading the report button can take (the button is at the top of the page,
// where a broken bar and a healthy one sit in the same place).
//
// Returns undefined when there is nowhere to put it.
export const probeStickyTop = (): number | undefined => {
	const host = document.getElementById("content");
	if (!host) {
		return undefined;
	}
	const probe = document.createElement("div");
	probe.style.cssText =
		"position:sticky;top:0;height:0;width:0;pointer-events:none";
	try {
		host.prepend(probe);
		return num(probe.getBoundingClientRect().top);
	} catch {
		return undefined;
	} finally {
		probe.remove();
	}
};

// See headerLift in the snapshot: the header's offset inside its own parent,
// which is zero for a header that is not sticking and `scrollY` for one that
// is, in whatever coordinate space the rects happen to be reported in.
const headerLift = (
	header: HTMLElement | null,
	rect: DOMRect | undefined,
): number | string => {
	const parent = header?.parentElement;
	if (!parent || !rect) {
		return "-";
	}
	return num(rect.top - parent.getBoundingClientRect().top);
};

// EVERY ANCESTOR OF THE HEADER, all the way to <html>.
//
// position:sticky is broken by things that live on ancestors, not on the
// element: an overflow that turns one into a scrollport of its own, a transform
// or filter that makes one the containing block, a `contain` that cuts the
// subtree off from the scroller. The old clipping-ancestor check looked for
// exactly one of those and stopped at <body>, so it answered "none" for a
// header whose ancestors were never fully examined. This prints the chain and
// lets the reader judge, which is what a diagnostic is for.
const ancestorChain = (element: HTMLElement): string => {
	const parts: string[] = [];
	let node: HTMLElement | null = element.parentElement;
	let depth = 0;
	while (node && depth++ < 12) {
		const style = getComputedStyle(node);
		const rect = node.getBoundingClientRect();
		const flags = [
			style.position,
			`ovf=${style.overflowX}/${style.overflowY}`,
			`top=${num(rect.top)}`,
			`h=${num(rect.height)}`,
		];
		// Only worth the characters when they are set to something.
		if (style.transform !== "none") {
			flags.push(`transform=${style.transform}`);
		}
		if (style.filter !== "none") {
			flags.push(`filter=${style.filter}`);
		}
		if (style.willChange !== "auto") {
			flags.push(`will-change=${style.willChange}`);
		}
		if (style.contain !== "none") {
			flags.push(`contain=${style.contain}`);
		}
		if (style.zoom !== "1" && style.zoom !== "") {
			flags.push(`zoom=${style.zoom}`);
		}
		parts.push(`${describe(node)}[${flags.join(" ")}]`);
		node = node.parentElement;
	}
	return parts.join(" < ");
};

export const collectHeaderSnapshot = (): HeaderSnapshot => {
	const header = document.querySelector<HTMLElement>(
		".navbar-border.sticky-top",
	);
	const content = document.getElementById("content");
	const headerStyle = header ? getComputedStyle(header) : undefined;
	const contentStyle = content ? getComputedStyle(content) : undefined;
	const rect = header?.getBoundingClientRect();
	const vv = window.visualViewport;

	// THE ELEMENT THAT OVERFLOWS ITS OWN BOX, which is what makes iOS widen the
	// layout viewport in the first place.
	//
	// The first version of this asked which elements render wider than
	// window.screen.width, and that question is circular: once iOS HAS widened
	// the viewport, every full-width element in the page reports exactly the new
	// width, so the scan named an innocent wrapper and said nothing about the
	// cause. A field report duly came back naming the app's own layout column.
	//
	// scrollWidth > clientWidth is not circular. It is true only of an element
	// whose CONTENT does not fit inside it, whatever the viewport happens to be,
	// so it names the thing that is actually too wide and stays silent about
	// everything that is merely as wide as the page. Elements that scroll on
	// purpose are skipped - a horizontally scrollable table is doing its job.
	// Bounded walk; "-" when nothing overflows.
	const overflowingElement = () => {
		try {
			let worst: { over: number; desc: string } | undefined;
			let seen = 0;
			for (const el of document.querySelectorAll("body *")) {
				if (seen++ > 4000) {
					break;
				}
				// Not every element answers these (an <svg> child reports zero for
				// both), so an unusable difference is skipped rather than ranked.
				const over = el.scrollWidth - el.clientWidth;
				if (!Number.isFinite(over) || over <= 1) {
					continue;
				}
				// Ties go to the LATER element, which in document order is the
				// deeper one: a nested column and its parent overflow by exactly
				// the same amount, and the inner box is nearer the thing at fault.
				if (worst && over < worst.over) {
					continue;
				}
				// An element that is allowed to scroll sideways contains its own
				// overflow and cannot push the viewport out.
				const { overflowX } = getComputedStyle(el);
				if (overflowX !== "visible" && overflowX !== "clip") {
					continue;
				}
				// The overflowing box is the CONTAINER; the thing that is actually
				// too wide is a child sticking out of it, and that is the one worth
				// naming. Verified in a browser: a 900px div in a 440px page shows
				// up as its parent overflowing, not as itself.
				worst = {
					over,
					desc: `${describe(el)} content=${el.scrollWidth} client=${el.clientWidth} via ${widestChild(el)}`,
				};
			}
			return worst ? worst.desc : "-";
		} catch {
			return "(scan failed)";
		}
	};

	// Which child is sticking out of an overflowing box. Direct children only -
	// the walk above visits every box in the page, so the deepest overflowing
	// container is the one that gets here and its own children are the answer.
	const widestChild = (el: Element) => {
		let widest: { width: number; desc: string } | undefined;
		for (const child of el.children) {
			const width = child.getBoundingClientRect().width;
			if (!widest || width > widest.width) {
				widest = { width, desc: `${describe(child)} w=${num(width)}` };
			}
		}
		return widest ? widest.desc : "(no children)";
	};

	// DOES STICKY WORK ON THIS PAGE AT ALL?
	//
	// This is the measurement five rounds of reports could not make. Everything
	// else describes the header, and a header at its static position looks the
	// same whether WebKit lost this one element's sticky node or froze the whole
	// scrolling tree - two faults with completely different fixes.
	//
	// So put a brand-new sticky element in the page, right where the header
	// lives, and read where it lands. A node created a moment ago cannot be
	// stale, so:
	//
	//   probe stuck (0), header not      -> the header's own node is stale, and
	//                                       rebuilding it is the right repair
	//   probe at -scrollY, like header   -> the scrolling tree is frozen for the
	//                                       whole document; no per-element
	//                                       repair can help
	//
	// Zero-height so inserting it moves nothing, and removed before returning.
	const stickyProbe = () => {
		const top = probeStickyTop();
		return top === undefined ? "-" : String(top);
	};

	return {
		version: window.bbgmVersion,
		ua: navigator.userAgent,
		// A home-screen PWA is the configuration the fault shows up in.
		standalone:
			window.matchMedia("(display-mode: standalone)").matches ||
			(navigator as { standalone?: boolean }).standalone === true,
		url: window.location.pathname,
		scrollY: num(window.scrollY),
		docHeight: num(document.documentElement.scrollHeight),
		innerHeight: window.innerHeight,
		// Height/offset/scale of the visible viewport against the layout one. A
		// non-zero offsetTop or a scale away from 1 means sticky is anchored above
		// what the user can see - which is a different fault to a stale node.
		visualViewport: vv
			? `${num(vv.height)}@${num(vv.offsetTop)}x${vv.scale.toFixed(2)}`
			: "-",
		// Width and the physical screen, for catching a GHOST height: a resume
		// can restore a stale keyboard-sized vv.height with no keyboard present,
		// and the tell is a height wildly short of what width * screen aspect
		// says it should be. The focused element settles whether a keyboard
		// could even be up.
		visualViewportWidth: vv ? num(vv.width) : "-",
		screen: `${window.screen.width}x${window.screen.height}`,
		activeElement: document.activeElement
			? document.activeElement.tagName.toLowerCase()
			: "(none)",
		// A document wider than the screen is what makes iOS zoom out in the first
		// place, so it is the thing to chase if scale is not 1.
		docWidth: num(document.documentElement.scrollWidth),
		innerWidth: window.innerWidth,
		headerTransform: header?.style.transform || "(none)",
		headerFound: header !== null,
		headerTop: rect ? num(rect.top) : "-",
		headerHeight: rect ? num(rect.height) : "-",
		headerPosition: headerStyle?.position,
		headerCssTop: headerStyle?.top,
		headerDisplay: headerStyle?.display,
		headerZIndex: headerStyle?.zIndex,
		headerInlinePosition: header?.style.position || "(none)",
		// A non-visible overflow on any ancestor silently breaks position:sticky,
		// so name the culprit if there is one.
		clippingAncestor: header ? findClippingAncestor(header) : "-",
		// HOW FAR STICKY HAS LIFTED THE HEADER OFF ITS OWN STATIC POSITION, and
		// what it should be. This is the one header measurement that does not
		// depend on knowing which viewport the rects are reported against,
		// because both rects are read in the same breath and the coordinate space
		// cancels: a header sticking properly sits `scrollY` below where the flow
		// would have put it, and a header that has come unstuck sits at zero,
		// whatever iOS believes about scale, panning or the size of the screen.
		//
		// Five reports argued about coordinate spaces. This number ends that
		// argument - though not the other one: on iOS the main thread's rects go
		// stale during a flick and its momentum, so like every other reading here
		// it only means something with the page at rest.
		headerLift: headerLift(header, rect),
		headerLiftExpected: num(window.scrollY),
		// A sticky element created a moment ago, measured where the header is.
		// See stickyProbe: it separates a stale node from a frozen scrolling
		// tree, which is the difference between a repairable fault and one that
		// no repair of the element can touch.
		stickyProbe: stickyProbe(),
		// WHICH ELEMENT IS ACTUALLY SCROLLING. A document scroller is the whole
		// premise of both bars (see the header CSS); if this ever reads anything
		// but html, sticky is anchored somewhere nobody designed for.
		scroller: document.scrollingElement
			? describe(document.scrollingElement)
			: "-",
		docScrollTop: num(document.documentElement.scrollTop),
		bodyScrollTop: num(document.body.scrollTop),
		modalPinned: document.querySelector(".ios-modal-pinned") !== null,
		contentPosition: contentStyle?.position,
		contentPaddingTop: contentStyle?.paddingTop,
		contentTop: content ? num(content.getBoundingClientRect().top) : "-",
		contentHeight: content ? num(content.getBoundingClientRect().height) : "-",
		// The full chain, because the single-culprit check above stopped at
		// <body> and could only report one kind of culprit.
		headerAncestors: header ? ancestorChain(header) : "-",
		// The bottom ticker comes unstuck the same way. What matters for it is the
		// gap between its bottom edge and the foot of the layout viewport: zero on
		// a healthy bar, whatever it drifted by on a broken one.
		...tickerFields(),
		// THE ELEMENT THAT BROKE THE VIEWPORT, when one has. iOS expands the
		// layout viewport to fit anything that renders wider than the screen and
		// keeps the expansion until the next launch - a field device sat at 518px
		// on a 440pt screen for days, both sticky bars anchored partly off the
		// glass, and minimum-scale=1 did NOT stop it (a later report from the
		// build that added it came back at 518 again). So this has to name the
		// element on its own merits rather than by width - see
		// overflowingElement.
		overflowingElement: overflowingElement(),
	};
};

const tickerFields = (): HeaderSnapshot => {
	const ticker = document.querySelector<HTMLElement>(".league-ticker");
	if (!ticker) {
		return { tickerFound: false };
	}
	const rect = ticker.getBoundingClientRect();
	const style = getComputedStyle(ticker);
	return {
		tickerFound: true,
		tickerBottom: num(rect.bottom),
		tickerGap: num(document.documentElement.clientHeight - rect.bottom),
		tickerPosition: style.position,
		tickerInlinePosition: ticker.style.position || "(none)",
		tickerTransform: ticker.style.transform || "(none)",
	};
};

// The nearest ancestor whose overflow would stop the header sticking, if any.
const findClippingAncestor = (element: HTMLElement): string => {
	let node = element.parentElement;
	while (node && node !== document.body) {
		const { overflow, overflowX, overflowY } = getComputedStyle(node);
		if (
			[overflow, overflowX, overflowY].some(
				(value) => value !== "visible" && value !== "",
			)
		) {
			const id = node.id ? `#${node.id}` : "";
			const cls = node.className
				? `.${String(node.className).trim().split(/\s+/).join(".")}`
				: "";
			return `${node.tagName.toLowerCase()}${id}${cls} (${overflow}/${overflowX}/${overflowY})`;
		}
		node = node.parentElement;
	}
	return "none";
};

export const buildHeaderReport = (): string =>
	formatHeaderReport(collectHeaderSnapshot(), getHeaderLog());

// Clipboard access is gesture-gated and can simply be absent (older iOS, an
// insecure origin), so fall back to the legacy path before giving up - a report
// nobody can copy is useless.
export const copyText = async (text: string): Promise<boolean> => {
	try {
		await navigator.clipboard.writeText(text);
		return true;
	} catch {
		// Fall through.
	}

	try {
		const textarea = document.createElement("textarea");
		textarea.value = text;
		textarea.setAttribute("readonly", "");
		textarea.style.position = "fixed";
		textarea.style.top = "0";
		textarea.style.opacity = "0";
		document.body.append(textarea);
		textarea.select();
		textarea.setSelectionRange(0, text.length);
		const copied = document.execCommand("copy");
		textarea.remove();
		return copied;
	} catch {
		return false;
	}
};
