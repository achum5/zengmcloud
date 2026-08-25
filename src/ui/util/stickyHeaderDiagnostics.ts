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

export const collectHeaderSnapshot = (): HeaderSnapshot => {
	const header = document.querySelector<HTMLElement>(
		".navbar-border.sticky-top",
	);
	const content = document.getElementById("content");
	const headerStyle = header ? getComputedStyle(header) : undefined;
	const contentStyle = content ? getComputedStyle(content) : undefined;
	const rect = header?.getBoundingClientRect();
	const vv = window.visualViewport;

	// The deepest element whose right edge sticks out past the screen without
	// any ancestor clipping it - the kind of element that expands the iOS layout
	// viewport. Bounded walk; "-" when nothing does.
	const widestElement = () => {
		try {
			const limit = Math.max(window.screen.width, 320);
			const clips = (el: Element) => {
				for (let a = el.parentElement; a; a = a.parentElement) {
					if (getComputedStyle(a).overflowX !== "visible") {
						return true;
					}
				}
				return false;
			};
			let worst: { right: number; desc: string } | undefined;
			let seen = 0;
			for (const el of document.querySelectorAll("body *")) {
				if (seen++ > 4000) {
					break;
				}
				const r = el.getBoundingClientRect();
				if (r.right > limit + 2 && (!worst || r.right > worst.right)) {
					if (clips(el)) {
						continue;
					}
					const cls =
						typeof el.className === "string"
							? el.className.split(" ").slice(0, 3).join(".")
							: "";
					worst = {
						right: Math.round(r.right),
						desc: `${el.tagName.toLowerCase()}${cls ? `.${cls}` : ""} right=${Math.round(r.right)} w=${Math.round(r.width)}`,
					};
				}
			}
			return worst ? worst.desc : "-";
		} catch {
			return "(scan failed)";
		}
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
		modalPinned: document.querySelector(".ios-modal-pinned") !== null,
		contentPosition: contentStyle?.position,
		contentPaddingTop: contentStyle?.paddingTop,
		// The bottom ticker comes unstuck the same way. What matters for it is the
		// gap between its bottom edge and the foot of the layout viewport: zero on
		// a healthy bar, whatever it drifted by on a broken one.
		...tickerFields(),
		// THE ELEMENT THAT BROKE THE VIEWPORT, when one has. iOS expands the
		// layout viewport to fit anything that renders wider than the screen and
		// keeps the expansion until the next launch - a field device sat at 518px
		// on a 440pt screen for days, both sticky bars anchored partly off the
		// glass. minimum-scale=1 now prevents the expansion, so an overwide
		// element shows up as a pannable page instead; either way, this names it.
		widestElement: widestElement(),
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
