import clsx from "clsx";
import { ErrorBoundary } from "../../../components/ErrorBoundary.tsx";
import {
	useEffect,
	useRef,
	useState,
	type CSSProperties,
	type ReactNode,
} from "react";

// THE GAME PAGE AS ONE LONG PAGE, a menu floating at the foot of the window:
// each of its sections a button that takes you there, the one you are in
// lit up as you scroll - and the quarter the box score shows, to the left of
// it.
export type DockSection = {
	id: string;
	label: string;
	icon: ReactNode;
};

const svg = (children: ReactNode) => (
	<svg
		width="20"
		height="20"
		viewBox="0 0 20 20"
		fill="none"
		stroke="currentColor"
		strokeWidth="1.6"
		strokeLinecap="round"
		strokeLinejoin="round"
		aria-hidden
	>
		{children}
	</svg>
);

export const DOCK_ICONS = {
	box: svg(
		<>
			<rect x="2.5" y="3" width="4" height="3.5" rx="0.6" />
			<rect x="8" y="3" width="4" height="3.5" rx="0.6" />
			<rect x="13.5" y="3" width="4" height="3.5" rx="0.6" />
			<rect x="2.5" y="8.25" width="4" height="3.5" rx="0.6" />
			<rect x="8" y="8.25" width="4" height="3.5" rx="0.6" />
			<rect x="13.5" y="8.25" width="4" height="3.5" rx="0.6" />
			<rect x="2.5" y="13.5" width="4" height="3.5" rx="0.6" />
			<rect x="8" y="13.5" width="4" height="3.5" rx="0.6" />
			<rect x="13.5" y="13.5" width="4" height="3.5" rx="0.6" />
		</>,
	),
	recap: svg(
		<>
			<rect x="3" y="2.5" width="14" height="15" rx="1.5" />
			<path d="M6 6.5h8M6 10h8M6 13.5h5" />
		</>,
	),
	stats: svg(
		<>
			<path d="M3 17h14" />
			<rect x="4.5" y="9" width="2.5" height="6" rx="0.5" />
			<rect x="8.75" y="5" width="2.5" height="10" rx="0.5" />
			<rect x="13" y="11" width="2.5" height="4" rx="0.5" />
		</>,
	),
	flow: svg(
		<>
			<path d="M2.5 16.5h15" />
			<path d="M3 13l3.5-4 3 2.5L13 6l4 3.5" />
		</>,
	),
	shots: svg(
		<>
			<rect x="2.5" y="3" width="15" height="14" rx="1" />
			<path d="M7 3v5.5h6V3" />
			<path d="M5 17a5 5 0 0 1 10 0" />
		</>,
	),
	plays: svg(
		<>
			<rect x="3" y="3" width="14" height="6" rx="1.2" />
			<rect x="3" y="11" width="14" height="6" rx="1.2" />
		</>,
	),
	lineups: svg(
		<>
			<circle cx="7.5" cy="7" r="2.8" />
			<circle cx="13.5" cy="7.5" r="2.3" />
			<path d="M2.5 16.5c.6-3 2.6-4.6 5-4.6s4.4 1.6 5 4.6" />
			<path d="M13 12c2 .1 3.6 1.5 4.3 4.3" />
		</>,
	),
	onoff: svg(
		<>
			<path d="M10 2.5v7" />
			<path d="M5.5 5.3a6.5 6.5 0 1 0 9 0" />
		</>,
	),
	live: svg(
		<>
			<circle cx="10" cy="10" r="2" fill="currentColor" />
			<path d="M6.5 13.5a5 5 0 0 1 0-7M13.5 6.5a5 5 0 0 1 0 7" />
			<path d="M4 16a8.5 8.5 0 0 1 0-12M16 4a8.5 8.5 0 0 1 0 12" />
		</>,
	),
	player: svg(
		<>
			<circle cx="10" cy="6.5" r="3.2" />
			<path d="M3.5 17.5c.8-3.6 3.3-5.5 6.5-5.5s5.7 1.9 6.5 5.5" />
		</>,
	),
	ratings: svg(
		<>
			<path d="M10 2.8l2.2 4.6 5 .7-3.6 3.5.9 5-4.5-2.4-4.5 2.4.9-5L2.8 8.1l5-.7z" />
		</>,
	),
	career: svg(
		<>
			<path d="M6 3h8v4.5a4 4 0 0 1-8 0z" />
			<path d="M6 4.5H3.5a2.5 2.5 0 0 0 2.6 3M14 4.5h2.5a2.5 2.5 0 0 1-2.6 3" />
			<path d="M10 11.5v3M7 17h6M8 14.5h4" />
		</>,
	),
	schedule: svg(
		<>
			<rect x="3" y="4" width="14" height="13" rx="1.5" />
			<path d="M3 8h14M7 2.5v3M13 2.5v3" />
			<path d="M6.5 11.5h2M11.5 11.5h2M6.5 14.5h2" />
		</>,
	),
	finances: svg(
		<>
			<circle cx="10" cy="10" r="7.5" />
			<path d="M12.6 7.3c-.5-.9-1.5-1.4-2.6-1.4-1.5 0-2.6.8-2.6 2s1.1 1.7 2.6 2.1 2.6.9 2.6 2.1-1.1 2-2.6 2c-1.2 0-2.2-.5-2.7-1.4M10 4.5v11" />
		</>,
	),
	picks: svg(
		<>
			<rect x="4" y="2.5" width="12" height="15" rx="1.5" />
			<path d="M7 6.5h6M7 10h6M7 13.5h3.5" />
		</>,
	),
	moves: svg(
		<>
			<path d="M3.5 7h12M12.5 4l3 3-3 3" />
			<path d="M16.5 13h-12M7.5 10l-3 3 3 3" />
		</>,
	),
	more: svg(
		<>
			<circle cx="4.5" cy="10" r="0.6" fill="currentColor" />
			<circle cx="10" cy="10" r="0.6" fill="currentColor" />
			<circle cx="15.5" cy="10" r="0.6" fill="currentColor" />
		</>,
	),
	info: svg(
		<>
			<circle cx="10" cy="10" r="7.5" />
			<path d="M10 9v5" />
			<circle cx="10" cy="6.3" r="0.4" fill="currentColor" />
		</>,
	),
};

// THE SECTION YOU ARE IN, AS YOU SCROLL: the last one whose top has come up
// past a line a little way down the window (under anything pinned over the
// top - topPx, the live game's court) - at the foot of the page, the last one
// in sight. Read once a frame while the page moves, so the menu keeps up with
// it.
export const useScrollSpy = (ids: string[], topPx = 0) => {
	const [active, setActive] = useState(ids[0]);
	// (A button tapped stays lit while the page scrolls to it - the sections it
	// passes on the way don't take it - until the page stops.)
	const heldUntil = useRef(0);
	const key = `${ids.join(",")}|${topPx}`;
	useEffect(() => {
		if (typeof window === "undefined") {
			return;
		}
		let frame = 0;
		const read = () => {
			frame = 0;
			if (Date.now() < heldUntil.current) {
				return;
			}
			const vh = window.innerHeight;
			const pinned =
				Number.parseFloat(
					getComputedStyle(document.documentElement).getPropertyValue(
						"--teamb-h",
					),
				) || 0;
			const top = Math.max(topPx, 60 + pinned);
			const line = top + 0.3 * Math.max(0, vh - top);
			const atFoot =
				window.scrollY + vh >= document.documentElement.scrollHeight - 2;
			let pick: string | undefined;
			for (const id of ids) {
				const el = document.getElementById(`recap-${id}`);
				if (!el) {
					continue;
				}
				const y = el.getBoundingClientRect().top;
				if (y <= line || (atFoot && y < vh - 80)) {
					pick = id;
				}
			}
			setActive(pick ?? ids[0]);
		};
		const onScroll = () => {
			if (frame === 0) {
				frame = requestAnimationFrame(read);
			}
		};
		// (A tap's hold let go once the page stops.)
		const onScrollEnd = () => {
			if (heldUntil.current > Date.now()) {
				heldUntil.current = Date.now() + 120;
			}
		};
		window.addEventListener("scroll", onScroll, { passive: true });
		window.addEventListener("resize", onScroll);
		window.addEventListener("scrollend", onScrollEnd);
		read();
		return () => {
			window.removeEventListener("scroll", onScroll);
			window.removeEventListener("resize", onScroll);
			window.removeEventListener("scrollend", onScrollEnd);
			cancelAnimationFrame(frame);
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [key]);
	const choose = (id: string) => {
		heldUntil.current = Date.now() + 2500;
		setActive(id);
	};
	return [active, choose] as const;
};

export const RecapDock = ({
	sections,
	quarter,
	topPx,
}: {
	sections: DockSection[];
	quarter?: ReactNode;
	topPx?: number;
}) => {
	const [active, setActive] = useScrollSpy(
		sections.map((s) => s.id),
		topPx,
	);
	// (On a phone the bar slides sideways: the one you are in kept in view.)
	const navRef = useRef<HTMLElement>(null);
	useEffect(() => {
		const nav = navRef.current;
		const button = nav?.querySelector<HTMLElement>("button.active");
		if (!nav || !button || nav.scrollWidth <= nav.clientWidth) {
			return;
		}
		const left = button.offsetLeft - (nav.clientWidth - button.offsetWidth) / 2;
		nav.scrollTo({ left: Math.max(0, left), behavior: "smooth" });
	}, [active]);
	return (
		<div className="recap-dock-wrap">
			{quarter ? <div className="recap-quarter">{quarter}</div> : null}
			<nav className="recap-dock" aria-label="Sections" ref={navRef}>
				{sections.map((s) => (
					<button
						key={s.id}
						type="button"
						className={clsx({ active: active === s.id })}
						onClick={() => {
							setActive(s.id);
							jumpToSection(s.id);
						}}
					>
						{s.icon}
						<span>{s.label}</span>
					</button>
				))}
			</nav>
		</div>
	);
};

// TO A SECTION: every section above it drawn first (see RecapSections - a
// section is drawn only once it comes near the window), then there.
const JUMP_EVENT = "recap-jump";
export const jumpToSection = (id: string) => {
	const event = new CustomEvent(JUMP_EVENT, { detail: id, cancelable: true });
	window.dispatchEvent(event);
	// (No RecapSections on the page - every section already drawn: straight
	// there.)
	if (!event.defaultPrevented) {
		document
			.getElementById(`recap-${id}`)
			?.scrollIntoView({ behavior: "smooth", block: "start" });
	}
};

export type RecapSectionDef = DockSection & {
	// Over it (its label unless given; null for none - a section with its own).
	title?: string | null;
	body: ReactNode;
};

// About how tall a section is before it is drawn, so the page is about as long
// as it will be.
const UNDRAWN_HEIGHT = 420;

// A SECTION, DRAWN ONCE IT IS NEAR: its heading always, its body once it comes
// within a window or so of being seen (or a jump goes to it or past it) - and
// kept from then on. The page as one long page costs no more to open than the
// part of it on screen.
const LazySection = ({
	section,
	force,
}: {
	section: RecapSectionDef;
	force: boolean;
}) => {
	const ref = useRef<HTMLElement>(null);
	const [near, setNear] = useState(false);
	const shown = near || force;
	useEffect(() => {
		if (shown) {
			return;
		}
		const el = ref.current;
		if (!el || typeof IntersectionObserver === "undefined") {
			setNear(true);
			return;
		}
		const observer = new IntersectionObserver(
			(entries) => {
				if (entries.some((e) => e.isIntersecting)) {
					setNear(true);
					observer.disconnect();
				}
			},
			{ rootMargin: "100% 0px 100% 0px" },
		);
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, [shown]);
	const style: CSSProperties | undefined = shown
		? undefined
		: { minHeight: UNDRAWN_HEIGHT };
	return (
		<section
			ref={ref}
			id={`recap-${section.id}`}
			className="recap-section"
			style={style}
		>
			{section.title === null ? null : (
				<h2 className="recap-section-title">
					{section.title ?? section.label}
				</h2>
			)}
			{/* (A section that fails says so, in its place - the page goes on.) */}
			{shown ? <ErrorBoundary local>{section.body}</ErrorBoundary> : null}
		</section>
	);
};

// THE PAGE'S SECTIONS AND THE MENU AT ITS FOOT. The first section drawn at
// once, the rest as they come near; a jump draws every section down to the one
// jumped to before scrolling, so it lands where it will stay.
export const RecapSections = ({
	sections,
	quarter,
	topPx,
}: {
	sections: RecapSectionDef[];
	quarter?: ReactNode;
	topPx?: number;
}) => {
	const [drawnTo, setDrawnTo] = useState(0);
	const wrapRef = useRef<HTMLDivElement>(null);
	const ids = sections.map((x) => x.id).join(",");
	useEffect(() => {
		const list = ids.split(",");
		const onJump = (e: Event) => {
			const id = (e as CustomEvent<string>).detail;
			const i = list.indexOf(id);
			if (i < 0) {
				return;
			}
			e.preventDefault();
			setDrawnTo((upTo) => Math.max(upTo, i));
			// (Two frames: the sections above it drawn and laid out.)
			requestAnimationFrame(() => {
				requestAnimationFrame(() => {
					const target = document.getElementById(`recap-${id}`);
					target?.scrollIntoView({ behavior: "smooth", block: "start" });
					// (Sections above it still loading - the team page's - grow,
					// and push it down: for a moment after, follow it.)
					const box = wrapRef.current;
					if (!target || !box || typeof ResizeObserver === "undefined") {
						return;
					}
					let grew = false;
					const observer = new ResizeObserver(() => {
						if (grew) {
							target.scrollIntoView({ block: "start" });
						}
						grew = true;
					});
					observer.observe(box);
					window.setTimeout(() => {
						observer.disconnect();
					}, 2500);
				});
			});
		};
		window.addEventListener(JUMP_EVENT, onJump);
		return () => {
			window.removeEventListener(JUMP_EVENT, onJump);
		};
	}, [ids]);
	return (
		<>
			<div className="recap-sections" ref={wrapRef}>
				{sections.map((x, i) => (
					<LazySection key={x.id} section={x} force={i <= drawnTo} />
				))}
			</div>
			<RecapDock sections={sections} quarter={quarter} topPx={topPx} />
		</>
	);
};

// FEWER BUTTONS, ALL ON SCREEN: sections folded into the one before them on
// the page (each under its own smaller heading), and shorter names on the
// menu - so the menu fits a phone without sliding.
export const foldSections = (
	sections: RecapSectionDef[],
	folds: [into: string, from: string, heading: string | null][],
	labels: Record<string, string> = {},
): RecapSectionDef[] => {
	const byId = new Map(sections.map((x) => [x.id, x]));
	const gone = new Set<string>();
	const extra = new Map<string, ReactNode[]>();
	for (const [into, from, heading] of folds) {
		const target = byId.get(into);
		const source = byId.get(from);
		if (!target || !source) {
			continue;
		}
		gone.add(from);
		const list = extra.get(into) ?? [];
		list.push(
			<div key={from} className="recap-folded" id={`recap-${from}`}>
				{heading === null ? null : (
					<h3 className="recap-subtitle">{heading}</h3>
				)}
				{source.body}
			</div>,
		);
		extra.set(into, list);
	}
	return sections
		.filter((x) => !gone.has(x.id))
		.map((x) => {
			const more = extra.get(x.id);
			const label = labels[x.id] ?? x.label;
			return {
				...x,
				label,
				// (Its heading stays what it was, not the short name.)
				title: x.title === undefined ? x.label : x.title,
				body: more ? (
					<>
						{x.body}
						{more}
					</>
				) : (
					x.body
				),
			};
		});
};
