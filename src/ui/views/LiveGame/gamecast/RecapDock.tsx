import clsx from "clsx";
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
	info: svg(
		<>
			<circle cx="10" cy="10" r="7.5" />
			<path d="M10 9v5" />
			<circle cx="10" cy="6.3" r="0.4" fill="currentColor" />
		</>,
	),
};

const band = (topPx: number) => {
	if (topPx <= 0 || typeof window === "undefined") {
		return "-35% 0px -55% 0px";
	}
	const vh = window.innerHeight;
	const rest = Math.max(1, vh - topPx);
	const top = topPx + 0.2 * rest;
	const bottom = vh - (topPx + 0.35 * rest);
	return `-${Math.round(top)}px 0px -${Math.round(Math.max(0, bottom))}px 0px`;
};

// The section in the middle band of the window, as you scroll - of the part
// of it under anything pinned over the top (topPx: the live game's court).
export const useScrollSpy = (ids: string[], topPx = 0) => {
	const [active, setActive] = useState(ids[0]);
	const key = `${ids.join(",")}|${topPx}`;
	useEffect(() => {
		const els = ids
			.map((id) => document.getElementById(`recap-${id}`))
			.filter((el): el is HTMLElement => el !== null);
		if (els.length === 0 || typeof IntersectionObserver === "undefined") {
			return;
		}
		const seen = new Map<string, boolean>();
		const observer = new IntersectionObserver(
			(entries) => {
				for (const e of entries) {
					seen.set(e.target.id.replace(/^recap-/, ""), e.isIntersecting);
				}
				const first = ids.find((id) => seen.get(id));
				if (first) {
					setActive(first);
				}
			},
			{ rootMargin: band(topPx) },
		);
		for (const el of els) {
			observer.observe(el);
		}
		return () => {
			observer.disconnect();
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [key]);
	return [active, setActive] as const;
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
	window.dispatchEvent(new CustomEvent(JUMP_EVENT, { detail: id }));
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
			{shown ? section.body : null}
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
	const ids = sections.map((x) => x.id).join(",");
	useEffect(() => {
		const list = ids.split(",");
		const onJump = (e: Event) => {
			const id = (e as CustomEvent<string>).detail;
			const i = list.indexOf(id);
			if (i < 0) {
				return;
			}
			setDrawnTo((upTo) => Math.max(upTo, i));
			// (Two frames: the sections above it drawn and laid out.)
			requestAnimationFrame(() => {
				requestAnimationFrame(() => {
					document
						.getElementById(`recap-${id}`)
						?.scrollIntoView({ behavior: "smooth", block: "start" });
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
			<div className="recap-sections">
				{sections.map((x, i) => (
					<LazySection key={x.id} section={x} force={i <= drawnTo} />
				))}
			</div>
			<RecapDock sections={sections} quarter={quarter} topPx={topPx} />
		</>
	);
};
