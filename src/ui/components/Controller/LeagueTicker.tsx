import {
	memo,
	useCallback,
	useEffect,
	useLayoutEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import clsx from "clsx";
import { localActions, useLocal } from "../../util/local.ts";
import { helpers } from "../../util/helpers.ts";
import { safeLocalStorage } from "../../util/safeLocalStorage.ts";
import { SafeHtml } from "../SafeHtml.tsx";
import {
	buildTickerSegments,
	buildTickerStream,
	segmentDurationSeconds,
	segmentTravelPx,
	tickerMayUpdate,
	type TickerHeader,
	type TickerItem,
} from "../../../common/ticker.ts";

// The ESPN bar, pinned to the bottom: the whole league's day scrolling past in
// one continuous loop - every score, the rest of today's slate with its point
// spread, the best individual performances, where the award races stand, and
// the news.
//
// The feed is assembled league-wide in the worker (updateTickerItems.ts). This
// side is a renderer, and it owns four behaviours worth naming.
//
// THE LOOK. It is broadcast chrome, not a page element: a dark bar in both
// themes, the way a score bar looks on television and on ESPN's own site. What
// distinguishes one kind of item from another is TYPE, not decoration - weight
// and colour carry the winner, the spread, the player, the award. An earlier
// version put a coloured pill in front of every item, which turned a strip of
// information into a strip of stickers.
//
// EVERYTHING GOES SOMEWHERE. A score opens its box score, a player opens their
// page, an award opens the race, a news item opens the feed. Note that the item
// bodies are therefore made of several sibling links rather than one wrapping
// link - an anchor cannot contain another anchor, and the news text arrives as
// HTML that already has player and team links inside it.
//
// FREEZING. While a live game is on screen the whole thing stops updating - see
// tickerMayUpdate. A ticker is the one widget that can spoil a game you are
// actively watching, and in a shared league it would do it to every follower at
// once. It holds the last stream it was given until the playback ends.
//
// THE ANIMATION. The track is duplicated and translated by -50%, the oldest
// marquee trick there is and still the right one: a single compositor-driven
// transform, no layout work per frame. It pauses on hover and on touch so the
// links inside it can be clicked, and it does not run at all under
// prefers-reduced-motion, where the bar stays and can be scrolled by hand.
//
// THE PAGE UNDERNEATH. Being fixed, it covers the bottom of every page unless
// the document is given that much extra scrollable space - see the body class.

// THE LEFT PANE.
//
// Two things it can hold. Usually the name of the block going past - SCORES,
// ODDS, MVP, TRANSACTIONS - said once, in a place that does not move. For a
// single game it holds the score itself, stacked, while that game's stat lines
// scroll beside it, which is the thing a broadcast ticker does that a list
// cannot.
//
// It used to be a label printed in front of every item, so a run of eighteen
// news items meant reading "TRANSACTIONS" eighteen times to learn one thing.
const Pane = ({ header }: { header: TickerHeader }) => {
	if (header.kind === "label") {
		return <span className="league-ticker-pane-label">{header.text}</span>;
	}

	const { away, home } = header;
	const side = (team: typeof away, lost: boolean) => (
		<span className={clsx("league-ticker-pane-row", { dim: lost })}>
			<span className="league-ticker-pane-abbrev">{team.abbrev}</span>
			<span className="league-ticker-pane-pts">{team.pts ?? 0}</span>
		</span>
	);
	const awayPts = away.pts ?? 0;
	const homePts = home.pts ?? 0;

	return (
		<span className="league-ticker-pane-score">
			{side(away, awayPts < homePts)}
			{side(home, homePts < awayPts)}
		</span>
	);
};

const Score = ({ item }: { item: Extract<TickerItem, { type: "score" }> }) => {
	const { away, home } = item;
	const awayPts = away.pts ?? 0;
	const homePts = home.pts ?? 0;
	const href = helpers.leagueUrl([
		"game_log",
		item.boxScoreTeam,
		item.season,
		item.gid,
	]);

	// The loser goes dim. It is the fastest way to read a final without reading
	// the numbers at all.
	const side = (team: typeof away, pts: number, lost: boolean) => (
		<>
			<span className={clsx("league-ticker-abbrev", { dim: lost })}>
				{team.abbrev}
			</span>
			<span className={clsx("league-ticker-pts", { dim: lost })}>{pts}</span>
		</>
	);

	return (
		<a className="league-ticker-item" href={href}>
			{side(away, awayPts, awayPts < homePts)}
			{side(home, homePts, homePts < awayPts)}
			<span className="league-ticker-state">
				{item.overtimes
					? `FINAL/${item.overtimes > 1 ? item.overtimes : ""}OT`
					: "FINAL"}
			</span>
		</a>
	);
};

const Upcoming = ({
	item,
}: {
	item: Extract<TickerItem, { type: "upcoming" }>;
}) => (
	<a
		className="league-ticker-item"
		href={helpers.leagueUrl(["daily_schedule"])}
	>
		<span className="league-ticker-abbrev">{item.away.abbrev}</span>
		<span className="league-ticker-at">@</span>
		<span className="league-ticker-abbrev">{item.home.abbrev}</span>
		{item.line ? <span className="league-ticker-line">{item.line}</span> : null}
	</a>
);

const Performance = ({
	item,
}: {
	item: Extract<TickerItem, { type: "performance" }>;
}) => (
	<span className="league-ticker-item">
		<a
			className="league-ticker-name"
			href={helpers.leagueUrl(["player", item.pid])}
		>
			{item.name}
		</a>
		<a
			className="league-ticker-stat"
			href={helpers.leagueUrl([
				"game_log",
				item.boxScoreTeam,
				item.season,
				item.gid,
			])}
		>
			{item.stat}
		</a>
	</span>
);

const Race = ({ item }: { item: Extract<TickerItem, { type: "race" }> }) => (
	<span className="league-ticker-item">
		{item.entries.map((entry, i) => (
			<span className="league-ticker-entry" key={entry.pid}>
				{i > 0 ? <span className="league-ticker-sep" /> : null}
				<a
					className="league-ticker-name"
					href={helpers.leagueUrl(["player", entry.pid])}
				>
					{entry.name}
				</a>
				{entry.odds ? (
					<span className="league-ticker-odds">{entry.odds}</span>
				) : null}
			</span>
		))}
	</span>
);

// The event text arrives as HTML with its own player and team links.
const News = ({ item }: { item: Extract<TickerItem, { type: "news" }> }) => (
	<span className="league-ticker-item league-ticker-news">
		<SafeHtml dirty={item.text} />
	</span>
);

const Item = ({ item }: { item: TickerItem }) => {
	if (item.type === "score") {
		return <Score item={item} />;
	}
	if (item.type === "upcoming") {
		return <Upcoming item={item} />;
	}
	if (item.type === "performance") {
		return <Performance item={item} />;
	}
	if (item.type === "race") {
		return <Race item={item} />;
	}
	return <News item={item} />;
};

const STORAGE_KEY = "bbgmShowLeagueTicker";

// A beat at the end of a block before the next one takes over, so the last item
// is not whipped away the instant it arrives.
const END_PAUSE_MS = 500;

// How long each page sits still under reduced motion.
const REDUCED_STEP_MS = 5000;

// Where the transform has actually reached, mid-glide. "none" and anything
// unparseable mean it has not moved.
const readOffset = (element: HTMLElement): number => {
	const { transform } = window.getComputedStyle(element);
	if (!transform || transform === "none") {
		return 0;
	}
	try {
		return Math.max(0, -new DOMMatrixReadOnly(transform).m41);
	} catch {
		return 0;
	}
};

export const LeagueTicker = memo(() => {
	const { lid, liveGameInProgress, mpLiveBroadcast, tickerItems } = useLocal([
		"lid",
		"liveGameInProgress",
		"mpLiveBroadcast",
		"tickerItems",
	]);

	const [show, setShow] = useState(
		() => safeLocalStorage.getItem(STORAGE_KEY) !== "false",
	);

	// Held across a live game rather than recomputed, so nothing that happens
	// during a playback reaches the screen until it is over.
	const frozen = useRef<TickerItem[]>([]);

	const mayUpdate = tickerMayUpdate({
		liveGameInProgress,
		watchingBroadcast: !!mpLiveBroadcast?.active,
	});

	const items = useMemo(() => {
		if (!mayUpdate) {
			return frozen.current;
		}
		const next = buildTickerStream(tickerItems);
		frozen.current = next;
		return next;
	}, [tickerItems, mayUpdate]);

	const segments = useMemo(() => buildTickerSegments(items), [items]);

	// WHICH BLOCK IS PLAYING, AND WHAT MAKES IT MOVE ON.
	//
	// A COUNTER, NOT AN INDEX INTO THE LIST. Every advance produces a new React
	// key even when the list has only one block in it, so that block is remounted
	// and its crawl runs again. Keyed on a plain index it parked at the end of its
	// one animation and stayed there forever, which is precisely what a preseason
	// league looks like: no games, no odds, no award races, and every event in the
	// log a transaction - one block, played once, frozen.
	const [cursor, setCursor] = useState(0);
	const segment =
		segments.length > 0 ? segments[cursor % segments.length]! : undefined;
	const blockKey = segment ? `${segment.key}#${cursor}` : undefined;

	// Advance at most once per block, whichever clock gets there first.
	const advancedFrom = useRef<string>(undefined);
	const advance = useCallback(() => {
		if (blockKey === undefined || advancedFrom.current === blockKey) {
			return;
		}
		advancedFrom.current = blockKey;
		setCursor((previous) => previous + 1);
	}, [blockKey]);

	// Reduced motion: keep the bar, drop the movement. Blocks still change - the
	// clock below does not depend on there being an animation to end.
	const [animate, setAnimate] = useState(true);
	useEffect(() => {
		const query = window.matchMedia("(prefers-reduced-motion: reduce)");
		const apply = () => setAnimate(!query.matches);
		apply();
		query.addEventListener("change", apply);
		return () => query.removeEventListener("change", apply);
	}, []);

	// HOLDING IT STILL TO CLICK SOMETHING - but only where there is a real
	// pointer. On a touch screen :hover latches after a tap and does not clear
	// until you tap somewhere else, which would stop the ticker dead; and a tap
	// on a link works whether or not the crawl paused first.
	const [pointer, setPointer] = useState(false);
	useEffect(() => {
		const query = window.matchMedia("(hover: hover) and (pointer: fine)");
		const apply = () => setPointer(query.matches);
		apply();
		query.addEventListener("change", apply);
		return () => query.removeEventListener("change", apply);
	}, []);
	const [held, setHeld] = useState(false);
	const holding = pointer && held;

	// HOW FAR AND HOW LONG THIS BLOCK HAS TO TRAVEL.
	//
	// Two layout reads per block - the viewport's width and the block's own - and
	// nothing per frame. Both are layout values rather than animated positions, so
	// they are honest on every platform. The crawl is held off until they are
	// taken, so it cannot start from the wrong place and jump.
	const viewportRef = useRef<HTMLDivElement>(null);
	const runRef = useRef<HTMLDivElement>(null);
	const viewportWidth = useRef(0);
	const [run, setRun] = useState<
		{ key: string; travel: number; duration: number } | undefined
	>();

	useLayoutEffect(() => {
		const measure = () => {
			const viewport = viewportRef.current;
			const element = runRef.current;
			if (!viewport || !element || blockKey === undefined) {
				return;
			}
			viewportWidth.current = viewport.getBoundingClientRect().width;
			const travel = segmentTravelPx(
				element.scrollWidth,
				viewportWidth.current,
			);
			setRun({
				key: blockKey,
				travel,
				duration: segmentDurationSeconds(travel),
			});
		};
		measure();
		window.addEventListener("resize", measure);
		return () => window.removeEventListener("resize", measure);
	}, [blockKey, items]);

	const ready = blockKey !== undefined && run?.key === blockKey;

	// HOW THE CONTENTS ACTUALLY MOVE.
	//
	// A transform this component sets, glided by a CSS transition - NOT a CSS
	// @keyframes animation. The difference is what happens when the animation does
	// not run, and on a real device that turns out to be a thing that happens: the
	// bar sat on its first transaction, correctly positioned, correctly measured,
	// and never moved a pixel. A keyframes animation that does not start leaves the
	// element exactly where it was and there is no way to notice.
	//
	// A transform is a fact about the element. If the transition runs, it glides;
	// if the transition is refused for any reason, the transform still lands and
	// the block jumps to its end instead. Degraded, but never static - and the
	// clock keeps running underneath either way.
	const [glide, setGlide] = useState<{
		key: string;
		offset: number;
		ms: number;
	}>();

	// Frozen where it stands when a mouse rests on it, by reading back the
	// transform the transition has reached. Resuming glides the rest of the way in
	// proportion, rather than restarting the block under the cursor.
	const frozenOffset = useRef(0);
	useEffect(() => {
		if (!holding || !runRef.current || blockKey === undefined) {
			return;
		}
		frozenOffset.current = readOffset(runRef.current);
		setGlide({ key: blockKey, offset: frozenOffset.current, ms: 0 });
	}, [blockKey, holding]);

	// THE CLOCK, and the movement it drives. A timer, not the end of an animation.
	//
	// animationend was the clock once, and it is a fine signal right up until there
	// is no animation to end - reduced motion turns it off, a block narrower than
	// the bar has nothing to travel, a paused animation never finishes, and a
	// browser may skip one that changes nothing. A timer cannot be skipped.
	useEffect(() => {
		if (!show || !ready || holding || blockKey === undefined) {
			return;
		}

		const from = frozenOffset.current;
		frozenOffset.current = 0;
		const remaining = Math.max(0, run.travel - from);
		const timers: ReturnType<typeof setTimeout>[] = [];

		if (animate) {
			// One glide to the far end, then on to the next block.
			const ms =
				run.travel > 0 ? run.duration * 1000 * (remaining / run.travel) : 0;
			setGlide({ key: blockKey, offset: run.travel, ms });
			timers.push(
				setTimeout(advance, Math.max(run.duration * 1000, ms) + END_PAUSE_MS),
			);
		} else {
			// Someone who has asked for less motion gets the block a page at a time
			// rather than a crawl - discrete, and still readable, where holding it
			// perfectly still would just be a frozen ticker.
			const page = Math.max(160, viewportWidth.current * 0.8);
			let offset = from;
			const step = () => {
				if (offset >= run.travel) {
					advance();
					return;
				}
				offset = Math.min(run.travel, offset + page);
				setGlide({ key: blockKey, offset, ms: 0 });
				timers.push(setTimeout(step, REDUCED_STEP_MS));
			};
			setGlide({ key: blockKey, offset: from, ms: 0 });
			timers.push(setTimeout(step, REDUCED_STEP_MS));
		}

		return () => {
			for (const timer of timers) {
				clearTimeout(timer);
			}
		};
	}, [
		advance,
		animate,
		blockKey,
		holding,
		ready,
		run?.duration,
		run?.travel,
		show,
	]);

	// `glide && ...`, not `glide?.key === blockKey`: with no league both sides are
	// undefined, the comparison is true, and the next line reads through nothing.
	const playing = glide !== undefined && glide.key === blockKey;
	const offset = playing ? glide.offset : 0;
	const glideMs = playing ? glide.ms : 0;

	const visible = lid !== undefined && segments.length > 0;
	useEffect(() => {
		localActions.update({ leagueTickerVisible: visible });

		// The bar is position:fixed, so it covers the bottom of every page unless
		// the document is given that much extra scrollable space. Without this the
		// last row of a table, or the buttons at the foot of a form, simply cannot
		// be reached. Collapsed it is only a sliver, so it takes back most of it.
		document.body.classList.toggle("has-league-ticker", visible);
		document.body.classList.toggle(
			"has-league-ticker-collapsed",
			visible && !show,
		);

		return () => {
			localActions.update({ leagueTickerVisible: false });
			document.body.classList.remove(
				"has-league-ticker",
				"has-league-ticker-collapsed",
			);
		};
	}, [visible, show]);

	if (!visible) {
		return null;
	}

	return (
		<div
			className={clsx("league-ticker", { collapsed: !show, held: holding })}
			onPointerEnter={() => setHeld(true)}
			onPointerLeave={() => setHeld(false)}
		>
			{show && segment ? (
				<div className="league-ticker-pane">
					{/* Keyed on the block, so React remounts it and the entrance
					    animation replays every time the pane changes - the change is
					    the signal that a new block has started. */}
					<div className="league-ticker-pane-in" key={blockKey}>
						<Pane header={segment.header} />
					</div>
				</div>
			) : null}
			<div className="league-ticker-viewport" ref={viewportRef}>
				{show && segment ? (
					<div
						className="league-ticker-run"
						// Names the block AND the pass, so a single-block feed still
						// changes it every time round. Read by the browser tests to see
						// the player advance when the pane text cannot show it.
						data-block={blockKey}
						// How far this block has to go, and how far it has got. The
						// browser tests assert on these, because "the block changed" and
						// "the contents moved" are different questions and only the
						// second one was ever really being asked.
						data-travel={ready ? Math.round(run.travel) : -1}
						key={blockKey}
						ref={runRef}
						style={{
							// Only as far as its own overflow, so the bar is full for the
							// whole block. A block that already fits travels nothing.
							transform: `translate3d(${-offset}px, 0, 0)`,
							transitionDuration: `${Math.round(glideMs)}ms`,
						}}
					>
						{segment.items.map((item) => (
							<Item key={item.key} item={item} />
						))}
					</div>
				) : null}
			</div>
			<button
				className="league-ticker-toggle"
				type="button"
				title={show ? "Hide ticker" : "Show ticker"}
				onClick={() => {
					const next = !show;
					setShow(next);
					safeLocalStorage.setItem(STORAGE_KEY, next ? "true" : "false");
				}}
			>
				{/* Drawn rather than an icon font: collapsed, the bar is 16px tall and
				    a glyph that size is a smudge. */}
				<span className={clsx("league-ticker-caret", { up: !show })} />
			</button>
		</div>
	);
});
