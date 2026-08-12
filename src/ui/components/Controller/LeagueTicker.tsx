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

	// WHICH BLOCK IS PLAYING.
	//
	// State, not an observation. The player holds an index, the block at that
	// index is the only thing rendered, and it advances when that block's crawl
	// finishes. Nothing has to measure where a moving element currently is - which
	// is what the previous version did, and why the pane never changed on iOS,
	// where a compositor-driven transform does not report back to the main thread.
	const [index, setIndex] = useState(0);
	const segment = segments[Math.min(index, segments.length - 1)];
	const advance = useCallback(() => {
		setIndex((previous) =>
			segments.length > 0 ? (previous + 1) % segments.length : 0,
		);
	}, [segments.length]);

	// A refreshed feed can be shorter than the one being played.
	useEffect(() => {
		setIndex((previous) =>
			segments.length > 0 && previous >= segments.length ? 0 : previous,
		);
	}, [segments]);

	// Reduced motion: keep the bar, drop the movement. Blocks still change, on a
	// timer instead of at the end of a crawl, and the contents can be scrolled by
	// hand.
	const [animate, setAnimate] = useState(true);
	useEffect(() => {
		const query = window.matchMedia("(prefers-reduced-motion: reduce)");
		const apply = () => setAnimate(!query.matches);
		apply();
		query.addEventListener("change", apply);
		return () => query.removeEventListener("change", apply);
	}, []);

	// HOW FAR AND HOW LONG THIS BLOCK HAS TO TRAVEL.
	//
	// Two layout reads per block - the viewport's width and the block's own - and
	// nothing per frame. Both are layout values rather than animated positions, so
	// they are honest on every platform. The crawl is held off until they are
	// taken, so it cannot start from the wrong place and jump.
	const viewportRef = useRef<HTMLDivElement>(null);
	const runRef = useRef<HTMLDivElement>(null);
	const [run, setRun] = useState<
		{ key: string; travel: number; duration: number } | undefined
	>();
	const key = segment?.key;

	useLayoutEffect(() => {
		const measure = () => {
			const viewport = viewportRef.current;
			const element = runRef.current;
			if (!viewport || !element || key === undefined) {
				return;
			}
			const travel = segmentTravelPx(
				element.scrollWidth,
				viewport.getBoundingClientRect().width,
			);
			setRun({ key, travel, duration: segmentDurationSeconds(travel) });
		};
		measure();
		window.addEventListener("resize", measure);
		return () => window.removeEventListener("resize", measure);
	}, [key, items]);

	// Under reduced motion there is no crawl to end, so time the block instead.
	useEffect(() => {
		if (animate || segments.length < 2) {
			return;
		}
		const timer = setTimeout(advance, 9000);
		return () => clearTimeout(timer);
	}, [advance, animate, key, segments.length]);

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

	const ready = run?.key === key;

	return (
		<div className={clsx("league-ticker", { collapsed: !show })}>
			{show && segment ? (
				<div className="league-ticker-pane">
					{/* Keyed on the block, so React remounts it and the entrance
					    animation replays every time the pane changes - the change is
					    the signal that a new block has started. */}
					<div className="league-ticker-pane-in" key={segment.key}>
						<Pane header={segment.header} />
					</div>
				</div>
			) : null}
			<div className="league-ticker-viewport" ref={viewportRef}>
				{show && segment ? (
					<div
						className={clsx("league-ticker-run", {
							"league-ticker-crawl": animate && ready,
						})}
						key={segment.key}
						ref={runRef}
						onAnimationEnd={advance}
						style={
							ready
								? {
										// Starts a full viewport to the right and finishes a full
										// block-width to the left, so it enters and leaves clean.
										["--run-from" as any]: `${run.from}px`,
										animationDuration: `${run.duration}s`,
									}
								: undefined
						}
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
