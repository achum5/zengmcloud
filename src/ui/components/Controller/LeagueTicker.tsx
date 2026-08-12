import {
	memo,
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
import { categories } from "../../../common/transactionInfo.ts";
import {
	buildTickerStream,
	tickerDurationSeconds,
	tickerMayUpdate,
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

// WHAT THE LEFT PANE SAYS.
//
// The name of whatever is going past right now - SCORES, then ODDS, then MVP,
// then TRANSACTIONS - and it changes as the marquee reaches each new kind of
// item. That is the whole point of the pane: it used to be a label printed in
// front of EVERY item, which meant reading "INJURIES" forty times to learn one
// thing. Said once, in a fixed place, it costs nothing and the items get to be
// only their own content.
const railLabel = (item: TickerItem): string => {
	switch (item.type) {
		case "score": {
			return "Scores";
		}
		case "upcoming": {
			return "Odds";
		}
		case "performance": {
			return "Top Performers";
		}
		case "race": {
			// The award itself - MVP, DPOY - not the generic word "award".
			return item.label;
		}
		default: {
			const known = item.category !== undefined && item.category in categories;
			return known
				? categories[item.category as keyof typeof categories].text
				: "News";
		}
	}
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
		<span className="league-ticker-stat">{item.stat}</span>
		<a
			className="league-ticker-aside"
			href={helpers.leagueUrl([
				"game_log",
				item.boxScoreTeam,
				item.season,
				item.gid,
			])}
		>
			{item.game}
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

	// WHICH ITEM THE LEFT PANE IS NAMING.
	//
	// Read off the marquee's real position rather than computed from the clock.
	// The animation pauses on hover and on touch, and under reduced motion there
	// is no animation at all - the bar is scrolled by hand - so elapsed time says
	// nothing reliable about where the track actually is. One rect read does.
	//
	// Item offsets within the track are measured once per layout: they only move
	// when the feed or the window changes, and reading fifty of them on every
	// frame would be a layout flush several times a second for a caption.
	const viewportRef = useRef<HTMLDivElement>(null);
	const trackRef = useRef<HTMLDivElement>(null);
	const offsets = useRef<number[]>([]);
	const cycle = useRef(0);
	const [leading, setLeading] = useState(0);

	useLayoutEffect(() => {
		const measure = () => {
			const track = trackRef.current;
			if (!track) {
				return;
			}
			// Direct children only - the second copy is nested inside its own span,
			// so this is one pass of the loop and nothing repeats.
			offsets.current = [
				...track.querySelectorAll<HTMLElement>(":scope > .league-ticker-item"),
			].map((node) => node.offsetLeft);
			// Where the duplicate starts IS the length of one pass.
			const duplicate = track.querySelector<HTMLElement>(":scope > [data-dup]");
			cycle.current = duplicate ? duplicate.offsetLeft : track.scrollWidth / 2;
		};
		measure();
		window.addEventListener("resize", measure);
		return () => window.removeEventListener("resize", measure);
	}, [items, show]);

	useEffect(() => {
		if (!show) {
			return;
		}
		let frame = 0;
		let last = 0;
		const tick = (now: number) => {
			frame = requestAnimationFrame(tick);
			// A caption does not need sixty updates a second, and each one costs a
			// pair of rect reads.
			if (now - last < 120) {
				return;
			}
			last = now;
			const viewport = viewportRef.current;
			const track = trackRef.current;
			if (!viewport || !track || cycle.current <= 0) {
				return;
			}
			// How far into one pass of the track the viewport's left edge currently
			// sits, wrapped, because the track loops.
			const into =
				viewport.getBoundingClientRect().left -
				track.getBoundingClientRect().left;
			const x = ((into % cycle.current) + cycle.current) % cycle.current;
			let index = 0;
			for (const [i, offset] of offsets.current.entries()) {
				if (offset > x) {
					break;
				}
				index = i;
			}
			setLeading((previous) => (previous === index ? previous : index));
		};
		frame = requestAnimationFrame(tick);
		return () => cancelAnimationFrame(frame);
	}, [show, items]);

	// Reduced motion: keep the bar, drop the movement.
	const [animate, setAnimate] = useState(true);
	useEffect(() => {
		const query = window.matchMedia("(prefers-reduced-motion: reduce)");
		const apply = () => setAnimate(!query.matches);
		apply();
		query.addEventListener("change", apply);
		return () => query.removeEventListener("change", apply);
	}, []);

	const visible = lid !== undefined && items.length > 0;
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

	const duration = tickerDurationSeconds(items.length);

	return (
		<div className={clsx("league-ticker", { collapsed: !show })}>
			{show ? (
				<div className="league-ticker-label">
					{railLabel(items[leading] ?? items[0]!)}
				</div>
			) : null}
			<div className="league-ticker-viewport" ref={viewportRef}>
				{show ? (
					<div
						className={clsx("league-ticker-track", {
							"league-ticker-animate": animate,
						})}
						ref={trackRef}
						style={animate ? { animationDuration: `${duration}s` } : undefined}
					>
						{/* Twice, so the loop has no seam: the second copy is scrolling
						    into place as the first scrolls out. */}
						{items.map((item) => (
							<Item key={item.key} item={item} />
						))}
						<span aria-hidden="true" className="d-flex" data-dup="">
							{items.map((item) => (
								<Item key={`dup-${item.key}`} item={item} />
							))}
						</span>
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
