import { memo, useEffect, useMemo, useRef, useState } from "react";
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

const Kicker = ({ href, label }: { href: string; label: string }) => (
	<a className="league-ticker-kicker" href={href}>
		{label}
	</a>
);

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
		<Kicker href={helpers.leagueUrl(["award_races"])} label={item.label} />
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

const News = ({ item }: { item: Extract<TickerItem, { type: "news" }> }) => {
	const known = item.category !== undefined && item.category in categories;
	return (
		<span className="league-ticker-item">
			<Kicker
				href={helpers.leagueUrl(["news"])}
				label={
					known
						? categories[item.category as keyof typeof categories].text
						: "News"
				}
			/>
			{/* The event text arrives as HTML with its own player and team links. */}
			<span className="league-ticker-news">
				<SafeHtml dirty={item.text} />
			</span>
		</span>
	);
};

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
			<div className="league-ticker-viewport">
				{show ? (
					<div
						className={clsx("league-ticker-track", {
							"league-ticker-animate": animate,
						})}
						style={animate ? { animationDuration: `${duration}s` } : undefined}
					>
						{/* Twice, so the loop has no seam: the second copy is scrolling
						    into place as the first scrolls out. */}
						{items.map((item) => (
							<Item key={item.key} item={item} />
						))}
						<span aria-hidden="true" className="d-flex">
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
