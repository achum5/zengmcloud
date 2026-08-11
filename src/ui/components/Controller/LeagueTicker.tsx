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
// side is a renderer, and it owns three behaviours worth naming.
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

const Chip = ({ label, className }: { label: string; className: string }) => (
	<span className={`badge league-ticker-badge me-2 ${className}`}>{label}</span>
);

const Item = ({ item }: { item: TickerItem }) => {
	if (item.type === "score") {
		const { away, home } = item;
		const awayWon = (away.pts ?? 0) > (home.pts ?? 0);
		const homeWon = (home.pts ?? 0) > (away.pts ?? 0);
		return (
			<a
				className="league-ticker-item text-reset text-decoration-none"
				href={helpers.leagueUrl([
					"game_log",
					item.boxScoreTeam,
					item.season,
					item.gid,
				])}
			>
				<span className={clsx({ "fw-bold": awayWon })}>
					{away.abbrev} {away.pts}
				</span>
				<span className="text-body-secondary mx-1">-</span>
				<span className={clsx({ "fw-bold": homeWon })}>
					{home.pts} {home.abbrev}
				</span>
				<span className="text-body-secondary ms-1 league-ticker-tag">
					{item.overtimes
						? `${item.overtimes > 1 ? item.overtimes : ""}OT`
						: "F"}
				</span>
			</a>
		);
	}

	if (item.type === "upcoming") {
		return (
			<a
				className="league-ticker-item text-reset text-decoration-none"
				href={helpers.leagueUrl(["daily_schedule"])}
			>
				<span>
					{item.away.abbrev} @ {item.home.abbrev}
				</span>
				{item.line ? (
					<span className="text-body-secondary ms-2 league-ticker-tag">
						{item.line}
					</span>
				) : null}
			</a>
		);
	}

	if (item.type === "performance") {
		return (
			<a
				className="league-ticker-item text-reset text-decoration-none"
				href={helpers.leagueUrl([
					"game_log",
					item.boxScoreTeam,
					item.season,
					item.gid,
				])}
			>
				<Chip label="Top" className="bg-info" />
				{item.text}
			</a>
		);
	}

	if (item.type === "race") {
		return (
			<a
				className="league-ticker-item text-reset text-decoration-none"
				href={helpers.leagueUrl(["award_races"])}
			>
				<Chip label={item.label} className="bg-warning" />
				{item.text}
			</a>
		);
	}

	const known = item.category !== undefined && item.category in categories;
	return (
		<span className="league-ticker-item">
			<Chip
				label={
					known
						? categories[item.category as keyof typeof categories].text
						: "News"
				}
				className={
					known
						? categories[item.category as keyof typeof categories].className
						: "bg-secondary"
				}
			/>
			<SafeHtml dirty={item.text} />
		</span>
	);
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
		// be reached.
		document.body.classList.toggle("has-league-ticker", visible);

		return () => {
			localActions.update({ leagueTickerVisible: false });
			document.body.classList.remove("has-league-ticker");
		};
	}, [visible]);

	if (!visible) {
		return null;
	}

	const duration = tickerDurationSeconds(items.length);

	return (
		<div className="league-ticker">
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
				className="btn btn-secondary p-0 league-ticker-toggle"
				title={show ? "Hide ticker" : "Show ticker"}
				onClick={() => {
					const next = !show;
					setShow(next);
					safeLocalStorage.setItem(STORAGE_KEY, next ? "true" : "false");
				}}
			>
				<span
					className={clsx(
						"glyphicon",
						show ? "glyphicon-menu-down" : "glyphicon-menu-up",
					)}
				/>
			</button>
		</div>
	);
});
