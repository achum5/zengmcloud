import { memo, useEffect, useMemo, useRef, useState } from "react";
import clsx from "clsx";
import { localActions, useLocal } from "../../util/local.ts";
import type { LocalStateUI } from "../../../common/types.ts";
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

type LocalGame = LocalStateUI["games"][number];
type LocalNews = LocalStateUI["tickerNews"][number];

// The ESPN bar, pinned to the bottom: today's scores and the league's news
// scrolling past in one continuous loop.
//
// Two things about it are load-bearing rather than cosmetic.
//
// FREEZING. While a live game is on screen the whole thing stops updating -
// see tickerMayUpdate. A ticker is the one widget that can spoil a game you are
// actively watching, and in a shared league it would do it to every follower at
// once. It holds the last stream it was given until the playback ends, exactly
// as the top score bar holds prevGames.
//
// THE ANIMATION. The track is duplicated and translated by -50%, which is the
// oldest marquee trick there is and still the right one: it is a single
// compositor-driven transform with no layout work per frame, so it costs
// essentially nothing even on a phone. It pauses on hover and on touch so the
// links inside it can actually be clicked, and it does not run at all for
// anyone who has asked for reduced motion - for them the bar is still there and
// still scrollable by hand.

const Score = ({ game }: { game: LocalGame }) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	const abbrev = (tid: number) => teamInfoCache[tid]?.abbrev ?? "???";
	const [away, home] = game.teams;
	const final = away.pts !== undefined && home.pts !== undefined;

	// The winner in bold, the way a scoreboard does it.
	const winner = !final
		? undefined
		: (away.pts ?? 0) > (home.pts ?? 0)
			? 0
			: (home.pts ?? 0) > (away.pts ?? 0)
				? 1
				: undefined;

	return (
		<a
			className="league-ticker-item text-reset text-decoration-none"
			href={helpers.leagueUrl(
				final ? ["game_log", "-1", "-1", game.gid] : ["daily_schedule"],
			)}
		>
			<span className={clsx({ "fw-bold": winner === 0 })}>
				{abbrev(away.tid)} {final ? away.pts : ""}
			</span>
			<span className="text-body-secondary mx-1">{final ? "-" : "@"}</span>
			<span className={clsx({ "fw-bold": winner === 1 })}>
				{final ? `${home.pts} ` : ""}
				{abbrev(home.tid)}
			</span>
			{final ? (
				<span className="text-body-secondary ms-1 league-ticker-tag">
					{game.overtimes ? `${game.overtimes}OT` : "F"}
				</span>
			) : null}
		</a>
	);
};

const News = ({ item }: { item: LocalNews }) => {
	const className =
		item.category && item.category in categories
			? categories[item.category as keyof typeof categories].className
			: "bg-secondary";

	return (
		<span className="league-ticker-item">
			<span className={`badge league-ticker-badge me-2 ${className}`}>
				{item.category && item.category in categories
					? categories[item.category as keyof typeof categories].text
					: "News"}
			</span>
			<SafeHtml dirty={item.text} />
		</span>
	);
};

const STORAGE_KEY = "bbgmShowLeagueTicker";

export const LeagueTicker = memo(() => {
	const { games, lid, liveGameInProgress, mpLiveBroadcast, tickerNews } =
		useLocal([
			"games",
			"lid",
			"liveGameInProgress",
			"mpLiveBroadcast",
			"tickerNews",
		]);

	const [show, setShow] = useState(
		() => safeLocalStorage.getItem(STORAGE_KEY) !== "false",
	);

	// Everything the bar is currently showing. Held across a live game rather
	// than recomputed, so nothing that happens during a playback reaches the
	// screen until it is over.
	const frozen = useRef<TickerItem[]>([]);

	const watchingBroadcast = !!mpLiveBroadcast?.active;
	const mayUpdate = tickerMayUpdate({
		liveGameInProgress,
		watchingBroadcast,
	});

	const items = useMemo(() => {
		if (!mayUpdate) {
			return frozen.current;
		}
		const next = buildTickerStream({
			games: games.map((game) => ({
				gid: game.gid,
				final: game.teams[0].pts !== undefined,
			})),
			news: tickerNews,
		});
		frozen.current = next;
		return next;
	}, [games, tickerNews, mayUpdate]);

	const gamesByGid = useMemo(() => {
		const map = new Map<number, LocalGame>();
		for (const game of games) {
			map.set(game.gid, game);
		}
		return map;
	}, [games]);

	const newsByEid = useMemo(() => {
		const map = new Map<number, LocalNews>();
		for (const item of tickerNews) {
			map.set(item.eid, item);
		}
		return map;
	}, [tickerNews]);

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

	const rendered = items
		.map((item) => {
			if (item.type === "game") {
				const game = gamesByGid.get(item.gid);
				return game ? <Score key={`g${item.gid}`} game={game} /> : null;
			}
			const news = newsByEid.get(item.eid);
			return news ? <News key={`n${item.eid}`} item={news} /> : null;
		})
		.filter(Boolean);

	const duration = tickerDurationSeconds(rendered.length);

	return (
		<div className={clsx("league-ticker", { "league-ticker-hidden": !show })}>
			{show ? (
				<div className="league-ticker-viewport">
					<div
						className={clsx("league-ticker-track", {
							"league-ticker-animate": animate,
						})}
						style={animate ? { animationDuration: `${duration}s` } : undefined}
					>
						{/* Twice, so the loop has no seam: the second copy is scrolling
						    into place as the first scrolls out. */}
						{rendered}
						<span aria-hidden="true" className="d-flex">
							{rendered}
						</span>
					</div>
				</div>
			) : (
				<div className="league-ticker-viewport" />
			)}
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
