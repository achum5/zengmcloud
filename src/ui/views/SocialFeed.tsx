import { useMemo, useState } from "react";
import clsx from "clsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import type { View } from "../../common/types.ts";
import {
	Avatar,
	Icon,
	SocialThread,
	VerifiedBadge,
	type TeamLike,
} from "./SocialPost.tsx";

// THE TIMELINE.
//
// Grouped by day, because a league's day IS its unit of time. Within a day the
// posts run newest-first on a real clock: the games finish through the evening
// and the news lands during the afternoon before them, so a day reads the way
// a timeline does rather than as an unordered pile.
//
// Two tabs. "For you" is the whole league; the second is the user's own team -
// its accounts and everything said about it - which is the timeline a fan
// actually keeps open.

type Tab = "all" | "team";

const SocialFeed = ({
	accountCount,
	days,
	errorMessage,
	feed,
	hasMore,
	pictures,
	season,
	suggested,
	teams,
	userTid,
}: View<"socialFeed">) => {
	useTitleBar({ title: "League Feed" });

	const [tab, setTab] = useState<Tab>("all");
	const [query, setQuery] = useState("");

	const teamByTid = useMemo(
		() => new Map<number, TeamLike>((teams ?? []).map((t: any) => [t.tid, t])),
		[teams],
	);

	if (errorMessage) {
		return <p className="alert alert-warning d-inline-block">{errorMessage}</p>;
	}

	const userTeam = teams.find((t: any) => t.tid === userTid);

	const aboutTeam = (post: { tid?: number; tids?: number[] }) =>
		post.tid === userTid || (post.tids ?? []).includes(userTid);

	const q = query.trim().toLowerCase();
	const matchesQuery = (post: {
		text: string;
		name: string;
		handle: string;
		replies?: { text: string; name: string; handle: string }[];
	}) =>
		q === "" ||
		post.text.toLowerCase().includes(q) ||
		post.name.toLowerCase().includes(q) ||
		post.handle.toLowerCase().includes(q) ||
		(post.replies ?? []).some(
			(r) =>
				r.text.toLowerCase().includes(q) ||
				r.name.toLowerCase().includes(q) ||
				r.handle.toLowerCase().includes(q),
		);

	const visibleDays = feed.map((day: any) => ({
		day: day.day,
		posts: day.posts.filter(
			(post: any) => (tab === "all" || aboutTeam(post)) && matchesQuery(post),
		),
	}));
	const totalPosts = visibleDays.reduce(
		(sum: number, day: any) => sum + day.posts.length,
		0,
	);

	// WHAT'S HAPPENING: the teams the loaded timeline is talking about most,
	// counted from what is on the page rather than stored anywhere.
	const trends = (() => {
		const counts = new Map<number, number>();
		for (const day of feed) {
			for (const post of day.posts) {
				const tids = new Set<number>([
					...(post.tids ?? []),
					...(post.tid === undefined ? [] : [post.tid]),
				]);
				for (const tid of tids) {
					counts.set(tid, (counts.get(tid) ?? 0) + 1 + post.replies.length);
				}
			}
		}
		return [...counts]
			.map(([tid, n]) => ({ tid, n, team: teamByTid.get(tid) as any }))
			.filter((t) => t.team)
			.sort((a, b) => b.n - a.n || a.tid - b.tid)
			.slice(0, 5);
	})();

	return (
		<div className="social-layout" data-social-ready>
			<div className="social-main">
				<div className="social-header">
					<div className="social-header-row">
						<div className="social-header-title flex-grow-1">Home</div>
						<a
							className="social-header-sub text-decoration-none"
							href={helpers.leagueUrl(["social_accounts"])}
						>
							{accountCount} accounts
						</a>
					</div>
					<div className="social-tabs">
						<button
							className={clsx("social-tab", tab === "all" && "active")}
							onClick={() => setTab("all")}
							type="button"
						>
							For you
						</button>
						<button
							className={clsx("social-tab", tab === "team" && "active")}
							onClick={() => setTab("team")}
							type="button"
						>
							{userTeam ? userTeam.name : "Your team"}
						</button>
					</div>
				</div>

				{totalPosts === 0 ? (
					<div className="social-empty">
						<div className="social-empty-title">Nothing here yet</div>
						{q !== ""
							? "No posts match that."
							: tab === "team"
								? "Nobody has said anything about your team yet. Sim a day."
								: "Nothing has happened yet this season. Sim a day."}
					</div>
				) : null}

				{visibleDays.map((day: any) =>
					day.posts.length === 0 ? null : (
						<div key={day.day}>
							<div className="social-day">
								<span className="social-day-pill">
									{day.day === 0 ? "Offseason" : `Day ${day.day}`}
								</span>
							</div>
							{day.posts.map((post: any) => (
								<SocialThread
									key={post.id}
									pictures={pictures}
									post={post}
									teamByTid={teamByTid}
								/>
							))}
						</div>
					),
				)}

				{hasMore ? (
					<button
						className="social-more"
						onClick={() => {
							realtimeUpdate(
								[],
								helpers.leagueUrl(["social", String(season), String(days + 4)]),
							);
						}}
						type="button"
					>
						Show more
					</button>
				) : null}
			</div>

			<aside className="social-side">
				<label className="social-search">
					<Icon name="search" />
					<input
						onChange={(event) => setQuery(event.target.value)}
						placeholder="Search"
						type="search"
						value={query}
					/>
				</label>

				{trends.length > 0 ? (
					<div className="social-card">
						<h3>What&rsquo;s happening</h3>
						{trends.map((t, i) => (
							<a
								key={t.tid}
								className="social-trend"
								href={helpers.leagueUrl([
									"roster",
									`${t.team.abbrev}_${t.team.tid}`,
								])}
							>
								<div className="social-trend-kicker">
									{i === 0 ? "Trending" : `${i + 1} · Trending`} · Basketball
								</div>
								<div className="social-trend-title">
									{t.team.region} {t.team.name}
								</div>
								<div className="social-trend-count">
									{t.n} {t.n === 1 ? "post" : "posts"}
								</div>
							</a>
						))}
					</div>
				) : null}

				{suggested && suggested.length > 0 ? (
					<div className="social-card">
						<h3>Who to follow</h3>
						{suggested.map((a: any) => (
							<div key={a.accountId} className="social-suggest">
								<a href={helpers.leagueUrl(["social", a.handle])}>
									<Avatar
										account={a}
										picture={pictures[a.accountId]}
										size={40}
										team={
											a.tid === undefined ? undefined : teamByTid.get(a.tid)
										}
									/>
								</a>
								<div className="social-suggest-body">
									<a
										className="social-name"
										href={helpers.leagueUrl(["social", a.handle])}
									>
										{a.name} {a.verified ? <VerifiedBadge /> : null}
									</a>
									<span className="social-handle">@{a.handle}</span>
								</div>
								<a
									className="btn-social-follow"
									href={helpers.leagueUrl(["social", a.handle])}
								>
									Follow
								</a>
							</div>
						))}
						<a
							className="social-card-more"
							href={helpers.leagueUrl(["social_accounts"])}
						>
							Show more
						</a>
					</div>
				) : null}

				<div className="social-foot">
					{accountCount} accounts ·{" "}
					<a href={helpers.leagueUrl(["social_accounts"])}>Manage</a>
				</div>
			</aside>
		</div>
	);
};

export default SocialFeed;
