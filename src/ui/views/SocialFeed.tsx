import { useEffect, useMemo, useState } from "react";
import clsx from "clsx";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { feedDayLabel } from "../util/feedDayLabel.ts";
import { local } from "../util/local.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { safeLocalStorage } from "../util/safeLocalStorage.ts";
import type { View } from "../../common/types.ts";
import { Icon, SocialThread, type TeamLike } from "./SocialPost.tsx";
import {
	SocialShell,
	SocialTopBar,
	sectionHref,
	type SocialSection,
} from "./SocialShell.tsx";

// THE TIMELINE.
//
// One column of posts, newest first, stamped the way a timeline stamps them:
// tonight's posts by how long ago they went up ("12m", "3h"), anything older
// by its night ("Nov 21", or "Day 41" without the NBA calendar).
//
// Tabs across the top work like lists: the whole league, your own team (its
// accounts and everything said about it), and then the media, the players
// and the fans on their own. Explore holds search and what the league is
// talking about; Notifications is everyone else talking about your team.

type Tab = "all" | "team" | "media" | "players" | "fans";

const FAN_ARCHETYPES = new Set(["homerFan", "doomerFan", "casualFan", "troll"]);

type Post = {
	id: string;
	accountId: string;
	handle: string;
	name: string;
	kind: "player" | "team" | "media";
	archetypeId?: string;
	text: string;
	time?: string;
	minutes?: number;
	tid?: number;
	tids?: number[];
	replies: {
		text: string;
		name: string;
		handle: string;
		time?: string;
	}[];
};

const inTab = (tab: Tab, post: Post, userTid: number) => {
	switch (tab) {
		case "all":
			return true;
		case "team":
			return post.tid === userTid || (post.tids ?? []).includes(userTid);
		case "players":
			return post.kind === "player";
		case "fans":
			return FAN_ARCHETYPES.has(post.archetypeId ?? "");
		case "media":
			return (
				post.kind === "media" && !FAN_ARCHETYPES.has(post.archetypeId ?? "")
			);
	}
};

// Anybody but your team's own account talking about your team.
const isNotification = (
	post: Post,
	userTid: number,
	meId: string | undefined,
) => post.accountId !== meId && (post.tids ?? []).includes(userTid);

const clockMinutes = (label: string | undefined) => {
	const match = /^(\d{1,2}):(\d{2})\s*([AP]M)$/.exec(label ?? "");
	if (!match) {
		return undefined;
	}
	return (
		(Number(match[1]) % 12) * 60 +
		(match[3] === "PM" ? 720 : 0) +
		Number(match[2])
	);
};

const sinceLabel = (now: number, then: number | undefined) => {
	if (then === undefined) {
		return undefined;
	}
	const minutes = Math.max(1, now - then);
	return minutes < 60 ? `${minutes}m` : `${Math.floor(minutes / 60)}h`;
};

const SECTIONS_IN_PLACE = new Set<SocialSection>([
	"home",
	"explore",
	"notifications",
]);

const sectionFromHash = (): SocialSection => {
	const hash = window.location.hash.slice(1) as SocialSection;
	return SECTIONS_IN_PLACE.has(hash) ? hash : "home";
};

const seenKey = () => `bbgmFeedSeen${local.getState().lid ?? ""}`;

const SocialFeed = ({
	accountCount,
	currentSeason,
	days,
	errorMessage,
	feed,
	hasMore,
	me,
	pictures,
	playerNames,
	season,
	teams,
	userTid,
}: View<"socialFeed">) => {
	useTitleBar({ title: "League Feed" });

	const [section, setSection] = useState<SocialSection>(sectionFromHash);
	const [tab, setTab] = useState<Tab>("all");
	const [query, setQuery] = useState("");
	const [seen, setSeen] = useState(() => safeLocalStorage.getItem(seenKey()));

	const teamByTid = useMemo(
		() => new Map<number, TeamLike>((teams ?? []).map((t: any) => [t.tid, t])),
		[teams],
	);

	const newest = feed?.[0];
	const newestStamp =
		newest && season === currentSeason ? `${season}:${newest.day}` : undefined;

	// Back and forward between sections, and links in from other pages.
	useEffect(() => {
		const onHash = () => {
			setSection(sectionFromHash());
		};
		window.addEventListener("hashchange", onHash);
		return () => {
			window.removeEventListener("hashchange", onHash);
		};
	}, []);

	// Opening Notifications reads them, the way it does on the real thing.
	useEffect(() => {
		if (section === "notifications" && newestStamp !== undefined) {
			safeLocalStorage.setItem(seenKey(), newestStamp);
			setSeen(newestStamp);
		}
	}, [section, newestStamp]);

	if (errorMessage) {
		return <p className="alert alert-warning d-inline-block">{errorMessage}</p>;
	}

	const goTo = (next: SocialSection) => {
		setSection(next);
		const { pathname, search } = window.location;
		window.history.replaceState(
			window.history.state,
			"",
			`${pathname}${search}${next === "home" ? "" : `#${next}`}`,
		);
		window.scrollTo({ top: 0 });
	};

	const userTeam = teams.find((t: any) => t.tid === userTid);

	// Tonight's posts are stamped against the last thing said tonight, so the
	// top of the timeline reads as just now and the afternoon as hours ago.
	const tonight: Post[] =
		newest !== undefined && newestStamp !== undefined ? newest.posts : [];
	let now = 0;
	if (tonight.length > 0) {
		for (const post of tonight) {
			now = Math.max(now, post.minutes ?? clockMinutes(post.time) ?? 0);
			for (const reply of post.replies) {
				now = Math.max(now, clockMinutes(reply.time) ?? 0);
			}
		}
		now += 1;
	}

	const stamped = (feed as any[]).flatMap((day) =>
		(day.posts as Post[]).map((post) => {
			if (day === newest && newestStamp !== undefined) {
				return {
					...post,
					time: sinceLabel(now, post.minutes ?? clockMinutes(post.time)),
					replies: post.replies.map((reply) => ({
						...reply,
						time: sinceLabel(now, clockMinutes(reply.time)),
					})),
				};
			}
			const label = feedDayLabel(day.day, day.date);
			return {
				...post,
				time: label,
				replies: post.replies.map((reply) => ({ ...reply, time: label })),
			};
		}),
	);

	const q = query.trim().toLowerCase();
	const matchesQuery = (post: Post) =>
		q === "" ||
		post.text.toLowerCase().includes(q) ||
		post.name.toLowerCase().includes(q) ||
		post.handle.toLowerCase().includes(q) ||
		post.replies.some(
			(r) =>
				r.text.toLowerCase().includes(q) ||
				r.name.toLowerCase().includes(q) ||
				r.handle.toLowerCase().includes(q),
		);

	const notificationPosts = stamped.filter((post) =>
		isNotification(post, userTid, me?.accountId),
	);
	const unread =
		newestStamp !== undefined && seen !== newestStamp
			? tonight.filter((post) => isNotification(post, userTid, me?.accountId))
					.length
			: 0;

	const visible =
		section === "notifications"
			? notificationPosts
			: section === "explore"
				? q === ""
					? []
					: stamped.filter(matchesQuery)
				: stamped.filter(
						(post) => inTab(tab, post, userTid) && matchesQuery(post),
					);

	// WHAT'S HAPPENING: the teams the loaded timeline is talking about most,
	// counted from what is on the page rather than stored anywhere.
	const trends = (() => {
		const counts = new Map<number, number>();
		for (const post of stamped) {
			const tids = new Set<number>([
				...(post.tids ?? []),
				...(post.tid === undefined ? [] : [post.tid]),
			]);
			for (const tid of tids) {
				counts.set(tid, (counts.get(tid) ?? 0) + 1 + post.replies.length);
			}
		}
		return [...counts]
			.map(([tid, n]) => ({ tid, n, team: teamByTid.get(tid) as any }))
			.filter((t) => t.team)
			.sort((a, b) => b.n - a.n || a.tid - b.tid)
			.slice(0, 10);
	})();

	const tabs: { id: Tab; label: string }[] = [
		{ id: "all", label: "For you" },
		{ id: "team", label: userTeam ? userTeam.name : "Your team" },
		{ id: "media", label: "Media" },
		{ id: "players", label: "Players" },
		{ id: "fans", label: "Fans" },
	];

	const search = (
		<label className="social-search">
			<Icon name="search" />
			<input
				onChange={(event) => setQuery(event.target.value)}
				placeholder="Search"
				type="search"
				value={query}
			/>
		</label>
	);

	const topBar = (
		<SocialTopBar
			active={section}
			me={me}
			notifications={unread}
			onSection={goTo}
			pictures={pictures}
			teamByTid={teamByTid}
		/>
	);

	const emptyText =
		section === "notifications"
			? "When someone talks about your team, it shows up here."
			: q !== ""
				? "No posts match that."
				: tab === "all"
					? "Nothing has happened yet this season. Sim a day."
					: tab === "team"
						? "Nobody has said anything about your team yet. Sim a day."
						: "Nothing from them yet. Sim a day.";

	return (
		<SocialShell
			active={section}
			me={me}
			notifications={unread}
			onSection={goTo}
			pictures={pictures}
			side={section === "explore" ? null : search}
			teamByTid={teamByTid}
		>
			<div className="social-header">
				{topBar}
				{section === "home" ? (
					<div className="social-tabs" role="tablist">
						{tabs.map((t) => (
							<button
								key={t.id}
								aria-selected={tab === t.id}
								className={clsx("social-tab", tab === t.id && "active")}
								onClick={() => {
									setTab(t.id);
									window.scrollTo({ top: 0 });
								}}
								role="tab"
								type="button"
							>
								<span>{t.label}</span>
							</button>
						))}
					</div>
				) : section === "explore" ? (
					<div className="social-header-search">{search}</div>
				) : (
					<div className="social-header-row">
						<div className="social-header-title">Notifications</div>
					</div>
				)}
			</div>

			{section === "explore" && q === "" ? (
				<>
					{trends.length > 0 ? (
						<div className="social-section-title">Trending</div>
					) : null}
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
								{i + 1} · Basketball · Trending
							</div>
							<div className="social-trend-title">
								{t.team.region} {t.team.name}
							</div>
							<div className="social-trend-count">
								{t.n} {t.n === 1 ? "post" : "posts"}
							</div>
						</a>
					))}
					<a className="social-foot" href={sectionHref("accounts")}>
						{accountCount} accounts
					</a>
				</>
			) : visible.length === 0 ? (
				<div className="social-empty">
					<div className="social-empty-title">Nothing here yet</div>
					{emptyText}
				</div>
			) : (
				visible.map((post) => (
					<SocialThread
						key={post.id}
						pictures={pictures}
						playerNames={playerNames}
						post={post as any}
						season={season}
						teamByTid={teamByTid}
					/>
				))
			)}

			{hasMore && section !== "explore" ? (
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
		</SocialShell>
	);
};

export default SocialFeed;
