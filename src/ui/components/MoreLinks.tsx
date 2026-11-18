import { Fragment, useLayoutEffect, useRef, type ReactNode } from "react";
import {
	NO_LOTTERY_DRAFT_TYPES,
	REAL_PLAYERS_INFO,
} from "../../common/constants.ts";
import type { DraftType, PlayerStatType } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { bySport } from "../../common/sportFunctions.ts";
import type { LeagueUrlParts } from "../router/types.ts";
import { PageBanner, useTeamColors } from "./TeamBanner.tsx";

type Link = {
	url: string | Readonly<LeagueUrlParts>;
	name: string;
	className?: string;
};

// Pinned under the top bar as the page scrolls: all of it on a phone (its
// menus and its pages), its pages on a wider screen (the rest tucked up under
// the bar). How much of it stays in view goes to --teamb-h, for whatever is
// scrolled to under it.
const TOP_BAR = 52;
const usePinnedBanner = (on: boolean) => {
	const ref = useRef<HTMLDivElement>(null);
	useLayoutEffect(() => {
		const el = ref.current;
		if (!el) {
			return;
		}
		const place = () => {
			const nav = el.querySelector<HTMLElement>(".tb-nav");
			const phone = window.matchMedia("(max-width: 575.98px)").matches;
			const hide =
				phone || !nav
					? 0
					: Math.max(
							0,
							nav.getBoundingClientRect().top -
								el.getBoundingClientRect().top -
								10,
						);
			el.style.setProperty("--teamb-top", `${TOP_BAR - hide}px`);
			// (Not pinned - the game log's, where the score pins instead: nothing
			// held over the page.)
			const pinned = getComputedStyle(el).position === "sticky";
			document.documentElement.style.setProperty(
				"--teamb-h",
				pinned ? `${Math.round(el.offsetHeight - hide)}px` : "0px",
			);
		};
		place();
		const observer =
			typeof ResizeObserver === "undefined"
				? undefined
				: new ResizeObserver(place);
		observer?.observe(el);
		window.addEventListener("resize", place);
		return () => {
			observer?.disconnect();
			window.removeEventListener("resize", place);
			document.documentElement.style.removeProperty("--teamb-h");
		};
	}, [on]);
	return ref;
};

// A TEAM'S PAGES UNDER ITS BANNER (the refreshed look): its logo, city and
// name in its colors, its pages along the foot of it - the one you are on
// marked - and the page's dropdowns in it, in place of the title bar.
const TeamPageBanner = ({
	tid,
	links,
	page,
	slim,
	actions,
	colors: pageColors,
}: {
	tid: number;
	links: Link[];
	page: string;
	slim?: boolean;
	actions?: ReactNode;
	colors?: string[];
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const info = teamInfoCache[tid];
	const fetched = useTeamColors(pageColors ? undefined : tid);
	const colors = pageColors ?? fetched;
	const ref = usePinnedBanner(info !== undefined);
	if (!info) {
		return null;
	}
	return (
		<PageBanner
			// (One strip on every team page, so it never changes size as you go
			// between them; on the game log it sits on top of the game's banner.)
			className={slim ? "teamb teamb-slim teamb-joined" : "teamb teamb-slim"}
			teamName
			t={{
				tid,
				abbrev: info.abbrev,
				region: info.region,
				name: info.name,
				imgURL: info.imgURL,
				imgURLSmall: info.imgURLSmall,
				colors,
			}}
			href={helpers.leagueUrl(["roster", `${info.abbrev}_${tid}`])}
			sub={
				<nav className="tb-nav">
					{links.map(({ url, name }, i) => {
						const key = typeof url === "string" ? url : url[0];
						return (
							<a
								key={key + String(i)}
								className={key === page ? "active" : undefined}
								href={typeof url === "string" ? url : helpers.leagueUrl(url)}
							>
								{name}
							</a>
						);
					})}
				</nav>
			}
			bannerRef={ref}
		>
			{actions}
		</PageBanner>
	);
};

export const MoreLinks = (
	props: (
		| {
				type: "team";
				tid: number;
				season?: number;
				// The refreshed look's banner as one slim strip (over a game's
				// own banner, the game log), and buttons at its end.
				slim?: boolean;
				actions?: ReactNode;
				// The team's colors where the page has them (its box score).
				colors?: string[];
		  }
		| {
				type: "draft";
				draftType: DraftType | "dummy" | undefined;
				tid?: number;
				season?: number;
		  }
		| {
				type: "playerRatings";
				season: number;
		  }
		| {
				type: "playerStats";
				season?: number;
				statType?: string;
		  }
		| {
				type: "teamStats";
				season: number;
		  }
		| {
				type: "freeAgents";
		  }
		| {
				type: "league";
		  }
		| {
				type: "importExport";
		  }
		| {
				type: "awards";
				season?: number;
		  }
		| {
				type: "schedule";
		  }
		| {
				type: "globalSettings";
		  }
		| {
				type: "leaders";
				playoffs: "playoffs" | "regularSeason" | "combined";
				season: number | "career" | "all";
				statType: PlayerStatType;
		  }
		| {
				type: "playerNotes";
		  }
	) & {
		page: string;
		keepSelfLink?: boolean;
	},
) => {
	const { keepSelfLink, page } = props;

	const {
		college,
		godMode,
		refreshedLook,
		rotationPlans,
		season: currentSeason,
	} = useLocal([
		"college",
		"godMode",
		"refreshedLook",
		"rotationPlans",
		"season",
	]);

	let links: {
		url: string | Readonly<LeagueUrlParts>;
		name: string;
		className?: string;
	}[];
	if (props.type === "team") {
		const { season, tid } = props;
		const team = helpers.getTeamUrlPart(tid);

		links = [
			{
				url: ["roster", team, season],
				name: "Roster",
			},
			...(college
				? []
				: [
						{
							url: ["team_finances", team] as const,
							name: "Finances",
						},
					]),
			{
				url: ["game_log", team, season],
				name: "Game Log",
			},
			...(college
				? []
				: [{ url: ["draft_picks", team] as const, name: "Draft Picks" }]),
			{
				url: ["team_history", team],
				name: "History",
			},
			{
				url:
					season !== undefined && season !== currentSeason
						? ["head2head", team, season]
						: ["head2head", team],
				name: "Head-to-Head",
			},
			{
				url: ["schedule", team],
				name: "Schedule",
			},
			{
				url: ["transactions", team, season],
				name: "Transactions",
			},
			{
				url: ["news", team, season],
				name: "News Feed",
			},
			{
				url: ["intrasquad", team],
				name: "Intrasquad",
			},
		];

		if (__SPORT === "basketball") {
			links.splice(links.findIndex((link) => link.name === "Game Log") + 1, 0, {
				url: ["lineups", team, season],
				name: "Lineups",
			});
		}

		// The rotation page is basketball's depth chart, and only exists in a
		// league that has turned rotation plans on.
		if (__SPORT === "basketball" && rotationPlans) {
			links.unshift({
				url: ["rotation", team],
				name: "Rotation",
			});
		}

		if (
			bySport({
				baseball: true,
				basketball: false,
				football: true,
				hockey: true,
			})
		) {
			if (__SPORT === "baseball") {
				links.unshift(
					{
						url: ["depth", "L", team],
						name: "Batting Order",
					},
					{
						url: ["depth", "D", team],
						name: "Defense",
					},
					{
						url: ["depth", "P", team],
						name: "Pitching",
					},
				);
			} else {
				links.unshift({
					url: [
						"depth",
						bySport({
							baseball: "",
							basketball: "",
							football: "QB",
							hockey: "F",
						}),
						team,
					],
					name: bySport({
						baseball: "",
						basketball: "",
						football: "Depth Chart",
						hockey: "Lines",
					}),
				});
			}
		}

		if (page === "team_history") {
			links.push({
				url: [
					"player_stats",
					team,
					"career",
					bySport({
						baseball: "batting",
						football: "passing",
						basketball: "totals",
						hockey: "skater",
					}),
				],
				name: "Franchise Leaders",
			});
		}
	} else if (props.type === "draft") {
		const { draftType, season, tid } = props;
		const team = tid === undefined ? undefined : helpers.getTeamUrlPart(tid);

		links = [
			// { url: ["draft"], name: "Draft", },
			{
				url: ["draft_scouting"],
				name:
					draftType === "freeAgents" ? "Upcoming Prospects" : "Draft Scouting",
			},
			{
				url: team === undefined ? ["draft_picks"] : ["draft_picks", team],
				name: "Draft Picks",
			},
			{
				url: ["draft_lottery", season],
				name: NO_LOTTERY_DRAFT_TYPES.has(draftType as any)
					? "Draft Order"
					: "Draft Lottery",
			},
			{
				url: ["draft_history", season],
				name:
					draftType === "freeAgents" ? "Prospects History" : "Draft History",
			},
			{
				url:
					team === undefined
						? ["draft_team_history"]
						: ["draft_team_history", team],
				name: "Team History",
			},
			{
				url: ["notes", "draftPick"],
				name: "Draft Pick Notes",
			},
		];
	} else if (props.type === "awards") {
		const { season } = props;

		links = [
			{
				url: ["award_races", season],
				name: "Award Races",
			},
			{
				url: ["award_settings"],
				name: "Award Settings",
			},
		];

		if (godMode) {
			links.push({
				url: ["edit_award_winners", season],
				name: "Edit Award Winners",
				className: "god-mode",
			});
		}
	} else if (props.type === "playerRatings") {
		const { season } = props;

		links = [
			{
				url: ["player_ratings", season],
				name: "Main Ratings",
			},
			{
				url: ["player_rating_dists", season],
				name: "Rating Distributions",
			},
		];
	} else if (props.type === "playerStats") {
		const { season, statType } = props;
		links = [
			{
				url: ["player_stat_dists", season],
				name: "Stat Distributions",
			},
		];

		if (season === undefined || page !== "player_stats") {
			links.unshift({
				url: ["player_stats", season],
				name: page === "player_stats" ? "Per Game" : "Main Stats",
			});
		} else {
			links.unshift({
				url: [
					"player_stats",
					"all",
					"career",
					__SPORT === "basketball" || statType === undefined
						? "totals"
						: statType,
				],
				name: "Career Totals",
			});
		}
	} else if (props.type === "teamStats") {
		const { season } = props;

		links = [
			{
				url: ["team_stats", season],
				name: "Main Stats",
			},
			{
				url: ["team_stat_dists", season],
				name: "Stat Distributions",
			},
			{ url: ["league_stats"], name: "League Stats" },
		];
	} else if (props.type === "freeAgents") {
		links = [
			{
				url: ["free_agents"],
				name: "Current Free Agents",
			},
			{
				url: ["upcoming_free_agents"],
				name: "Upcoming Free Agents",
			},
		];
	} else if (props.type === "league") {
		links = [
			{ url: ["league_stats"], name: "League Stats" },
			{ url: ["head2head_all"], name: "Head-to-Head" },
			{ url: ["history_all"], name: "League History" },
			{ url: ["team_records"], name: "Team Records" },
			{ url: ["awards_records"], name: "Awards Records" },
			{
				url: ["all_star", "history"],
				name: "All-Star History",
			},
			{ url: ["season_preview"], name: "Season Previews" },
			{ url: ["notes", "teamSeason"], name: "Team Notes" },
		];
	} else if (props.type === "importExport") {
		links = [
			{ url: ["import_players"], name: "Import Players" },
			...(REAL_PLAYERS_INFO
				? [
						{
							url: ["import_players_real"] as const,
							name: "Import Real Players",
						},
					]
				: []),
			{ url: ["export_players"], name: "Export Players" },
			{ url: ["export_league"], name: "Export League" },
		];
	} else if (props.type === "schedule") {
		links = [{ url: ["schedule"], name: "Team Schedule" }];
	} else if (props.type === "globalSettings") {
		links = [
			{ url: "/settings", name: "Global Settings" },
			{ url: "/settings/default", name: "Default New League Settings" },
			...(window.mobile
				? []
				: [{ url: "/settings/keyboard", name: "Keyboard Shortcuts" }]),
		];
	} else if (props.type === "leaders") {
		const { playoffs, season, statType } = props;

		const defaultStat = bySport({
			baseball: "ba",
			basketball: "pts",
			football: "pssYds",
			hockey: "g",
		});

		links = [
			{
				url: ["leaders", season, statType, playoffs],
				name: "League Leaders",
			},
			{
				url: ["leaders_years", defaultStat, statType, playoffs],
				name: "Yearly Leaders",
			},
			{
				url: ["leaders_progressive", defaultStat, statType, playoffs],
				name: "Progressive Leaders",
			},
			{
				url: [
					"player_stats",
					"all",
					season,
					bySport({
						baseball: "batting",
						football: "passing",
						basketball: statType,
						hockey: "skater",
					}),
					playoffs,
				],
				name: "Player Stats",
			},
		];
	} else if (props.type === "playerNotes") {
		links = [
			{ url: ["notes", "player"], name: "Player Notes" },
			{ url: ["watch_list"], name: "Watch List" },
		];
	} else {
		throw new Error("Invalid MoreLinks type");
	}

	if (links.length === 0) {
		return null;
	}

	if (refreshedLook && props.type === "team") {
		return (
			<TeamPageBanner
				tid={props.tid}
				links={links}
				page={page}
				slim={props.slim}
				actions={props.actions}
				colors={props.colors}
			/>
		);
	}

	return (
		<p>
			More:{" "}
			{links
				.filter(({ url }) => {
					if (keepSelfLink) {
						return true;
					}
					const key = typeof url === "string" ? url : url[0];
					return key !== page;
				})
				.map(({ className, url, name }, i) => {
					return (
						<Fragment key={url[0] + String(i)}>
							{i > 0 ? " | " : null}
							<a
								className={className}
								href={typeof url === "string" ? url : helpers.leagueUrl(url)}
							>
								{name}
							</a>
						</Fragment>
					);
				})}
		</p>
	);
};
