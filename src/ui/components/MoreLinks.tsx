import {
	Fragment,
	useContext,
	useLayoutEffect,
	useRef,
	type ReactNode,
} from "react";
import { EmbeddedView } from "./EmbeddedView.tsx";
import {
	NO_LOTTERY_DRAFT_TYPES,
	REAL_PLAYERS_INFO,
} from "../../common/constants.ts";
import type { DraftType, PlayerStatType } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { bySport } from "../../common/sportFunctions.ts";
import type { LeagueUrlParts } from "../router/types.ts";
import { PageBanner, PinnedBar, useTeamColors } from "./TeamBanner.tsx";
import { TeamLogoInline } from "./TeamLogoInline.tsx";

type Link = {
	url: string | Readonly<LeagueUrlParts>;
	name: string;
	className?: string;
};

// Pinned under the top bar as the page scrolls (a team's slim strip); how
// much of it stays in view goes to --teamb-h, for whatever is scrolled to
// under it - nothing, when it isn't pinned (a team's own page's big banner).
const TOP_BAR = 52;
const usePinnedBanner = (on: boolean) => {
	const ref = useRef<HTMLDivElement>(null);
	useLayoutEffect(() => {
		const el = ref.current;
		if (!el) {
			return;
		}
		const place = () => {
			el.style.setProperty("--teamb-top", `${TOP_BAR}px`);
			const pinned = getComputedStyle(el).position === "sticky";
			document.documentElement.style.setProperty(
				"--teamb-h",
				pinned ? `${Math.round(el.offsetHeight)}px` : "0px",
			);
		};
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

// A TEAM'S BANNER OVER ITS PAGES (the refreshed look): its logo, the season
// and its name (each its own dropdown) in its colors, and the page's other
// dropdowns in it, in place of the title bar. One slim strip on most of its
// pages, pinned as the page scrolls; on its own page (the roster) big, its
// record, payroll... in it, and a slim bar pinned in its place once it has
// scrolled away.
const TeamPageBanner = ({
	tid,
	info,
	pinnedSub,
}: {
	tid: number;
	info?: ReactNode;
	pinnedSub?: ReactNode;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const t = teamInfoCache[tid];
	const colors = useTeamColors(tid);
	const big = info !== undefined;
	const ref = usePinnedBanner(t !== undefined);
	if (!t) {
		return null;
	}
	const team = {
		tid,
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		imgURL: t.imgURL,
		imgURLSmall: t.imgURLSmall,
		colors,
	};
	return (
		<>
			<PageBanner
				className={big ? "teamb teamb-big" : "teamb teamb-slim"}
				teamName
				// (Its "More Info" pages at the foot of its page instead.)
				skipMoreInfo={big}
				t={team}
				// (Its logo to its page - the one long page, where its other pages
				// are; no tabs.)
				href={helpers.leagueUrl(["roster", `${t.abbrev}_${tid}`])}
				sub={info}
				bannerRef={ref}
			/>
			{big ? (
				<PinnedBar watch=".teamb-big" colors={colors}>
					<TeamLogoInline
						imgURL={t.imgURL}
						imgURLSmall={t.imgURLSmall}
						size={26}
					/>
					<span className="pin-bar-name">
						<span className="tb-region">{t.region} </span>
						{t.name}
					</span>
					{pinnedSub ? <span className="pin-bar-sub">{pinnedSub}</span> : null}
				</PinnedBar>
			) : null}
		</>
	);
};

// (Drawn inside another page - see EmbeddedView - none: that page has its
// own.)
export const MoreLinks = (props: Parameters<typeof MoreLinksShown>[0]) =>
	useContext(EmbeddedView) ? null : <MoreLinksShown {...props} />;

const MoreLinksShown = (
	props: (
		| {
				type: "team";
				tid: number;
				season?: number;
				// The team's own page (the refreshed look): its banner big, with
				// these in it (its record, payroll...), and this by its name in
				// the bar pinned once it has scrolled away.
				info?: ReactNode;
				pinnedSub?: ReactNode;
				// Its pages as a list of buttons, not a banner (less these).
				asList?: boolean;
				exclude?: string[];
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

	// THE TEAM'S OTHER PAGES AS A LIST (the refreshed look's team page, at its
	// foot): BBGM's buttons, one to each.
	if (props.type === "team" && props.asList) {
		const skip = new Set(props.exclude ?? []);
		// (And its players' ratings, stats and bios that season - the title
		// bar's "More Info", not in the team page's banner.)
		const team = helpers.getTeamUrlPart(props.tid);
		const season = props.season ?? "";
		const more: Link[] = [
			{ url: ["player_ratings", team, season], name: "Player Ratings" },
			{ url: ["player_stats", team, season], name: "Player Stats" },
			{ url: ["player_bios", team, season], name: "Player Bios" },
		];
		return (
			<div className="team-pages">
				{[...links, ...more]
					.filter(({ url }) => {
						const key = typeof url === "string" ? url : url[0];
						return !skip.has(key) && key !== page;
					})
					.map(({ url, name }, i) => (
						<a
							key={String(i)}
							className="btn btn-light-bordered"
							href={typeof url === "string" ? url : helpers.leagueUrl(url)}
						>
							{name}
						</a>
					))}
			</div>
		);
	}

	if (refreshedLook && props.type === "team") {
		return (
			<TeamPageBanner
				tid={props.tid}
				info={props.info}
				pinnedSub={props.pinnedSub}
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
