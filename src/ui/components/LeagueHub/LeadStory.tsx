import { useState } from "react";
import { formatGameDate } from "../../../common/calendarDates.ts";
import { WatchReplayButton } from "../BoxScoreWrapper.tsx";
import { Markdown } from "../Markdown.tsx";
import { MatchupBanner, useAllTeamColors } from "../TeamBanner.tsx";
import { TeamLeaders } from "../../views/LiveGame/gamecast/GameRecap.tsx";
import { teamsInDisplayOrder } from "../../util/boxScoreDisplayOrder.ts";
import { helpers } from "../../util/helpers.ts";
import {
	linkifyRecap,
	linkRecapSegments,
	type RecapLink,
	type SentenceGame,
} from "../../util/linkifyRecap.ts";
import { useLocal } from "../../util/local.ts";
import {
	isDayNoteExpanded,
	setDayNoteExpanded,
} from "../../util/dailyScheduleUiState.ts";

// THE NIGHT'S TOP STORY, at the top of the hub: the game the night's written
// recap leads with, as a broadcast banner in its two teams' colors, and under
// it the recap itself as the story of the night - headline, deck, the lede,
// and the rest a tap away. Before the night is played, its best matchup.

type TeamInfo = { tid: number; abbrev: string; region: string; name: string };

const dayLabel = (date: string | undefined, day: number, long?: boolean) =>
	date !== undefined
		? formatGameDate(date, long ? "full" : "shortWithYear")
		: `Day ${day}`;

const finalText = (overtimes: number | undefined) => {
	const ot = overtimes ?? 0;
	return ot === 0 ? "Final" : ot === 1 ? "Final/OT" : `Final/${ot}OT`;
};

const useBannerTeam = () => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const colors = useAllTeamColors();
	return (t: { tid: number }): TeamInfo & Record<string, any> => {
		const info = teamInfoCache[t.tid];
		return {
			...t,
			abbrev: info?.abbrev ?? "",
			region: info?.region ?? "",
			name: info?.name ?? "",
			imgURL: info?.imgURL,
			imgURLSmall: info?.imgURLSmall,
			colors: colors?.[t.tid],
		};
	};
};

const rosterUrl = (t: TeamInfo, season: number) =>
	helpers.leagueUrl(["roster", `${t.abbrev}_${t.tid}`, season]);

// The night's written recap as a story: its first line the headline, its
// next paragraph the deck, then the lede; the rest behind "Keep reading".
const Article = ({
	day,
	links,
	note,
	season,
	sentenceGames,
}: {
	day: number;
	links: RecapLink[];
	note: string;
	season: number;
	sentenceGames: SentenceGame[];
}) => {
	const [expanded, setExpandedState] = useState(() =>
		isDayNoteExpanded(season, day),
	);
	const setExpanded = (value: boolean) => {
		setExpandedState(value);
		setDayNoteExpanded(season, day, value);
	};

	const linked = linkifyRecap(note.trim(), links);
	const lines = linked.split("\n");
	const headIndex = lines.findIndex((line) => line.trim() !== "");
	const headline = (lines[headIndex] ?? "").replaceAll("**", "");
	const paragraphs = lines
		.slice(headIndex + 1)
		.join("\n")
		.split(/\n\s*\n|\n/)
		.map((p) => p.trim())
		.filter((p) => p !== "");
	const [deck, ...body] = paragraphs;
	const SHOWN = 1;
	const shown = expanded ? body : body.slice(0, SHOWN);
	const linkSegments =
		sentenceGames.length > 0
			? (text: string) => linkRecapSegments(text, sentenceGames)
			: undefined;

	return (
		<div className="lh-article">
			<h2 className="lh-headline">
				<Markdown>{headline}</Markdown>
			</h2>
			{deck ? (
				<div className="lh-deck">
					<Markdown>{deck}</Markdown>
				</div>
			) : null}
			<div className="lh-body">
				<Markdown linkSegments={linkSegments}>{shown.join("\n\n")}</Markdown>
			</div>
			{body.length > SHOWN ? (
				<button
					type="button"
					className="btn btn-link p-0 lh-more"
					onClick={() => setExpanded(!expanded)}
				>
					{expanded ? "Show less" : "Keep reading"}
				</button>
			) : null}
		</div>
	);
};

// A game still to play: each side's player to watch and what he's
// averaging.
const PlayerToWatch = ({ p }: { p: any }) => {
	if (!p) {
		return null;
	}
	const words = String(p.name).trim().split(/\s+/);
	const short =
		words.length > 1 ? `${words[0]![0]}. ${words.slice(1).join(" ")}` : p.name;
	const stat = (x: number | undefined) => (x ?? 0).toFixed(1);
	return (
		<>
			<div className="mb-lead">
				<a href={helpers.leagueUrl(["player", p.pid])}>{short}</a>{" "}
				<span className="lh-watch-pos">{p.ratings?.pos}</span>
			</div>
			<div className="lh-watch-stats">
				<b>{stat(p.stats?.pts)}</b> PPG <b>{stat(p.stats?.trb)}</b> RPG{" "}
				<b>{stat(p.stats?.ast)}</b> APG
			</div>
		</>
	);
};

export const LeadStory = ({
	date,
	day,
	dayNote,
	game,
	links,
	playersToWatch,
	season,
	sentenceGames,
	upcoming,
}: {
	playersToWatch?: [any, any];
	date: string | undefined;
	day: number;
	dayNote: string | undefined;
	// A completed game (as stored), or one still to play (as listed).
	game: any;
	links: RecapLink[];
	season: number;
	sentenceGames: SentenceGame[];
	upcoming: boolean;
}) => {
	const bannerTeam = useBannerTeam();
	const [away, home] = teamsInDisplayOrder(game.teams as [any, any]).map(
		bannerTeam,
	) as [any, any];

	let banner;
	if (upcoming) {
		banner = (
			<MatchupBanner
				className="lh-banner"
				teams={[away, home]}
				hrefs={[rosterUrl(away, season), rosterUrl(home, season)]}
				subs={[helpers.formatRecord(away), helpers.formatRecord(home)]}
				status="Game of the night"
			/>
		);
	} else {
		const winner =
			home.pts > away.pts ? 1 : away.pts > home.pts ? 0 : undefined;
		banner = (
			<MatchupBanner
				className="lh-banner"
				teams={[away, home]}
				numbers={[away.pts, home.pts]}
				won={winner}
				hrefs={[rosterUrl(away, season), rosterUrl(home, season)]}
				subs={[
					away.won !== undefined ? helpers.formatRecord(away) : null,
					home.won !== undefined ? helpers.formatRecord(home) : null,
				]}
				status={
					<>
						{finalText(game.overtimes)}
						<WatchReplayButton gid={game.gid} className="mb-replay" />
					</>
				}
				tag={
					<a
						href={helpers.leagueUrl([
							"game_log",
							`${home.abbrev}_${home.tid}`,
							season,
							game.gid,
						])}
					>
						Box score
					</a>
				}
				leaders={[
					<TeamLeaders key="away" t={away} />,
					<TeamLeaders key="home" t={home} />,
				]}
			/>
		);
	}

	return (
		<section className="lh-lead">
			<div className="lh-kicker">
				<span className="lh-kicker-tag">
					{upcoming ? "Tonight" : "Top story"}
				</span>
				<span className="lh-kicker-date">{dayLabel(date, day, true)}</span>
			</div>
			{banner}
			{!upcoming ? (
				<div className="mb-leaders-row">
					<div className="mb-leaders">
						<TeamLeaders t={away} />
					</div>
					<div className="mb-leaders">
						<TeamLeaders t={home} />
					</div>
				</div>
			) : playersToWatch ? (
				<>
					<div className="lh-watch-title">Players to watch</div>
					<div className="mb-leaders-row">
						{/* (Display order: away, then home.) */}
						<div className="mb-leaders">
							<PlayerToWatch p={playersToWatch[1]} />
						</div>
						<div className="mb-leaders">
							<PlayerToWatch p={playersToWatch[0]} />
						</div>
					</div>
				</>
			) : null}
			{dayNote ? (
				<Article
					day={day}
					links={links}
					note={dayNote}
					season={season}
					sentenceGames={sentenceGames}
				/>
			) : null}
		</section>
	);
};
