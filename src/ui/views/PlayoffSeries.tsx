import clsx from "clsx";
import type { CSSProperties, ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";
import { formatGameDate } from "../../common/calendarDates.ts";
import { GameCard } from "../components/LeagueHub/GameCard.tsx";
import { RailCard } from "../components/LeagueHub/HubRail.tsx";
import { GameNote } from "../components/GameNote.tsx";
import { buildRecapLinksForGame } from "../util/linkifyRecap.ts";
import { bannerColors } from "../components/TeamBanner.tsx";
import { usePlayerFace } from "../util/playerFaces.ts";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import { getCols } from "../../common/getCols.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import { wrappedPlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import type {
	SeriesPlayer,
	SeriesTeamTotals,
} from "../../worker/views/playoffSeries.ts";

// ONE PLAYOFF SERIES, as NBA.com and ESPN show one: the two teams and where
// it stands across the top, then every game (the card and the recap the
// Daily Schedule gives it) and the ones still to come, with who led it, the
// teams side by side, the regular season between them and their playoff
// history down the side, and everybody's averages under it all.

type Props = View<"playoffSeries">;
type SeriesTeam = Props["teams"][number];

const Logo = ({ url, size }: { url: string | undefined; size: number }) =>
	url ? (
		<img
			src={url}
			alt=""
			width={size}
			height={size}
			style={{ objectFit: "contain" }}
		/>
	) : null;

const rosterUrl = (t: { abbrev: string; tid: number }, season: number) =>
	helpers.leagueUrl(["roster", `${t.abbrev}_${t.tid}`, season]);

// "BOS leads 3-2", "Series tied 2-2", "BOS wins 4-2".
const seriesStatus = (
	teams: Props["teams"],
	numGamesToWin: number,
): string | undefined => {
	const [home, away] = teams;
	if (home.won === away.won) {
		return home.won === 0 ? undefined : `Series tied ${home.won}-${away.won}`;
	}
	const [lead, hi, lo] =
		home.won > away.won
			? [home.abbrev, home.won, away.won]
			: [away.abbrev, away.won, home.won];
	return `${lead} ${hi >= numGamesToWin ? "wins" : "leads"} ${hi}-${lo}`;
};

const HeroTeam = ({
	t,
	side,
	season,
	out,
}: {
	t: SeriesTeam;
	side: "away" | "home";
	season: number;
	out: boolean;
}) => {
	const cap = (
		<a
			className={side === "home" ? "ps-cap ps-cap-home" : "ps-cap ps-cap-away"}
			href={rosterUrl(t, season)}
			aria-label={`${t.region} ${t.name}`}
		>
			<Logo url={t.imgURL ?? t.imgURLSmall} size={64} />
		</a>
	);
	const words = (
		<div
			className={clsx(
				"ps-team",
				side === "home" ? "ps-team-home" : "ps-team-away",
				{ out },
			)}
		>
			<div className="ps-team-seed">
				No. {t.seed} · {helpers.formatRecord(t.record)}
			</div>
			<div className="ps-team-region">{t.region}</div>
			<div
				className={clsx("ps-team-name", {
					"ps-team-name-long": t.name.length >= 9,
				})}
			>
				{t.name}
			</div>
		</div>
	);
	const wins = (
		<div
			className={clsx(
				"ps-wins",
				side === "home" ? "ps-wins-home" : "ps-wins-away",
				{ out },
			)}
		>
			{t.won}
		</div>
	);
	// (Each end's logo on the outside, the games won by the middle.)
	return side === "home" ? (
		<>
			{wins}
			{words}
			{cap}
		</>
	) : (
		<>
			{cap}
			{words}
			{wins}
		</>
	);
};

const Hero = ({
	teams,
	season,
	label,
	numGames,
	numGamesToWin,
	nextDate,
}: Pick<Props, "teams" | "season" | "label" | "numGames" | "numGamesToWin"> & {
	nextDate: string | undefined;
}) => {
	const [home, away] = teams;
	const a = bannerColors(away.colors);
	const h = bannerColors(home.colors);
	const status = seriesStatus(teams, numGamesToWin);
	const over = Math.max(home.won, away.won) >= numGamesToWin;
	return (
		<div
			className="ps-hero"
			style={
				{
					"--ps-a-bg": a.bg,
					"--ps-a-fg": a.fg,
					"--ps-a-accent": a.accent,
					"--ps-h-bg": h.bg,
					"--ps-h-fg": h.fg,
					"--ps-h-accent": h.accent,
				} as CSSProperties
			}
		>
			<div className="ps-hero-tag">
				{season} {label}
			</div>
			<HeroTeam
				t={away}
				side="away"
				season={season}
				out={over && away.won < home.won}
			/>
			<div className="ps-mid">
				{status ? (
					<div className="ps-status">{status}</div>
				) : (
					<div className="ps-status">
						{nextDate ? `Game 1 · ${formatGameDate(nextDate)}` : "Game 1"}
					</div>
				)}
				<div className="ps-bestof">Best of {numGames}</div>
			</div>
			<HeroTeam
				t={home}
				side="home"
				season={season}
				out={over && home.won < away.won}
			/>
		</div>
	);
};

const UpcomingGames = ({
	upcoming,
	teams,
}: Pick<Props, "upcoming" | "teams">) => {
	const byTid = new Map(teams.map((t) => [t.tid, t]));
	// (On the NBA calendar, the night of each.)
	const dated = upcoming.some((game) => game.date);
	return (
		<ul className={clsx("ps-next", { "ps-next-dated": dated })}>
			{upcoming.map((game) => {
				const host = byTid.get(game.hostTid)!;
				const visitor = teams.find((t) => t.tid !== game.hostTid)!;
				return (
					<li
						key={game.gameNum}
						className={clsx({ "ps-next-maybe": game.ifNecessary })}
					>
						<span className="ps-next-num">Game {game.gameNum}</span>
						{dated ? (
							<span className="ps-next-date">
								{game.date ? formatGameDate(game.date) : null}
							</span>
						) : null}
						<span className="ps-next-where">
							{visitor.abbrev} at
							<Logo url={host.imgURLSmall ?? host.imgURL} size={18} />
							{host.abbrev}
						</span>
						{game.ifNecessary ? (
							<span className="ps-next-if">If necessary</span>
						) : null}
					</li>
				);
			})}
		</ul>
	);
};

const Face = ({ pid, season }: { pid: number; season: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, season, lid);
	return (
		<div className="ps-face">
			{face && (face.face || face.imgURL) ? (
				<PlayerPicture
					face={face.face}
					imgURL={face.imgURL}
					colors={face.colors}
					jersey={face.jersey}
					lazy
				/>
			) : null}
		</div>
	);
};

const lastName = (name: string) => {
	const space = name.indexOf(" ");
	return space > 0 ? name.slice(space + 1) : name;
};

const perGame = (p: SeriesPlayer, key: "pts" | "trb" | "ast") =>
	(key === "trb" ? p.orb + p.drb : p[key]) / Math.max(1, p.gp);

// Each team's leader in points, rebounds and assists a game.
const Leaders = ({
	players,
	teams,
	season,
}: Pick<Props, "players" | "teams" | "season">) => {
	const [home, away] = teams;
	const best = (tid: number, key: "pts" | "trb" | "ast") => {
		let out: SeriesPlayer | undefined;
		for (const p of players) {
			if (p.tid !== tid) {
				continue;
			}
			if (
				!out ||
				perGame(p, key) > perGame(out, key) ||
				(perGame(p, key) === perGame(out, key) && p.min > out.min)
			) {
				out = p;
			}
		}
		return out;
	};
	const Side = ({
		p,
		k,
		side,
	}: {
		p: SeriesPlayer | undefined;
		k: "pts" | "trb" | "ast";
		side: "away" | "home";
	}) =>
		p ? (
			<a
				className={clsx(
					"ps-leader",
					side === "home" ? "ps-leader-home" : "ps-leader-away",
				)}
				href={helpers.leagueUrl(["player", p.pid])}
			>
				<Face pid={p.pid} season={season} />
				<span className="ps-leader-words">
					<span className="ps-leader-name">{lastName(p.name)}</span>
					<b className="ps-leader-value">{perGame(p, k).toFixed(1)}</b>
				</span>
			</a>
		) : (
			<span />
		);
	return (
		<RailCard title="Series leaders" className="lh-o2">
			<div className="ps-leaders-head">
				<Logo url={away.imgURLSmall ?? away.imgURL} size={22} />
				<span>Per game</span>
				<Logo url={home.imgURLSmall ?? home.imgURL} size={22} />
			</div>
			<ul className="lh-list">
				{(
					[
						["pts", "PTS"],
						["trb", "REB"],
						["ast", "AST"],
					] as const
				).map(([k, label]) => (
					<li key={k} className="ps-leaders-row">
						<Side p={best(away.tid, k)} k={k} side="away" />
						<span className="ps-leaders-label">{label}</span>
						<Side p={best(home.tid, k)} k={k} side="home" />
					</li>
				))}
			</ul>
		</RailCard>
	);
};

const pct = (made: number, att: number) =>
	att > 0 ? ((100 * made) / att).toFixed(1) : "-";

const TeamStats = ({
	totals,
	teams,
}: {
	totals: (SeriesTeamTotals | undefined)[];
	teams: Props["teams"];
}) => {
	const [home, away] = teams;
	const [h, a] = totals;
	if (!h || !a) {
		return null;
	}
	const g = (t: SeriesTeamTotals, value: number) => value / Math.max(1, t.gp);
	const rows: [string, (t: SeriesTeamTotals) => number, number, boolean][] = [
		["Points", (t) => g(t, t.pts), 1, true],
		["FG%", (t) => (t.fga ? (100 * t.fg) / t.fga : 0), 1, true],
		["3P%", (t) => (t.tpa ? (100 * t.tp) / t.tpa : 0), 1, true],
		["FT%", (t) => (t.fta ? (100 * t.ft) / t.fta : 0), 1, true],
		["Rebounds", (t) => g(t, t.orb + t.drb), 1, true],
		["Assists", (t) => g(t, t.ast), 1, true],
		["Turnovers", (t) => g(t, t.tov), 1, false],
		["Steals", (t) => g(t, t.stl), 1, true],
		["Blocks", (t) => g(t, t.blk), 1, true],
	];
	return (
		<RailCard title="Team stats" className="lh-o3">
			<table className="ps-compare">
				<thead>
					<tr>
						<th>
							<Logo url={away.imgURLSmall ?? away.imgURL} size={18} />{" "}
							{away.abbrev}
						</th>
						<th className="ps-compare-label">Per game</th>
						<th>
							{home.abbrev}{" "}
							<Logo url={home.imgURLSmall ?? home.imgURL} size={18} />
						</th>
					</tr>
				</thead>
				<tbody>
					{rows.map(([label, f, digits, higherBetter]) => {
						const av = f(a);
						const hv = f(h);
						const aBetter = higherBetter ? av > hv : av < hv;
						const hBetter = higherBetter ? hv > av : hv < av;
						return (
							<tr key={label}>
								<td className={clsx({ "fw-bold": aBetter })}>
									{av.toFixed(digits)}
								</td>
								<td className="ps-compare-label">{label}</td>
								<td className={clsx({ "fw-bold": hBetter })}>
									{hv.toFixed(digits)}
								</td>
							</tr>
						);
					})}
				</tbody>
			</table>
		</RailCard>
	);
};

const RegularSeason = ({
	meetings,
	seasonRecord,
	teams,
	season,
}: Pick<Props, "meetings" | "seasonRecord" | "teams" | "season">) => {
	const [home, away] = teams;
	const byTid = new Map(teams.map((t) => [t.tid, t]));
	if (!seasonRecord && meetings.length === 0) {
		return null;
	}
	let record: ReactNode = null;
	if (seasonRecord) {
		const { won, lost, tied } = seasonRecord;
		const tie = tied > 0 ? `-${tied}` : "";
		record =
			won === lost
				? `Split ${won}-${lost}${tie}`
				: won > lost
					? `${home.abbrev} ${won}-${lost}${tie}`
					: `${away.abbrev} ${lost}-${won}${tie}`;
	}
	return (
		<RailCard
			title="Regular season"
			className="lh-o4"
			more={{
				href: helpers.leagueUrl([
					"rivalry",
					`${home.abbrev}_${home.tid}`,
					`${away.abbrev}_${away.tid}`,
				]),
				text: "All-time",
			}}
		>
			{record ? <div className="ps-record">{record}</div> : null}
			{meetings.length > 0 ? (
				<ul className="lh-list">
					{meetings.map((game) => {
						const [t0, t1] = game.teams;
						const h = byTid.get(t0.tid)!;
						const v = byTid.get(t1.tid)!;
						const ot = helpers.overtimeText(game.overtimes, game.numPeriods);
						return (
							<li key={game.gid} className="ps-meeting">
								<a
									href={helpers.leagueUrl([
										"game_log",
										`${h.abbrev}_${h.tid}`,
										season,
										game.gid,
									])}
								>
									<span className="ps-meeting-when">
										{game.date
											? formatGameDate(game.date, "monthDay")
											: `Day ${game.day}`}
									</span>
									<span className={clsx({ "fw-bold": game.winner === 1 })}>
										{v.abbrev} {t1.pts}
									</span>
									<span className="text-body-secondary">at</span>
									<span className={clsx({ "fw-bold": game.winner === 0 })}>
										{h.abbrev} {t0.pts}
									</span>
									{ot ? (
										<span className="text-body-secondary">{ot}</span>
									) : null}
								</a>
							</li>
						);
					})}
				</ul>
			) : null}
		</RailCard>
	);
};

const PlayoffHistory = ({
	pastMeetings,
	teams,
}: Pick<Props, "pastMeetings" | "teams">) => {
	if (pastMeetings.length === 0) {
		return null;
	}
	const [home, away] = teams;
	return (
		<RailCard title="Playoff history" className="lh-o5">
			<ul className="lh-list">
				{pastMeetings.map((m) => (
					<li key={m.season} className="ps-history">
						<a
							href={helpers.leagueUrl([
								"playoff_series",
								m.season,
								`${m.abbrevs[home.tid]}_${home.tid}`,
								`${m.abbrevs[away.tid]}_${away.tid}`,
							])}
						>
							<span className="ps-history-season">{m.season}</span>
							<span className="ps-history-round">{m.label}</span>
							<span className="ps-history-result">
								{m.winnerTid === undefined
									? `${m.won}-${m.lost}`
									: `${m.abbrevs[m.winnerTid]} ${m.won}-${m.lost}`}
							</span>
						</a>
					</li>
				))}
			</ul>
		</RailCard>
	);
};

const PlayerTables = ({
	players,
	teams,
	season,
}: Pick<Props, "players" | "teams" | "season">) => {
	const cols = getCols([
		"Name",
		"Pos",
		"stat:gp",
		"stat:min",
		"stat:pts",
		"stat:trb",
		"stat:ast",
		"stat:stl",
		"stat:blk",
		"stat:tov",
		"stat:fgp",
		"stat:tpp",
		"stat:ftp",
		"stat:pm",
	]);
	const a = (p: SeriesPlayer, value: number) =>
		helpers.roundStat(value / Math.max(1, p.gp), "pts");
	return (
		<div className="lh-o6 ps-players">
			{[teams[1], teams[0]].map((t) => {
				const rows = players
					.filter((p) => p.tid === t.tid)
					.sort((x, y) => y.min / y.gp - x.min / x.gp)
					.map((p) => ({
						key: p.pid,
						data: [
							wrappedPlayerNameLabels({
								pid: p.pid,
								season,
								legacyName: p.name,
							}),
							p.pos,
							p.gp,
							a(p, p.min),
							a(p, p.pts),
							a(p, p.orb + p.drb),
							a(p, p.ast),
							a(p, p.stl),
							a(p, p.blk),
							a(p, p.tov),
							pct(p.fg, p.fga),
							pct(p.tp, p.tpa),
							pct(p.ft, p.fta),
							a(p, p.pm),
						],
					}));
				return (
					<section key={t.tid} className="ps-players-team">
						<h2 className="lh-section-title">
							<a href={rosterUrl(t, season)}>
								<Logo url={t.imgURLSmall ?? t.imgURL} size={24} /> {t.region}{" "}
								{t.name}
							</a>
						</h2>
						<DataTable
							cols={cols}
							defaultSort={[3, "desc"]}
							defaultStickyCols={1}
							name="PlayoffSeriesPlayers"
							rows={rows}
						/>
					</section>
				);
			})}
		</div>
	);
};

const PlayoffSeries = ({
	boxScoresDeleted,
	label,
	meetings,
	numGames,
	numGamesToWin,
	pastMeetings,
	playedGames,
	players,
	season,
	seasonRecord,
	teams,
	totals,
	upcoming,
}: Props) => {
	useTitleBar({
		title: label,
		jumpTo: true,
		jumpToSeason: season,
	});
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const nextDate = upcoming[0]?.date;

	return (
		<>
			<Hero
				teams={teams}
				season={season}
				label={label}
				numGames={numGames}
				numGamesToWin={numGamesToWin}
				nextDate={nextDate}
			/>
			<div className="lh-grid ps-grid">
				<div className="lh-main">
					{playedGames.length > 0 || upcoming.length > 0 ? (
						<section className="lh-o1">
							{playedGames.length > 0 ? (
								<div className="lh-games lh-finals">
									{playedGames.map((game) => (
										<div className="lh-game" key={game.gid}>
											{game.date ? (
												<div className="ps-game-date">
													{formatGameDate(game.date, "long")}
												</div>
											) : null}
											<div
												className={
													game.note ? "daily-game-with-note" : "daily-game"
												}
											>
												<GameCard game={game}>
													{game.note ? (
														<GameNote
															gid={game.gid}
															note={game.note}
															links={buildRecapLinksForGame(
																game,
																(tid) => teamInfoCache[tid],
															)}
														/>
													) : null}
												</GameCard>
											</div>
										</div>
									))}
								</div>
							) : null}
							{upcoming.length > 0 ? (
								<UpcomingGames upcoming={upcoming} teams={teams} />
							) : null}
						</section>
					) : null}
					{boxScoresDeleted ? (
						<p className="lh-o1 text-body-secondary mb-0">
							Box scores from this series weren't kept.
						</p>
					) : null}
				</div>
				<div className="lh-rail">
					{players.length > 0 ? (
						<Leaders players={players} teams={teams} season={season} />
					) : null}
					<TeamStats totals={totals} teams={teams} />
					<RegularSeason
						meetings={meetings}
						seasonRecord={seasonRecord}
						teams={teams}
						season={season}
					/>
					<PlayoffHistory pastMeetings={pastMeetings} teams={teams} />
				</div>
			</div>
			{players.length > 0 ? (
				<PlayerTables players={players} teams={teams} season={season} />
			) : null}
		</>
	);
};

export default PlayoffSeries;
