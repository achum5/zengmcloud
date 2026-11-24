import clsx from "clsx";
import type { CSSProperties } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { formatGameDate } from "../../common/calendarDates.ts";
import { RailCard } from "../components/LeagueHub/HubRail.tsx";
import { bannerColors } from "../components/TeamBanner.tsx";
import getWinner from "../../common/getWinner.ts";

// TWO TEAMS, ALL TIME: who leads the regular season series across the top,
// every playoff meeting, the regular season year by year, the totals side by
// side and their latest games. The top is the playoff series page's, with
// all-time wins where a series' games won go.

type Props = View<"rivalry">;
type RivalryTeam = Props["teams"][number];

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

const teamPart = (t: { abbrev: string; tid: number }) => `${t.abbrev}_${t.tid}`;

const record = (won: number, lost: number, tied: number) =>
	`${won}-${lost}${tied > 0 ? `-${tied}` : ""}`;

const HeroTeam = ({
	t,
	side,
	wins,
	series,
}: {
	t: RivalryTeam;
	side: "away" | "home";
	wins: number;
	series: number;
}) => {
	const cap = (
		<a
			className={side === "home" ? "ps-cap ps-cap-home" : "ps-cap ps-cap-away"}
			href={helpers.leagueUrl(["roster", teamPart(t)])}
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
			)}
		>
			<div className="ps-team-seed">
				{series === 1 ? "1 playoff series won" : `${series} playoff series won`}
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
	const number = (
		<div
			className={clsx(
				"ps-wins",
				"rv-wins",
				side === "home" ? "ps-wins-home" : "ps-wins-away",
			)}
		>
			{wins}
		</div>
	);
	return side === "home" ? (
		<>
			{number}
			{words}
			{cap}
		</>
	) : (
		<>
			{cap}
			{words}
			{number}
		</>
	);
};

const Hero = ({
	teams,
	totals,
	since,
}: {
	teams: Props["teams"];
	totals: { won: number; lost: number; tied: number; series: [number, number] };
	since: number | undefined;
}) => {
	const [a, b] = teams;
	const ca = bannerColors(a.colors);
	const cb = bannerColors(b.colors);
	const { won, lost, tied } = totals;
	const status =
		won + lost + tied === 0
			? "Never met"
			: won === lost
				? `Tied ${record(won, lost, tied)}`
				: won > lost
					? `${a.abbrev} leads ${record(won, lost, tied)}`
					: `${b.abbrev} leads ${record(lost, won, tied)}`;
	return (
		<div
			className="ps-hero rv-hero"
			style={
				{
					"--ps-a-bg": ca.bg,
					"--ps-a-fg": ca.fg,
					"--ps-a-accent": ca.accent,
					"--ps-h-bg": cb.bg,
					"--ps-h-fg": cb.fg,
					"--ps-h-accent": cb.accent,
				} as CSSProperties
			}
		>
			<div className="ps-hero-tag">
				{since === undefined ? "All-time" : `All-time since ${since}`}
			</div>
			<HeroTeam t={a} side="away" wins={won} series={totals.series[0]} />
			<div className="ps-mid">
				<div className="ps-status">{status}</div>
				<div className="ps-bestof">Regular season</div>
			</div>
			<HeroTeam t={b} side="home" wins={lost} series={totals.series[1]} />
		</div>
	);
};

const Meetings = ({ meetings, teams }: Pick<Props, "meetings" | "teams">) => {
	const [a, b] = teams;
	return (
		<RailCard title="Playoff meetings" className="lh-o2">
			{meetings.length === 0 ? (
				<p className="rv-none">Never met in the playoffs.</p>
			) : (
				<ul className="lh-list">
					{meetings.map((m) => {
						const winner = m.winnerTid;
						const hi = winner === undefined ? undefined : m.imgURLs[winner];
						const score =
							winner === b.tid ? `${m.lost}-${m.won}` : `${m.won}-${m.lost}`;
						return (
							<li key={m.season}>
								<a
									className={clsx("rv-meeting", { "rv-finals": m.finals })}
									href={helpers.leagueUrl([
										"playoff_series",
										m.season,
										`${m.abbrevs[a.tid]}_${a.tid}`,
										`${m.abbrevs[b.tid]}_${b.tid}`,
									])}
								>
									<span className="rv-meeting-season">{m.season}</span>
									<span className="rv-meeting-round">{m.label}</span>
									<span className="rv-meeting-result">
										{winner === undefined ? (
											`${m.abbrevs[a.tid]} ${m.won}, ${m.abbrevs[b.tid]} ${m.lost}`
										) : (
											<>
												<Logo url={hi} size={22} />
												<span>
													{m.abbrevs[winner]} won {score}
												</span>
											</>
										)}
									</span>
								</a>
							</li>
						);
					})}
				</ul>
			)}
		</RailCard>
	);
};

const Seasons = ({ seasons, teams }: Pick<Props, "seasons" | "teams">) => {
	const [a, b] = teams;
	if (seasons.length === 0) {
		return null;
	}
	return (
		<RailCard title="Season by season" className="lh-o4">
			<ul className="rv-seasons">
				{seasons.map((s) => {
					const games = s.won + s.lost + s.tied;
					const lead = s.won > s.lost ? a : s.lost > s.won ? b : undefined;
					const colors = lead ? bannerColors(lead.colors) : undefined;
					return (
						<li
							key={s.season}
							className={clsx("rv-season", { "rv-season-even": !lead })}
							style={
								colors
									? ({ "--rv-lead": colors.bg } as CSSProperties)
									: undefined
							}
						>
							<span className="rv-season-year">{s.season}</span>
							<span className="rv-season-record">
								{games === 0
									? "-"
									: lead
										? `${lead.abbrev} ${record(Math.max(s.won, s.lost), Math.min(s.won, s.lost), s.tied)}`
										: `Split ${record(s.won, s.lost, s.tied)}`}
							</span>
							{s.playoffs ? (
								<span className="rv-season-playoffs">
									{s.playoffs.label}
									{s.playoffs.winnerTid === a.tid
										? `: ${a.abbrev}`
										: s.playoffs.winnerTid === b.tid
											? `: ${b.abbrev}`
											: ""}
								</span>
							) : null}
						</li>
					);
				})}
			</ul>
		</RailCard>
	);
};

const Totals = ({
	teams,
	seasons,
	meetings,
}: Pick<Props, "teams" | "seasons" | "meetings">) => {
	const [a, b] = teams;
	let won = 0;
	let lost = 0;
	let pts = 0;
	let oppPts = 0;
	let gp = 0;
	for (const s of seasons) {
		won += s.won;
		lost += s.lost;
		pts += s.pts;
		oppPts += s.oppPts;
		gp += s.won + s.lost + s.tied;
	}
	const count = (f: (m: (typeof meetings)[number]) => boolean) =>
		meetings.filter(f).length;
	const rows: [string, number | string, number | string, boolean][] = [
		["Regular season wins", won, lost, true],
		[
			"Points per game",
			gp ? (pts / gp).toFixed(1) : "-",
			gp ? (oppPts / gp).toFixed(1) : "-",
			gp > 0,
		],
		[
			"Playoff games won",
			meetings.reduce((sum, m) => sum + m.won, 0),
			meetings.reduce((sum, m) => sum + m.lost, 0),
			true,
		],
		[
			"Playoff series won",
			count((m) => m.winnerTid === a.tid),
			count((m) => m.winnerTid === b.tid),
			true,
		],
		[
			"Finals won",
			count((m) => m.finals && m.winnerTid === a.tid),
			count((m) => m.finals && m.winnerTid === b.tid),
			true,
		],
	];
	return (
		<RailCard title="All-time" className="lh-o3">
			<table className="ps-compare">
				<thead>
					<tr>
						<th>
							<Logo url={a.imgURLSmall ?? a.imgURL} size={18} /> {a.abbrev}
						</th>
						<th className="ps-compare-label" />
						<th>
							{b.abbrev} <Logo url={b.imgURLSmall ?? b.imgURL} size={18} />
						</th>
					</tr>
				</thead>
				<tbody>
					{rows.map(([label, x, y, compare]) => (
						<tr key={label}>
							<td
								className={clsx({
									"fw-bold": compare && Number(x) > Number(y),
								})}
							>
								{x}
							</td>
							<td className="ps-compare-label">{label}</td>
							<td
								className={clsx({
									"fw-bold": compare && Number(y) > Number(x),
								})}
							>
								{y}
							</td>
						</tr>
					))}
				</tbody>
			</table>
		</RailCard>
	);
};

const Recent = ({ recent, teams }: Pick<Props, "recent" | "teams">) => {
	if (recent.length === 0) {
		return null;
	}
	const byTid = new Map(teams.map((t) => [t.tid, t]));
	const [a] = teams;
	const wins = recent.filter((game) => {
		const i = game.teams[0].tid === a.tid ? 0 : 1;
		return getWinner(game.teams) === i;
	}).length;
	const losses = recent.filter((game) => {
		const winner = getWinner(game.teams);
		const i = game.teams[0].tid === a.tid ? 0 : 1;
		return winner !== -1 && winner !== i;
	}).length;
	const [b] = teams.slice(1);
	const lead =
		wins === losses
			? `Split ${wins}-${losses}`
			: wins > losses
				? `${a.abbrev} ${wins}-${losses}`
				: `${b!.abbrev} ${losses}-${wins}`;
	return (
		<RailCard title={`Last ${recent.length} games`} className="lh-o5">
			<div className="ps-record">{lead}</div>
			<ul className="lh-list">
				{recent.map((game) => {
					const [t0, t1] = game.teams;
					const home = byTid.get(t0.tid)!;
					const away = byTid.get(t1.tid)!;
					const winner = getWinner(game.teams);
					const ot = helpers.overtimeText(game.overtimes ?? 0, game.numPeriods);
					return (
						<li key={game.gid}>
							<a
								className="rv-game"
								href={helpers.leagueUrl([
									"game_log",
									teamPart(home),
									game.season,
									game.gid,
								])}
							>
								<span className="rv-game-when">
									{game.date
										? formatGameDate(game.date, "withYear")
										: `${game.season}, day ${game.day}`}
									{game.playoffs ? (
										<span className="rv-game-tag">Playoffs</span>
									) : null}
								</span>
								<span className="rv-game-score">
									<span className={clsx({ "fw-bold": winner === 1 })}>
										{away.abbrev} {t1.pts}
									</span>
									{" @ "}
									<span className={clsx({ "fw-bold": winner === 0 })}>
										{home.abbrev} {t0.pts}
									</span>
									{ot ? ` ${ot}` : ""}
								</span>
							</a>
						</li>
					);
				})}
			</ul>
		</RailCard>
	);
};

const Rivalry = ({ meetings, recent, seasons, teams }: Props) => {
	const [a, b] = teams;
	useTitleBar({ title: `${a.abbrev} vs. ${b.abbrev}` });

	let won = 0;
	let lost = 0;
	let tied = 0;
	for (const s of seasons) {
		won += s.won;
		lost += s.lost;
		tied += s.tied;
	}
	const since =
		seasons.length > 0 ? Math.min(...seasons.map((s) => s.season)) : undefined;

	return (
		<>
			<Hero
				teams={teams}
				totals={{
					won,
					lost,
					tied,
					series: [
						meetings.filter((m) => m.winnerTid === a.tid).length,
						meetings.filter((m) => m.winnerTid === b.tid).length,
					],
				}}
				since={since}
			/>
			<div className="lh-grid">
				<div className="lh-main">
					<Meetings meetings={meetings} teams={teams} />
					<Seasons seasons={seasons} teams={teams} />
				</div>
				<div className="lh-rail">
					<Totals teams={teams} seasons={seasons} meetings={meetings} />
					<Recent recent={recent} teams={teams} />
				</div>
			</div>
		</>
	);
};

export default Rivalry;
