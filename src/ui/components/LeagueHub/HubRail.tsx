import clsx from "clsx";
import { useState, type ReactNode } from "react";
import type {
	HubInjury,
	HubPerformer,
} from "../../../worker/views/dailyScheduleHub.ts";
import type { RecapDayStandings } from "../../../worker/util/getDayGamesForRecap.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { PlayerPicture } from "../PlayerPicture.tsx";

// THE HUB'S SIDE PANELS, ESPN's right rail: the night's top performers, the
// night's headlines, the standings and who got hurt. Each is a small card
// of the game's own numbers, linked through to the full pages.

const Logo = ({ tid, size = 20 }: { tid: number; size?: number }) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const t = teamInfoCache[tid];
	const url = t?.imgURLSmall ?? t?.imgURL;
	return url ? (
		<img
			className="lh-logo"
			src={url}
			alt=""
			width={size}
			height={size}
			style={{ objectFit: "contain" }}
		/>
	) : null;
};

const useTeam = (tid: number) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	return teamInfoCache[tid];
};

const rosterUrl = (abbrev: string | undefined, tid: number, season: number) =>
	helpers.leagueUrl(["roster", `${abbrev ?? tid}_${tid}`, season]);

const boxScoreUrl = (
	abbrev: string | undefined,
	tid: number,
	season: number,
	gid: number,
) => helpers.leagueUrl(["game_log", `${abbrev ?? tid}_${tid}`, season, gid]);

export const RailCard = ({
	title,
	more,
	children,
	className,
}: {
	title: string;
	more?: { href: string; text: string };
	children: ReactNode;
	className?: string;
}) => (
	<section className={clsx("lh-card", className)}>
		<div className="lh-card-head">
			<h2 className="lh-card-title">{title}</h2>
			{more ? (
				<a className="lh-card-more" href={more.href}>
					{more.text}
				</a>
			) : null}
		</div>
		{children}
	</section>
);

const Face = ({ pid, season }: { pid: number; season: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, season, lid);
	return (
		<div className="lh-face">
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

// "36 PTS 8 REB 7 AST", and a big night's steals, blocks or threes after.
const statLine = (p: HubPerformer) => {
	const items: [number, string][] = [
		[p.pts, "PTS"],
		[p.trb, "REB"],
		[p.ast, "AST"],
	];
	const extras: [number, string, number][] = [
		[p.stl, "STL", 4],
		[p.blk, "BLK", 4],
		[p.tp, "3PM", 6],
	];
	for (const [value, label, min] of extras) {
		if (value >= min) {
			items.push([value, label]);
		}
	}
	return items;
};

const PerformerRow = ({
	p,
	rank,
	season,
}: {
	p: HubPerformer;
	rank: number;
	season: number;
}) => {
	const t = useTeam(p.tid);
	const opp = useTeam(p.oppTid);
	const ot =
		p.overtimes === 0 ? "" : p.overtimes === 1 ? " OT" : ` ${p.overtimes}OT`;
	const playerUrl = helpers.leagueUrl(["player", p.pid]);
	return (
		<li className="lh-perf">
			<span className="lh-perf-rank">{rank}</span>
			{/* (The face goes to the player too; the name is the link a
			    keyboard or a screen reader gets.) */}
			<a
				className="lh-face-link"
				href={playerUrl}
				tabIndex={-1}
				aria-hidden="true"
			>
				<Face pid={p.pid} season={season} />
			</a>
			<div className="lh-perf-main">
				<div className="lh-perf-name">
					<a href={playerUrl}>{p.name}</a>
				</div>
				<div className="lh-perf-team">
					<Logo tid={p.tid} size={16} /> {t?.abbrev}{" "}
					<a href={boxScoreUrl(t?.abbrev, p.tid, season, p.gid)}>
						{p.home ? "vs" : "@"} {opp?.abbrev}, {p.won ? "W" : "L"}{" "}
						{p.score[0]}-{p.score[1]}
						{ot}
					</a>
				</div>
				<div className="lh-perf-stats">
					{statLine(p).map(([value, label]) => (
						<span key={label} className="lh-stat">
							<b>{value}</b> {label}
						</span>
					))}
				</div>
			</div>
		</li>
	);
};

export const TopPerformers = ({
	performers,
	season,
}: {
	performers: HubPerformer[];
	season: number;
}) =>
	performers.length > 0 ? (
		<RailCard title="Top performers">
			<ol className="lh-list">
				{performers.map((p, i) => (
					<PerformerRow key={p.pid} p={p} rank={i + 1} season={season} />
				))}
			</ol>
		</RailCard>
	) : null;

// Each game's recap headline, to its box score.
export const Headlines = ({
	games,
	season,
}: {
	games: { gid: number; note?: string; teams: [{ tid: number }, any] }[];
	season: number;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const items = games
		.map((game) => {
			const first = (game.note ?? "")
				.split("\n")
				.find((line) => line.trim() !== "");
			if (!first) {
				return undefined;
			}
			const text = first
				.replaceAll(/[#*_]/g, "")
				.replaceAll(/\[([^\]]+)]\([^)]*\)/g, "$1")
				.trim();
			const home = game.teams[0].tid;
			return {
				gid: game.gid,
				text,
				href: boxScoreUrl(teamInfoCache[home]?.abbrev, home, season, game.gid),
			};
		})
		.filter((x) => x !== undefined);
	if (items.length === 0) {
		return null;
	}
	return (
		<RailCard title="Headlines" className="lh-headlines">
			<ul className="lh-list">
				{items.map((item) => (
					<li key={item.gid} className="lh-headline-item">
						<a href={item.href}>{item.text}</a>
					</li>
				))}
			</ul>
		</RailCard>
	);
};

export const HubStandings = ({
	playedTids,
	season,
	standings,
	userTid,
	wonTids,
}: {
	playedTids: Set<number>;
	season: number;
	standings: RecapDayStandings;
	userTid: number;
	wonTids: Set<number>;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const userConf = standings.confs.findIndex((conf) =>
		conf.teams.some((t) => t.tid === userTid),
	);
	const [confIndex, setConfIndex] = useState(Math.max(0, userConf));
	const conf = standings.confs[confIndex] ?? standings.confs[0];
	if (!conf) {
		return null;
	}
	const short = (name: string) => name.replace(/ Conference$/, "");
	// Not t.gb: that is from the team listed first, and goes negative for a team
	// below it with the better won-lost difference.
	const gb = helpers.gbFromBest(conf.teams);
	return (
		<RailCard
			title="Standings"
			more={{
				href: helpers.leagueUrl(["standings", season]),
				text: "Full standings",
			}}
		>
			{standings.confs.length > 1 ? (
				<div className="lh-tabs" role="tablist">
					{standings.confs.map((c, i) => (
						<button
							key={c.name}
							type="button"
							role="tab"
							aria-selected={i === confIndex}
							className={clsx("lh-tab", { active: i === confIndex })}
							onClick={() => setConfIndex(i)}
						>
							{short(c.name)}
						</button>
					))}
				</div>
			) : null}
			<table className="lh-standings">
				<thead>
					<tr>
						<th colSpan={2} />
						<th>W</th>
						<th>L</th>
						<th>GB</th>
						<th />
					</tr>
				</thead>
				<tbody>
					{conf.teams.map((t) => {
						const tid = t.tid ?? -1;
						const info = teamInfoCache[tid];
						const played = playedTids.has(tid);
						return (
							<tr
								key={tid}
								className={clsx({
									"lh-user": tid === userTid,
									"lh-cut":
										standings.playoffSpots !== undefined &&
										t.rank === standings.playoffSpots,
								})}
							>
								<td className="lh-rank">{t.rank}</td>
								<td className="lh-team">
									<Logo tid={tid} size={16} />
									<a href={rosterUrl(info?.abbrev ?? t.abbrev, tid, season)}>
										{info?.region ?? t.region}
									</a>
								</td>
								<td>{t.won}</td>
								<td>{t.lost}</td>
								<td>{gb(t) === 0 ? "-" : gb(t)}</td>
								<td>
									{played ? (
										<span
											className={clsx(
												"lh-result",
												wonTids.has(tid) ? "won" : "lost",
											)}
											title={wonTids.has(tid) ? "Won tonight" : "Lost tonight"}
										>
											{wonTids.has(tid) ? "W" : "L"}
										</span>
									) : null}
								</td>
							</tr>
						);
					})}
				</tbody>
			</table>
		</RailCard>
	);
};

const InjuryRow = ({ injury }: { injury: HubInjury }) => {
	const t = useTeam(injury.tid);
	return (
		<li className="lh-injury">
			<Logo tid={injury.tid} size={16} />
			<div>
				<a href={helpers.leagueUrl(["player", injury.pid])}>{injury.name}</a>{" "}
				<span className="text-body-secondary">{t?.abbrev}</span>
				<div className="lh-injury-what">
					{injury.type}, out {injury.gamesRemaining}{" "}
					{injury.gamesRemaining === 1 ? "game" : "games"}
				</div>
			</div>
		</li>
	);
};

export const InjuryReport = ({
	injuries,
	season,
}: {
	injuries: HubInjury[];
	season: number;
}) =>
	injuries.length > 0 ? (
		<RailCard
			title="Injuries"
			more={{
				href: helpers.leagueUrl(["injuries", season]),
				text: "All injuries",
			}}
		>
			<ul className="lh-list">
				{injuries.map((injury) => (
					<InjuryRow key={injury.pid} injury={injury} />
				))}
			</ul>
		</RailCard>
	) : null;
