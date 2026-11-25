import clsx from "clsx";
import type { CSSProperties } from "react";
import type { NationalTag as NationalTagType } from "../../../common/types.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { NationalTag } from "../NationalTag.tsx";
import { bannerColors, useAllTeamColors } from "../TeamBanner.tsx";
import { Face, Logo, Side, useBrand } from "./GameCard.tsx";
import { spreadLine } from "./spreadLine.ts";

// A GAME STILL TO BE PLAYED, on the played game's card (GameCard): each
// team's colors at its end, its city and name - and where a played game has
// its score, its record, with its overall beside it. Between them, when it
// is (or its network) and the line; hung from the foot, watch it or sim it;
// beside it all, each team's best player.

export type UpcomingCardTeam = {
	tid: number;
	ovr?: number;
	won?: number;
	lost?: number;
	tied?: number;
	otl?: number;
	playoffs?: { seed: number };
};

export type UpcomingCardGame = {
	gid: number;
	season: number;
	// Home's side, positive when home is favored (see getUpcoming).
	spread?: number;
	total?: number;
	national?: NationalTagType;
	teams: [UpcomingCardTeam, UpcomingCardTeam];
};

// Watch it or sim it, where this device can.
export type UpcomingCardActions = {
	// The user's own team's game.
	own: boolean;
	disabled: boolean;
	// Why not, when disabled.
	title?: string;
	onWatch: () => void;
	onSim: () => void;
};

const recordOf = (t: UpcomingCardTeam) =>
	t.playoffs
		? `No. ${t.playoffs.seed}`
		: t.won !== undefined && t.lost !== undefined
			? helpers.formatRecord({ ...t, won: t.won, lost: t.lost })
			: undefined;

const UpcomingWords = ({
	t,
	side,
}: {
	t: UpcomingCardTeam;
	side: "away" | "home";
}) => {
	const brand = useBrand(t);
	const record = recordOf(t);
	return (
		<div
			className={clsx(
				"lh-gc-team",
				side === "home" ? "lh-gc-team-home" : "lh-gc-team-away",
			)}
		>
			<div className="lh-gc-region">{brand.region}</div>
			<div
				className={clsx("lh-gc-name", {
					"lh-gc-name-long": brand.name.length >= 8,
					"lh-gc-name-xlong": brand.name.length >= 11,
				})}
			>
				{brand.name}
			</div>
			<div className="lh-gc-scoreline">
				{record ? <div className="lh-gc-up-record">{record}</div> : null}
				{t.ovr !== undefined ? (
					<div className="lh-gc-record">{t.ovr} ovr</div>
				) : null}
			</div>
		</div>
	);
};

// A team's best player going in: his overall and his averages.
const ToWatch = ({
	t,
	p,
	season,
}: {
	t: UpcomingCardTeam;
	p: any;
	season: number;
}) => {
	const brand = useBrand(t);
	if (!p) {
		return null;
	}
	const name: string = p.name ?? "";
	const space = name.indexOf(" ");
	const first = space > 0 ? name.slice(0, space) : "";
	const last = space > 0 ? name.slice(space + 1) : name;
	const cols: [string, string][] = [
		[String(p.ratings?.ovr ?? ""), "OVR"],
		[(p.stats?.pts ?? 0).toFixed(1), "PTS"],
		[(p.stats?.trb ?? 0).toFixed(1), "REB"],
		[(p.stats?.ast ?? 0).toFixed(1), "AST"],
	];
	return (
		<div className="lh-gc-team-leaders">
			<span className="lh-gc-leader-logo">
				<Logo url={brand.smallLogo} size={18} />
			</span>
			<div className="lh-gc-team-players">
				<div className="lh-gc-leader">
					<a
						className="lh-gc-player"
						href={helpers.leagueUrl(["player", p.pid])}
					>
						<Face pid={p.pid} season={season} />
						<span className="lh-gc-leader-name">
							<span className="lh-gc-first">{first}</span>
							<span className="lh-gc-last">{last}</span>
						</span>
					</a>
					<div className="lh-gc-stats">
						{cols.map(([value, label]) => (
							<div key={label} className="lh-gc-stat">
								<b>{value}</b>
								<span>{label}</span>
							</div>
						))}
					</div>
				</div>
			</div>
		</div>
	);
};

const PlayIcon = () => (
	<svg viewBox="0 0 10 10" aria-hidden="true">
		<path d="M2 1.2v7.6L8.6 5z" fill="currentColor" />
	</svg>
);

const SimIcon = () => (
	<svg viewBox="0 0 12 10" aria-hidden="true">
		<path d="M0.8 1.2v7.6L5.8 5zM6 1.2v7.6L11 5z" fill="currentColor" />
	</svg>
);

export const UpcomingCard = ({
	game,
	players,
	actions,
	when,
}: {
	game: UpcomingCardGame;
	// Each team's best player, home first (see getTopPlayers).
	players?: [any, any];
	actions?: UpcomingCardActions;
	// "Today", or the day it's on.
	when: string;
}) => {
	const { season: currentSeason } = useLocal(["season"]);
	const season = game.season ?? currentSeason;
	const colors = useAllTeamColors();
	const [home, away] = game.teams;
	const homeBrand = useBrand(home);
	const awayBrand = useBrand(away);
	const line = spreadLine(game.spread, homeBrand.abbrev, awayBrand.abbrev);

	const a = bannerColors(colors?.[away.tid]);
	const h = bannerColors(colors?.[home.tid]);
	const halves = {
		"--gc-a-bg": a.bg,
		"--gc-a-fg": a.fg,
		"--gc-h-bg": h.bg,
		"--gc-h-fg": h.fg,
	} as CSSProperties;

	return (
		<div className="lh-gc lh-gc-upcoming">
			<div className="lh-gc-row">
				<div className="lh-gc-main">
					<div
						className={clsx("lh-gc-card", actions && "lh-gc-has-acts")}
						style={halves}
					>
						<Side
							t={away}
							side="away"
							colors={colors?.[away.tid]}
							logo={awayBrand.logo}
						/>
						<UpcomingWords t={away} side="away" />
						<div className="lh-gc-mid">
							<div className="lh-gc-final lh-gc-when">
								{game.national ? <NationalTag tag={game.national} /> : when}
							</div>
							{line ? (
								<div className="lh-gc-line" title="Spread">
									{line}
								</div>
							) : null}
							{game.total !== undefined ? (
								<div
									className={clsx("lh-gc-line", line && "lh-gc-line-2")}
									title="Over/under"
								>
									O/U {game.total}
								</div>
							) : null}
						</div>
						<UpcomingWords t={home} side="home" />
						<Side
							t={home}
							side="home"
							colors={colors?.[home.tid]}
							logo={homeBrand.logo}
						/>
						{/* Watch or sim it, on a tab hung from the foot over the seam. */}
						{actions ? (
							<div
								className={clsx("lh-gc-acts", actions.own && "lh-gc-acts-own")}
								title={actions.title}
							>
								<button
									type="button"
									disabled={actions.disabled}
									onClick={actions.onWatch}
								>
									<PlayIcon />
									Watch
								</button>
								<button
									type="button"
									disabled={actions.disabled}
									onClick={actions.onSim}
								>
									<SimIcon />
									Sim
								</button>
							</div>
						) : null}
					</div>
				</div>
				<div className="lh-gc-leaders">
					<ToWatch t={away} p={players?.[1]} season={season} />
					<ToWatch t={home} p={players?.[0]} season={season} />
				</div>
			</div>
		</div>
	);
};
