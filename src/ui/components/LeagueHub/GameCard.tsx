import clsx from "clsx";
import type { CSSProperties, ReactNode } from "react";
import getWinner from "../../../common/getWinner.ts";
import { processPlayerStats } from "../../../common/processPlayerStats.ts";
import { getBestPlayerBoxScore } from "../../../common/getBestPlayerBoxScore.ts";
import type { NationalTag as NationalTagType } from "../../../common/types.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { NationalTag } from "../NationalTag.tsx";
import { PlayerPicture } from "../PlayerPicture.tsx";
import { bannerColors, useAllTeamColors } from "../TeamBanner.tsx";
import { spreadLine } from "./spreadLine.ts";

// A NIGHT'S GAME IN THE HUB, as NBA.com's scoreboard shows one: a card with
// each team's colors at its end, its city, name and score, "Final" (under
// its network, if it was on one) and the series between them, linked to the
// box score, with the game's recap line under it; beside both, each team's
// two best lines of the night (on a phone, each team's top game score under
// the card instead).

type Branding = { region: string; name: string; abbrev: string };

export type GameCardTeam = {
	tid: number;
	pts: number;
	won?: number;
	lost?: number;
	tied?: number;
	otl?: number;
	players?: any[];
	playoffs?: { seed: number; won: number; lost: number };
	branding?: Branding & { imgURL?: string; imgURLSmall?: string };
};

export type GameCardGame = {
	gid: number;
	season?: number;
	overtimes?: number;
	numPeriods?: number;
	playoffs?: boolean;
	numGamesToWinSeries?: number;
	// The pregame line, home's side (Game.spread).
	spread?: number;
	// The national network it was on, if any (see nationalTagsPlayed).
	national?: NationalTagType;
	teams: [GameCardTeam, GameCardTeam];
};

export const useBrand = (t: {
	tid: number;
	branding?: GameCardTeam["branding"];
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const info = teamInfoCache[t.tid];
	return {
		region: t.branding?.region ?? info?.region ?? "",
		name: t.branding?.name ?? info?.name ?? "",
		abbrev: t.branding?.abbrev ?? info?.abbrev ?? "",
		logo:
			t.branding?.imgURL ??
			t.branding?.imgURLSmall ??
			info?.imgURL ??
			info?.imgURLSmall,
		smallLogo:
			t.branding?.imgURLSmall ??
			t.branding?.imgURL ??
			info?.imgURLSmall ??
			info?.imgURL,
	};
};

export const Logo = ({
	url,
	size,
}: {
	url: string | undefined;
	size: number;
}) =>
	url ? (
		<img
			src={url}
			alt=""
			width={size}
			height={size}
			style={{ objectFit: "contain" }}
		/>
	) : null;

// "SAS leads 3-2", after the game.
const seriesLine = (game: GameCardGame, abbrevs: [string, string]) => {
	const [home, away] = game.teams;
	const toWin = game.numGamesToWinSeries;
	if (!game.playoffs || !home.playoffs || !away.playoffs || !toWin) {
		return undefined;
	}
	if (toWin <= 1) {
		return undefined;
	}
	const h = home.playoffs.won;
	const a = away.playoffs.won;
	const number = `Game ${h + a}`;
	if (h === a) {
		return [number, `Series tied ${h}-${a}`];
	}
	const [lead, hi, lo] = h > a ? [abbrevs[0], h, a] : [abbrevs[1], a, h];
	return [number, `${lead} ${hi === toWin ? "wins" : "leads"} ${hi}-${lo}`];
};

// A box score line's columns: points, rebounds, assists, then what stood
// out (steals, blocks, threes), then the shooting.
const statColumns = (p: any): [string, string][] => {
	const s = processPlayerStats(p, ["pts", "trb", "ast", "stl", "blk", "tp"]);
	const cols: [string, string][] = [
		[String(s.pts ?? 0), "PTS"],
		[String(s.trb ?? 0), "REB"],
		[String(s.ast ?? 0), "AST"],
	];
	const extras = (
		[
			[s.stl ?? 0, "STL", 2],
			[s.blk ?? 0, "BLK", 2],
			[s.tp ?? 0, "3PM", 3],
		] as [number, string, number][]
	)
		.filter(([value, , min]) => value >= min)
		.sort((a, b) => b[0] - b[2] - (a[0] - a[2]));
	for (const [value, label] of extras.slice(0, 2)) {
		cols.push([String(value), label]);
	}
	if (cols.length < 5 && p.fga !== undefined) {
		cols.push([`${p.fg ?? 0}/${p.fga}`, "FG"]);
	}
	if (cols.length < 5 && p.fta !== undefined) {
		cols.push([`${p.ft ?? 0}/${p.fta}`, "FT"]);
	}
	return cols.slice(0, 5);
};

export const Face = ({ pid, season }: { pid: number; season: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, season, lid);
	return (
		<div className="lh-gc-face">
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

// ON A PHONE, under a game's card (no room for the lines beside it): a
// player from each team on his own side, mirrored like the card - his face,
// his name, and a line of numbers.
export type PhonePlayer = { p: any; cols: [string, string][] };

const PhonePlayerLine = ({
	player: { p, cols },
	season,
}: {
	player: PhonePlayer;
	season: number;
}) => {
	const name: string = p.name ?? "";
	const space = name.indexOf(" ");
	const short = space > 0 ? `${name[0]}. ${name.slice(space + 1)}` : name;
	return (
		<a className="lh-gc-prev" href={helpers.leagueUrl(["player", p.pid])}>
			<Face pid={p.pid} season={season} />
			<span className="lh-gc-prev-text">
				<span className="lh-gc-prev-name">{short}</span>
				<span className="lh-gc-prev-stats">
					{cols.map(([value, label]) => (
						<span key={label}>
							<b>{value}</b>
							{label}
						</span>
					))}
				</span>
			</span>
		</a>
	);
};

export const PhonePlayers = ({
	away,
	home,
	season,
}: {
	away?: PhonePlayer;
	home?: PhonePlayer;
	season: number;
}) => {
	if (!away && !home) {
		return null;
	}
	return (
		<div className="lh-gc-previews">
			<div className="lh-gc-prev-side">
				{away ? <PhonePlayerLine player={away} season={season} /> : null}
			</div>
			<div className="lh-gc-prev-side lh-gc-prev-home">
				{home ? <PhonePlayerLine player={home} season={season} /> : null}
			</div>
		</div>
	);
};

// A team's top game score of the night, with his points, rebounds and
// assists (a phone's line under the card).
const topGameScore = (players: any[] | undefined): PhonePlayer | undefined => {
	let best: any;
	let bestScore = -Infinity;
	for (const p of players ?? []) {
		if (!p.min) {
			continue;
		}
		const score = helpers.gameScore(p);
		if (score > bestScore) {
			best = p;
			bestScore = score;
		}
	}
	if (!best) {
		return undefined;
	}
	const s = processPlayerStats(best, ["pts", "trb", "ast"]);
	return {
		p: best,
		cols: [
			[String(s.pts ?? 0), "PTS"],
			[String(s.trb ?? 0), "REB"],
			[String(s.ast ?? 0), "AST"],
		],
	};
};

// A team's two best lines of the night, best first.
const bestTwo = (players: any[] | undefined): any[] => {
	if (!players) {
		return [];
	}
	const first = getBestPlayerBoxScore(players);
	if (!first) {
		return [];
	}
	const rest = players.filter((p) => p !== first.p);
	const second = getBestPlayerBoxScore(rest);
	return second ? [first.p, second.p] : [first.p];
};

const Leader = ({ p, season }: { p: any; season: number }) => {
	const name: string = p.name ?? "";
	const space = name.indexOf(" ");
	const first = space > 0 ? name.slice(0, space) : "";
	const last = space > 0 ? name.slice(space + 1) : name;
	return (
		<div className="lh-gc-leader">
			{/* (The face and the name one link to the player.) */}
			<a className="lh-gc-player" href={helpers.leagueUrl(["player", p.pid])}>
				<Face pid={p.pid} season={season} />
				<span className="lh-gc-leader-name">
					<span className="lh-gc-first">{first}</span>
					<span className="lh-gc-last">{last}</span>
				</span>
			</a>
			<div className="lh-gc-stats">
				{statColumns(p).map(([value, label]) => (
					<div
						key={label}
						className={clsx("lh-gc-stat", { frac: value.includes("/") })}
					>
						<b>{value}</b>
						<span>{label}</span>
					</div>
				))}
			</div>
		</div>
	);
};

const TeamLeaders = ({ t, season }: { t: GameCardTeam; season: number }) => {
	const brand = useBrand(t);
	const players = bestTwo(t.players);
	if (players.length === 0) {
		return null;
	}
	return (
		<div className="lh-gc-team-leaders">
			<span className="lh-gc-leader-logo">
				<Logo url={brand.smallLogo} size={18} />
			</span>
			<div className="lh-gc-team-players">
				{players.map((p) => (
					<Leader key={p.pid} p={p} season={season} />
				))}
			</div>
		</div>
	);
};

export const Side = ({
	t,
	side,
	colors,
	logo,
	href,
	label,
}: {
	t: { tid: number };
	side: "away" | "home";
	colors: string[] | undefined;
	logo: string | undefined;
	// (Where the card itself isn't a link: the logo to the team's page.)
	href?: string;
	label?: string;
}) => {
	const { accent } = bannerColors(colors);
	const props = {
		className: clsx(
			"lh-gc-side",
			// (Whole class names: the build keeps only styles whose names it
			// finds written out in the code.)
			side === "home" ? "lh-gc-side-home" : "lh-gc-side-away",
		),
		style: { "--gc-accent": accent } as CSSProperties,
		"data-tid": t.tid,
	};
	return href ? (
		<a {...props} href={href} aria-label={label} title={label}>
			<Logo url={logo} size={44} />
		</a>
	) : (
		<div {...props}>
			<Logo url={logo} size={44} />
		</div>
	);
};

const TeamWords = ({
	t,
	side,
	lost,
}: {
	t: GameCardTeam;
	side: "away" | "home";
	lost: boolean;
}) => {
	const brand = useBrand(t);
	// Its record (in the playoffs, its seed).
	const record = t.playoffs
		? `No. ${t.playoffs.seed}`
		: t.won !== undefined && t.lost !== undefined
			? helpers.formatRecord({ ...t, won: t.won, lost: t.lost })
			: undefined;
	return (
		<div
			className={clsx(
				"lh-gc-team",
				side === "home" ? "lh-gc-team-home" : "lh-gc-team-away",
				{ lost },
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
				<div className="lh-gc-score">{t.pts}</div>
				{record ? <div className="lh-gc-record">{record}</div> : null}
			</div>
		</div>
	);
};

export const GameCard = ({
	game,
	children,
}: {
	game: GameCardGame;
	// The recap line, under the card.
	children?: ReactNode;
}) => {
	const { season: currentSeason } = useLocal(["season"]);
	const season = game.season ?? currentSeason;
	const colors = useAllTeamColors();
	const [home, away] = game.teams;
	const homeBrand = useBrand(home);
	const awayBrand = useBrand(away);
	const winner = getWinner(game.teams);
	const ot = helpers.overtimeText(game.overtimes, game.numPeriods);
	const series = seriesLine(game, [homeBrand.abbrev, awayBrand.abbrev]);
	const line = spreadLine(game.spread, homeBrand.abbrev, awayBrand.abbrev);
	const boxScore = helpers.leagueUrl([
		"game_log",
		`${homeBrand.abbrev}_${home.tid}`,
		season,
		game.gid,
	]);

	// Each half in its team's colors, the two meeting on a slant.
	const a = bannerColors(colors?.[away.tid]);
	const h = bannerColors(colors?.[home.tid]);
	const halves = {
		"--gc-a-bg": a.bg,
		"--gc-a-fg": a.fg,
		"--gc-h-bg": h.bg,
		"--gc-h-fg": h.fg,
	} as CSSProperties;

	return (
		<div className="lh-gc">
			<div className="lh-gc-row">
				<div className="lh-gc-main">
					<a
						className={clsx("lh-gc-card", series && "lh-gc-has-series")}
						style={halves}
						href={boxScore}
						title="Box score"
						aria-label={`${awayBrand.region} ${awayBrand.name} ${away.pts}, ${homeBrand.region} ${homeBrand.name} ${home.pts}, box score`}
					>
						<Side
							t={away}
							side="away"
							colors={colors?.[away.tid]}
							logo={awayBrand.logo}
						/>
						<TeamWords t={away} side="away" lost={winner === 0} />
						{/* The series, on a tag hung from the top over the seam. */}
						{series ? (
							<div className="lh-gc-series">
								{series[0]} · {series[1]}
							</div>
						) : null}
						<div className="lh-gc-mid">
							{game.national ? (
								<div className="lh-gc-net">
									<NationalTag tag={game.national} />
								</div>
							) : null}
							<div className="lh-gc-final">Final{ot ? `/${ot}` : ""}</div>
							{line ? (
								<div className="lh-gc-line" title="Pregame spread">
									{line}
								</div>
							) : null}
						</div>
						<TeamWords t={home} side="home" lost={winner === 1} />
						<Side
							t={home}
							side="home"
							colors={colors?.[home.tid]}
							logo={homeBrand.logo}
						/>
					</a>
					<PhonePlayers
						away={topGameScore(away.players)}
						home={topGameScore(home.players)}
						season={season}
					/>
					{children}
				</div>
				<div className="lh-gc-leaders">
					<TeamLeaders t={away} season={season} />
					<TeamLeaders t={home} season={season} />
				</div>
			</div>
		</div>
	);
};
