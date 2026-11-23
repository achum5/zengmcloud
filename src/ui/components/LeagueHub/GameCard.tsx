import clsx from "clsx";
import type { CSSProperties } from "react";
import getWinner from "../../../common/getWinner.ts";
import { processPlayerStats } from "../../../common/processPlayerStats.ts";
import { getBestPlayerBoxScore } from "../../../common/getBestPlayerBoxScore.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { PlayerPicture } from "../PlayerPicture.tsx";
import { bannerColors, useAllTeamColors } from "../TeamBanner.tsx";

// A NIGHT'S GAME IN THE HUB, as NBA.com's scoreboard shows one: a card with
// each team's colors at its end, its city, name and score, "Final" and the
// series between them, linked to the box score; beside it, each team's best
// line of the night.

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
	teams: [GameCardTeam, GameCardTeam];
};

const useBrand = (t: GameCardTeam) => {
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

const Face = ({ pid, season }: { pid: number; season: number }) => {
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

const Leader = ({ t, season }: { t: GameCardTeam; season: number }) => {
	const brand = useBrand(t);
	const best = t.players ? getBestPlayerBoxScore(t.players) : undefined;
	if (!best) {
		return null;
	}
	const p = best.p as any;
	const name: string = p.name ?? "";
	const space = name.indexOf(" ");
	const first = space > 0 ? name.slice(0, space) : "";
	const last = space > 0 ? name.slice(space + 1) : name;
	return (
		<div className="lh-gc-leader">
			<span className="lh-gc-leader-logo">
				<Logo url={brand.smallLogo} size={18} />
			</span>
			<Face pid={p.pid} season={season} />
			<a
				className="lh-gc-leader-name"
				href={helpers.leagueUrl(["player", p.pid])}
			>
				<span className="lh-gc-first">{first}</span>
				<span className="lh-gc-last">{last}</span>
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

const Side = ({
	t,
	side,
	colors,
	logo,
}: {
	t: GameCardTeam;
	side: "away" | "home";
	colors: string[] | undefined;
	logo: string | undefined;
}) => {
	const { accent } = bannerColors(colors);
	return (
		<div
			className={clsx("lh-gc-side", `lh-gc-side-${side}`)}
			style={{ "--gc-accent": accent } as CSSProperties}
			data-tid={t.tid}
		>
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
		<div className={clsx("lh-gc-team", `lh-gc-team-${side}`, { lost })}>
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

export const GameCard = ({ game }: { game: GameCardGame }) => {
	const { season: currentSeason } = useLocal(["season"]);
	const season = game.season ?? currentSeason;
	const colors = useAllTeamColors();
	const [home, away] = game.teams;
	const homeBrand = useBrand(home);
	const awayBrand = useBrand(away);
	const winner = getWinner(game.teams);
	const ot = helpers.overtimeText(game.overtimes, game.numPeriods);
	const series = seriesLine(game, [homeBrand.abbrev, awayBrand.abbrev]);
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
				<a
					className="lh-gc-card"
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
					<div className="lh-gc-mid">
						<div className="lh-gc-final">Final{ot ? `/${ot}` : ""}</div>
						{series ? (
							<div className="lh-gc-series">
								{series[0]}
								<br />
								{series[1]}
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
				<div className="lh-gc-leaders">
					<Leader t={away} season={season} />
					<Leader t={home} season={season} />
				</div>
			</div>
		</div>
	);
};
