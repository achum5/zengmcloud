import { useEffect, useState, type ReactNode } from "react";
import { helpers } from "../../../util/helpers.ts";
import { useUserTeamFirst } from "../../../util/liveBoxScoreLayout.ts";
import {
	PeriodPicker,
	teamsInPeriod,
	type BoxPeriods,
	type PeriodPick,
} from "../../../components/PeriodPicker.tsx";
import clsx from "clsx";
import { PlayerNameLabels } from "../../../components/PlayerNameLabels.tsx";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { Switch } from "./Gamecast.tsx";
import {
	DataTable,
	type DataTableRow,
} from "../../../components/DataTable/index.tsx";
import { getCols } from "../../../../common/getCols.ts";
import type { Col as DataCol } from "../../../../common/types.ts";

// THE BOX SCORE ON A PHONE (the refreshed look): a team at a time, its name
// over the column heads, its starters, then its bench under the heads again;
// each man his face, his initial and name and his number; who didn't play
// and why; the team's totals. Three ways to look at it - the usual line, the
// shooting, the rates - each with the columns this device has picked for it.

type Line = Record<string, any>;
type Col = {
	label: string;
	title: string;
	get: (p: Line, t: Line, o: Line) => ReactNode;
	total?: (t: Line, o: Line) => ReactNode;
	pct?: (t: Line) => ReactNode;
};

const dash = "-";
const div = (a: number, b: number) => (b > 0 ? a / b : undefined);
const fixed = (v: number | undefined, d = 1) =>
	v === undefined || !Number.isFinite(v) ? dash : v.toFixed(d);
const pctOf = (a: number, b: number) => fixed(div(100 * a, b));
const minutes = (min: number) => {
	const secs = Math.round(min * 60);
	return `${Math.floor(secs / 60)}:${String(secs % 60).padStart(2, "0")}`;
};
const share = (t: Line) => (t.min > 0 ? t.min / 5 : 48);
const oppPoss = (o: Line) => o.fga + 0.44 * o.fta - o.orb + o.tov;

export const COLS: Record<string, Col> = {
	min: {
		label: "MP",
		title: "Minutes",
		get: (p) => minutes(p.min),
	},
	fg: {
		label: "FG",
		title: "Field goals",
		get: (p) => `${p.fg}-${p.fga}`,
		total: (t) => `${t.fg}-${t.fga}`,
		pct: (t) => pctOf(t.fg, t.fga),
	},
	tp: {
		label: "3P",
		title: "Three pointers",
		get: (p) => `${p.tp}-${p.tpa}`,
		total: (t) => `${t.tp}-${t.tpa}`,
		pct: (t) => pctOf(t.tp, t.tpa),
	},
	ft: {
		label: "FT",
		title: "Free throws",
		get: (p) => `${p.ft}-${p.fta}`,
		total: (t) => `${t.ft}-${t.fta}`,
		pct: (t) => pctOf(t.ft, t.fta),
	},
	fgp: {
		label: "FG%",
		title: "Field goal percentage",
		get: (p) => pctOf(p.fg, p.fga),
		total: (t) => pctOf(t.fg, t.fga),
	},
	tpp: {
		label: "3P%",
		title: "Three point percentage",
		get: (p) => pctOf(p.tp, p.tpa),
		total: (t) => pctOf(t.tp, t.tpa),
	},
	ftp: {
		label: "FT%",
		title: "Free throw percentage",
		get: (p) => pctOf(p.ft, p.fta),
		total: (t) => pctOf(t.ft, t.fta),
	},
	efg: {
		label: "eFG%",
		title: "Effective field goal percentage",
		get: (p) => pctOf(p.fg + 0.5 * p.tp, p.fga),
		total: (t) => pctOf(t.fg + 0.5 * t.tp, t.fga),
	},
	ts: {
		label: "TS%",
		title: "True shooting percentage",
		get: (p) => pctOf(p.pts, 2 * (p.fga + 0.44 * p.fta)),
		total: (t) => pctOf(t.pts, 2 * (t.fga + 0.44 * t.fta)),
	},
	trb: {
		label: "TRB",
		title: "Rebounds",
		get: (p) => p.orb + p.drb,
		total: (t) => t.orb + t.drb,
	},
	orb: {
		label: "ORB",
		title: "Offensive rebounds",
		get: (p) => p.orb,
		total: (t) => t.orb,
	},
	drb: {
		label: "DRB",
		title: "Defensive rebounds",
		get: (p) => p.drb,
		total: (t) => t.drb,
	},
	ast: {
		label: "AST",
		title: "Assists",
		get: (p) => p.ast,
		total: (t) => t.ast,
	},
	stl: {
		label: "STL",
		title: "Steals",
		get: (p) => p.stl,
		total: (t) => t.stl,
	},
	blk: {
		label: "BLK",
		title: "Blocks",
		get: (p) => p.blk,
		total: (t) => t.blk,
	},
	ba: {
		label: "BA",
		title: "Blocks against",
		get: (p) => p.ba,
		total: (t) => t.ba,
	},
	tov: {
		label: "TOV",
		title: "Turnovers",
		get: (p) => p.tov,
		total: (t) => t.tov,
	},
	pf: {
		label: "PF",
		title: "Personal fouls",
		get: (p) => p.pf,
		total: (t) => t.pf,
	},
	pm: {
		label: "+/-",
		title: "Plus/minus",
		get: (p) => (p.pm > 0 ? `+${p.pm}` : p.pm),
	},
	pts: {
		label: "PTS",
		title: "Points",
		get: (p) => p.pts,
		total: (t) => t.pts,
	},
	gmsc: {
		label: "GmSc",
		title: "Game score",
		get: (p) => fixed(helpers.gameScore(p as any)),
	},
	usg: {
		label: "USG%",
		title: "Usage: the share of his team's plays he ended while on the floor",
		get: (p, t) =>
			fixed(
				div(
					100 * (p.fga + 0.44 * p.fta + p.tov) * share(t),
					p.min * (t.fga + 0.44 * t.fta + t.tov),
				),
			),
	},
	astp: {
		label: "AST%",
		title: "Share of his teammates' baskets he assisted while on the floor",
		get: (p, t) => fixed(div(100 * p.ast, (p.min / share(t)) * t.fg - p.fg)),
	},
	stlp: {
		label: "STL%",
		title: "Share of the other team's plays he stole while on the floor",
		get: (p, t, o) => fixed(div(100 * p.stl * share(t), p.min * oppPoss(o))),
	},
	blkp: {
		label: "BLK%",
		title: "Share of the other team's twos he blocked while on the floor",
		get: (p, t, o) =>
			fixed(div(100 * p.blk * share(t), p.min * (o.fga - o.tpa))),
	},
	tovp: {
		label: "TOV%",
		title: "Turnovers per 100 plays he ended",
		get: (p) => fixed(div(100 * p.tov, p.fga + 0.44 * p.fta + p.tov)),
	},
	orbp: {
		label: "ORB%",
		title: "Share of his team's misses he rebounded while on the floor",
		get: (p, t, o) =>
			fixed(div(100 * p.orb * share(t), p.min * (t.orb + o.drb))),
	},
	drbp: {
		label: "DRB%",
		title: "Share of the other team's misses he rebounded while on the floor",
		get: (p, t, o) =>
			fixed(div(100 * p.drb * share(t), p.min * (t.drb + o.orb))),
	},
};

export type BoxView = "trad" | "adv";
const VIEWS: [BoxView, string][] = [
	["trad", "Traditional"],
	["adv", "Advanced"],
];
// Each view's columns; which of them show, and in what order, is this
// device's to choose with BBGM's own Customize columns (the table's menu).
const VIEW_COLS: Record<BoxView, string[]> = {
	trad: [
		"min",
		"pts",
		"trb",
		"ast",
		"fg",
		"tp",
		"ft",
		"orb",
		"drb",
		"stl",
		"blk",
		"ba",
		"tov",
		"pf",
		"pm",
	],
	adv: [
		"min",
		"usg",
		"astp",
		"stlp",
		"blkp",
		"tovp",
		"orbp",
		"drbp",
		"efg",
		"ts",
		"gmsc",
	],
};

// A player's name as BBGM's own tables show it (his face, his number, his
// name to his page, his skills, an injury, the ratings and stats button).
export const NameCell = ({
	p,
	season,
	exhibition,
}: {
	p: Line;
	season: number;
	exhibition?: boolean;
}) => (
	<td>
		<PlayerNameLabels
			injury={p.injury}
			jerseyNumber={p.jerseyNumber}
			pid={p.pid}
			skills={p.skills}
			legacyName={p.name}
			disableNameLink={exhibition}
			season={season}
		/>
	</td>
);

// THE BOX SCORE AS ONE OF BBGM'S OWN TABLES (its menu: Customize columns,
// Download spreadsheet, Reset table): each team under its name, its
// starters, a line, its bench, who didn't play, its totals.
// ONE TEAM'S TABLE: its name over it (its logo by it), its starters, its
// bench, those who didn't play, its totals. Its columns kept the same as the
// other team's (see syncSettings).
const BoxTable = ({
	boxScore,
	view,
	live,
	t,
	o,
	titleEnd,
}: {
	boxScore: any;
	view: BoxView;
	live?: boolean;
	t: Line;
	o: Line;
	// By its menu: the quarter picker (see PeriodPicker).
	titleEnd?: ReactNode;
}) => {
	const keys = VIEW_COLS[view];
	const cols: DataCol[] = [
		...getCols(["Name", "Pos"]),
		...keys.map((k): DataCol => ({
			title: COLS[k]!.label,
			desc: COLS[k]!.title,
			sortSequence: ["desc", "asc"],
			sortType: "number",
		})),
	];
	const season: number = boxScore.season;
	const exhibition = !!boxScore.exhibition;
	const numStarters = boxScore.numPlayersOnCourt ?? 5;
	const blanks = keys.map(() => null);
	const rows: DataTableRow[] = [];
	{
		// (A game still going: only the hurt are out - everyone else may yet
		// play.)
		const out = (p: Line) =>
			!(p.min > 0) &&
			(!live || (p.injury?.gamesRemaining > 0 && !p.injury?.playingThrough));
		const played = t.players.filter((p: Line) => !out(p));
		const dnp = t.players.filter(out);
		// (Live, the starters are the first five, as the box score lists them.)
		const starters = live
			? t.players.slice(0, numStarters).filter((p: Line) => !out(p))
			: played.filter((p: Line) => p.gs === 1).slice(0, numStarters);
		const bench = played.filter((p: Line) => !starters.includes(p));
		[...starters, ...bench].forEach((p: Line, k: number) => {
			rows.push({
				key: `${t.tid}:${p.pid}`,
				classNames: clsx({
					separator: k + 1 === starters.length && bench.length > 0,
					"table-warning": live && p.inGame,
				}),
				data: [
					{
						value: (
							<PlayerNameLabels
								injury={p.injury}
								jerseyNumber={p.jerseyNumber}
								pid={p.pid}
								skills={p.skills}
								legacyName={p.name}
								disableNameLink={exhibition}
								season={season}
							/>
						),
						searchValue: p.name,
						exportValue: p.name,
					},
					p.pos,
					...keys.map((c) => COLS[c]!.get(p, t, o)),
				],
			});
		});
		for (const p of dnp) {
			rows.push({
				key: `${t.tid}:${p.pid}`,
				data: [
					{
						value: (
							<PlayerNameLabels
								injury={p.injury}
								jerseyNumber={p.jerseyNumber}
								pid={p.pid}
								skills={p.skills}
								legacyName={p.name}
								disableNameLink={exhibition}
								season={season}
							/>
						),
						searchValue: p.name,
						exportValue: p.name,
					},
					p.pos,
					{
						value: `DNP - ${
							p.injury?.gamesRemaining > 0 && !p.injury?.playingThrough
								? p.injury.type
								: "Coach's decision"
						}`,
						colSpanToEnd: true,
					},
					...blanks.slice(1),
				],
			});
		}
		rows.push({
			key: `total${t.tid}`,
			classNames: "bsp-total-row",
			data: ["Total", null, ...keys.map((c) => COLS[c]!.total?.(t, o) ?? null)],
		});
		if (keys.some((c) => COLS[c]!.pct)) {
			rows.push({
				key: `pct${t.tid}`,
				classNames: "bsp-total-row",
				data: [
					"Percentages",
					null,
					...keys.map((c) => COLS[c]!.pct?.(t) ?? null),
				],
			});
		}
	}
	const name = `${t.region} ${t.name}`;
	return (
		<DataTable
			classNameWrapper="mb-3"
			cols={cols}
			defaultSort="disableSort"
			defaultStickyCols={1}
			hideAllControls
			name={`BoxScore:${view}`}
			rows={rows}
			syncSettings
			titleEnd={titleEnd}
			title={
				<span className="bsp-team">
					<TeamLogoInline
						imgURL={t.imgURL}
						imgURLSmall={t.imgURLSmall}
						size={24}
					/>
					{t.tid >= 0 ? (
						<a
							href={helpers.leagueUrl([
								"roster",
								`${t.abbrev}_${t.tid}`,
								season,
							])}
						>
							{name}
						</a>
					) : (
						name
					)}
				</span>
			}
		/>
	);
};

export const BoxScorePhone = ({
	boxScore,
	extraViews,
	extra,
	live,
	title,
	periods,
}: {
	// Each team's by the quarter too, picked by its name (see PeriodPicker).
	periods?: BoxPeriods;
	// The section's heading, drawn here - its tabs by it.
	title?: string;
	boxScore: any;
	// A game still going: who is on the floor marked, as BBGM's live box
	// score marks them.
	live?: boolean;
	// More ways to look at it (rotations), and what each shows.
	extraViews?: [string, string][];
	extra?: (view: string) => ReactNode;
}) => {
	const [view, setView] = useState<string>("trad");
	const [picks, setPicks] = useState<Record<number, PeriodPick>>({});
	const boxView = VIEWS.find(([v]) => v === view)?.[0];
	const live2 = live && !boxScore.gameOver;
	const tabs = (
		<Switch
			options={[...VIEWS, ...(extraViews ?? [])]}
			value={view}
			onChange={setView}
		/>
	);
	const teams = useUserTeamFirst(boxScore.teams as [any, any]);
	return (
		<div className="bsp">
			{title ? (
				// (The section's heading, its ways to look at it by it - one row.)
				<div className="bsp-head-row">
					<h2 className="recap-section-title">{title}</h2>
					{tabs}
				</div>
			) : (
				<div className="bsp-views">{tabs}</div>
			)}
			{boxView
				? teams.map((t: Line) => {
						const pick = picks[t.tid] ?? "all";
						// (This team's quarter, and the other's in it.)
						const cut = teamsInPeriod(boxScore, periods, pick, t.tid);
						return (
							<BoxTable
								key={`${boxView}:${t.tid}`}
								boxScore={boxScore}
								view={boxView}
								live={live2 && pick === "all"}
								t={cut.t}
								o={cut.o}
								titleEnd={
									periods && periods.labels.length > 0 ? (
										<PeriodPicker
											periods={periods}
											value={pick}
											onChange={(p) => {
												setPicks((was) => ({ ...was, [t.tid]: p }));
											}}
										/>
									) : undefined
								}
							/>
						);
					})
				: extra?.(view)}
		</div>
	);
};

// A phone, by the width of the window.
export const useIsPhone = () => {
	const query = "(max-width: 575.98px)";
	const [phone, setPhone] = useState(
		() => typeof window !== "undefined" && window.matchMedia(query).matches,
	);
	useEffect(() => {
		const mq = window.matchMedia(query);
		const on = () => setPhone(mq.matches);
		mq.addEventListener("change", on);
		return () => {
			mq.removeEventListener("change", on);
		};
	}, []);
	return phone;
};
