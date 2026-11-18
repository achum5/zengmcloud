import { useEffect, useState, type CSSProperties, type ReactNode } from "react";
import { helpers } from "../../../util/helpers.ts";
import { safeLocalStorage } from "../../../util/safeLocalStorage.ts";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { teamsInDisplayOrder } from "../../../util/boxScoreDisplayOrder.ts";
import { RatingsStatsPopover } from "../../../components/RatingsStatsPopover/index.tsx";
import { Headshot, Switch } from "./Gamecast.tsx";

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
		label: "MIN",
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
		label: "3PT",
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
		label: "REB",
		title: "Rebounds",
		get: (p) => p.orb + p.drb,
		total: (t) => t.orb + t.drb,
	},
	orb: {
		label: "OREB",
		title: "Offensive rebounds",
		get: (p) => p.orb,
		total: (t) => t.orb,
	},
	drb: {
		label: "DREB",
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
		label: "TO",
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
		label: "GS",
		title: "Game score",
		get: (p) => fixed(helpers.gameScore(p)),
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
		label: "OREB%",
		title: "Share of his team's misses he rebounded while on the floor",
		get: (p, t, o) =>
			fixed(div(100 * p.orb * share(t), p.min * (t.orb + o.drb))),
	},
	drbp: {
		label: "DREB%",
		title: "Share of the other team's misses he rebounded while on the floor",
		get: (p, t, o) =>
			fixed(div(100 * p.drb * share(t), p.min * (t.drb + o.orb))),
	},
};

export type BoxView = "trad" | "shoot" | "adv";
const VIEWS: [BoxView, string][] = [
	["trad", "Traditional"],
	["shoot", "Shooting"],
	["adv", "Advanced"],
];
// (Points second: the names are wide, so the numbers past the first few
// slide - the ones looked at most in view.)
const DEFAULT_COLS: Record<BoxView, string[]> = {
	trad: [
		"min",
		"pts",
		"trb",
		"ast",
		"fg",
		"tp",
		"ft",
		"stl",
		"blk",
		"tov",
		"pf",
		"pm",
	],
	shoot: ["min", "fg", "tp", "ft", "efg", "ts", "orb", "drb"],
	adv: ["min", "usg", "astp", "stlp", "blkp", "tovp", "gmsc"],
};
// A view's columns in the order they are shown: its own first, as they are
// by default, then the rest.
const orderOf = (view: BoxView) => [
	...DEFAULT_COLS[view],
	...Object.keys(COLS).filter((k) => !DEFAULT_COLS[view].includes(k)),
];

// This device's columns for a view.
const colsKey = (view: BoxView) => `bbgmBoxCols:${view}`;
const loadCols = (view: BoxView) => {
	try {
		const raw = safeLocalStorage.getItem(colsKey(view));
		if (raw) {
			const list = (JSON.parse(raw) as string[]).filter((c) => c in COLS);
			if (list.length > 0) {
				return list;
			}
		}
	} catch {
		// The defaults, then.
	}
	return DEFAULT_COLS[view];
};

// How wide the names are (this device's): the column the same width in
// both teams' tables, a name longer than it cut short.
const NAME_WIDTHS = {
	narrow: "6.6rem",
	normal: "8.8rem",
	wide: "11rem",
} as const;
type NameWidth = keyof typeof NAME_WIDTHS;
const NAME_OPTIONS: [NameWidth, string][] = [
	["narrow", "Narrow"],
	["normal", "Normal"],
	["wide", "Wide"],
];
const NAME_WIDTH_KEY = "bbgmBoxNameWidth";
const loadNameWidth = (): NameWidth => {
	const raw = safeLocalStorage.getItem(NAME_WIDTH_KEY);
	return raw && raw in NAME_WIDTHS ? (raw as NameWidth) : "normal";
};

const shortName = (name: string) => {
	const words = name.trim().split(/\s+/);
	return words.length > 1
		? `${words[0]![0]}. ${words.slice(1).join(" ")}`
		: name;
};

// His face (bare, no circle behind it), his name to his page, and BBGM's
// ratings and stats button by it.
const PlayerCell = ({ p, season }: { p: Line; season: number }) => (
	<th className="bsp-name">
		<div className="bsp-name-in">
			<a href={helpers.leagueUrl(["player", p.pid])}>
				<Headshot pid={p.pid} season={season} size={24} />
				<span className="bsp-pname">{shortName(p.name)}</span>
			</a>
			<span className="bsp-pop">
				<RatingsStatsPopover pid={p.pid} season={season} />
			</span>
		</div>
	</th>
);

const TeamTable = ({
	t,
	o,
	cols,
	season,
	numStarters,
	live,
}: {
	t: Line;
	o: Line;
	cols: string[];
	season: number;
	numStarters: number;
	live?: boolean;
}) => {
	// (A game still going: everyone who can still play is on the bench, not
	// DNP - only the hurt are out.)
	const out = (p: Line) =>
		live ? !(p.min > 0) && p.injury?.gamesRemaining > 0 : !(p.min > 0);
	const played = t.players.filter((p: Line) => !out(p));
	const dnp = t.players.filter(out);
	// (Live, the starters are the first five, as the box score lists them.)
	const starters = live
		? t.players.slice(0, numStarters).filter((p: Line) => !out(p))
		: played.filter((p: Line) => p.gs === 1).slice(0, numStarters);
	const bench = played.filter((p: Line) => !starters.includes(p));
	const heads = (first: ReactNode, className: string) => (
		<tr className={className}>
			<th className="bsp-name">{first}</th>
			{cols.map((c) => (
				<th key={c} title={COLS[c]!.title}>
					{COLS[c]!.label}
				</th>
			))}
		</tr>
	);
	const row = (p: Line) => (
		<tr key={p.pid} className={live && p.inGame ? "bsp-on" : undefined}>
			<PlayerCell p={p} season={season} />
			{cols.map((c) => (
				<td key={c}>{COLS[c]!.get(p, t, o)}</td>
			))}
		</tr>
	);
	return (
		<div className="bsp-wrap">
			<table className="bsp-table">
				<thead>
					{heads(
						<span className="bsp-team">
							<TeamLogoInline
								imgURL={t.imgURL}
								imgURLSmall={t.imgURLSmall}
								size={16}
							/>
							{(t.region || t.name || "").toUpperCase()}
						</span>,
						"bsp-head",
					)}
				</thead>
				<tbody>
					{starters.map(row)}
					{bench.length > 0 || dnp.length > 0
						? heads("BENCH", "bsp-head bsp-bench")
						: null}
					{bench.map(row)}
					{dnp.map((p: Line) => (
						<tr key={p.pid} className="bsp-dnp">
							<PlayerCell p={p} season={season} />
							<td colSpan={cols.length}>
								DNP —{" "}
								{p.injury?.gamesRemaining > 0
									? `Injury${p.injury.type ? ` (${p.injury.type})` : ""}`
									: "Coach's decision"}
							</td>
						</tr>
					))}
				</tbody>
				<tfoot>
					<tr>
						<th className="bsp-name">TOTALS</th>
						{cols.map((c) => (
							<th key={c}>{COLS[c]!.total?.(t, o) ?? null}</th>
						))}
					</tr>
					{cols.some((c) => COLS[c]!.pct) ? (
						<tr className="bsp-pct">
							<th className="bsp-name" />
							{cols.map((c) => (
								<th key={c}>{COLS[c]!.pct?.(t) ?? null}</th>
							))}
						</tr>
					) : null}
				</tfoot>
			</table>
		</div>
	);
};

const ColumnPicker = ({
	view,
	cols,
	setCols,
	nameWidth,
	setNameWidth,
}: {
	view: BoxView;
	cols: string[];
	setCols: (cols: string[]) => void;
	nameWidth: NameWidth;
	setNameWidth: (width: NameWidth) => void;
}) => {
	const save = (next: string[]) => {
		setCols(next);
		safeLocalStorage.setItem(colsKey(view), JSON.stringify(next));
	};
	const order = orderOf(view);
	return (
		<div className="bsp-picker">
			<div className="bsp-picker-names">
				<span>Names</span>
				<Switch
					options={NAME_OPTIONS}
					value={nameWidth}
					onChange={(width) => {
						setNameWidth(width);
						safeLocalStorage.setItem(NAME_WIDTH_KEY, width);
					}}
				/>
			</div>
			{order.map((key) => {
				const c = COLS[key]!;
				return (
					<label
						key={key}
						className="form-check form-check-inline bsp-pick"
						title={c.title}
					>
						<input
							className="form-check-input"
							type="checkbox"
							checked={cols.includes(key)}
							onChange={() => {
								const on = cols.includes(key);
								// (In the order they are listed here.)
								const next = order.filter((k) =>
									k === key ? !on : cols.includes(k),
								);
								if (next.length > 0) {
									save(next);
								}
							}}
						/>
						<span className="form-check-label">{c.label}</span>
					</label>
				);
			})}
			<button
				type="button"
				className="btn btn-link btn-sm p-0"
				onClick={() => {
					safeLocalStorage.removeItem(colsKey(view));
					safeLocalStorage.removeItem(NAME_WIDTH_KEY);
					setCols(DEFAULT_COLS[view]);
					setNameWidth("normal");
				}}
			>
				Reset
			</button>
		</div>
	);
};

export const BoxScorePhone = ({
	boxScore,
	extraViews,
	extra,
	live,
}: {
	boxScore: any;
	// A game still going: who is on the floor marked, as BBGM's live box
	// score marks them.
	live?: boolean;
	// More ways to look at it (rotations), and what each shows.
	extraViews?: [string, string][];
	extra?: (view: string) => ReactNode;
}) => {
	const [view, setView] = useState<string>("trad");
	const boxView = (VIEWS.some(([v]) => v === view) ? view : "trad") as BoxView;
	const [cols, setCols] = useState(() => loadCols(boxView));
	const [picking, setPicking] = useState(false);
	const [nameWidth, setNameWidth] = useState(loadNameWidth);
	useEffect(() => {
		setCols(loadCols(boxView));
		setPicking(false);
	}, [boxView]);
	const [away, home] = teamsInDisplayOrder(boxScore.teams as [any, any]);
	const numStarters = boxScore.numPlayersOnCourt ?? 5;
	const custom = VIEWS.some(([v]) => v === view);
	return (
		<div
			className="bsp"
			style={{ "--bsp-name-w": NAME_WIDTHS[nameWidth] } as CSSProperties}
		>
			<div className="bsp-views">
				<Switch
					options={[...VIEWS, ...(extraViews ?? [])]}
					value={view}
					onChange={setView}
				/>
				{custom ? (
					<button
						type="button"
						className={`btn btn-sm ${picking ? "btn-secondary" : "btn-light-bordered"}`}
						onClick={() => setPicking(!picking)}
						title="Columns"
					>
						Columns
					</button>
				) : null}
			</div>
			{picking && custom ? (
				<ColumnPicker
					view={boxView}
					cols={cols}
					setCols={setCols}
					nameWidth={nameWidth}
					setNameWidth={setNameWidth}
				/>
			) : null}
			{custom
				? [away, home].map((t, i) => (
						<TeamTable
							key={t.tid}
							t={t}
							o={i === 0 ? home : away}
							cols={cols}
							season={boxScore.season}
							numStarters={numStarters}
							live={live && !boxScore.gameOver}
						/>
					))
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
