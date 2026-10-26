import {
	useEffect,
	useId,
	useRef,
	useState,
	type PointerEvent,
	type ReactNode,
} from "react";
import type {
	BoxScoreFlow,
	LineupLine,
	OnCourt,
	Stint,
} from "../../worker/util/gameTimeline.ts";
import { formatClock } from "../../common/formatClock.ts";
import { helpers } from "../util/helpers.ts";
import { teamsInDisplayOrder } from "../util/boxScoreDisplayOrder.ts";

// THE GAME OVER TIME, UNDER ITS BOX SCORE: the lead as it went, who was on the
// floor when, and how each five did. Drawn from what the sim logged as it
// played (see worker/util/gameTimeline.ts); games from before that show none
// of it.

type Side = 0 | 1;

const periodLabel = (k: number, numPeriods: number) => {
	if (k >= numPeriods) {
		const ot = k - numPeriods + 1;
		return ot === 1 ? "OT" : `${ot}OT`;
	}
	if (numPeriods === 4) {
		return `Q${k + 1}`;
	}
	if (numPeriods === 2) {
		return `H${k + 1}`;
	}
	return `P${k + 1}`;
};

const periodOf = (t: number, periodEnds: number[]) => {
	const k = periodEnds.findIndex((end) => t < end);
	return k < 0 ? periodEnds.length - 1 : k;
};

// "Q3 5:12": the game clock at a moment of game played.
const clockAt = (t: number, periodEnds: number[], numPeriods: number) => {
	const k = periodOf(t, periodEnds);
	return `${periodLabel(k, numPeriods)} ${formatClock(Math.max(0, periodEnds[k]! - t))}`;
};

const formatMin = (sec: number) => {
	const s = Math.round(sec);
	return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
};

const shortName = (name: string | undefined) => {
	if (!name) {
		return "";
	}
	const i = name.indexOf(" ");
	return i > 0 ? `${name[0]}. ${name.slice(i + 1)}` : name;
};

// Points per 100 possessions.
const rating = (pts: number, poss: number) =>
	poss > 0 ? (100 * pts) / poss : undefined;

const net = (x: OnCourt) => {
	const o = rating(x.pts, x.poss);
	const d = rating(x.ptsOpp, x.possOpp);
	return o === undefined || d === undefined ? undefined : o - d;
};

const fmt = (x: number | undefined, signed = false) =>
	x === undefined ? "—" : signed ? helpers.plusMinus(x, 1) : x.toFixed(1);

// Team colors for the two sides: each team's main color, unless it would be
// lost on the page (near white) or be taken for the other team's.
const hexRgb = (hex: string | undefined) => {
	const m = /^#?([\da-f]{6})$/i.exec(hex ?? "");
	if (!m) {
		return undefined;
	}
	const n = Number.parseInt(m[1]!, 16);
	return [(n >> 16) & 255, (n >> 8) & 255, n & 255] as const;
};
const lightness = (rgb: readonly number[]) =>
	(0.2126 * rgb[0]! + 0.7152 * rgb[1]! + 0.0722 * rgb[2]!) / 255;
const distance = (a: readonly number[], b: readonly number[]) =>
	Math.hypot(a[0]! - b[0]!, a[1]! - b[1]!, a[2]! - b[2]!);
const FALLBACK: [string, string] = ["#2c6fbb", "#d9662b"];

const sideColors = (teams: any[]): [string, string] => {
	const pick = (side: Side, avoid?: readonly number[]) => {
		const colors: string[] = teams[side]?.colors ?? [];
		for (const c of colors) {
			const rgb = hexRgb(c);
			if (
				rgb &&
				lightness(rgb) < 0.85 &&
				(!avoid || distance(rgb, avoid) > 90)
			) {
				return c;
			}
		}
		return undefined;
	};
	const zero = pick(0) ?? FALLBACK[0];
	const one = pick(1, hexRgb(zero)) ?? FALLBACK[1];
	return [zero, one];
};

const useWidth = () => {
	const ref = useRef<HTMLDivElement>(null);
	const [width, setWidth] = useState(0);
	useEffect(() => {
		const el = ref.current;
		if (!el) {
			return;
		}
		const update = () => {
			setWidth(el.clientWidth);
		};
		update();
		const observer = new ResizeObserver(update);
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, []);
	return { ref, width };
};

const LeadChart = ({
	charts,
	order,
	abbrevs,
	colors,
	numPeriods,
}: {
	charts: BoxScoreFlow;
	order: [Side, Side];
	abbrevs: [string, string];
	colors: [string, string];
	numPeriods: number;
}) => {
	const { ref, width } = useWidth();
	const clipId = useId();
	const [hover, setHover] = useState<number | undefined>();

	const { scores, total, periodEnds } = charts;
	const [top, bottom] = order;
	// Up is the team listed first ahead.
	const leadOf = (s: [number, number, number]) => s[1 + top]! - s[1 + bottom]!;
	const maxLead = Math.max(...scores.map((s) => Math.abs(leadOf(s))));
	// A multiple of 4, so the half-way ticks are whole points too.
	const range = Math.max(4, Math.ceil(maxLead / 4) * 4);

	const H = 180;
	const padL = 30;
	const padR = 8;
	const padT = 8;
	const padB = 20;
	const plotW = Math.max(1, width - padL - padR);
	const plotH = H - padT - padB;
	const x = (t: number) => padL + (t / total) * plotW;
	const y = (lead: number) => padT + ((range - lead) / (2 * range)) * plotH;
	const y0 = y(0);

	// The lead holds until the next score, then steps.
	let line = `M${x(0)},${y0}`;
	let prev = 0;
	for (const s of scores) {
		const lead = leadOf(s);
		line += ` L${x(s[0])},${y(prev)} L${x(s[0])},${y(lead)}`;
		prev = lead;
	}
	line += ` L${x(total)},${y(prev)}`;
	const area = `${line} L${x(total)},${y0} Z`;

	const at = (t: number) => {
		let i = 0;
		while (i + 1 < scores.length && scores[i + 1]![0] <= t) {
			i += 1;
		}
		return scores[i]!;
	};

	const onMove = (event: PointerEvent<SVGSVGElement>) => {
		const rect = event.currentTarget.getBoundingClientRect();
		const px = event.clientX - rect.left;
		const t = ((px - padL) / plotW) * total;
		setHover(Math.min(total, Math.max(0, t)));
	};

	const hovered = hover === undefined ? undefined : at(hover);
	const ticks = [range, range / 2, 0, -range / 2, -range];

	return (
		<div ref={ref} className="position-relative mb-3">
			{width > 0 ? (
				<svg
					width={width}
					height={H}
					role="img"
					aria-label={`Lead over the game: ${abbrevs[top]} up top, ${abbrevs[bottom]} below`}
					onPointerMove={onMove}
					onPointerLeave={() => {
						setHover(undefined);
					}}
					style={{ touchAction: "pan-y", display: "block" }}
				>
					<defs>
						<clipPath id={`${clipId}-up`}>
							<rect x={0} y={0} width={width} height={y0} />
						</clipPath>
						<clipPath id={`${clipId}-down`}>
							<rect x={0} y={y0} width={width} height={H - y0} />
						</clipPath>
					</defs>
					{ticks.map((tick) => (
						<g key={tick}>
							{tick === 0 ? null : (
								<line
									x1={padL}
									x2={padL + plotW}
									y1={y(tick)}
									y2={y(tick)}
									stroke="var(--bs-border-color)"
									strokeWidth={0.5}
								/>
							)}
							<text
								x={padL - 6}
								y={y(tick)}
								dy="0.32em"
								textAnchor="end"
								fontSize={11}
								fill="var(--bs-secondary-color)"
								style={{ fontVariantNumeric: "tabular-nums" }}
							>
								{Math.abs(tick)}
							</text>
						</g>
					))}
					{periodEnds.slice(0, -1).map((end) => (
						<line
							key={end}
							x1={x(end)}
							x2={x(end)}
							y1={padT}
							y2={padT + plotH}
							stroke="var(--bs-border-color)"
							strokeWidth={1}
						/>
					))}
					{periodEnds.map((end, k) => (
						<text
							key={end}
							x={x(((k > 0 ? periodEnds[k - 1]! : 0) + end) / 2)}
							y={H - 5}
							textAnchor="middle"
							fontSize={11}
							fill="var(--bs-secondary-color)"
						>
							{periodLabel(k, numPeriods)}
						</text>
					))}
					{[top, bottom].map((side, i) => (
						<g
							key={side}
							clipPath={`url(#${clipId}-${i === 0 ? "up" : "down"})`}
						>
							<path d={area} fill={colors[side]} fillOpacity={0.25} />
							<path
								d={line}
								fill="none"
								stroke={colors[side]}
								strokeWidth={2}
								strokeLinejoin="round"
							/>
						</g>
					))}
					<line
						x1={padL}
						x2={padL + plotW}
						y1={y0}
						y2={y0}
						stroke="var(--bs-secondary-color)"
						strokeWidth={1}
					/>
					{hover === undefined ? null : (
						<line
							x1={x(hover)}
							x2={x(hover)}
							y1={padT}
							y2={padT + plotH}
							stroke="var(--bs-body-color)"
							strokeWidth={1}
						/>
					)}
				</svg>
			) : (
				<div style={{ height: H }} />
			)}
			<div
				className="position-absolute small text-body-secondary"
				style={{ left: padL + 6, top: padT }}
			>
				<Swatch color={colors[top]} /> {abbrevs[top]}
			</div>
			<div
				className="position-absolute small text-body-secondary"
				style={{ left: padL + 6, top: padT + plotH - 20 }}
			>
				<Swatch color={colors[bottom]} /> {abbrevs[bottom]}
			</div>
			{hover !== undefined && hovered ? (
				<div
					className="position-absolute bg-body border rounded px-2 py-1 small shadow-sm"
					style={{
						left: Math.min(Math.max(0, x(hover) + 8), Math.max(0, width - 150)),
						top: padT,
						pointerEvents: "none",
						whiteSpace: "nowrap",
						fontVariantNumeric: "tabular-nums",
					}}
				>
					<div className="text-body-secondary">
						{clockAt(hover, periodEnds, numPeriods)}
					</div>
					<div>
						{abbrevs[top]} {hovered[1 + top]}, {abbrevs[bottom]}{" "}
						{hovered[1 + bottom]}
					</div>
				</div>
			) : null}
		</div>
	);
};

const Swatch = ({ color }: { color: string }) => (
	<span
		className="d-inline-block rounded-1 align-middle"
		style={{ width: 10, height: 10, backgroundColor: color }}
	/>
);

const PeriodScale = ({
	total,
	periodEnds,
	numPeriods,
}: {
	total: number;
	periodEnds: number[];
	numPeriods: number;
}) => (
	<svg width="100%" height={14} style={{ display: "block" }}>
		{periodEnds.map((end, k) => (
			<text
				key={end}
				x={`${(100 * ((k > 0 ? periodEnds[k - 1]! : 0) + end)) / 2 / total}%`}
				y={11}
				textAnchor="middle"
				fontSize={11}
				fill="var(--bs-secondary-color)"
			>
				{periodLabel(k, numPeriods)}
			</text>
		))}
	</svg>
);

const StintBar = ({
	stint,
	total,
	periodEnds,
	numPeriods,
	color,
}: {
	stint: Stint | undefined;
	total: number;
	periodEnds: number[];
	numPeriods: number;
	color: string;
}) => (
	<svg width="100%" height={14} style={{ display: "block" }}>
		<rect x={0} y={6} width="100%" height={2} fill="var(--bs-border-color)" />
		{periodEnds.slice(0, -1).map((end) => (
			<line
				key={end}
				x1={`${(100 * end) / total}%`}
				x2={`${(100 * end) / total}%`}
				y1={0}
				y2={14}
				stroke="var(--bs-border-color)"
				strokeWidth={1}
			/>
		))}
		{stint?.spans.map(([a, b]) => (
			<rect
				key={a}
				x={`${(100 * a) / total}%`}
				y={2}
				width={`${(100 * (b - a)) / total}%`}
				height={10}
				rx={2}
				fill={color}
			>
				<title>
					{clockAt(a, periodEnds, numPeriods)} –{" "}
					{clockAt(Math.max(a, b - 0.001), periodEnds, numPeriods)}
				</title>
			</rect>
		))}
	</svg>
);

const RotationTable = ({
	t,
	side,
	charts,
	color,
	numPeriods,
}: {
	t: any;
	side: Side;
	charts: BoxScoreFlow;
	color: string;
	numPeriods: number;
}) => {
	const stints = new Map(charts.rotation[side].map((s) => [s.pid, s]));
	const players = (t.players as any[]).filter((p) => stints.has(p.pid));
	const onCourt = charts.onCourt?.[side];
	const offOf = (on: OnCourt | undefined): OnCourt | undefined => {
		if (!onCourt || !on) {
			return undefined;
		}
		const team = onCourt.team;
		return {
			sec: team.sec - on.sec,
			poss: team.poss - on.poss,
			possOpp: team.possOpp - on.possOpp,
			pts: team.pts - on.pts,
			ptsOpp: team.ptsOpp - on.ptsOpp,
		};
	};
	// Too little of the game without him to say anything about it.
	const MIN_OFF_POSS = 10;

	return (
		<div className="table-responsive mb-3">
			<table className="table table-sm table-borderless mb-0 align-middle">
				<thead>
					<tr>
						<th>{t.abbrev}</th>
						<th style={{ width: "100%", minWidth: 140 }}>
							<PeriodScale
								total={charts.total}
								periodEnds={charts.periodEnds}
								numPeriods={numPeriods}
							/>
						</th>
						<th className="text-end">MIN</th>
						{onCourt ? (
							<>
								<th
									className="text-end"
									title="Net rating with him on the floor"
								>
									ON
								</th>
								<th
									className="text-end"
									title="Net rating with him off the floor"
								>
									OFF
								</th>
							</>
						) : null}
					</tr>
				</thead>
				<tbody style={{ fontVariantNumeric: "tabular-nums" }}>
					{players.map((p) => {
						const stint = stints.get(p.pid);
						const on = onCourt?.byPid[p.pid];
						const off = offOf(on);
						const sec = (stint?.spans ?? []).reduce(
							(sum, [a, b]) => sum + b - a,
							0,
						);
						return (
							<tr key={p.pid}>
								<td className="text-nowrap">{shortName(p.name)}</td>
								<td>
									<StintBar
										stint={stint}
										total={charts.total}
										periodEnds={charts.periodEnds}
										numPeriods={numPeriods}
										color={color}
									/>
								</td>
								<td className="text-end">{formatMin(sec)}</td>
								{onCourt ? (
									<>
										<td className="text-end">
											{fmt(on ? net(on) : undefined, true)}
										</td>
										<td className="text-end">
											{fmt(
												off && off.poss + off.possOpp >= 2 * MIN_OFF_POSS
													? net(off)
													: undefined,
												true,
											)}
										</td>
									</>
								) : null}
							</tr>
						);
					})}
				</tbody>
			</table>
		</div>
	);
};

// Less than this together and a five's ratings are noise.
const MIN_LINEUP_SEC = 120;

const LineupTable = ({ t, lines }: { t: any; lines: LineupLine[] }) => {
	const names = new Map<number, string>(
		(t.players as any[]).map((p) => [p.pid, shortName(p.name)]),
	);
	// Fives that played a real stretch, most minutes first.
	const shown = lines.filter((line) => line.sec >= MIN_LINEUP_SEC).slice(0, 5);
	if (shown.length === 0) {
		return null;
	}
	return (
		<div className="table-responsive mb-3">
			<table className="table table-striped table-borderless table-sm table-hover mb-0">
				<thead>
					<tr>
						<th>{t.abbrev}</th>
						<th className="text-end">MIN</th>
						<th className="text-end">+/-</th>
						<th className="text-end" title="Points scored per 100 possessions">
							ORtg
						</th>
						<th className="text-end" title="Points allowed per 100 possessions">
							DRtg
						</th>
						<th className="text-end">NRtg</th>
					</tr>
				</thead>
				<tbody style={{ fontVariantNumeric: "tabular-nums" }}>
					{shown.map((line) => (
						<tr key={line.pids.join(",")}>
							<td className="text-nowrap">
								{line.pids.map((pid) => names.get(pid) ?? "?").join(", ")}
							</td>
							<td className="text-end">{formatMin(line.sec)}</td>
							<td className="text-end">
								{helpers.plusMinus(line.pts - line.ptsOpp, 0)}
							</td>
							<td className="text-end">{fmt(rating(line.pts, line.poss))}</td>
							<td className="text-end">
								{fmt(rating(line.ptsOpp, line.possOpp))}
							</td>
							<td className="text-end">{fmt(net(line), true)}</td>
						</tr>
					))}
				</tbody>
			</table>
		</div>
	);
};

const Section = ({
	title,
	children,
}: {
	title: string;
	children: ReactNode;
}) => (
	<div className="mb-3">
		<h2>{title}</h2>
		{children}
	</div>
);

export const GameCharts = ({ boxScore }: { boxScore: any }) => {
	const charts: BoxScoreFlow | undefined = boxScore.charts;
	if (!charts) {
		return null;
	}
	const order = teamsInDisplayOrder<Side>([0, 1]);
	const teams = boxScore.teams as [any, any];
	const abbrevs: [string, string] = [teams[0].abbrev, teams[1].abbrev];
	const colors = sideColors(teams);
	const numPeriods = boxScore.numPeriods ?? 4;
	const lineups = charts.lineups;

	return (
		<>
			<Section title="Game Flow">
				<LeadChart
					charts={charts}
					order={order}
					abbrevs={abbrevs}
					colors={colors}
					numPeriods={numPeriods}
				/>
			</Section>
			<Section title="Rotations">
				{order.map((side) => (
					<RotationTable
						key={side}
						t={teams[side]}
						side={side}
						charts={charts}
						color={colors[side]}
						numPeriods={numPeriods}
					/>
				))}
			</Section>
			{lineups &&
			lineups.some((lines) => lines.some((l) => l.sec >= MIN_LINEUP_SEC)) ? (
				<Section title="Lineups">
					{order.map((side) => (
						<LineupTable key={side} t={teams[side]} lines={lineups[side]} />
					))}
				</Section>
			) : null}
		</>
	);
};
