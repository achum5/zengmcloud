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
import { MAX_LINEUPS, MIN_LINEUP_SEC } from "../../common/lineups.ts";
import clsx from "clsx";
import { LineupFaces } from "./LineupFaces.tsx";
import { GAME_S, HOME_EDGE, winProb } from "../views/LiveGame/winProb.ts";
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

// The game as it went: up top the score margin, the lead stepping as each
// basket goes in; under it each side's chance of winning from there (see
// LiveGame/winProb.ts) - which moves with the clock as well as the score.
const LeadChart = ({
	charts,
	order,
	abbrevs,
	colors,
	numPeriods,
	expected,
}: {
	charts: BoxScoreFlow;
	order: [Side, Side];
	abbrevs: [string, string];
	colors: [string, string];
	numPeriods: number;
	// How many points team 0 was expected to win by, before the tip.
	expected: number;
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

	const H1 = 150;
	const GAP = 22;
	const H2 = 96;
	const padL = 34;
	const padR = 8;
	const padT = 8;
	const padB = 20;
	const H = padT + H1 + GAP + H2 + padB;
	const top2 = padT + H1 + GAP;
	const plotW = Math.max(1, width - padL - padR);
	const x = (t: number) => padL + (t / total) * plotW;
	const y = (lead: number) => padT + ((range - lead) / (2 * range)) * H1;
	const y0 = y(0);
	// (The team listed first's chance, 1 at the top.)
	const yW = (p: number) => top2 + (1 - p) * H2;
	const yW0 = yW(0.5);

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

	// Regulation, and how much of it (or of an overtime, a sliver of one
	// game's worth) is still to play at t.
	const reg = periodEnds[numPeriods - 1] ?? total;
	const left = (t: number) => {
		if (t < reg) {
			return (reg - t) / reg;
		}
		const end = periodEnds.find((e) => e > t) ?? total;
		return (end - t) / reg;
	};
	const chance = (t: number, s: [number, number, number]) => {
		const p0 = winProb({
			margin: s[1] - s[2],
			left: left(t),
			spread: expected,
			length: reg / GAME_S,
		});
		return top === 0 ? p0 : 1 - p0;
	};
	// Every few seconds, and either side of every score.
	const STEP = Math.max(2, total / 600);
	const samples: [number, number][] = [];
	let k = 0;
	for (let t = 0; t <= total; t += STEP) {
		while (k + 1 < scores.length && scores[k + 1]![0] <= t) {
			k += 1;
			const s = scores[k]!;
			if (s[0] > 0) {
				samples.push([s[0], chance(s[0], scores[k - 1]!)]);
			}
			samples.push([s[0], chance(s[0], s)]);
		}
		samples.push([t, chance(t, scores[k]!)]);
	}
	samples.push([total, chance(total, scores.at(-1)!)]);
	const wLine = samples
		.map(([t, p], i) => `${i === 0 ? "M" : "L"}${x(t)},${yW(p)}`)
		.join(" ");
	const wArea = `${wLine} L${x(total)},${yW0} L${x(0)},${yW0} Z`;

	const onMove = (event: PointerEvent<SVGSVGElement>) => {
		const rect = event.currentTarget.getBoundingClientRect();
		const px = event.clientX - rect.left;
		const t = ((px - padL) / plotW) * total;
		setHover(Math.min(total, Math.max(0, t)));
	};

	const hovered = hover === undefined ? undefined : at(hover);
	const ticks = [range, range / 2, 0, -range / 2, -range];
	const pct = (p: number) => `${Math.round(100 * p)}%`;

	const panels: {
		key: string;
		d: string;
		fill: string;
		mid: number;
		top: number;
		h: number;
	}[] = [
		{ key: "lead", d: line, fill: area, mid: y0, top: padT, h: H1 },
		{ key: "win", d: wLine, fill: wArea, mid: yW0, top: top2, h: H2 },
	];

	return (
		<div ref={ref} className="position-relative mb-3">
			{width > 0 ? (
				<svg
					width={width}
					height={H}
					role="img"
					aria-label={`Lead and win probability over the game: ${abbrevs[top]} up top, ${abbrevs[bottom]} below`}
					onPointerMove={onMove}
					onPointerDown={onMove}
					onPointerLeave={() => {
						setHover(undefined);
					}}
					style={{ touchAction: "pan-y", display: "block" }}
				>
					<defs>
						{panels.map((panel) => (
							<g key={panel.key}>
								<clipPath id={`${clipId}-${panel.key}-up`}>
									<rect
										x={0}
										y={panel.top}
										width={width}
										height={panel.mid - panel.top}
									/>
								</clipPath>
								<clipPath id={`${clipId}-${panel.key}-down`}>
									<rect
										x={0}
										y={panel.mid}
										width={width}
										height={panel.top + panel.h - panel.mid}
									/>
								</clipPath>
							</g>
						))}
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
					{[1, 0.75, 0.25, 0].map((p) => (
						<g key={p}>
							<line
								x1={padL}
								x2={padL + plotW}
								y1={yW(p)}
								y2={yW(p)}
								stroke="var(--bs-border-color)"
								strokeWidth={0.5}
							/>
							<text
								x={padL - 6}
								y={yW(p)}
								dy="0.32em"
								textAnchor="end"
								fontSize={11}
								fill="var(--bs-secondary-color)"
								style={{ fontVariantNumeric: "tabular-nums" }}
							>
								{pct(p >= 0.5 ? p : 1 - p)}
							</text>
						</g>
					))}
					{periodEnds
						.slice(0, -1)
						.map((end) =>
							panels.map((panel) => (
								<line
									key={`${end}-${panel.key}`}
									x1={x(end)}
									x2={x(end)}
									y1={panel.top}
									y2={panel.top + panel.h}
									stroke="var(--bs-border-color)"
									strokeWidth={1}
								/>
							)),
						)}
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
					{panels.map((panel) =>
						[top, bottom].map((side, i) => (
							<g
								key={`${panel.key}-${side}`}
								clipPath={`url(#${clipId}-${panel.key}-${i === 0 ? "up" : "down"})`}
							>
								<path d={panel.fill} fill={colors[side]} fillOpacity={0.25} />
								<path
									d={panel.d}
									fill="none"
									stroke={colors[side]}
									strokeWidth={2}
									strokeLinejoin="round"
								/>
							</g>
						)),
					)}
					{panels.map((panel) => (
						<line
							key={panel.key}
							x1={padL}
							x2={padL + plotW}
							y1={panel.mid}
							y2={panel.mid}
							stroke="var(--bs-secondary-color)"
							strokeWidth={1}
						/>
					))}
					<text
						x={padL}
						y={top2 - 7}
						fontSize={11}
						fontWeight={600}
						fill="var(--bs-secondary-color)"
					>
						WIN %
					</text>
					{hover === undefined
						? null
						: panels.map((panel) => (
								<line
									key={panel.key}
									x1={x(hover)}
									x2={x(hover)}
									y1={panel.top}
									y2={panel.top + panel.h}
									stroke="var(--bs-body-color)"
									strokeWidth={1}
								/>
							))}
					{hover === undefined || !hovered ? null : (
						<>
							<circle
								cx={x(hover)}
								cy={y(leadOf(hovered))}
								r={3.5}
								fill={colors[leadOf(hovered) >= 0 ? top : bottom]}
								stroke="var(--bs-body-bg)"
								strokeWidth={1.5}
							/>
							<circle
								cx={x(hover)}
								cy={yW(chance(hover, hovered))}
								r={3.5}
								fill={colors[chance(hover, hovered) >= 0.5 ? top : bottom]}
								stroke="var(--bs-body-bg)"
								strokeWidth={1.5}
							/>
						</>
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
				style={{ left: padL + 6, top: padT + H1 - 20 }}
			>
				<Swatch color={colors[bottom]} /> {abbrevs[bottom]}
			</div>
			{hover !== undefined && hovered ? (
				<HoverCard
					left={Math.min(Math.max(0, x(hover) + 10), Math.max(0, width - 170))}
					top={padT}
					clock={clockAt(hover, periodEnds, numPeriods)}
					abbrevs={abbrevs}
					colors={colors}
					order={order}
					score={hovered}
					chance={chance(hover, hovered)}
				/>
			) : null}
		</div>
	);
};

// A chance to win, as a percent - never 0 or 100 until it is over.
const winPct = (p: number) =>
	p >= 1
		? "100%"
		: p <= 0
			? "0%"
			: p > 0.995
				? ">99%"
				: p < 0.005
					? "<1%"
					: `${Math.round(100 * p)}%`;

// The moment under the pointer: the clock, the score, who leads by how much,
// and each side's chance of winning from there.
const HoverCard = ({
	left,
	top,
	clock,
	abbrevs,
	colors,
	order,
	score,
	chance,
}: {
	left: number;
	top: number;
	clock: string;
	abbrevs: [string, string];
	colors: [string, string];
	order: [Side, Side];
	score: [number, number, number];
	chance: number;
}) => {
	const [a, b] = order;
	const lead = score[1 + a]! - score[1 + b]!;
	return (
		<div
			className="position-absolute bg-body border rounded-2 px-2 py-1 small shadow"
			style={{
				left,
				top,
				minWidth: 150,
				pointerEvents: "none",
				whiteSpace: "nowrap",
				fontVariantNumeric: "tabular-nums",
			}}
		>
			<div className="text-body-secondary mb-1">{clock}</div>
			{([a, b] as const).map((side, i) => (
				<div key={side} className="d-flex align-items-center gap-2">
					<Swatch color={colors[side]} />
					<span className="fw-semibold">{abbrevs[side]}</span>
					<span className="ms-auto">{score[1 + side]}</span>
					<span
						className="text-body-secondary"
						style={{ width: 36, textAlign: "right" }}
					>
						{winPct(i === 0 ? chance : 1 - chance)}
					</span>
				</div>
			))}
			<div className="text-body-secondary mt-1">
				{lead === 0
					? "Tied"
					: `${abbrevs[lead > 0 ? a : b]} by ${Math.abs(lead)}`}
			</div>
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

// Each five: its faces, minutes and possessions, and how it scored and
// defended per 100 possessions.
const LineupTable = ({
	t,
	lines,
	pics,
}: {
	t: any;
	lines: LineupLine[];
	pics: BoxScoreFlow["pics"];
}) => {
	const shown = lines
		.filter((line) => line.sec >= MIN_LINEUP_SEC)
		.slice(0, MAX_LINEUPS);
	if (shown.length === 0) {
		return null;
	}
	return (
		<div className="table-responsive">
			<table className="table table-striped table-borderless table-sm mb-0 align-middle">
				<thead>
					<tr>
						<th>{t.region ?? t.abbrev}</th>
						<th className="text-end">MIN</th>
						<th className="text-end">POSS</th>
						<th className="text-end" title="Points scored per 100 possessions">
							ORTG
						</th>
						<th className="text-end" title="Points allowed per 100 possessions">
							DRTG
						</th>
						<th className="text-end">NRTG</th>
					</tr>
				</thead>
				<tbody style={{ fontVariantNumeric: "tabular-nums" }}>
					{shown.map((line) => {
						const nrtg = net(line);
						return (
							<tr key={line.pids.join(",")}>
								<td>
									<LineupFaces
										pids={line.pids}
										pics={pics}
										colors={t.colors}
										jersey={t.jersey}
									/>
								</td>
								<td className="text-end">{formatMin(line.sec)}</td>
								<td className="text-end">{line.poss}</td>
								<td className="text-end">{fmt(rating(line.pts, line.poss))}</td>
								<td className="text-end">
									{fmt(rating(line.ptsOpp, line.possOpp))}
								</td>
								<td
									className={clsx(
										"text-end fw-bold",
										nrtg === undefined
											? undefined
											: nrtg > 0
												? "text-success"
												: nrtg < 0
													? "text-danger"
													: undefined,
									)}
								>
									{fmt(nrtg, true)}
								</td>
							</tr>
						);
					})}
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
					expected={
						typeof boxScore.spread === "number" &&
						Number.isFinite(boxScore.spread)
							? boxScore.spread
							: boxScore.neutralSite
								? 0
								: (HOME_EDGE *
										(charts.periodEnds[numPeriods - 1] ?? charts.total)) /
									GAME_S
					}
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
					<div className="row g-3">
						{order.map((side) => (
							<div key={side} className="col-xl-6">
								<LineupTable
									t={teams[side]}
									lines={lineups[side]}
									pics={charts.pics}
								/>
							</div>
						))}
					</div>
				</Section>
			) : null}
		</>
	);
};
