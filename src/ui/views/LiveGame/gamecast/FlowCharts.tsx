// HOW THE GAME WENT, two ways, as a broadcast's app shows it: the chance
// each side had of winning at every moment (the line above the middle the
// visitors', below it the home team's), and the two scores climbing side
// by side. Touch or point at it for where it stood then - tapped or
// clicked, it stays there.

import clsx from "clsx";
import { useEffect, useId, useRef, useState, type PointerEvent } from "react";
import { helpers } from "../../../util/helpers.ts";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { liveLead } from "../../../components/GameCharts.basketball.tsx";
import { GAME_S, HOME_EDGE, winProb } from "../winProb.ts";

type Flow = {
	scores: [number, number, number][];
	periodEnds: number[];
	total: number;
	// How far the game has got (live: so far; finished: all of it).
	upTo: number;
};

export const flowOf = (boxScore: any, live: boolean): Flow | undefined => {
	const numPeriods = boxScore.numPeriods ?? 4;
	if (live) {
		const now = liveLead(boxScore, numPeriods);
		return now ? { ...now.charts, upTo: now.upTo } : undefined;
	}
	const c = boxScore.charts;
	return c?.scores ? { ...c, upTo: c.total } : undefined;
};

const useWidth = () => {
	const ref = useRef<HTMLDivElement>(null);
	const [width, setWidth] = useState(0);
	useEffect(() => {
		const el = ref.current;
		if (!el) {
			return;
		}
		const ro = new ResizeObserver(() => setWidth(el.clientWidth));
		ro.observe(el);
		setWidth(el.clientWidth);
		return () => ro.disconnect();
	}, []);
	return { ref, width };
};

const Logo = ({ t }: { t: any }) => (
	<TeamLogoInline
		alt={t.abbrev}
		className="flex-shrink-0"
		imgURL={t.imgURL}
		imgURLSmall={t.imgURLSmall}
		includePlaceholderIfNoLogo
		size={28}
	/>
);

// (Picked this near the end of it, pixels: the end.)
const END_PX = 6;

const pctText = (p: number) =>
	`${(100 * p).toFixed(p > 0.995 || p < 0.005 ? 0 : 1)}%`;

export const FlowCharts = ({
	boxScore,
	colors,
	live,
}: {
	boxScore: any;
	// The two teams' colors, box score order (0 home, 1 away).
	colors: [string, string];
	live?: boolean;
}) => {
	const [view, setView] = useState<"win" | "flow">("win");
	// The moment under the mouse, and the one picked (tapped, clicked or
	// dragged to): kept once the finger is up or the mouse gone.
	const [hover, setHover] = useState<number | undefined>();
	const [pin, setPin] = useState<number | undefined>();
	const { ref, width } = useWidth();
	const id = useId().replace(/:/g, "");
	const flow = flowOf(boxScore, !!live);
	if (!flow || flow.total <= 0) {
		return null;
	}
	const numPeriods = boxScore.numPeriods ?? 4;
	const { scores, periodEnds, total, upTo } = flow;
	const home = boxScore.teams[0];
	const away = boxScore.teams[1];

	// The chance the home team wins at t (see GameCharts' LeadChart).
	const reg = periodEnds[numPeriods - 1] ?? total;
	const expected =
		typeof boxScore.spread === "number" && Number.isFinite(boxScore.spread)
			? boxScore.spread
			: boxScore.neutralSite
				? 0
				: (HOME_EDGE * reg) / GAME_S;
	const left = (t: number) => {
		if (t < reg) {
			return (reg - t) / reg;
		}
		const end = periodEnds.find((e) => e > t) ?? total;
		return (end - t) / reg;
	};
	const scoreAt = (t: number) => {
		let i = 0;
		while (i + 1 < scores.length && scores[i + 1]![0] <= t) {
			i += 1;
		}
		return scores[i]!;
	};
	const homeChance = (t: number) => {
		const s = scoreAt(t);
		const over = !live && t >= total;
		if (over) {
			return s[1] > s[2] ? 1 : s[1] < s[2] ? 0 : 0.5;
		}
		return winProb({
			margin: s[1] - s[2],
			left: left(t),
			spread: expected,
			length: reg / GAME_S,
		});
	};

	const H = 190;
	const PAD_T = 8;
	const PAD_B = 22;
	const W = Math.max(width, 200);
	const plotH = H - PAD_T - PAD_B;
	const x = (t: number) => (t / total) * W;
	const tOf = (px: number) => Math.max(0, Math.min(upTo, (px / W) * total));

	// The periods across the foot, and a line at each end.
	const starts = [0, ...periodEnds.slice(0, -1)];
	const periodName = (k: number) =>
		k < numPeriods
			? helpers.ordinal(k + 1)
			: k - numPeriods === 0
				? "OT"
				: `${k - numPeriods + 1}OT`;

	const sel = hover ?? pin;
	const at = sel ?? upTo;
	const sNow = scoreAt(at);
	const pHome = homeChance(at);
	const period = Math.max(
		0,
		periodEnds.findIndex((e) => at <= e),
	);
	const clock = Math.max(0, Math.round((periodEnds[period] ?? total) - at));
	const when = `${Math.floor(clock / 60)}:${String(clock % 60).padStart(2, "0")} - ${periodName(period)}`;

	// Win probability: away above the middle, home below.
	const yP = (pAway: number) => PAD_T + (1 - pAway) * plotH;
	let winPath = "";
	{
		const STEP = Math.max(2, total / 500);
		const pts: string[] = [];
		let k = 0;
		for (let t = 0; t <= upTo; t += STEP) {
			while (k + 1 < scores.length && scores[k + 1]![0] <= t) {
				k += 1;
				const tt = scores[k]![0];
				pts.push(
					`${x(tt)},${yP(1 - homeChance(tt - 0.001))}`,
					`${x(tt)},${yP(1 - homeChance(tt))}`,
				);
			}
			pts.push(`${x(t)},${yP(1 - homeChance(t))}`);
		}
		pts.push(`${x(upTo)},${yP(1 - homeChance(upTo))}`);
		winPath = `M${pts.join(" L")}`;
	}
	const mid = yP(0.5);

	// Game flow: each side's score as it climbs.
	const top = Math.max(
		25,
		Math.ceil(
			Math.max(sNow[1], sNow[2], ...scores.map((s) => Math.max(s[1], s[2]))) /
				25,
		) * 25,
	);
	const yS = (pts: number) => PAD_T + (1 - pts / top) * plotH;
	const stepPath = (side: 1 | 2) => {
		let d = `M0,${yS(0)}`;
		let last = 0;
		for (const s of scores) {
			if (s[0] > upTo) {
				break;
			}
			d += ` L${x(s[0])},${yS(last)} L${x(s[0])},${yS(s[side])}`;
			last = s[side];
		}
		return `${d} L${x(upTo)},${yS(last)}`;
	};
	let changes = 0;
	let leader: 0 | 1 | undefined;
	for (const s of scores) {
		const now = s[1] > s[2] ? 0 : s[1] < s[2] ? 1 : undefined;
		if (now !== undefined) {
			if (leader !== undefined && leader !== now) {
				changes += 1;
			}
			leader = now;
		}
	}

	const pxAt = (e: PointerEvent<SVGSVGElement>) =>
		e.clientX - e.currentTarget.getBoundingClientRect().left;
	// (Picked at the end - or past it, live - back to now.)
	const pick = (px: number) => {
		setPin(px >= x(upTo) - END_PX ? undefined : tOf(px));
		setHover(undefined);
	};
	const onMove = (e: PointerEvent<SVGSVGElement>) => {
		if (e.pointerType === "mouse" && e.buttons === 0) {
			setHover(tOf(pxAt(e)));
		} else {
			pick(pxAt(e));
		}
	};

	return (
		<div className="flow-charts">
			<div className="btn-group btn-group-sm w-100 mb-3" role="group">
				{(
					[
						["win", "Win Probability"],
						["flow", "Game Flow"],
					] as const
				).map(([key, label]) => (
					<button
						key={key}
						type="button"
						className={`btn ${view === key ? "btn-secondary" : "btn-light-bordered"}`}
						onClick={() => setView(key)}
					>
						{label}
					</button>
				))}
			</div>
			{view === "win" ? (
				<div className="flow-team">
					<Logo t={away} />
					<span className="fw-bold">{away.abbrev}</span>
					<span className="flow-key" style={{ background: colors[1] }} />
					<span className="ms-auto flow-big">{pctText(1 - pHome)}</span>
				</div>
			) : (
				<div className="flow-team">
					<Logo t={away} />
					<span className="flow-big">{sNow[2]}</span>
					<span className="fw-bold">{away.abbrev}</span>
					<span className="flow-key" style={{ background: colors[1] }} />
					<span className="mx-auto text-center lh-sm">
						<span className="fw-bold">{changes}</span>
						<br />
						<span className="text-body-secondary small">
							{helpers.plural("Lead Change", changes)}
						</span>
					</span>
					<span className="flow-key" style={{ background: colors[0] }} />
					<span className="fw-bold">{home.abbrev}</span>
					<span className="flow-big">{sNow[1]}</span>
					<Logo t={home} />
				</div>
			)}
			<div ref={ref} className="flow-plot">
				{width > 0 ? (
					<svg
						width={W}
						height={H}
						onPointerMove={onMove}
						onPointerDown={(e) => pick(pxAt(e))}
						onPointerLeave={() => setHover(undefined)}
						style={{ touchAction: "pan-y", display: "block" }}
					>
						{/* Periods. */}
						{starts.map((t0, k) => (
							<g key={k}>
								{k > 0 ? (
									<line
										x1={x(t0)}
										x2={x(t0)}
										y1={PAD_T}
										y2={PAD_T + plotH}
										className="flow-grid"
									/>
								) : null}
								<text
									x={(x(t0) + x(periodEnds[k] ?? total)) / 2}
									y={H - 6}
									textAnchor="middle"
									className="flow-label"
								>
									{periodName(k)}
								</text>
							</g>
						))}
						{view === "win" ? (
							<>
								<defs>
									<clipPath id={`${id}-above`}>
										<rect x={0} y={0} width={W} height={mid} />
									</clipPath>
									<clipPath id={`${id}-below`}>
										<rect x={0} y={mid} width={W} height={H} />
									</clipPath>
								</defs>
								{[0, 0.25, 0.75, 1].map((p) => (
									<line
										key={p}
										x1={0}
										x2={W}
										y1={yP(p)}
										y2={yP(p)}
										className="flow-grid"
									/>
								))}
								<line x1={0} x2={W} y1={mid} y2={mid} className="flow-mid" />
								<path
									d={`${winPath} L${x(upTo)},${mid} L0,${mid} Z`}
									className="flow-fill"
								/>
								<path
									d={winPath}
									fill="none"
									stroke={colors[1]}
									strokeWidth={2}
									clipPath={`url(#${id}-above)`}
								/>
								<path
									d={winPath}
									fill="none"
									stroke={colors[0]}
									strokeWidth={2}
									clipPath={`url(#${id}-below)`}
								/>
							</>
						) : (
							<>
								{[0.25, 0.5, 0.75, 1].map((f) => (
									<g key={f}>
										<line
											x1={0}
											x2={W}
											y1={yS(top * f)}
											y2={yS(top * f)}
											className="flow-grid"
										/>
										<text
											x={W - 2}
											y={yS(top * f) - 3}
											textAnchor="end"
											className="flow-label"
										>
											{Math.round(top * f)}
										</text>
									</g>
								))}
								<path
									d={stepPath(2)}
									fill="none"
									stroke={colors[1]}
									strokeWidth={2}
								/>
								<path
									d={stepPath(1)}
									fill="none"
									stroke={colors[0]}
									strokeWidth={2}
								/>
							</>
						)}
						{sel !== undefined ? (
							<line
								x1={x(sel)}
								x2={x(sel)}
								y1={PAD_T}
								y2={PAD_T + plotH}
								className="flow-cursor"
							/>
						) : null}
					</svg>
				) : null}
			</div>
			{view === "win" ? (
				<div className="flow-team">
					<Logo t={home} />
					<span className="fw-bold">{home.abbrev}</span>
					<span className="flow-key" style={{ background: colors[0] }} />
					<span className="ms-auto flow-big">{pctText(pHome)}</span>
				</div>
			) : null}
			<div
				className={clsx("flow-moment", {
					"text-body-secondary": sel === undefined,
				})}
			>
				<span>{sel === undefined ? (live ? "Now" : "Final") : when}</span>
				<span className="ms-auto fw-bold">
					{away.abbrev} {sNow[2]} - {sNow[1]} {home.abbrev}
				</span>
			</div>
		</div>
	);
};
