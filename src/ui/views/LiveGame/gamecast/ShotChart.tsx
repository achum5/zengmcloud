// THE SHOT CHART: every shot where the 3D court had it let go from (see
// shots.ts) - the visitors' at the left basket, the home team's at the
// right, made ones filled in, misses rings; and each, pointed at, the play
// it was.

import { useMemo, useState } from "react";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { helpers } from "../../../util/helpers.ts";
import { periodLabel, type FeedEntry } from "./playFeed.ts";
import type { ShotSpot } from "./shots.ts";

const W = 94;
const H = 50;
// The rims (feet in from each baseline), the arc, and where the corner three
// meets it.
const RIM = 5.25;
const ARC = 23.75;
const CORNER = 22;
const CORNER_X = RIM + Math.sqrt(ARC * ARC - CORNER * CORNER);

const End = ({ right }: { right?: boolean }) => {
	const f = (x: number) => (right ? W - x : x);
	const sweep = right ? 0 : 1;
	return (
		<g className="shot-lines">
			<rect x={right ? W - 19 : 0} y={H / 2 - 8} width={19} height={16} />
			<circle cx={f(19)} cy={H / 2} r={6} />
			<path
				d={`M${f(0)},${H / 2 - CORNER} L${f(CORNER_X)},${H / 2 - CORNER} A${ARC},${ARC} 0 0 ${sweep} ${f(CORNER_X)},${H / 2 + CORNER} L${f(0)},${H / 2 + CORNER}`}
			/>
			<line x1={f(4)} x2={f(4)} y1={H / 2 - 3} y2={H / 2 + 3} />
			<circle cx={f(RIM)} cy={H / 2} r={0.75} />
		</g>
	);
};

// "M. Smith", for the popup.
const shortName = (name: string | undefined) => {
	if (!name) {
		return "";
	}
	const parts = name.split(" ");
	return parts.length > 1
		? `${parts[0]![0]}. ${parts.slice(1).join(" ")}`
		: name;
};

export const ShotChart = ({
	spots,
	teams,
	colors,
	names,
	plays,
	numPeriods = 4,
}: {
	spots: ShotSpot[];
	// Box score order (0 home, 1 away), as their colors.
	teams: [any, any];
	colors: [string, string];
	names?: Map<number, string>;
	// The game's lines, for the play each shot was (by its cursor).
	plays?: readonly FeedEntry[];
	numPeriods?: number;
}) => {
	const [at, setAt] = useState<number | undefined>();
	const byCursor = useMemo(() => {
		const m = new Map<number, FeedEntry>();
		for (const e of plays ?? []) {
			if (e.cursor !== undefined) {
				m.set(e.cursor, e);
			}
		}
		return m;
	}, [plays]);
	if (spots.length === 0) {
		return null;
	}
	// (The court's side 0 is the visitors - box score team 1.)
	const teamOf = (side: 0 | 1) => (side === 0 ? teams[1] : teams[0]);
	const colorOf = (side: 0 | 1) => (side === 0 ? colors[1] : colors[0]);
	const line = (side: 0 | 1) => {
		const mine = spots.filter((s) => s.side === side);
		const made = mine.filter((s) => s.made).length;
		return `${made}-${mine.length}`;
	};
	const tip = at !== undefined ? spots[at] : undefined;
	return (
		<div className="shot-chart">
			<div className="position-relative">
				<svg viewBox={`-1 -1 ${W + 2} ${H + 2}`} className="shot-court">
					<rect x={0} y={0} width={W} height={H} className="shot-floor" />
					<g className="shot-lines">
						<line x1={W / 2} x2={W / 2} y1={0} y2={H} />
						<circle cx={W / 2} cy={H / 2} r={6} />
					</g>
					<End />
					<End right />
					{spots.map((s, i) => (
						<g
							key={i}
							className="shot-dot"
							onMouseEnter={() => setAt(i)}
							onMouseLeave={() => setAt((a) => (a === i ? undefined : a))}
							onClick={() => setAt((a) => (a === i ? undefined : i))}
						>
							<circle
								cx={s.x}
								cy={s.y}
								r={0.95}
								fill={s.made ? colorOf(s.side) : "var(--bs-body-bg)"}
								stroke={colorOf(s.side)}
								strokeWidth={at === i ? 0.7 : 0.4}
							/>
							{/* (Easier to point at than the dot.) */}
							<circle cx={s.x} cy={s.y} r={1.8} fill="transparent" />
						</g>
					))}
				</svg>
				{tip ? (
					<ShotTip
						spot={tip}
						play={byCursor.get(tip.cursor)}
						team={teamOf(tip.side)}
						teams={teams}
						names={names}
						numPeriods={numPeriods}
					/>
				) : null}
			</div>
			<div className="shot-legend">
				{([0, 1] as const).map((side) => (
					<div key={side} className="d-flex align-items-center gap-2">
						<TeamLogoInline
							imgURL={teamOf(side).imgURL}
							imgURLSmall={teamOf(side).imgURLSmall}
							includePlaceholderIfNoLogo
							size={22}
						/>
						<span className="fw-bold">{teamOf(side).abbrev}</span>
						<span className="text-body-secondary">{line(side)}</span>
						<svg width={14} height={14} viewBox="0 0 14 14">
							<circle cx={7} cy={7} r={5} fill={colorOf(side)} />
						</svg>
						<span className="text-body-secondary small">Made</span>
						<svg width={14} height={14} viewBox="0 0 14 14">
							<circle
								cx={7}
								cy={7}
								r={4.5}
								fill="none"
								stroke={colorOf(side)}
								strokeWidth={2}
							/>
						</svg>
						<span className="text-body-secondary small">Missed</span>
					</div>
				))}
			</div>
		</div>
	);
};

const W_ALL = W + 2;
const H_ALL = H + 2;

// THE PLAY A SHOT WAS, over its dot: who, made or missed and from how far,
// when, and the score after it.
const ShotTip = ({
	spot,
	play,
	team,
	teams,
	names,
	numPeriods,
}: {
	spot: ShotSpot;
	play: FeedEntry | undefined;
	team: any;
	teams: [any, any];
	names?: Map<number, string>;
	numPeriods: number;
}) => {
	const rim = spot.side === 0 ? RIM : W - RIM;
	const feet = Math.round(Math.hypot(spot.x - rim, spot.y - H / 2));
	const e: any = play?.event;
	const by =
		typeof e?.pidAst === "number"
			? `Assist: ${shortName(names?.get(e.pidAst))}`
			: typeof e?.type === "string" &&
				  e.type.startsWith("blk") &&
				  typeof e.pid === "number"
				? `Blocked by ${shortName(names?.get(e.pid))}`
				: undefined;
	const when = play
		? [play.time, periodLabel(play.period, numPeriods, helpers.ordinal)]
				.filter(Boolean)
				.join(" - ")
		: undefined;
	// (Clear of the edges: off to one side near them, and under the dot up
	// at the top.)
	const left = ((spot.x + 1) / W_ALL) * 100;
	const top = ((spot.y + 1) / H_ALL) * 100;
	const dx = left < 20 ? "0%" : left > 80 ? "-100%" : "-50%";
	const below = top < 35;
	return (
		<div
			className="shot-tip"
			style={{
				left: `${left}%`,
				top: `${top}%`,
				transform: `translate(${dx}, ${below ? "12px" : "calc(-100% - 12px)"})`,
			}}
		>
			<div className="d-flex align-items-center gap-2 fw-bold">
				<TeamLogoInline
					imgURL={team?.imgURL}
					imgURLSmall={team?.imgURLSmall}
					includePlaceholderIfNoLogo
					size={18}
				/>
				{names?.get(spot.pid) ?? ""}
			</div>
			<div>
				{spot.made ? "Made" : "Missed"} {spot.three ? "3PT" : "2PT"}, {feet} ft
			</div>
			{by ? <div className="text-body-secondary">{by}</div> : null}
			{when ? <div className="text-body-secondary">{when}</div> : null}
			{play?.pts ? (
				<div className="text-body-secondary">
					{teams[1]?.abbrev} {play.pts[1]} - {play.pts[0]} {teams[0]?.abbrev}
				</div>
			) : null}
		</div>
	);
};
