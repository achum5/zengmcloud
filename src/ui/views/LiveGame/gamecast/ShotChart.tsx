// THE SHOT CHART: every shot where the 3D court had it let go from (see
// shots.ts) - the visitors' at the left basket, the home team's at the
// right, made ones filled in, misses rings.

import { useState } from "react";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
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

export const ShotChart = ({
	spots,
	teams,
	colors,
	names,
}: {
	spots: ShotSpot[];
	// Box score order (0 home, 1 away), as their colors.
	teams: [any, any];
	colors: [string, string];
	names?: Map<number, string>;
}) => {
	const [only, setOnly] = useState<"both" | 0 | 1>("both");
	if (spots.length === 0) {
		return null;
	}
	// (The court's side 0 is the visitors - box score team 1.)
	const teamOf = (side: 0 | 1) => (side === 0 ? teams[1] : teams[0]);
	const colorOf = (side: 0 | 1) => (side === 0 ? colors[1] : colors[0]);
	const shown = spots.filter((s) => only === "both" || s.side === only);
	const line = (side: 0 | 1) => {
		const mine = spots.filter((s) => s.side === side);
		const made = mine.filter((s) => s.made).length;
		return `${made}-${mine.length}`;
	};
	return (
		<div className="shot-chart">
			<div className="btn-group btn-group-sm mb-2" role="group">
				{(
					[
						["both", "Both"],
						[0, teamOf(0).abbrev],
						[1, teamOf(1).abbrev],
					] as const
				).map(([key, label]) => (
					<button
						key={String(key)}
						type="button"
						className={`btn ${only === key ? "btn-secondary" : "btn-light-bordered"}`}
						onClick={() => setOnly(key)}
					>
						{label}
					</button>
				))}
			</div>
			<svg viewBox={`-1 -1 ${W + 2} ${H + 2}`} className="shot-court">
				<rect x={0} y={0} width={W} height={H} className="shot-floor" />
				<g className="shot-lines">
					<line x1={W / 2} x2={W / 2} y1={0} y2={H} />
					<circle cx={W / 2} cy={H / 2} r={6} />
				</g>
				<End />
				<End right />
				{shown.map((s, i) => (
					<circle
						key={i}
						cx={s.x}
						cy={s.y}
						r={0.95}
						fill={s.made ? colorOf(s.side) : "var(--bs-body-bg)"}
						stroke={colorOf(s.side)}
						strokeWidth={0.4}
					>
						<title>
							{`${names?.get(s.pid) ?? ""} ${s.made ? "made" : "missed"} ${s.three ? "3PT" : "2PT"}`.trim()}
						</title>
					</circle>
				))}
			</svg>
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
