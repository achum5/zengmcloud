import { useEffect, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { PlayerPicture } from "../../components/PlayerPicture.tsx";

// A stylized top-down half court that plays a tiny animation for every shot in
// a live sim: the ball flies from a (synthesized) spot on the floor to the rim,
// swishes on a make or rims out on a miss, and each attempt leaves a dot behind
// - so a live shot chart accumulates as the game plays. Shot LOCATIONS are
// fictional: the sim only knows coarse zones (at rim / low post / mid-range /
// three), so each shot is placed at a random spot inside its zone.

// Court geometry in feet. Half court: 50 wide, 47 deep, baseline at y=0.
const W = 50;
const H = 47;
const RIM = { x: 25, y: 5.25 };
const THREE_R = 23.75;
const CORNER_X = 22; // distance from rim center to the corner-three line
// Where the corner-three line meets the arc.
const CORNER_Y = RIM.y + Math.sqrt(THREE_R ** 2 - CORNER_X ** 2);

export type CourtZone = "atRim" | "lowPost" | "midRange" | "three" | "ft";

export type CourtShot = {
	key: number; // increments per shot, retriggers the animation
	pid: number;
	name: string;
	t: 0 | 1; // display team index (0 = top/away, 1 = bottom/home)
	made: boolean;
	blocked: boolean;
	zone: CourtZone;
	x: number;
	y: number;
};

export type CourtDot = {
	key: number;
	x: number;
	y: number;
	made: boolean;
	t: 0 | 1;
};

const degToRad = (deg: number) => (deg * Math.PI) / 180;

const rand = (lo: number, hi: number) => lo + Math.random() * (hi - lo);

// A fictional-but-plausible court position for a shot in a zone. Angles are
// measured from the baseline around the rim (90° = straight toward half
// court), biased away from the very edges so spots stay on the floor.
export const synthShotSpot = (zone: CourtZone): { x: number; y: number } => {
	if (zone === "ft") {
		return { x: 25 + rand(-0.5, 0.5), y: 19 };
	}

	let r: number;
	let theta: number;
	if (zone === "atRim") {
		r = rand(0.5, 4);
		theta = rand(20, 160);
	} else if (zone === "lowPost") {
		r = rand(4.5, 10);
		theta = rand(25, 155);
	} else if (zone === "midRange") {
		r = rand(11, 20.5);
		theta = rand(12, 168);
	} else {
		// Three: mostly around the arc, sometimes from the corners.
		if (Math.random() < 0.22) {
			// Corner three, along the sideline.
			const left = Math.random() < 0.5;
			return { x: left ? rand(1.2, 3.2) : rand(46.8, 48.8), y: rand(2, 11) };
		}
		r = rand(THREE_R + 1, THREE_R + 4);
		theta = rand(28, 152);
	}

	const x = RIM.x + r * Math.cos(degToRad(theta));
	const y = RIM.y + r * Math.sin(degToRad(theta));
	return {
		x: Math.min(48.8, Math.max(1.2, x)),
		y: Math.min(45, Math.max(1, y)),
	};
};

// Map a play-by-play event type to what the court should animate, or undefined
// for non-shot events. Blocks resolve their shooter/zone from the preceding
// attempt event, which the caller tracks.
export const courtActionFromEventType = (
	type: string,
):
	| { kind: "attempt"; zone: CourtZone }
	| { kind: "result"; zone: CourtZone; made: boolean; blocked?: boolean }
	| undefined => {
	switch (type) {
		case "fgaTipIn":
		case "fgaPutBack":
		case "fgaAtRim":
			return { kind: "attempt", zone: "atRim" };
		case "fgaLowPost":
			return { kind: "attempt", zone: "lowPost" };
		case "fgaMidRange":
			return { kind: "attempt", zone: "midRange" };
		case "fgaTp":
		case "fgaTpFake":
			return { kind: "attempt", zone: "three" };
		case "fgTipIn":
		case "fgTipInAndOne":
		case "fgPutBack":
		case "fgPutBackAndOne":
		case "fgAtRim":
		case "fgAtRimAndOne":
			return { kind: "result", zone: "atRim", made: true };
		case "fgLowPost":
		case "fgLowPostAndOne":
			return { kind: "result", zone: "lowPost", made: true };
		case "fgMidRange":
		case "fgMidRangeAndOne":
			return { kind: "result", zone: "midRange", made: true };
		case "tp":
		case "tpAndOne":
			return { kind: "result", zone: "three", made: true };
		case "missTipIn":
		case "missPutBack":
		case "missAtRim":
			return { kind: "result", zone: "atRim", made: false };
		case "missLowPost":
			return { kind: "result", zone: "lowPost", made: false };
		case "missMidRange":
			return { kind: "result", zone: "midRange", made: false };
		case "missTp":
			return { kind: "result", zone: "three", made: false };
		case "blkAtRim":
		case "blkTipIn":
		case "blkPutBack":
			return { kind: "result", zone: "atRim", made: false, blocked: true };
		case "blkLowPost":
			return { kind: "result", zone: "lowPost", made: false, blocked: true };
		case "blkMidRange":
			return { kind: "result", zone: "midRange", made: false, blocked: true };
		case "blkTp":
			return { kind: "result", zone: "three", made: false, blocked: true };
		case "ft":
			return { kind: "result", zone: "ft", made: true };
		case "missFt":
			return { kind: "result", zone: "ft", made: false };
		default:
			return undefined;
	}
};

// Fallback dot/trail colors until a team's real color is learned from the
// first shooter's face data (which carries the team's jersey colors).
const FALLBACK_COLORS: [string, string] = ["#fd7e14", "#0d6efd"];

const FLIGHT_MS = 650;
const OUTCOME_MS = 450;

const LiveCourt = ({
	shot,
	dots,
	homeTid,
	season,
}: {
	shot: CourtShot | undefined;
	dots: CourtDot[];
	homeTid: number | undefined;
	season: number | undefined;
}) => {
	const { lid, teamInfoCache } = useLocal(["lid", "teamInfoCache"]);

	// The shooter's face doubles as the source of each team's real color.
	const faceData = usePlayerFace(shot?.pid, season, lid);
	const teamColors = useRef<[string, string]>([...FALLBACK_COLORS]);
	if (shot && faceData?.colors?.[0]) {
		teamColors.current[shot.t] = faceData.colors[0];
	}

	const ballRef = useRef<SVGCircleElement | null>(null);
	const ringRef = useRef<SVGCircleElement | null>(null);
	const rafRef = useRef<number | undefined>(undefined);
	const [, forceRender] = useState(0);

	// Imperative flight animation: rAF drives the ball's position/size directly
	// on the SVG node (no React re-render per frame).
	useEffect(() => {
		if (!shot) {
			return;
		}
		const ball = ballRef.current;
		const ring = ringRef.current;
		if (!ball) {
			return;
		}
		if (rafRef.current !== undefined) {
			cancelAnimationFrame(rafRef.current);
		}

		const from = { x: shot.x, y: shot.y };
		const to = { ...RIM };
		// Where a miss caroms to.
		const bounce = {
			x: to.x + rand(-6, 6),
			y: to.y + rand(2, 7),
		};
		// Where a block knocks it.
		const swat = {
			x: from.x + rand(-9, 9),
			y: Math.min(40, from.y + rand(4, 10)),
		};

		ball.style.opacity = "1";
		if (ring) {
			ring.style.opacity = "0";
		}

		const start = performance.now();
		const step = (now: number) => {
			const elapsed = now - start;

			if (shot.blocked) {
				// Rises briefly, then gets swatted away.
				const p = Math.min(1, elapsed / FLIGHT_MS);
				if (p < 0.35) {
					const q = p / 0.35;
					ball.setAttribute("cx", String(from.x + (to.x - from.x) * q * 0.3));
					ball.setAttribute("cy", String(from.y + (to.y - from.y) * q * 0.3));
					ball.setAttribute("r", String(0.9 + 0.5 * q));
				} else {
					const q = (p - 0.35) / 0.65;
					const bx = from.x + (to.x - from.x) * 0.105;
					const by = from.y + (to.y - from.y) * 0.105;
					ball.setAttribute("cx", String(bx + (swat.x - bx) * q));
					ball.setAttribute("cy", String(by + (swat.y - by) * q));
					ball.setAttribute("r", String(1.4 - 0.6 * q));
					ball.style.opacity = String(1 - 0.7 * q);
				}
				if (p < 1) {
					rafRef.current = requestAnimationFrame(step);
				}
				return;
			}

			if (elapsed <= FLIGHT_MS) {
				// Straight-line travel; the ball swells mid-flight to fake the arc.
				const p = elapsed / FLIGHT_MS;
				ball.setAttribute("cx", String(from.x + (to.x - from.x) * p));
				ball.setAttribute("cy", String(from.y + (to.y - from.y) * p));
				ball.setAttribute("r", String(0.9 + 0.9 * Math.sin(Math.PI * p)));
				rafRef.current = requestAnimationFrame(step);
				return;
			}

			const p = Math.min(1, (elapsed - FLIGHT_MS) / OUTCOME_MS);
			if (shot.made) {
				// Swish: the ball drops into the rim and a ring pulses out.
				ball.setAttribute("cx", String(to.x));
				ball.setAttribute("cy", String(to.y));
				ball.setAttribute("r", String(Math.max(0.05, 0.9 * (1 - p))));
				ball.style.opacity = String(1 - p * 0.6);
				if (ring) {
					ring.setAttribute("r", String(1 + 3.5 * p));
					ring.style.opacity = String(0.9 * (1 - p));
				}
			} else {
				// Rim out: carom off to the side and fade.
				ball.setAttribute("cx", String(to.x + (bounce.x - to.x) * p));
				ball.setAttribute("cy", String(to.y + (bounce.y - to.y) * p));
				ball.setAttribute("r", String(0.9 - 0.4 * p));
				ball.style.opacity = String(1 - 0.8 * p);
			}
			if (p < 1) {
				rafRef.current = requestAnimationFrame(step);
			} else {
				ball.style.opacity = "0";
				if (ring) {
					ring.style.opacity = "0";
				}
				// One re-render so the just-shot dot settles into the chart.
				forceRender((n) => n + 1);
			}
		};
		rafRef.current = requestAnimationFrame(step);

		return () => {
			if (rafRef.current !== undefined) {
				cancelAnimationFrame(rafRef.current);
			}
		};
	}, [shot?.key]);

	const homeInfo = homeTid !== undefined ? teamInfoCache?.[homeTid] : undefined;
	const lineColor = "var(--bs-secondary)";

	return (
		<div className="mb-2 position-relative">
			<svg
				viewBox={`0 0 ${W} ${H}`}
				style={{ width: "100%", display: "block" }}
				aria-hidden="true"
			>
				{/* Floor */}
				<rect
					x={0}
					y={0}
					width={W}
					height={H}
					rx={0.8}
					fill="var(--bs-tertiary-bg)"
					stroke={lineColor}
					strokeWidth={0.3}
				/>

				{/* Home logo watermark at center court */}
				{homeInfo?.imgURL ? (
					<image
						href={homeInfo.imgURL}
						x={25 - 8}
						y={31 - 8}
						width={16}
						height={16}
						opacity={0.16}
						preserveAspectRatio="xMidYMid meet"
					/>
				) : homeInfo ? (
					<text
						x={25}
						y={33}
						textAnchor="middle"
						fontSize={7}
						fontWeight={700}
						fill={lineColor}
						opacity={0.18}
					>
						{homeInfo.abbrev}
					</text>
				) : null}

				{/* Court lines */}
				<g fill="none" stroke={lineColor} strokeWidth={0.25} opacity={0.75}>
					{/* Paint + free-throw circle */}
					<rect x={17} y={0} width={16} height={19} />
					<circle cx={25} cy={19} r={6} />
					{/* Backboard + rim */}
					<line x1={22} y1={4} x2={28} y2={4} strokeWidth={0.4} />
					<circle cx={RIM.x} cy={RIM.y} r={0.95} />
					{/* Restricted area */}
					<path d={`M 21 ${RIM.y} A 4 4 0 0 0 29 ${RIM.y}`} />
					{/* Three-point line */}
					<line x1={3} y1={0} x2={3} y2={CORNER_Y} />
					<line x1={47} y1={0} x2={47} y2={CORNER_Y} />
					<path
						d={`M 3 ${CORNER_Y} A ${THREE_R} ${THREE_R} 0 0 0 47 ${CORNER_Y}`}
					/>
					{/* Half-court line + center circle */}
					<line x1={0} y1={H} x2={W} y2={H} />
					<path d={`M ${25 - 6} ${H} A 6 6 0 0 0 ${25 + 6} ${H}`} />
				</g>

				{/* Accumulated shot chart: filled = made, hollow = missed */}
				<g>
					{dots.map((dot) => (
						<circle
							key={dot.key}
							cx={dot.x}
							cy={dot.y}
							r={0.55}
							fill={dot.made ? teamColors.current[dot.t] : "none"}
							stroke={teamColors.current[dot.t]}
							strokeWidth={0.22}
							opacity={dot.made ? 0.85 : 0.55}
						/>
					))}
				</g>

				{/* Swish ring + the ball, driven imperatively */}
				<circle
					ref={ringRef}
					cx={RIM.x}
					cy={RIM.y}
					r={1}
					fill="none"
					stroke={shot ? teamColors.current[shot.t] : "#e8772e"}
					strokeWidth={0.35}
					style={{ opacity: 0 }}
				/>
				<circle
					ref={ballRef}
					cx={RIM.x}
					cy={RIM.y}
					r={0.9}
					fill="#e8772e"
					stroke="#7a3a12"
					strokeWidth={0.15}
					style={{ opacity: 0 }}
				/>
			</svg>

			{/* Shooter card for the current play */}
			{shot ? (
				<div
					className="position-absolute d-flex align-items-center gap-1 px-1 rounded"
					style={{
						right: 4,
						bottom: 4,
						background: "var(--bs-body-bg)",
						border: "1px solid var(--bs-border-color)",
						opacity: 0.95,
						maxWidth: "70%",
					}}
				>
					{faceData && (faceData.face || faceData.imgURL) ? (
						<span style={{ height: "2.2em", width: "1.5em" }}>
							<PlayerPicture
								face={faceData.face}
								imgURL={faceData.imgURL}
								colors={faceData.colors}
								jersey={faceData.jersey}
							/>
						</span>
					) : null}
					<span className="small text-truncate">
						{shot.name}
						<span
							className={shot.made ? "text-success" : "text-danger"}
							style={{ marginLeft: 4 }}
						>
							{shot.blocked
								? "blocked"
								: shot.made
									? shot.zone === "ft"
										? "FT ✓"
										: "✓"
									: shot.zone === "ft"
										? "FT ✗"
										: "✗"}
						</span>
					</span>
				</div>
			) : null}
		</div>
	);
};

export default LiveCourt;
