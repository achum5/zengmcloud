import { useEffect, useRef, useState, type ReactNode } from "react";
import { useLocal } from "../../util/local.ts";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { PlayerPicture } from "../../components/PlayerPicture.tsx";

// A full-court live-game graphic (FBGM-field style, but hardwood): team-colored
// rails with the team names behind each baseline, the home team's logo at
// center court (with the championship trophy behind it during the finals), and
// a SCENE for every play - the players involved appear standing at a spot on
// the floor with their face, name, and the exact play-by-play line, and the
// ball animates the outcome (swish, rim-out, block, steal, turnover...). Every
// field-goal attempt also leaves a dot behind (filled = made, hollow = miss),
// building a live shot chart; hover a dot for the who/when/score of that shot.
//
// Locations are stylized: the sim only knows coarse shot zones (at rim / low
// post / mid-range / three), so spots are synthesized inside the right zone.
// The away team always attacks the LEFT rim, home the RIGHT (fixed for
// readability, like broadcast graphics - real teams swap at halftime).

// Full court in feet: 94 x 50. Rails/aprons extend the viewBox around it.
const COURT_W = 94;
const COURT_H = 50;
const RAIL_W = 5; // team-colored strip behind each baseline
const APRON = 2.5; // sideline apron top/bottom
const VIEW = `${-RAIL_W} ${-APRON} ${COURT_W + 2 * RAIL_W} ${COURT_H + 2 * APRON}`;

const RIM_INSET = 5.25; // rim center distance from baseline
const THREE_R = 23.75;
const CORNER_DIST = 22; // rim center to corner-three line (across)
const CORNER_LEN = RIM_INSET + Math.sqrt(THREE_R ** 2 - CORNER_DIST ** 2);

export type CourtZone = "atRim" | "lowPost" | "midRange" | "three" | "ft";

export type CourtActor = {
	pid: number;
	name: string;
	// Court coordinates (x along the 94ft length, y across the 50ft width).
	x: number;
	y: number;
	role: "main" | "defender" | "in" | "out";
};

export type CourtSceneKind =
	| "attempt"
	| "make"
	| "miss"
	| "block"
	| "tov"
	| "stl"
	| "reb"
	| "foul"
	| "sub"
	| "other";

export type CourtScene = {
	key: number; // increments per scene, retriggers animations
	kind: CourtSceneKind;
	t: 0 | 1; // display team of the main actor (0 = away/left, 1 = home/right)
	actors: CourtActor[];
	text: ReactNode; // the play-by-play line, shown on the floor
	// Ball flight, for shot results: from the attempt spot to the rim.
	ballFrom?: { x: number; y: number };
	rimX?: number;
};

export type CourtDot = {
	key: number;
	x: number;
	y: number;
	made: boolean;
	t: 0 | 1;
	title: string; // hover tooltip: who/what/when/score
};

const degToRad = (deg: number) => (deg * Math.PI) / 180;
const rand = (lo: number, hi: number) => lo + Math.random() * (hi - lo);

// The rim each display team attacks (away left, home right).
export const rimXFor = (t: 0 | 1): number =>
	t === 0 ? RIM_INSET : COURT_W - RIM_INSET;

// Convert a spot expressed as (depth from the attacked baseline, position
// across the court) into full-court coordinates for team t.
const toCourt = (t: 0 | 1, depth: number, across: number) => ({
	x: t === 0 ? depth : COURT_W - depth,
	y: across,
});

// A fictional-but-plausible spot for a shot in a zone, oriented toward team
// t's rim. Angles fan out from the rim (90° = straight toward half court).
export const synthShotSpot = (
	t: 0 | 1,
	zone: CourtZone,
): { x: number; y: number } => {
	if (zone === "ft") {
		return toCourt(t, 19, 25 + rand(-0.5, 0.5));
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
		if (Math.random() < 0.22) {
			// Corner three, along the sideline.
			const nearSide = Math.random() < 0.5;
			return toCourt(
				t,
				rand(2, 11),
				nearSide ? rand(1.2, 3.2) : rand(46.8, 48.8),
			);
		}
		r = rand(THREE_R + 1, THREE_R + 4);
		theta = rand(28, 152);
	}

	const depth = RIM_INSET + r * Math.sin(degToRad(theta));
	const across = 25 + r * Math.cos(degToRad(theta));
	return toCourt(
		t,
		Math.min(46, Math.max(1, depth)),
		Math.min(48.8, Math.max(1.2, across)),
	);
};

// A generic spot for non-shot plays (turnovers, fouls...) in team t's
// frontcourt, away from the paint so faces don't sit on the rim.
export const synthPlaySpot = (t: 0 | 1): { x: number; y: number } =>
	toCourt(t, rand(14, 34), rand(8, 42));

// Rebound spot: right around the rim the shot was at.
export const synthReboundSpot = (rimT: 0 | 1): { x: number; y: number } =>
	toCourt(rimT, rand(3, 9), 25 + rand(-8, 8));

// Where subs stand while checking in: along the near sideline at midcourt, on
// their team's side.
export const benchSpot = (t: 0 | 1, i: number): { x: number; y: number } =>
	toCourt(t, 40 - i * 4, 2.5);

// Map a play-by-play event type to a shot descriptor, or undefined for
// non-shot events (which the caller handles separately).
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

export const zoneLabel = (zone: CourtZone): string => {
	switch (zone) {
		case "atRim":
			return "at the rim";
		case "lowPost":
			return "low post";
		case "midRange":
			return "mid-range";
		case "three":
			return "three";
		case "ft":
			return "free throw";
	}
};

export type CourtTeam = {
	tid: number;
	abbrev?: string;
	region?: string;
	name?: string;
	colors?: [string, string, string];
	imgURL?: string;
};

const FLIGHT_MS = 650;
const OUTCOME_MS = 450;

// One player standing on the floor: face above a name tag. Positioned in % of
// the court box so it scales with the SVG; position changes glide.
const FaceOnCourt = ({
	actor,
	season,
	lid,
	color,
	dim,
}: {
	actor: CourtActor;
	season: number | undefined;
	lid: number | undefined;
	color: string;
	dim?: boolean;
}) => {
	const faceData = usePlayerFace(actor.pid, season, lid);
	const left = ((actor.x + RAIL_W) / (COURT_W + 2 * RAIL_W)) * 100;
	const top = ((actor.y + APRON) / (COURT_H + 2 * APRON)) * 100;

	return (
		<div
			className="position-absolute text-center"
			style={{
				left: `${left}%`,
				top: `${top}%`,
				transform: "translate(-50%, -60%)",
				transition: "left 0.4s ease, top 0.4s ease",
				opacity: dim ? 0.75 : 1,
				pointerEvents: "none",
				zIndex: actor.role === "main" ? 3 : 2,
			}}
		>
			<div style={{ height: "2.6em", width: "1.8em", margin: "0 auto" }}>
				{faceData && (faceData.face || faceData.imgURL) ? (
					<PlayerPicture
						face={faceData.face}
						imgURL={faceData.imgURL}
						colors={faceData.colors}
						jersey={faceData.jersey}
					/>
				) : null}
			</div>
			<div
				className="badge rounded-pill"
				style={{
					background: "var(--bs-body-bg)",
					color: "var(--bs-body-color)",
					border: `1.5px solid ${color}`,
					fontSize: "0.65em",
					padding: "0.1em 0.45em",
					whiteSpace: "nowrap",
				}}
			>
				{actor.role === "in" ? "▲ " : actor.role === "out" ? "▼ " : ""}
				{actor.name}
			</div>
		</div>
	);
};

const teamColor = (team: CourtTeam | undefined, i: number, fallback: string) =>
	team?.colors?.[i] ?? fallback;

const LiveCourt = ({
	scene,
	dots,
	teams,
	finals,
	season,
}: {
	scene: CourtScene | undefined;
	dots: CourtDot[];
	// Display order: [away (left rim), home (right rim)].
	teams: [CourtTeam | undefined, CourtTeam | undefined];
	finals: boolean;
	season: number | undefined;
}) => {
	const { lid } = useLocal(["lid"]);

	const ballRef = useRef<SVGCircleElement | null>(null);
	const ringRef = useRef<SVGCircleElement | null>(null);
	const rafRef = useRef<number | undefined>(undefined);
	const [, forceRender] = useState(0);

	const away = teams[0];
	const home = teams[1];
	const awayColor = teamColor(away, 0, "#fd7e14");
	const homeColor = teamColor(home, 0, "#0d6efd");
	const sceneColor = scene?.t === 0 ? awayColor : homeColor;

	// Ball + effect animation, driven imperatively on the SVG nodes (no React
	// re-render per frame).
	useEffect(() => {
		if (!scene) {
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

		const hideBall = () => {
			ball.style.opacity = "0";
			if (ring) {
				ring.style.opacity = "0";
			}
		};

		// Scenes without ball flight: flash a colored pulse at the main actor
		// instead (red-ish for turnovers/fouls, team color otherwise).
		const main = scene.actors.find((a) => a.role === "main");
		const isShot =
			scene.kind === "make" || scene.kind === "miss" || scene.kind === "block";

		if (!isShot) {
			hideBall();
			if (
				ring &&
				main &&
				(scene.kind === "tov" ||
					scene.kind === "stl" ||
					scene.kind === "foul" ||
					scene.kind === "reb")
			) {
				const pulseColor =
					scene.kind === "foul"
						? "#eab308"
						: scene.kind === "tov" || scene.kind === "stl"
							? "#dc3545"
							: sceneColor;
				ring.setAttribute("cx", String(main.x));
				ring.setAttribute("cy", String(main.y));
				ring.setAttribute("stroke", pulseColor);
				const start = performance.now();
				const pulse = (now: number) => {
					const p = Math.min(1, (now - start) / 600);
					ring.setAttribute("r", String(1.5 + 4.5 * p));
					ring.style.opacity = String(0.9 * (1 - p));
					if (p < 1) {
						rafRef.current = requestAnimationFrame(pulse);
					}
				};
				rafRef.current = requestAnimationFrame(pulse);
			}
			return () => {
				if (rafRef.current !== undefined) {
					cancelAnimationFrame(rafRef.current);
				}
			};
		}

		const from = scene.ballFrom ?? { x: COURT_W / 2, y: COURT_H / 2 };
		const rimX = scene.rimX ?? rimXFor(scene.t);
		const to = { x: rimX, y: COURT_H / 2 };
		const made = scene.kind === "make";
		const blocked = scene.kind === "block";
		const bounce = { x: to.x + rand(-4, 4), y: to.y + rand(-6, 6) };
		const swat = {
			x: from.x + (from.x < to.x ? -1 : 1) * rand(4, 10),
			y: Math.min(46, Math.max(4, from.y + rand(-8, 8))),
		};

		if (ring) {
			ring.setAttribute("cx", String(to.x));
			ring.setAttribute("cy", String(to.y));
			ring.setAttribute("stroke", sceneColor);
			ring.style.opacity = "0";
		}
		ball.style.opacity = "1";

		const start = performance.now();
		const step = (now: number) => {
			const elapsed = now - start;

			if (blocked) {
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
				} else {
					hideBall();
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
			if (made) {
				// Swish: drop into the rim, ring pulse.
				ball.setAttribute("cx", String(to.x));
				ball.setAttribute("cy", String(to.y));
				ball.setAttribute("r", String(Math.max(0.05, 0.9 * (1 - p))));
				ball.style.opacity = String(1 - p * 0.6);
				if (ring) {
					ring.setAttribute("r", String(1 + 3.5 * p));
					ring.style.opacity = String(0.9 * (1 - p));
				}
			} else {
				// Rim out: carom and fade.
				ball.setAttribute("cx", String(to.x + (bounce.x - to.x) * p));
				ball.setAttribute("cy", String(to.y + (bounce.y - to.y) * p));
				ball.setAttribute("r", String(0.9 - 0.4 * p));
				ball.style.opacity = String(1 - 0.8 * p);
			}
			if (p < 1) {
				rafRef.current = requestAnimationFrame(step);
			} else {
				hideBall();
				// Settle the just-shot dot into the chart.
				forceRender((n) => n + 1);
			}
		};
		rafRef.current = requestAnimationFrame(step);

		return () => {
			if (rafRef.current !== undefined) {
				cancelAnimationFrame(rafRef.current);
			}
		};
	}, [scene?.key]);

	const lineColor = "#f8f5f0";
	const woodFill = "#c9a165";
	const woodLine = "#b3874e";

	// Half-court markings for one side, mirrored for the other.
	const halfMarkings = (mirror: boolean) => {
		const tx = mirror ? `translate(${COURT_W} 0) scale(-1 1)` : undefined;
		return (
			<g
				transform={tx}
				fill="none"
				stroke={lineColor}
				strokeWidth={0.25}
				opacity={0.9}
			>
				{/* Paint + free-throw circle */}
				<rect x={0} y={17} width={19} height={16} />
				<circle cx={19} cy={25} r={6} />
				{/* Backboard + rim */}
				<line x1={4} y1={22} x2={4} y2={28} strokeWidth={0.4} />
				<circle cx={RIM_INSET} cy={25} r={0.95} />
				{/* Restricted area */}
				<path d={`M ${RIM_INSET} 21 A 4 4 0 0 1 ${RIM_INSET} 29`} />
				{/* Three-point line: corners + arc */}
				<line x1={0} y1={3} x2={CORNER_LEN} y2={3} />
				<line x1={0} y1={47} x2={CORNER_LEN} y2={47} />
				<path
					d={`M ${CORNER_LEN} 3 A ${THREE_R} ${THREE_R} 0 0 1 ${CORNER_LEN} 47`}
				/>
			</g>
		);
	};

	// Vertical team name in a baseline rail.
	const railText = (team: CourtTeam | undefined, left: boolean) => {
		const label = (team?.name || team?.region || team?.abbrev || "")
			.toUpperCase()
			.slice(0, 14);
		if (!label) {
			return null;
		}
		const x = left ? -RAIL_W / 2 : COURT_W + RAIL_W / 2;
		return (
			<text
				x={0}
				y={0}
				transform={`translate(${x} 25) rotate(${left ? -90 : 90})`}
				textAnchor="middle"
				dominantBaseline="central"
				fontSize={3.2}
				fontWeight={700}
				letterSpacing={0.5}
				fill={teamColor(team, 1, "#ffffff")}
			>
				{label}
			</text>
		);
	};

	const mainActor = scene?.actors.find((a) => a.role === "main");
	// The play text bubble sits toward the open side of the court from the
	// main actor, so it never hangs off the edge.
	const bubbleLeftSide = (mainActor?.x ?? 0) > COURT_W / 2;

	return (
		<div className="mb-3 position-relative" style={{ userSelect: "none" }}>
			<svg viewBox={VIEW} style={{ width: "100%", display: "block" }}>
				{/* Aprons + floor */}
				<rect
					x={-RAIL_W}
					y={-APRON}
					width={COURT_W + 2 * RAIL_W}
					height={COURT_H + 2 * APRON}
					rx={1}
					fill={woodLine}
				/>
				<rect x={0} y={0} width={COURT_W} height={COURT_H} fill={woodFill} />
				{/* Plank seams */}
				<g stroke={woodLine} strokeWidth={0.1} opacity={0.5}>
					{Array.from({ length: 23 }, (_, i) => (
						<line
							key={i}
							x1={(i + 1) * 4}
							y1={0}
							x2={(i + 1) * 4}
							y2={COURT_H}
						/>
					))}
				</g>

				{/* Team rails behind each baseline */}
				<rect
					x={-RAIL_W}
					y={-APRON}
					width={RAIL_W}
					height={COURT_H + 2 * APRON}
					fill={awayColor}
				/>
				<rect
					x={COURT_W}
					y={-APRON}
					width={RAIL_W}
					height={COURT_H + 2 * APRON}
					fill={homeColor}
				/>
				{railText(away, true)}
				{railText(home, false)}

				{/* Center-court branding: trophy silhouette during the finals, then
				    the home logo on top */}
				{finals ? (
					<g transform={`translate(${COURT_W / 2} 25)`} opacity={0.3}>
						<circle cx={0} cy={-6.5} r={3.2} fill="#d4af37" />
						<path
							d="M -2.6 -4 L 2.6 -4 L 1.1 4.5 L -1.1 4.5 Z"
							fill="#d4af37"
						/>
						<rect x={-3.4} y={4.5} width={6.8} height={1.3} fill="#d4af37" />
						<rect x={-4.4} y={5.8} width={8.8} height={1.2} fill="#b8962e" />
					</g>
				) : null}
				{home?.imgURL ? (
					<image
						href={home.imgURL}
						x={COURT_W / 2 - 6}
						y={25 - 6}
						width={12}
						height={12}
						opacity={finals ? 0.75 : 0.55}
						preserveAspectRatio="xMidYMid meet"
					/>
				) : (
					<text
						x={COURT_W / 2}
						y={26.5}
						textAnchor="middle"
						fontSize={5}
						fontWeight={700}
						fill={homeColor}
						opacity={0.45}
					>
						{home?.abbrev ?? ""}
					</text>
				)}

				{/* Court lines */}
				<g fill="none" stroke={lineColor} strokeWidth={0.25} opacity={0.9}>
					<rect x={0} y={0} width={COURT_W} height={COURT_H} />
					<line x1={COURT_W / 2} y1={0} x2={COURT_W / 2} y2={COURT_H} />
					<circle cx={COURT_W / 2} cy={25} r={6} />
				</g>
				{halfMarkings(false)}
				{halfMarkings(true)}

				{/* Accumulated shot chart; hover a dot for the details */}
				<g>
					{dots.map((dot) => (
						<circle
							key={dot.key}
							cx={dot.x}
							cy={dot.y}
							r={0.6}
							fill={dot.made ? (dot.t === 0 ? awayColor : homeColor) : "none"}
							stroke={dot.t === 0 ? awayColor : homeColor}
							strokeWidth={0.25}
							opacity={dot.made ? 0.9 : 0.6}
							style={{ pointerEvents: "all" }}
						>
							<title>{dot.title}</title>
						</circle>
					))}
				</g>

				{/* Pulse ring (swish / turnover / foul) + the ball */}
				<circle
					ref={ringRef}
					cx={0}
					cy={0}
					r={1}
					fill="none"
					stroke={sceneColor}
					strokeWidth={0.4}
					style={{ opacity: 0, pointerEvents: "none" }}
				/>
				<circle
					ref={ballRef}
					cx={0}
					cy={0}
					r={0.9}
					fill="#e8772e"
					stroke="#7a3a12"
					strokeWidth={0.15}
					style={{ opacity: 0, pointerEvents: "none" }}
				/>
			</svg>

			{/* Players on the floor */}
			{scene
				? scene.actors.map((actor) => (
						<FaceOnCourt
							key={`${scene.key}-${actor.pid}-${actor.role}`}
							actor={actor}
							season={season}
							lid={lid}
							color={
								actor.role === "defender"
									? scene.t === 0
										? homeColor
										: awayColor
									: sceneColor
							}
							dim={actor.role === "defender"}
						/>
					))
				: null}

			{/* The play line, anchored near the action */}
			{scene && mainActor ? (
				<div
					className="position-absolute small rounded px-2 py-1"
					style={{
						top: `${((mainActor.y + APRON) / (COURT_H + 2 * APRON)) * 100}%`,
						...(bubbleLeftSide
							? {
									right: `${100 - ((mainActor.x + RAIL_W - 3) / (COURT_W + 2 * RAIL_W)) * 100}%`,
								}
							: {
									left: `${((mainActor.x + RAIL_W + 3) / (COURT_W + 2 * RAIL_W)) * 100}%`,
								}),
						transform: "translateY(-50%)",
						background: "var(--bs-body-bg)",
						border: `1px solid ${sceneColor}`,
						maxWidth: "38%",
						opacity: 0.95,
						pointerEvents: "none",
						zIndex: 4,
					}}
				>
					{scene.text}
				</div>
			) : null}
		</div>
	);
};

export default LiveCourt;
