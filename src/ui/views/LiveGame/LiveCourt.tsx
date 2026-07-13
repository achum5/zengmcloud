import {
	useEffect,
	useId,
	useMemo,
	useRef,
	useState,
	type ReactNode,
} from "react";
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
	role: "main" | "defender" | "victim" | "in" | "out";
};

// Default championship trophy shown at center court during a finals game (Larry
// O'Brien style). Rendered behind the home logo. Overridable per team later.
export const DEFAULT_TROPHY_URL = "https://i.imgur.com/c8cwwka.png";

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
	| "jump"
	| "other";

export type CourtScene = {
	key: number; // increments per scene, retriggers animations
	kind: CourtSceneKind;
	t: 0 | 1; // display team of the main actor (0 = away/left, 1 = home/right)
	actors: CourtActor[];
	text: ReactNode; // the play-by-play line, shown on the floor
	// A scored basket's running score line (both logos flanking the score),
	// shown centered under the play text.
	score?: ReactNode;
	// Ball flight. For shots: from the attempt spot (ballFrom) to the rim
	// (rimX). For rebounds: from the rim (ballFrom) to the rebounder (ballTo).
	ballFrom?: { x: number; y: number };
	ballTo?: { x: number; y: number };
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

// Lighten (amount > 0) or darken (< 0) a hex color, for plank seam lines.
const shade = (hex: string, amount: number): string => {
	const m = /^#?([0-9a-f]{6})$/i.exec(hex.trim());
	if (!m) {
		return hex;
	}
	const n = Number.parseInt(m[1]!, 16);
	const clamp = (v: number) => Math.max(0, Math.min(255, Math.round(v)));
	const r = clamp(((n >> 16) & 0xff) * (1 + amount));
	const g = clamp(((n >> 8) & 0xff) * (1 + amount));
	const b = clamp((n & 0xff) * (1 + amount));
	return `#${((r << 16) | (g << 8) | b).toString(16).padStart(6, "0")}`;
};

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
		r = rand(1, 4);
		theta = rand(25, 155);
	} else if (zone === "lowPost") {
		r = rand(4.5, 10);
		theta = rand(30, 150);
	} else if (zone === "midRange") {
		r = rand(11, 20);
		theta = rand(18, 162);
	} else {
		if (Math.random() < 0.3) {
			// Corner three: BEHIND the corner line, which sits at y=3 near the top
			// sideline and y=47 near the bottom. Placed in the y<3 / y>47 strip a
			// short way up from the baseline, so the shooter is genuinely outside
			// the arc instead of standing in front of it.
			const nearSide = Math.random() < 0.5;
			return toCourt(
				t,
				rand(3, 12),
				nearSide ? rand(1.4, 2.7) : rand(47.3, 48.6),
			);
		}
		r = rand(THREE_R + 1, THREE_R + 3.5);
		theta = rand(32, 148);
	}

	// Kept a few feet off every edge so the face centered on this spot (and its
	// name tag) never clips the sideline/baseline.
	const depth = RIM_INSET + r * Math.sin(degToRad(theta));
	const across = 25 + r * Math.cos(degToRad(theta));
	return toCourt(
		t,
		Math.min(43, Math.max(4, depth)),
		Math.min(46, Math.max(4, across)),
	);
};

// A last-second heave: way out from the rim, around or beyond half court.
export const synthHeaveSpot = (t: 0 | 1): { x: number; y: number } =>
	toCourt(t, rand(46, 58), 25 + rand(-14, 14));

// A generic spot for non-shot plays (turnovers, fouls...) in team t's
// frontcourt, away from the paint so faces don't sit on the rim.
export const synthPlaySpot = (t: 0 | 1): { x: number; y: number } =>
	toCourt(t, rand(14, 34), rand(8, 42));

// Rebound spot: right around the rim the shot was at.
export const synthReboundSpot = (rimT: 0 | 1): { x: number; y: number } =>
	toCourt(rimT, rand(3, 9), 25 + rand(-8, 8));

// Subs check in near half court: a single, well-spaced row so the faces +
// name tags don't cram together, set a little in from the sideline (not jammed
// against the very top edge).
export const scorerTableRow = (n: number): { x: number; y: number }[] => {
	const gap = Math.min(11, 62 / Math.max(1, n));
	const startX = COURT_W / 2 - ((n - 1) * gap) / 2;
	return Array.from({ length: n }, (_, i) => ({
		x: startX + i * gap,
		y: 11,
	}));
};

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
	court?: import("../../../common/types.ts").CourtStyle;
};

const FLIGHT_MS = 650;
const OUTCOME_MS = 450;

// Sizes are in container-query units (cqw = % of the court container width), so
// faces and text scale WITH the court on any screen, mobile included - clamped
// so they never get unreadably small on a phone or huge on a wide monitor.
const FACE_W = "clamp(20px, 3.6cqw, 46px)";
const FACE_H = "clamp(30px, 5.4cqw, 68px)";
const NAME_FONT = "clamp(8px, 1.4cqw, 13px)";

// Face-tag animation keyframes, injected once. The resting transform centers
// the whole tag ON its court point (face directly over the shot dot), baked
// into every frame so the animation doesn't fight the positioning.
const REST = "translate(-50%, -50%)";
const FACE_ANIM_CSS = `
@keyframes liveCourtShake {
	0%,100% { transform: ${REST} rotate(0deg); }
	15% { transform: ${REST} translateX(3px) rotate(7deg); }
	30% { transform: ${REST} translateX(-3px) rotate(-7deg); }
	45% { transform: ${REST} translateX(2px) rotate(5deg); }
	60% { transform: ${REST} translateX(-2px) rotate(-4deg); }
	75% { transform: ${REST} translateX(1px) rotate(2deg); }
}
@keyframes liveCourtSwipe {
	0% { transform: ${REST} rotate(0deg); }
	45% { transform: ${REST} translateX(-7px) rotate(-18deg); }
	70% { transform: ${REST} translateX(2px) rotate(6deg); }
	100% { transform: ${REST} rotate(0deg); }
}`;

// One player standing on the floor: face centered ON its court point (over the
// shot dot), with a name tag placed above or below to avoid colliding with a
// nearby player's tag. Positioned in % so it tracks the SVG as it scales.
const FaceOnCourt = ({
	actor,
	season,
	lid,
	color,
	anim,
	nameAbove,
}: {
	actor: CourtActor;
	season: number | undefined;
	lid: number | undefined;
	color: string;
	anim?: "shake" | "swipe";
	nameAbove?: boolean;
}) => {
	const faceData = usePlayerFace(actor.pid, season, lid);
	const left = ((actor.x + RAIL_W) / (COURT_W + 2 * RAIL_W)) * 100;
	const top = ((actor.y + APRON) / (COURT_H + 2 * APRON)) * 100;

	const animation =
		anim === "shake"
			? "liveCourtShake 0.5s ease"
			: anim === "swipe"
				? "liveCourtSwipe 0.5s ease"
				: undefined;

	const nameTag = (
		<div
			style={{
				position: "absolute",
				left: "50%",
				transform: "translateX(-50%)",
				[nameAbove ? "bottom" : "top"]: "100%",
				background: color,
				color: "#fff",
				borderRadius: 3,
				fontSize: NAME_FONT,
				fontWeight: 600,
				lineHeight: 1.3,
				padding: "0 4px",
				whiteSpace: "nowrap",
				textShadow: "0 1px 1px rgba(0,0,0,0.5)",
				boxShadow: "0 1px 2px rgba(0,0,0,0.4)",
			}}
		>
			{actor.role === "in" ? "▲ " : actor.role === "out" ? "▼ " : ""}
			{actor.name}
		</div>
	);

	return (
		<div
			className="position-absolute"
			style={{
				left: `${left}%`,
				top: `${top}%`,
				transform: REST,
				transition: animation ? undefined : "left 0.4s ease, top 0.4s ease",
				animation,
				pointerEvents: "none",
				zIndex: actor.role === "main" ? 5 : 4,
			}}
		>
			<div
				style={{
					position: "relative",
					height: FACE_H,
					width: FACE_W,
					filter: "drop-shadow(0 1px 2px rgba(0,0,0,0.5))",
				}}
			>
				{faceData && (faceData.face || faceData.imgURL) ? (
					<PlayerPicture
						face={faceData.face}
						imgURL={faceData.imgURL}
						colors={faceData.colors}
						jersey={faceData.jersey}
					/>
				) : null}
				{nameTag}
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

		const main = scene.actors.find((a) => a.role === "main");
		const isShot =
			scene.kind === "make" || scene.kind === "miss" || scene.kind === "block";

		// Rebound / opening tip: the ball travels from one spot to another (off
		// the rim to the rebounder, or tapped from center back behind the winner).
		if (
			(scene.kind === "reb" || scene.kind === "jump") &&
			scene.ballFrom &&
			scene.ballTo
		) {
			if (ring) {
				ring.style.opacity = "0";
			}
			const from = scene.ballFrom;
			const to = scene.ballTo;
			ball.style.opacity = "1";
			const start = performance.now();
			const step = (now: number) => {
				const p = Math.min(1, (now - start) / 520);
				ball.setAttribute("cx", String(from.x + (to.x - from.x) * p));
				ball.setAttribute("cy", String(from.y + (to.y - from.y) * p));
				// A little hop off the rim, then settle into the rebounder's hands.
				ball.setAttribute("r", String(0.9 + 0.45 * Math.sin(Math.PI * p)));
				ball.style.opacity = p < 0.82 ? "1" : String(1 - (p - 0.82) / 0.18);
				if (p < 1) {
					rafRef.current = requestAnimationFrame(step);
				} else {
					hideBall();
				}
			};
			rafRef.current = requestAnimationFrame(step);
			return () => {
				if (rafRef.current !== undefined) {
					cancelAnimationFrame(rafRef.current);
				}
			};
		}

		// Other non-shot scenes: flash a colored pulse at the main actor (amber
		// for a foul, red for a turnover/steal).
		if (!isShot) {
			hideBall();
			if (
				ring &&
				main &&
				(scene.kind === "tov" || scene.kind === "stl" || scene.kind === "foul")
			) {
				const pulseColor = scene.kind === "foul" ? "#eab308" : "#dc3545";
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
		// A block sends the ball sharply BACKWARD - away from the rim, past the
		// shooter - not just to the side.
		const backward = from.x < to.x ? -1 : 1;
		const swat = {
			x: Math.min(90, Math.max(4, from.x + backward * rand(10, 18))),
			y: Math.min(46, Math.max(4, from.y + rand(-6, 6))),
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

	// Court styling comes from the home team's custom court (Manage Teams →
	// Court), falling back to a neutral hardwood + the team's colors.
	const court = home?.court;
	const lineColor = court?.lines || "#f8f5f0";
	const woodFill = court?.floor || "#c9a165";
	const woodLine = shade(woodFill, -0.14);
	const floorPattern = court?.floorPattern ?? "hardwood";
	const paintColor = court?.paint || undefined; // undefined = no painted key
	const trophyURL = court?.trophyURL || DEFAULT_TROPHY_URL;
	const centerLogoURL = court?.logoURL || home?.imgURL;
	const secondaryLogoURL = court?.secondaryLogoURL || undefined;
	const sidelineImageURL = court?.sidelineImageURL || undefined;

	// Namespaced ids for the floor's SVG defs (grain filter + clip paths), so
	// two courts on one page (e.g. the editor preview) don't collide.
	const floorUid = useId().replace(/[^a-zA-Z0-9_-]/g, "");
	const grainId = `${floorUid}-grain`;
	const clipId = `${floorUid}-clip`;

	// An oversized field of long maple planks running along the court length,
	// with procedural grain. Drawn directly for "hardwood" and, rotated + clipped
	// to the court, for "diagonal" / "chevron".
	const plankField = (tag: string): ReactNode => {
		const PH = 2.3; // plank height (across the court)
		const x0 = -60;
		const x1 = COURT_W + 60;
		const w = x1 - x0;
		const y0 = -60;
		const y1 = COURT_H + 60;
		const bands: ReactNode[] = [];
		let idx = 0;
		for (let y = y0; y < y1; y += PH, idx++) {
			bands.push(
				<rect
					key={`${tag}p${idx}`}
					x={x0}
					y={y}
					width={w}
					height={PH}
					fill={shade(woodFill, idx % 2 === 0 ? 0.045 : -0.045)}
				/>,
			);
			bands.push(
				<line
					key={`${tag}s${idx}`}
					x1={x0}
					y1={y}
					x2={x1}
					y2={y}
					stroke={woodLine}
					strokeWidth={0.09}
					opacity={0.55}
				/>,
			);
		}
		return (
			<>
				<rect x={x0} y={y0} width={w} height={y1 - y0} fill={woodFill} />
				{bands}
				<rect
					x={x0}
					y={y0}
					width={w}
					height={y1 - y0}
					fill="#000"
					filter={`url(#${grainId})`}
					opacity={0.11}
				/>
			</>
		);
	};

	// A basketweave parquet: a checkerboard of blocks whose wood grain alternates
	// direction block to block, like the classic Boston Garden floor.
	const parquetField = (): ReactNode => {
		const B = 5.6; // block size
		const strips = 5;
		const sw = B / strips;
		const nx = Math.ceil(COURT_W / B) + 1;
		const ny = Math.ceil(COURT_H / B) + 1;
		const out: ReactNode[] = [];
		let bi = 0;
		for (let ix = 0; ix < nx; ix++) {
			for (let iy = 0; iy < ny; iy++) {
				const x = ix * B;
				const y = iy * B;
				const horiz = (ix + iy) % 2 === 0;
				out.push(
					<rect
						key={`pb${bi}`}
						x={x}
						y={y}
						width={B}
						height={B}
						fill={shade(woodFill, horiz ? 0.05 : -0.05)}
					/>,
				);
				for (let s = 1; s < strips; s++) {
					out.push(
						horiz ? (
							<line
								key={`pl${bi}-${s}`}
								x1={x}
								y1={y + s * sw}
								x2={x + B}
								y2={y + s * sw}
								stroke={woodLine}
								strokeWidth={0.06}
								opacity={0.5}
							/>
						) : (
							<line
								key={`pl${bi}-${s}`}
								x1={x + s * sw}
								y1={y}
								x2={x + s * sw}
								y2={y + B}
								stroke={woodLine}
								strokeWidth={0.06}
								opacity={0.5}
							/>
						),
					);
				}
				out.push(
					<rect
						key={`pbd${bi}`}
						x={x}
						y={y}
						width={B}
						height={B}
						fill="none"
						stroke={woodLine}
						strokeWidth={0.1}
						opacity={0.6}
					/>,
				);
				bi++;
			}
		}
		return <>{out}</>;
	};

	// The floor detail (grain/planks) for the chosen pattern, always clipped to
	// the court rectangle so it never paints over the rails/aprons.
	const cx = COURT_W / 2;
	const floorDetail = (): ReactNode => {
		if (floorPattern === "solid") {
			return null;
		}
		if (floorPattern === "parquet") {
			return <g clipPath={`url(#${clipId})`}>{parquetField()}</g>;
		}
		if (floorPattern === "diagonal") {
			return (
				<g clipPath={`url(#${clipId})`}>
					<g transform={`rotate(38 ${cx} 25)`}>{plankField("dg")}</g>
				</g>
			);
		}
		if (floorPattern === "chevron") {
			// Two mirrored diagonal fields meeting at the center line -> a V weave.
			return (
				<>
					<g clipPath={`url(#${clipId}-l)`}>
						<g transform={`rotate(34 ${cx} 25)`}>{plankField("cl")}</g>
					</g>
					<g clipPath={`url(#${clipId}-r)`}>
						<g transform={`rotate(-34 ${cx} 25)`}>{plankField("cr")}</g>
					</g>
				</>
			);
		}
		return <g clipPath={`url(#${clipId})`}>{plankField("hw")}</g>;
	};

	// The painted key (NBA-style colored lane), drawn under the lines.
	const paintFor = (mirror: boolean) => {
		if (!paintColor) {
			return null;
		}
		const tx = mirror ? `translate(${COURT_W} 0) scale(-1 1)` : undefined;
		return (
			<rect
				transform={tx}
				x={0}
				y={17}
				width={19}
				height={16}
				fill={paintColor}
			/>
		);
	};

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

	// The court belongs to the HOME team - both baselines and rails carry the
	// home branding, just like a real arena. Rail color/text come from the
	// custom court (apron) or the team colors.
	const homeRail = court?.apron || homeColor;
	const homeText = court?.apronText || teamColor(home, 1, "#ffffff");
	const railLabel = (home?.name || home?.region || home?.abbrev || "")
		.toUpperCase()
		.slice(0, 14);
	const railText = (left: boolean) => {
		if (!railLabel) {
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
				fill={homeText}
			>
				{railLabel}
			</text>
		);
	};

	const actorAnim = (actor: CourtActor): "shake" | "swipe" | undefined => {
		if (!scene) {
			return undefined;
		}
		if (scene.kind === "foul") {
			return actor.role === "main"
				? "swipe"
				: actor.role === "victim"
					? "shake"
					: undefined;
		}
		if (scene.kind === "stl" && actor.role === "victim") {
			return "shake";
		}
		return undefined;
	};

	const opposingColor = scene?.t === 0 ? homeColor : awayColor;

	// Give each actor's name tag a placement that avoids colliding with a nearby
	// player's tag: when two actors are within a few feet across the court, put
	// the main actor's tag below and the other's above.
	const nameAboveFor = (actor: CourtActor): boolean => {
		if (!scene) {
			return false;
		}
		const near = scene.actors.some(
			(o) => o !== actor && Math.abs(o.x - actor.x) < 9,
		);
		if (near) {
			return actor.role !== "main";
		}
		// Otherwise flip tags near the bottom edge so they stay on the court.
		return actor.y > COURT_H - 9;
	};

	// The play text sits BESIDE the action but must never cover a face. Anchor it
	// just past the edge of the whole actor cluster, on whichever side faces
	// center court, vertically centered on the cluster.
	const actorsForText = scene?.actors ?? [];
	const clusterMinX = actorsForText.length
		? Math.min(...actorsForText.map((a) => a.x))
		: COURT_W / 2;
	const clusterMaxX = actorsForText.length
		? Math.max(...actorsForText.map((a) => a.x))
		: COURT_W / 2;
	const clusterMidX = (clusterMinX + clusterMaxX) / 2;
	const clusterMidY = actorsForText.length
		? actorsForText.reduce((s, a) => s + a.y, 0) / actorsForText.length
		: COURT_H - 6;
	const bubbleGoesRight = clusterMidX < COURT_W / 2;
	// Start past the last face on that side (+ a face-width gap) so nobody's
	// covered.
	const bubbleEdgeX = bubbleGoesRight ? clusterMaxX + 6 : clusterMinX - 6;
	const bubbleEdgePct = ((bubbleEdgeX + RAIL_W) / (COURT_W + 2 * RAIL_W)) * 100;
	const textTopPct = ((clusterMidY + APRON) / (COURT_H + 2 * APRON)) * 100;

	// The court BACKGROUND - floor grain, painted key, lines, rails, and center
	// branding - depends only on the home team's court style, not on the current
	// play. The plank/parquet/chevron floors are hundreds (parquet: ~1000+) of
	// SVG nodes; rebuilding and reconciling them on EVERY play is what made the
	// live court lag, worst on mobile and on the fancy patterns. Memoizing it so
	// it's built once (and only rebuilt when the court style actually changes)
	// leaves only the ball, dots, faces, and text to update per play.
	const courtBackground = useMemo(
		() => (
			<>
				<defs>
					{/* Wood grain: fine dark streaks running along the plank length. */}
					<filter id={grainId} x="-5%" y="-5%" width="110%" height="110%">
						<feTurbulence
							type="fractalNoise"
							baseFrequency="0.02 0.75"
							numOctaves={4}
							seed={11}
							stitchTiles="stitch"
							result="n"
						/>
						<feColorMatrix
							in="n"
							type="matrix"
							values="0 0 0 0 0  0 0 0 0 0  0 0 0 0 0  0.7 0 0 0 -0.28"
						/>
					</filter>
					<clipPath id={clipId}>
						<rect x={0} y={0} width={COURT_W} height={COURT_H} />
					</clipPath>
					<clipPath id={`${clipId}-l`}>
						<rect x={0} y={0} width={cx} height={COURT_H} />
					</clipPath>
					<clipPath id={`${clipId}-r`}>
						<rect x={cx} y={0} width={COURT_W - cx} height={COURT_H} />
					</clipPath>
				</defs>
				{/* Colored apron frame (home team) around the whole floor - baselines
				    AND sidelines */}
				<rect
					x={-RAIL_W}
					y={-APRON}
					width={COURT_W + 2 * RAIL_W}
					height={COURT_H + 2 * APRON}
					rx={1}
					fill={homeRail}
				/>
				<rect x={0} y={0} width={COURT_W} height={COURT_H} fill={woodFill} />
				{/* Floor grain/planks for the chosen wood pattern */}
				{floorDetail()}
				{/* Sponsor images stretched lengthwise along each sideline apron */}
				{sidelineImageURL ? (
					<>
						<image
							href={sidelineImageURL}
							x={0}
							y={-APRON}
							width={COURT_W}
							height={APRON}
							preserveAspectRatio="none"
							opacity={0.97}
						/>
						<image
							href={sidelineImageURL}
							x={0}
							y={COURT_H}
							width={COURT_W}
							height={APRON}
							preserveAspectRatio="none"
							opacity={0.97}
						/>
					</>
				) : null}

				{/* Painted key (drawn under the lines) */}
				{paintFor(false)}
				{paintFor(true)}

				{railText(true)}
				{railText(false)}

				{/* Court lines */}
				<g fill="none" stroke={lineColor} strokeWidth={0.25} opacity={0.9}>
					<rect x={0} y={0} width={COURT_W} height={COURT_H} />
					<line x1={COURT_W / 2} y1={0} x2={COURT_W / 2} y2={COURT_H} />
					<circle cx={COURT_W / 2} cy={25} r={6} />
				</g>
				{halfMarkings(false)}
				{halfMarkings(true)}

				{/* Center-court branding OVER the lines. During a finals game a big
				    championship trophy dominates center court with the home logo in
				    front of its base; otherwise a large home logo. */}
				{finals ? (
					<image
						href={trophyURL}
						x={COURT_W / 2 - 17}
						y={25 - 21}
						width={34}
						height={42}
						opacity={0.97}
						preserveAspectRatio="xMidYMid meet"
					/>
				) : null}
				{centerLogoURL ? (
					<image
						href={centerLogoURL}
						x={COURT_W / 2 - (finals ? 13 : 15)}
						y={25 - (finals ? 13 : 15)}
						width={finals ? 26 : 30}
						height={finals ? 26 : 30}
						opacity={0.97}
						preserveAspectRatio="xMidYMid meet"
					/>
				) : !finals ? (
					<text
						x={COURT_W / 2}
						y={28}
						textAnchor="middle"
						fontSize={10}
						fontWeight={800}
						fill={homeColor}
						opacity={0.9}
					>
						{home?.abbrev ?? ""}
					</text>
				) : null}

				{/* Secondary logo, shown in each half-court (backcourt branding) */}
				{secondaryLogoURL ? (
					<>
						<image
							href={secondaryLogoURL}
							x={COURT_W * 0.27 - 6.5}
							y={25 - 6.5}
							width={13}
							height={13}
							opacity={0.82}
							preserveAspectRatio="xMidYMid meet"
						/>
						<image
							href={secondaryLogoURL}
							x={COURT_W * 0.73 - 6.5}
							y={25 - 6.5}
							width={13}
							height={13}
							opacity={0.82}
							preserveAspectRatio="xMidYMid meet"
						/>
					</>
				) : null}
			</>
		),
		// Rebuild only when the court styling actually changes - never per play.
		// The floorDetail/paintFor/halfMarkings/railText closures read exactly
		// these values, so they're covered.
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[
			grainId,
			clipId,
			cx,
			homeRail,
			woodFill,
			woodLine,
			lineColor,
			paintColor,
			floorPattern,
			sidelineImageURL,
			secondaryLogoURL,
			centerLogoURL,
			trophyURL,
			railLabel,
			homeText,
			homeColor,
			finals,
			home?.abbrev,
		],
	);

	// The accumulated shot chart. `dots` is append-only and mutated in place, so
	// its array identity is stable - key the memo on the count so it rebuilds
	// only when a new shot lands, not on every non-shot play.
	const dotsLayer = useMemo(
		() => (
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
		),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[dots, dots.length, awayColor, homeColor],
	);

	return (
		<div
			className="mb-3 position-relative"
			style={{
				userSelect: "none",
				containerType: "inline-size",
				// Own stacking context pinned at the base level, so real dropdown
				// menus (Play menu, fast-forward) always paint ABOVE the court's
				// absolutely-positioned faces/text instead of under them.
				isolation: "isolate",
				zIndex: 0,
			}}
		>
			<style>{FACE_ANIM_CSS}</style>
			<svg viewBox={VIEW} style={{ width: "100%", display: "block" }}>
				{/* Static court (floor, lines, branding) - memoized, see above */}
				{courtBackground}

				{/* Accumulated shot chart; hover a dot for the details */}
				{dotsLayer}

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

			{/* Players on the floor, centered on their spot. Keyed by player+role
			    (NOT the scene key) so a player who appears in back-to-back scenes is
			    REUSED and repositioned instead of being torn down and rebuilt - each
			    rebuild regenerates the whole facesjs SVG, which was a big per-play
			    cost on mobile. Only the rare animated actor (a foul swipe / steal
			    shake) keeps the scene key, so its one-shot CSS animation retriggers. */}
			{scene
				? scene.actors.map((actor) => {
						const anim = actorAnim(actor);
						return (
							<FaceOnCourt
								key={
									anim
										? `a${scene.key}-${actor.pid}-${actor.role}`
										: `${actor.pid}-${actor.role}`
								}
								actor={actor}
								season={season}
								lid={lid}
								color={
									actor.role === "defender" || actor.role === "victim"
										? opposingColor
										: sceneColor
								}
								anim={anim}
								nameAbove={nameAboveFor(actor)}
							/>
						);
					})
				: null}

			{/* The play line, beside the action - placed past the edge of the whole
			    player cluster so it never covers a face */}
			{scene ? (
				<div
					className="position-absolute"
					style={{
						top: `${textTopPct}%`,
						...(bubbleGoesRight
							? { left: `${bubbleEdgePct}%` }
							: { right: `${100 - bubbleEdgePct}%` }),
						transform: "translateY(-50%)",
						maxWidth: "40%",
						background: "rgba(0,0,0,0.82)",
						color: "#fff",
						borderLeft: `3px solid ${sceneColor}`,
						borderRadius: 3,
						padding: "2px 8px",
						fontSize: "clamp(10px, 2cqw, 15px)",
						fontWeight: 500,
						lineHeight: 1.25,
						pointerEvents: "none",
						zIndex: 3,
					}}
				>
					{scene.text}
					{scene.score ? (
						<div style={{ marginTop: 2, textAlign: "center" }}>
							{scene.score}
						</div>
					) : null}
				</div>
			) : null}
		</div>
	);
};

export default LiveCourt;
