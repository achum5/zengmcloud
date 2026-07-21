import {
	useEffect,
	useId,
	useLayoutEffect,
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
	role: "main" | "defender" | "victim" | "in" | "out" | "onCourt";
	// Display team (0 = away/left, 1 = home/right). Set for "onCourt" background
	// players so they're colored by their own team rather than the scene's.
	t?: 0 | 1;
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

// Deterministic half-court formation spots (depth from the attacked baseline,
// across the width), ordered roughly by position. Offense spaces the floor; the
// defense sits a step closer to the rim it's protecting. BOTH clusters live in
// the offense's frontcourt - the end the ball is at - so all ten faces read as
// one 5-on-5 set, and the spots are fixed (not random) so players hold their
// formation and only glide when the ball changes ends.
const OFFENSE_SPOTS: { depth: number; across: number }[] = [
	{ depth: 27, across: 25 }, // point, top of the key
	{ depth: 22, across: 42 }, // wing
	{ depth: 22, across: 8 }, // wing
	{ depth: 10, across: 38 }, // short corner / elbow
	{ depth: 7, across: 21 }, // low block
];
const DEFENSE_SPOTS: { depth: number; across: number }[] = [
	{ depth: 22, across: 25 },
	{ depth: 17, across: 39 },
	{ depth: 17, across: 11 },
	{ depth: 8, across: 31 },
	{ depth: 6, across: 20 },
];

// Rank a lineup player by court position so the sorted order fills the formation
// slots guard→wing→big, however the sim labels positions.
const posRank = (pos: string | undefined): number => {
	switch (pos) {
		case "PG":
			return 0;
		case "G":
			return 1;
		case "SG":
			return 2;
		case "GF":
			return 3;
		case "SF":
			return 4;
		case "F":
			return 5;
		case "PF":
			return 6;
		case "FC":
			return 7;
		case "C":
			return 8;
		default:
			return 4;
	}
};

export type LineupPlayer = {
	pid: number;
	name: string;
	pos?: string;
	inGame?: boolean;
};

// Build the background 5-on-5: every on-floor player who ISN'T already an actor
// in the play, placed at a fixed formation spot in the offense's frontcourt.
// `teams` is display order [away, home]; `offenseT` is the display team with the
// ball (both teams cluster at that team's attacking rim). Excluded pids (the
// play's actors) are drawn separately at their action spots, so their slot is
// simply left open.
export const buildLineupActors = ({
	teams,
	offenseT,
	excludePids,
}: {
	teams: [LineupPlayer[], LineupPlayer[]];
	offenseT: 0 | 1;
	excludePids: Set<number>;
}): CourtActor[] => {
	const out: CourtActor[] = [];
	for (const t of [0, 1] as const) {
		const spots = t === offenseT ? OFFENSE_SPOTS : DEFENSE_SPOTS;
		const onFloor = (teams[t] ?? [])
			.filter((p) => p.inGame && !excludePids.has(p.pid))
			.sort((a, b) => posRank(a.pos) - posRank(b.pos))
			.slice(0, spots.length);
		for (const [i, p] of onFloor.entries()) {
			const slot = spots[i]!;
			// Everyone stands in the offense's frontcourt (toCourt is oriented to
			// offenseT's attacked rim); offense and defense just use different depths.
			const { x, y } = toCourt(offenseT, slot.depth, slot.across);
			out.push({ pid: p.pid, name: p.name, x, y, role: "onCourt", t });
		}
	}
	return out;
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
// The fouled/stripped player gets ROCKED - a bigger, snappier recoil than a
// polite wobble - and the fouler/stripper takes a hard swiping CHOP. These read
// clearly even at the small on-court face size, which the gentle old versions
// did not.
const FACE_ANIM_CSS = `
@keyframes liveCourtShake {
	0%,100% { transform: ${REST} rotate(0deg) scale(1); }
	12% { transform: ${REST} translateX(6px) rotate(13deg) scale(1.08); }
	28% { transform: ${REST} translateX(-6px) rotate(-12deg) scale(1.06); }
	44% { transform: ${REST} translateX(4px) rotate(9deg) scale(1.04); }
	60% { transform: ${REST} translateX(-3px) rotate(-6deg) scale(1.02); }
	78% { transform: ${REST} translateX(2px) rotate(3deg) scale(1.01); }
}
@keyframes liveCourtSwipe {
	0% { transform: ${REST} rotate(0deg) scale(1); }
	35% { transform: ${REST} translateX(-11px) rotate(-27deg) scale(1.1); }
	55% { transform: ${REST} translateX(5px) rotate(12deg) scale(1.04); }
	75% { transform: ${REST} translateX(-2px) rotate(-5deg) scale(1.01); }
	100% { transform: ${REST} rotate(0deg) scale(1); }
}`;

// One player standing on the floor: face centered ON its court point (over the
// shot dot), with a name tag placed above or below to avoid colliding with a
// nearby player's tag. Moved with a compositor transform (translate3d in
// measured px) rather than left/top - animating left/top forces layout+paint
// on every transition frame for every face, which is what made the court chug
// on mobile. Falls back to left/top % for the one paint before the container
// is measured.
const FaceOnCourt = ({
	actor,
	season,
	lid,
	color,
	anim,
	nameAbove,
	size,
	background,
}: {
	actor: CourtActor;
	season: number | undefined;
	lid: number | undefined;
	color: string;
	// A background 5-on-5 player (not part of the current play): dimmed, no name
	// tag, and sat below the active actors and the play text.
	background?: boolean;
	anim?: "shake" | "swipe";
	nameAbove?: boolean;
	// Measured px size of the court container (see LiveCourt's ResizeObserver).
	size: { w: number; h: number } | undefined;
}) => {
	const faceData = usePlayerFace(actor.pid, season, lid);
	const fx = (actor.x + RAIL_W) / (COURT_W + 2 * RAIL_W);
	const fy = (actor.y + APRON) / (COURT_H + 2 * APRON);

	const animation =
		anim === "shake"
			? "liveCourtShake 0.62s ease"
			: anim === "swipe"
				? "liveCourtSwipe 0.6s ease"
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

	// The OUTER div only places the point on the court (compositor-friendly
	// translate3d, transitioned for the glide between plays). The INNER div
	// carries the centering REST transform and the one-shot shake/swipe
	// animations (whose keyframes bake REST in), so animating never fights the
	// positioning.
	const positionStyle = size
		? {
				left: 0,
				top: 0,
				transform: `translate3d(${fx * size.w}px, ${fy * size.h}px, 0)`,
				transition: "transform 0.4s ease",
				willChange: "transform",
			}
		: {
				left: `${fx * 100}%`,
				top: `${fy * 100}%`,
			};

	return (
		<div
			className="position-absolute"
			style={{
				...positionStyle,
				pointerEvents: "none",
				zIndex: background ? 2 : actor.role === "main" ? 5 : 4,
				opacity: background ? 0.72 : 1,
			}}
		>
			<div
				style={{
					position: "relative",
					height: FACE_H,
					width: FACE_W,
					transform: REST,
					animation,
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
				{background ? null : nameTag}
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
	const burstRef = useRef<SVGGElement | null>(null);
	const rafRef = useRef<number | undefined>(undefined);
	const [, forceRender] = useState(0);

	// Measured px size of the court container, so faces can be positioned with a
	// compositor transform (translate3d) instead of layout-triggering left/top.
	// useLayoutEffect measures before first paint; the ResizeObserver keeps it
	// current through resizes/rotations.
	const containerRef = useRef<HTMLDivElement | null>(null);
	const [size, setSize] = useState<{ w: number; h: number } | undefined>(
		undefined,
	);
	useLayoutEffect(() => {
		const el = containerRef.current;
		if (!el) {
			return;
		}
		const measure = () => {
			const w = el.clientWidth;
			const h = el.clientHeight;
			if (w > 0 && h > 0) {
				setSize((prev) =>
					prev && prev.w === w && prev.h === h ? prev : { w, h },
				);
			}
		};
		measure();
		const observer = new ResizeObserver(measure);
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, []);

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
		const burst = burstRef.current;
		if (!ball) {
			return;
		}
		if (rafRef.current !== undefined) {
			cancelAnimationFrame(rafRef.current);
		}
		// Clear any lingering impact burst from a previous scene.
		if (burst) {
			burst.style.opacity = "0";
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

		// Non-shot scenes get punchy, readable feedback (the small pulse-only
		// version read as almost nothing).
		if (!isShot) {
			hideBall();
			const victim = scene.actors.find((a) => a.role === "victim");
			const cleanup = () => {
				if (rafRef.current !== undefined) {
					cancelAnimationFrame(rafRef.current);
				}
			};

			// A steal: the ball is knocked LOOSE from the victim and darts to the
			// stealer, red sparks bursting at the poke, a pulse on the stealer. (The
			// victim's face also recoils via CSS.)
			if (scene.kind === "stl") {
				const from =
					scene.ballFrom ??
					(victim ? { x: victim.x, y: victim.y } : { x: COURT_W / 2, y: 25 });
				const to = scene.ballTo ?? (main ? { x: main.x, y: main.y } : from);
				const pokeDir = to.x >= from.x ? 1 : -1;
				if (ring) {
					ring.setAttribute("stroke", "#dc3545");
					ring.setAttribute("cx", String(main?.x ?? from.x));
					ring.setAttribute("cy", String(main?.y ?? from.y));
				}
				if (burst) {
					burst.style.color = "#f04444";
				}
				ball.style.opacity = "1";
				const start = performance.now();
				const step = (now: number) => {
					const p = Math.min(1, (now - start) / 640);
					let bx: number;
					let by: number;
					if (p < 0.32) {
						// Knocked loose: the ball jitters around the victim.
						const q = p / 0.32;
						bx = from.x + pokeDir * Math.sin(q * Math.PI * 3) * 1.6;
						by = from.y + Math.cos(q * Math.PI * 2.5) * 1.2;
					} else {
						// Then darts into the stealer's hands.
						const q = (p - 0.32) / 0.68;
						bx = from.x + (to.x - from.x) * q;
						by = from.y + (to.y - from.y) * q - Math.sin(Math.PI * q) * 1.2;
					}
					ball.setAttribute("cx", String(bx));
					ball.setAttribute("cy", String(by));
					ball.setAttribute("r", "0.9");
					ball.style.opacity =
						p < 0.86 ? "1" : String(Math.max(0, 1 - (p - 0.86) / 0.14));
					if (burst) {
						const bp = Math.min(1, p / 0.42);
						burst.setAttribute(
							"transform",
							`translate(${from.x} ${from.y}) scale(${0.5 + 2.3 * bp})`,
						);
						burst.style.opacity = String(0.85 * (1 - bp));
					}
					if (ring) {
						ring.setAttribute("r", String(1.5 + 4 * p));
						ring.style.opacity = String(0.75 * (1 - p));
					}
					if (p < 1) {
						rafRef.current = requestAnimationFrame(step);
					} else {
						hideBall();
						if (burst) {
							burst.style.opacity = "0";
						}
					}
				};
				rafRef.current = requestAnimationFrame(step);
				return cleanup;
			}

			// A foul: an impact burst + pulse at the point of contact between the
			// fouler and the man he hit. (Their faces chop / recoil via CSS.)
			if (scene.kind === "foul") {
				const bx =
					victim && main ? (victim.x + main.x) / 2 : (main?.x ?? COURT_W / 2);
				const by = victim && main ? (victim.y + main.y) / 2 : (main?.y ?? 25);
				if (ring) {
					ring.setAttribute("stroke", "#f59e0b");
					ring.setAttribute("cx", String(bx));
					ring.setAttribute("cy", String(by));
				}
				if (burst) {
					burst.style.color = "#fbbf24";
				}
				const start = performance.now();
				const step = (now: number) => {
					const p = Math.min(1, (now - start) / 520);
					if (burst) {
						burst.setAttribute(
							"transform",
							`translate(${bx} ${by}) scale(${0.4 + 2.9 * p})`,
						);
						burst.style.opacity = String(0.95 * (1 - p));
					}
					if (ring) {
						ring.setAttribute("r", String(1.5 + 5 * p));
						ring.style.opacity = String(0.85 * (1 - p));
					}
					if (p < 1) {
						rafRef.current = requestAnimationFrame(step);
					} else {
						if (ring) {
							ring.style.opacity = "0";
						}
						if (burst) {
							burst.style.opacity = "0";
						}
					}
				};
				rafRef.current = requestAnimationFrame(step);
				return cleanup;
			}

			// A turnover with no steal credited: a quick red pulse at the culprit.
			if (ring && main && scene.kind === "tov") {
				ring.setAttribute("cx", String(main.x));
				ring.setAttribute("cy", String(main.y));
				ring.setAttribute("stroke", "#dc3545");
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
			return cleanup;
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
	const baselineImageURL = court?.baselineImageURL || undefined;
	const cornerLogoURL = court?.cornerLogoURL || undefined;
	const benchImageURL = court?.benchImageURL || undefined;
	const centerText = court?.centerText || undefined;
	const benchText = court?.benchText || undefined;

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
	const centerTextColor = court?.centerTextColor || homeRail;
	const benchTextColor = court?.benchTextColor || homeText;
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
		// Only the play's actors have name tags, so only they can collide.
		const near = scene.actors.some(
			(o) => o !== actor && o.role !== "onCourt" && Math.abs(o.x - actor.x) < 9,
		);
		if (near) {
			return actor.role !== "main";
		}
		// Otherwise flip tags near the bottom edge so they stay on the court.
		return actor.y > COURT_H - 9;
	};

	// The play text sits BESIDE the action but must never cover a face. Anchor it
	// just past the edge of the whole actor cluster, on whichever side faces
	// center court, vertically centered on the cluster. Background 5-on-5 players
	// are excluded - the text hugs the ACTIVE play, not the whole floor.
	const actorsForText = (scene?.actors ?? []).filter(
		(a) => a.role !== "onCourt",
	);
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

				{/* Bench-side sponsor banner: a wide image along the BOTTOM sideline
				    apron only (the broadcast bench/scorer's-table side). */}
				{benchImageURL ? (
					<image
						href={benchImageURL}
						x={0}
						y={COURT_H}
						width={COURT_W}
						height={APRON}
						preserveAspectRatio="none"
						opacity={0.97}
					/>
				) : null}

				{/* Baseline branding: a logo/script in each backcourt behind the
				    baseline (e.g. the "THE FINALS" script). Drawn on the floor under
				    the lines so the key/arc stay visible on top. */}
				{baselineImageURL ? (
					<>
						<image
							href={baselineImageURL}
							x={COURT_W * 0.11 - 9}
							y={25 - 6}
							width={18}
							height={12}
							opacity={0.9}
							preserveAspectRatio="xMidYMid meet"
						/>
						<image
							href={baselineImageURL}
							x={COURT_W * 0.89 - 9}
							y={25 - 6}
							width={18}
							height={12}
							opacity={0.9}
							preserveAspectRatio="xMidYMid meet"
						/>
					</>
				) : null}

				{/* Quarter-court logos: one in each of the four quadrant corners. */}
				{cornerLogoURL
					? (
							[
								[COURT_W * 0.25, 10],
								[COURT_W * 0.25, COURT_H - 10],
								[COURT_W * 0.75, 10],
								[COURT_W * 0.75, COURT_H - 10],
							] as const
						).map(([qx, qy], i) => (
							<image
								key={`corner${i}`}
								href={cornerLogoURL}
								x={qx - 4.5}
								y={qy - 4.5}
								width={9}
								height={9}
								opacity={0.8}
								preserveAspectRatio="xMidYMid meet"
							/>
						))
					: null}

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

				{/* Center-court script text (e.g. "The Finals"), above the center
				    logo near the top sideline so it stays clear of the logo. */}
				{centerText ? (
					<text
						x={COURT_W / 2}
						y={6}
						textAnchor="middle"
						dominantBaseline="central"
						fontSize={3.4}
						fontWeight={800}
						fontStyle="italic"
						letterSpacing={0.4}
						fill={centerTextColor}
						opacity={0.95}
					>
						{centerText.slice(0, 24)}
					</text>
				) : null}

				{/* Bench-side sponsor text (e.g. "celtics.com"), running along the
				    bottom sideline just inside the court. */}
				{benchText ? (
					<text
						x={COURT_W / 2}
						y={COURT_H - 1.9}
						textAnchor="middle"
						dominantBaseline="central"
						fontSize={2.4}
						fontWeight={700}
						letterSpacing={0.8}
						fill={benchTextColor}
						opacity={0.95}
					>
						{benchText.slice(0, 30)}
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
			baselineImageURL,
			cornerLogoURL,
			benchImageURL,
			centerText,
			centerTextColor,
			benchText,
			benchTextColor,
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
			ref={containerRef}
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
			{/* The court is THREE stacked same-viewBox SVGs, not one. The ball is
			    animated by mutating attributes every animation frame, and when it
			    lived in the same SVG as the grain-filtered floor, every frame
			    repainted the whole filtered surface - unusably slow on mobile.
			    Splitting the layers means a ball frame repaints only its own tiny
			    overlay. */}
			<svg viewBox={VIEW} style={{ width: "100%", display: "block" }}>
				{/* Static court (floor, lines, branding) - memoized, see above */}
				{courtBackground}
			</svg>

			{/* Accumulated shot chart; hover a dot for the details. The svg itself
			    ignores the pointer so it never blocks the page; the dots opt back in
			    for their tooltips. */}
			<svg
				viewBox={VIEW}
				style={{
					position: "absolute",
					inset: 0,
					width: "100%",
					height: "100%",
					pointerEvents: "none",
				}}
			>
				{dotsLayer}
			</svg>

			{/* Pulse ring (swish / turnover / foul) + the ball, on their own
			    compositor layer (willChange) so per-frame mutation stays cheap. */}
			<svg
				viewBox={VIEW}
				style={{
					position: "absolute",
					inset: 0,
					width: "100%",
					height: "100%",
					pointerEvents: "none",
					willChange: "transform",
				}}
			>
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
				{/* Impact burst: short lines radiating from a point (a steal poke, a
				    foul collision), driven per-frame via its transform attribute so it
				    expands and fades. Lines use currentColor so the effect is recolored
				    per scene by setting the group's `color`. */}
				<g ref={burstRef} style={{ opacity: 0, pointerEvents: "none" }}>
					{Array.from({ length: 8 }, (_, i) => {
						const a = (i / 8) * 2 * Math.PI;
						return (
							<line
								key={i}
								x1={Math.cos(a) * 0.6}
								y1={Math.sin(a) * 0.6}
								x2={Math.cos(a) * 2.4}
								y2={Math.sin(a) * 2.4}
								stroke="currentColor"
								strokeWidth={0.4}
								strokeLinecap="round"
							/>
						);
					})}
				</g>
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
						const background = actor.role === "onCourt";
						// Background players are colored by their OWN team; the play's
						// actors by the scene team (or the opposing team for a
						// defender/victim).
						const color = background
							? actor.t === 0
								? awayColor
								: homeColor
							: actor.role === "defender" || actor.role === "victim"
								? opposingColor
								: sceneColor;
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
								color={color}
								anim={anim}
								nameAbove={nameAboveFor(actor)}
								size={size}
								background={background}
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
