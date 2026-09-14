import {
	useEffect,
	useLayoutEffect,
	useRef,
	useState,
	type CSSProperties,
	type ReactNode,
} from "react";
import {
	bezierAt,
	ENDZONE,
	type FieldActor,
	FIELD_LEN,
	FIELD_W,
	type FieldScene,
	type FieldTeam,
	HASH_FAR,
	HASH_NEAR,
	MID_Y,
	NUMBER_Y_FAR,
	NUMBER_Y_NEAR,
	SIDELINE,
	UPRIGHT_HALF_W,
} from "./fieldSpots.ts";
import {
	ballAngle,
	ballHeight,
	ballLift,
	fieldGlideSeconds,
	impactReaction,
	nextTumble,
	pathProgress,
	pointAlongPath,
	type BallFlight,
} from "./fieldAnimation.ts";
export type {
	FieldActor,
	FieldPlayer,
	FieldScene,
	FieldSceneKind,
	FieldTeam,
} from "./fieldSpots.ts";
import { range } from "../../../common/utils.ts";
import { useLocal } from "../../util/local.ts";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { PlayerPicture } from "../../components/PlayerPicture.tsx";

// A full-field live-game graphic, built to the same brief as the basketball
// court: a real field with the team names in their own end zones, the mowed
// stripes and hash marks that make a green rectangle read as a football field,
// and a SCENE for every play. All twenty-two are on the grass - the players
// involved appear with their FACE, name and the play-by-play line, while the
// other twenty read as small team-colored jersey-number CHIPS (clean,
// unmistakably "everybody else", and cheap to move). The ball animates every
// outcome: the snap, a spiral to a receiver, a carrier weaving downfield, a
// punt hanging, a kick through the uprights, a fumble tumbling loose.
//
// What is real and what is invented: the sim gives the line of scrimmage and
// the yards gained, so a play STARTS and ENDS where it really did. What has to
// be synthesized is everything between - which hash the ball was on, how the
// twenty-two lined up, the path the carrier ran - and that comes from the same
// seeded stream the court uses, so every device draws the same play.
//
// The away team (display team 0) always attacks RIGHT and the home team LEFT,
// fixed for readability the way broadcast graphics do it.

const VIEW = `${-SIDELINE} ${-SIDELINE} ${FIELD_LEN + 2 * SIDELINE} ${
	FIELD_W + 2 * SIDELINE
}`;



const teamColor = (team: FieldTeam | undefined, i: number, fallback: string) =>
	team?.colors?.[i] ?? fallback;

// Darken or lighten a hex color. Used for the mowed stripes and for keeping an
// end zone's lettering legible against whatever the team's primary is.
const shade = (hex: string, amount: number): string => {
	const m = /^#?([\da-f]{6})$/i.exec(hex.trim());
	if (!m) {
		return hex;
	}
	const n = Number.parseInt(m[1]!, 16);
	const ch = [(n >> 16) & 255, (n >> 8) & 255, n & 255].map((c) =>
		Math.max(0, Math.min(255, Math.round(c + amount * 255))),
	);
	return `#${ch.map((c) => c.toString(16).padStart(2, "0")).join("")}`;
};

// Perceptual luminance, so end zone lettering can pick black or white against
// the team's own color rather than always being white on a yellow end zone.
const luminance = (hex: string): number => {
	const m = /^#?([\da-f]{6})$/i.exec(hex.trim());
	if (!m) {
		return 0.5;
	}
	const n = Number.parseInt(m[1]!, 16);
	return (
		(0.2126 * ((n >> 16) & 255) +
			0.7152 * ((n >> 8) & 255) +
			0.0722 * (n & 255)) /
		255
	);
};

const readableOn = (bg: string): string =>
	luminance(bg) > 0.55 ? "#111" : "#fff";

// Sizes in container-query units (cqw = % of the field container's width), so
// faces and text scale WITH the field on any screen. A football field is twice
// as wide as it is tall and carries twenty-two men, so everything here is
// smaller than the court's equivalents - a face sized for five-on-five would
// bury the field.
const FACE_W = "clamp(14px, 2.5cqw, 34px)";
const FACE_H = "clamp(21px, 3.75cqw, 51px)";
const NAME_FONT = "clamp(7px, 1.05cqw, 11px)";
const CHIP_SIZE = "clamp(9px, 1.42cqw, 19px)";
const CHIP_FONT = "clamp(6px, 1cqw, 11px)";

const GROUND_SHADOW =
	"radial-gradient(ellipse at center, rgba(0,0,0,0.45), rgba(0,0,0,0) 70%)";

const REST = "translate(-50%, -50%)";

// A tackled man is DRIVEN backwards and goes down; a tackler delivers the hit.
// Both have to read at the small size a football field allows, which is why
// they are bigger, sharper moves than the court's equivalents.
const FIELD_ANIM_CSS = `
@keyframes liveFieldTackled {
	0% { transform: ${REST} rotate(0deg) scale(1); }
	22% { transform: ${REST} translateX(-5px) rotate(-16deg) scale(1.04); }
	48% { transform: ${REST} translateX(3px) rotate(24deg) scale(0.96); }
	72% { transform: ${REST} translateX(-2px) rotate(14deg) scale(0.93); }
	100% { transform: ${REST} rotate(8deg) scale(0.95); }
}
@keyframes liveFieldHit {
	0% { transform: ${REST} rotate(0deg) scale(1); }
	30% { transform: ${REST} translateX(7px) rotate(15deg) scale(1.12); }
	60% { transform: ${REST} translateX(-3px) rotate(-6deg) scale(1.04); }
	100% { transform: ${REST} rotate(0deg) scale(1); }
}
/* A receiver going up for the ball: off the ground, hang, land. */
@keyframes liveFieldLeap {
	0% { transform: ${REST} translateY(0) scale(1); }
	20% { transform: ${REST} translateY(6%) scale(0.98); }
	56% { transform: ${REST} translateY(-58%) scale(1.1); }
	80% { transform: ${REST} translateY(-32%) scale(1.06); }
	100% { transform: ${REST} translateY(0) scale(1); }
}
@keyframes liveFieldLeapShadow {
	0% { transform: translate(-50%, -50%) scale(1); opacity: 1; }
	56% { transform: translate(-50%, -50%) scale(0.5); opacity: 0.4; }
	100% { transform: translate(-50%, -50%) scale(1); opacity: 1; }
}
/* A kicker's plant and swing: a step in, a turn through the ball. */
@keyframes liveFieldKick {
	0% { transform: ${REST} rotate(0deg) scale(1); }
	35% { transform: ${REST} translateX(-4px) rotate(-13deg) scale(1.03); }
	58% { transform: ${REST} translateX(4px) rotate(18deg) scale(1.06); }
	100% { transform: ${REST} rotate(0deg) scale(1); }
}
/* Reaching the end zone: both arms up. A short, unmistakable celebration. */
/* On a phone the field is barely 200px tall and the play text wraps onto two
   lines; the drive line then collides with it, so below that width the drive
   gives way - the play itself is what matters. */
.live-field-drive { display: block; }
@container (max-width: 520px) {
	.live-field-drive { display: none; }
}
@keyframes liveFieldScore {
	0% { transform: ${REST} translateY(0) scale(1); }
	30% { transform: ${REST} translateY(-34%) scale(1.16); }
	55% { transform: ${REST} translateY(-8%) scale(1.08); }
	80% { transform: ${REST} translateY(-24%) scale(1.12); }
	100% { transform: ${REST} translateY(0) scale(1.02); }
}`;

// How long the ball's flight takes, scaled to the scene so it always lands
// before the next play starts. A snap is over in a blink; a punt hangs.
const flightMs = (flight: BallFlight, sceneMs: number | undefined): number => {
	const budget = Math.max(260, (sceneMs ?? 1100) * 0.78);
	const want =
		flight === "snap"
			? 190
			: flight === "pitch"
				? 240
				: flight === "punt"
					? 950
					: flight === "kick"
						? 800
						: flight === "loose"
							? 620
							: flight === "carry"
								? 850
								: 620;
	return Math.min(budget, want);
};

// The compositor-friendly placement for a body on the grass, shared by faces
// and chips: a translate3d transform in measured px (never left/top, which
// would force layout for twenty-two bodies every frame), TRANSITIONED so a
// change of field position reads as a run whose duration scales with the
// distance covered. Background players get a small deterministic stagger so a
// coverage unit flows downfield instead of sliding as one rigid block.
const useFieldGlide = (
	actor: FieldActor,
	size: { w: number; h: number } | undefined,
	background: boolean,
	sceneMs: number | undefined,
): CSSProperties => {
	const fx = (actor.x + SIDELINE) / (FIELD_LEN + 2 * SIDELINE);
	const fy = (actor.y + SIDELINE) / (FIELD_W + 2 * SIDELINE);
	const prevPos = useRef<{ x: number; y: number } | undefined>(undefined);
	const prev = prevPos.current;
	const moveDist = prev ? Math.hypot(actor.x - prev.x, actor.y - prev.y) : 0;
	useEffect(() => {
		prevPos.current = { x: actor.x, y: actor.y };
	}, [actor.x, actor.y]);
	const glideDur = fieldGlideSeconds(moveDist, sceneMs);
	const glideDelay = background
		? (((actor.pid * 2654435761) >>> 0) % 5) * 0.025
		: 0;
	if (!size) {
		return {
			left: `${fx * 100}%`,
			top: `${fy * 100}%`,
		};
	}
	// A man with a JOB is driven frame by frame along it (see the play loop in
	// LiveField), so he must not also be gliding: a CSS transition fighting a
	// per-frame transform is what turns a crisp break into a smear. He still
	// gets a starting transform for the first paint.
	if (actor.path && actor.path.length > 1) {
		const start = actor.path[0]!;
		const sx = (start.x + SIDELINE) / (FIELD_LEN + 2 * SIDELINE);
		const sy = (start.y + SIDELINE) / (FIELD_W + 2 * SIDELINE);
		return {
			left: 0,
			top: 0,
			transform: `translate3d(${sx * size.w}px, ${sy * size.h}px, 0)`,
			transition: "opacity 0.3s ease",
			willChange: "transform",
		};
	}
	return {
		left: 0,
		top: 0,
		transform: `translate3d(${fx * size.w}px, ${fy * size.h}px, 0)`,
		transition: `transform ${glideDur}s ease ${glideDelay}s, opacity 0.3s ease`,
		willChange: "transform",
	};
};

export type BodyAnim = "tackled" | "hit" | "leap" | "kick" | "score";

// One body on the grass, centered on its field point. Two looks, ONE component
// (and one element), always keyed by pid at the call site: a background player
// reads as a small team-colored jersey CHIP; a player featured in the current
// play becomes his FACE with a name tag. Because both looks share the same
// element, a receiver who steps into the play GLIDES from his formation chip to
// his catch point instead of one element vanishing and another popping in.
const BodyOnField = ({
	actor,
	season,
	lid,
	color,
	ring,
	background,
	anim,
	animKey,
	nameAbove,
	size,
	sceneMs,
	registerNode,
}: {
	actor: FieldActor;
	season: number | undefined;
	lid: number | undefined;
	color: string;
	// The team's secondary colour, used for a chip's ring. Two teams whose
	// primaries are both blue are otherwise a single mass of chips.
	ring: string;
	background: boolean;
	anim?: BodyAnim;
	animKey?: number;
	nameAbove?: boolean;
	size: { w: number; h: number } | undefined;
	sceneMs: number | undefined;
	// Hands this body's element to the play loop, which moves anybody carrying
	// a path frame by frame.
	registerNode: (pid: number, el: HTMLDivElement | null) => void;
}) => {
	const faceData = usePlayerFace(actor.pid, season, lid);
	const glide = useFieldGlide(actor, size, background, sceneMs);

	if (background) {
		return (
			<div
				className="position-absolute"
				ref={(el) => {
					registerNode(actor.pid, el);
				}}
				data-field-body={actor.pid}
				data-field-role={actor.role}
				data-field-team={actor.t}
				style={{ ...glide, pointerEvents: "none", zIndex: 2 }}
			>
				<div
					style={{
						position: "relative",
						width: CHIP_SIZE,
						height: CHIP_SIZE,
						transform: REST,
					}}
				>
					<div
						style={{
							position: "absolute",
							left: "50%",
							bottom: "-16%",
							transform: "translateX(-50%)",
							width: "92%",
							height: "34%",
							background: GROUND_SHADOW,
							pointerEvents: "none",
						}}
					/>
					<div
						style={{
							width: "100%",
							height: "100%",
							borderRadius: "50%",
							background: color,
							border: `1.5px solid ${ring}`,
							outline: "0.5px solid rgba(0,0,0,0.35)",
							boxShadow: "0 1px 3px rgba(0,0,0,0.45)",
							color: "#fff",
							display: "flex",
							alignItems: "center",
							justifyContent: "center",
							fontSize: CHIP_FONT,
							fontWeight: 700,
							lineHeight: 1,
							textShadow: "0 1px 1px rgba(0,0,0,0.5)",
						}}
					>
						{faceData?.jerseyNumber ?? ""}
					</div>
				</div>
			</div>
		);
	}

	const animation =
		anim === "tackled"
			? "liveFieldTackled 0.6s ease"
			: anim === "hit"
				? "liveFieldHit 0.55s ease"
				: anim === "leap"
					? "liveFieldLeap 0.7s ease"
					: anim === "kick"
						? "liveFieldKick 0.6s ease"
						: anim === "score"
							? "liveFieldScore 0.8s ease"
							: undefined;
	const shadowAnim =
		anim === "leap" ? "liveFieldLeapShadow 0.7s ease" : undefined;

	return (
		<div
			className="position-absolute"
			ref={(el) => {
				registerNode(actor.pid, el);
			}}
			data-field-body={actor.pid}
			data-field-role={actor.role}
			data-field-team={actor.t}
			style={{
				...glide,
				pointerEvents: "none",
				zIndex: actor.role === "main" ? 5 : 4,
			}}
		>
			<div
				key={anim ? `sh-${animKey}` : "sh-static"}
				style={{
					position: "absolute",
					left: 0,
					top: `calc(${FACE_H} * 0.48)`,
					transform: "translate(-50%, -50%)",
					width: `calc(${FACE_W} * 0.95)`,
					height: `calc(${FACE_H} * 0.14)`,
					background: GROUND_SHADOW,
					borderRadius: "50%",
					pointerEvents: "none",
					animation: shadowAnim,
				}}
			/>
			<div
				key={anim ? `anim-${animKey}` : "static"}
				style={{
					position: "relative",
					height: FACE_H,
					width: FACE_W,
					transform: REST,
					animation,
					filter: "drop-shadow(0 1px 2px rgba(0,0,0,0.55))",
				}}
			>
				<PlayerPicture
					face={faceData?.face}
					imgURL={faceData?.imgURL}
					colors={faceData?.colors}
					jersey={faceData?.jersey}
				/>
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
						padding: "0 3px",
						whiteSpace: "nowrap",
						textShadow: "0 1px 1px rgba(0,0,0,0.5)",
						boxShadow: "0 1px 2px rgba(0,0,0,0.4)",
					}}
				>
					{actor.name}
				</div>
			</div>
		</div>
	);
};


// Which one-shot animation a featured player gets, from the scene and his role
// in it. The ball carrier on a touchdown celebrates; a tackled runner goes
// down; the man who made the hit delivers it.
const animForRole = (
	scene: FieldScene,
	actor: FieldActor,
): BodyAnim | undefined => {
	if (actor.role === "defender") {
		return scene.kind === "sack" ||
			scene.kind === "fumble" ||
			scene.kind === "run" ||
			scene.kind === "pass"
			? "hit"
			: undefined;
	}
	if (actor.role === "passer") {
		// He has just let go of it; the ball is the thing to watch now.
		return undefined;
	}
	if (actor.role === "main") {
		if (scene.impact?.kind === "score") {
			return "score";
		}
		switch (scene.kind) {
			case "kick":
			case "punt":
			case "kickoff":
				return "kick";
			case "pass":
			case "interception":
				return "leap";
			case "sack":
				return "tackled";
			default:
				return undefined;
		}
	}
	return undefined;
};

const LiveField = ({
	scene,
	teams,
	season,
	sceneMs,
	neutralSite,
}: {
	scene: FieldScene | undefined;
	// Display order: [away (attacks right), home (attacks left)].
	teams: [FieldTeam | undefined, FieldTeam | undefined];
	season: number | undefined;
	sceneMs: number | undefined;
	neutralSite: boolean | undefined;
}) => {
	const { lid } = useLocal(["lid"]);

	const ballRef = useRef<SVGGElement | null>(null);
	const ballShadowRef = useRef<SVGEllipseElement | null>(null);
	const impactRef = useRef<SVGCircleElement | null>(null);
	const rafRef = useRef<number | undefined>(undefined);
	const spinRef = useRef({ deg: 0, x: 0, y: 0, has: false });

	// Every body's element, by pid, so the play loop can move the ones carrying
	// a job without a React render per frame.
	const bodyNodes = useRef(new Map<number, HTMLDivElement>());
	const playRafRef = useRef<number | undefined>(undefined);
	const registerNode = (pid: number, el: HTMLDivElement | null) => {
		if (el) {
			bodyNodes.current.set(pid, el);
		} else {
			bodyNodes.current.delete(pid);
		}
	};

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
	const awayRing = teamColor(away, 1, "#fff");
	const homeRing = teamColor(home, 1, "#fff");

	// THE PLAY. Twenty-two men with twenty-two jobs, walked along their paths in
	// one loop - the routes running, the line setting, the rush coming, the
	// coverage carrying its man. Positions are written straight onto the
	// elements, so a full eleven-on-eleven play costs no React render per frame.
	//
	// A man with no path is not in here at all: he keeps the CSS glide, which is
	// the right behaviour for the special-teams scenes and for anybody the scene
	// simply placed.
	useEffect(() => {
		if (!scene || !size) {
			return;
		}
		const pathed = scene.actors.filter((a) => a.path && a.path.length > 1);
		if (pathed.length === 0) {
			return;
		}
		if (playRafRef.current !== undefined) {
			cancelAnimationFrame(playRafRef.current);
		}
		// The play fills the scene, minus a beat at the end so the last frame is
		// held rather than cut off by the next play arriving.
		const dur = Math.max(320, (sceneMs ?? 1100) * 0.88);
		const start = performance.now();
		const place = (actor: FieldActor, at: { x: number; y: number }) => {
			const node = bodyNodes.current.get(actor.pid);
			if (!node) {
				return;
			}
			const fx = (at.x + SIDELINE) / (FIELD_LEN + 2 * SIDELINE);
			const fy = (at.y + SIDELINE) / (FIELD_W + 2 * SIDELINE);
			node.style.transform = `translate3d(${fx * size.w}px, ${fy * size.h}px, 0)`;
		};
		const step = (now: number) => {
			const playT = Math.min(1, (now - start) / dur);
			for (const actor of pathed) {
				place(
					actor,
					pointAlongPath(actor.path!, pathProgress(playT, actor.delay)),
				);
			}
			if (playT < 1) {
				playRafRef.current = requestAnimationFrame(step);
			}
		};
		playRafRef.current = requestAnimationFrame(step);
		return () => {
			if (playRafRef.current !== undefined) {
				cancelAnimationFrame(playRafRef.current);
			}
		};
	}, [scene, size, sceneMs]);

	// THE BALL. Driven imperatively on the SVG nodes so a flight costs no React
	// render per frame, exactly like the court's.
	useEffect(() => {
		if (!scene) {
			return;
		}
		const ball = ballRef.current;
		const ballShadow = ballShadowRef.current;
		const impact = impactRef.current;
		if (!ball) {
			return;
		}
		if (rafRef.current !== undefined) {
			cancelAnimationFrame(rafRef.current);
		}
		if (impact) {
			impact.style.opacity = "0";
		}
		// A new scene puts the ball somewhere else entirely: the tumble must not
		// lurch by the jump distance.
		spinRef.current.has = false;

		const flightInfo = scene.ball;
		if (!flightInfo) {
			ball.style.opacity = "0";
			if (ballShadow) {
				ballShadow.style.opacity = "0";
			}
			return;
		}

		const { flight, from, to, curve } = flightInfo;
		const dist = Math.hypot(to.x - from.x, to.y - from.y);
		const dur = flightMs(flight, sceneMs);
		const impactDur = 420;
		const p1 = curve?.[0] ?? {
			x: from.x + (to.x - from.x) / 3,
			y: from.y + (to.y - from.y) / 3,
		};
		const p2 = curve?.[1] ?? {
			x: from.x + (2 * (to.x - from.x)) / 3,
			y: from.y + (2 * (to.y - from.y)) / 3,
		};

		ball.style.opacity = "1";
		const start = performance.now();

		const step = (now: number) => {
			const elapsed = now - start;
			const p = Math.min(1, elapsed / dur);
			const at = bezierAt(from, p1, p2, to, p);
			const height = ballHeight(flight, dist, p);
			const lift = ballLift(height);

			// Direction of travel, for a spiral's nose and for accumulating tumble.
			const prev = spinRef.current;
			if (prev.has) {
				spinRef.current.deg = nextTumble(
					prev.deg,
					{ x: prev.x, y: prev.y },
					at,
					flight,
				);
			}
			const travelDeg = prev.has
				? (Math.atan2(at.y - prev.y, at.x - prev.x) * 180) / Math.PI
				: (Math.atan2(to.y - from.y, to.x - from.x) * 180) / Math.PI;
			spinRef.current.x = at.x;
			spinRef.current.y = at.y;
			spinRef.current.has = true;

			const deg = ballAngle(flight, travelDeg, spinRef.current.deg);
			ball.setAttribute(
				"transform",
				`translate(${at.x} ${at.y - height * 0.55}) rotate(${deg}) scale(${lift.scale})`,
			);
			if (ballShadow) {
				ballShadow.setAttribute("cx", String(at.x));
				ballShadow.setAttribute("cy", String(at.y));
				ballShadow.setAttribute("rx", String(1.5 * lift.shadowScale));
				ballShadow.setAttribute("ry", String(0.9 * lift.shadowScale));
				ballShadow.style.opacity = String(lift.shadowOpacity);
			}

			if (p < 1) {
				rafRef.current = requestAnimationFrame(step);
				return;
			}

			// The flight is over: whatever the play ended in gets its moment.
			if (impact && scene.impact) {
				const impactStart = performance.now();
				const bang = (t: number) => {
					const q = (t - impactStart) / impactDur;
					const r = impactReaction(scene.impact!.kind, q);
					impact.style.opacity = String(r.opacity);
					impact.setAttribute("r", String(1.6 * r.scale));
					if (q < 1) {
						rafRef.current = requestAnimationFrame(bang);
					}
				};
				impact.setAttribute("cx", String(scene.impact.at.x));
				impact.setAttribute("cy", String(scene.impact.at.y));
				rafRef.current = requestAnimationFrame(bang);
			}
		};
		rafRef.current = requestAnimationFrame(step);

		return () => {
			if (rafRef.current !== undefined) {
				cancelAnimationFrame(rafRef.current);
			}
		};
	}, [scene, sceneMs]);

	// End zones are painted in each team's own color: the left one belongs to the
	// away team, the right to the home team, which is the convention the box
	// score's field has always used and the one that makes "attacking right"
	// mean "attacking the home team's end zone".
	const leftEndzone = awayColor;
	const rightEndzone = homeColor;
	const midfieldLogo = neutralSite ? undefined : (home?.imgURL ?? undefined);

	const bodies: ReactNode[] = [];
	if (scene) {
		for (const actor of scene.actors) {
			const background = actor.role === "onField";
			const displayT = actor.t ?? scene.t;
			const color = displayT === 0 ? awayColor : homeColor;
			bodies.push(
				<BodyOnField
					key={actor.pid}
					actor={actor}
					season={season}
					lid={lid}
					color={color}
					ring={displayT === 0 ? awayRing : homeRing}
					background={background}
					anim={background ? undefined : animForRole(scene, actor)}
					animKey={scene.key}
					// A name tag below the face would fall off the bottom of a field
					// that is only 53 yards tall; near the far sideline it goes above.
					nameAbove={actor.y > FIELD_W * 0.72}
					size={size}
					sceneMs={sceneMs}
					registerNode={registerNode}
				/>,
			);
		}
	}

	return (
		<div className="mb-3">
			<style dangerouslySetInnerHTML={{ __html: FIELD_ANIM_CSS }} />
			<div
				ref={containerRef}
				className="position-relative w-100"
				data-field-scene={scene?.kind ?? "none"}
				data-field-key={scene?.key ?? 0}
				style={{
					containerType: "inline-size",
					aspectRatio: `${FIELD_LEN + 2 * SIDELINE} / ${FIELD_W + 2 * SIDELINE}`,
					borderRadius: 6,
					overflow: "hidden",
					background: "#123",
				}}
			>
				<svg
					viewBox={VIEW}
					preserveAspectRatio="none"
					style={{
						position: "absolute",
						inset: 0,
						width: "100%",
						height: "100%",
					}}
				>
					<defs>
						{/* The mowed stripes: alternating five-yard bands of two greens.
						    This one detail does more for "that's a football field" than
						    any other, and it costs two rectangles per band. */}
						<linearGradient id="liveFieldTurf" x1="0" y1="0" x2="0" y2="1">
							<stop offset="0%" stopColor="#1a7f38" />
							<stop offset="50%" stopColor="#17722f" />
							<stop offset="100%" stopColor="#125f28" />
						</linearGradient>
					</defs>

					{/* The apron outside the lines. */}
					<rect
						x={-SIDELINE}
						y={-SIDELINE}
						width={FIELD_LEN + 2 * SIDELINE}
						height={FIELD_W + 2 * SIDELINE}
						fill="#0e3d1c"
					/>

					<rect
						x={0}
						y={0}
						width={FIELD_LEN}
						height={FIELD_W}
						fill="url(#liveFieldTurf)"
					/>
					{/* Mowed bands, every five yards. */}
					{range(20).map((i) => (
						<rect
							key={i}
							x={ENDZONE + i * 5}
							y={0}
							width={5}
							height={FIELD_W}
							fill="#ffffff"
							opacity={i % 2 === 0 ? 0.035 : 0}
						/>
					))}

					{/* End zones. */}
					<rect
						x={0}
						y={0}
						width={ENDZONE}
						height={FIELD_W}
						fill={leftEndzone}
					/>
					<rect
						x={FIELD_LEN - ENDZONE}
						y={0}
						width={ENDZONE}
						height={FIELD_W}
						fill={rightEndzone}
					/>
					{/* A darker band along the back of each end zone, so a solid block of
					    team color still reads as having depth. */}
					<rect
						x={0}
						y={0}
						width={ENDZONE}
						height={FIELD_W}
						fill={shade(leftEndzone, -0.12)}
						opacity={0.35}
					/>
					<rect
						x={FIELD_LEN - ENDZONE}
						y={0}
						width={ENDZONE}
						height={FIELD_W}
						fill={shade(rightEndzone, -0.12)}
						opacity={0.35}
					/>
					<text
						x={ENDZONE / 2}
						y={MID_Y}
						fill={readableOn(leftEndzone)}
						fontSize={4.6}
						fontWeight={700}
						letterSpacing={0.9}
						textAnchor="middle"
						dominantBaseline="central"
						opacity={0.92}
						transform={`rotate(-90 ${ENDZONE / 2} ${MID_Y})`}
					>
						{(away?.name ?? "").toUpperCase()}
					</text>
					<text
						x={FIELD_LEN - ENDZONE / 2}
						y={MID_Y}
						fill={readableOn(rightEndzone)}
						fontSize={4.6}
						fontWeight={700}
						letterSpacing={0.9}
						textAnchor="middle"
						dominantBaseline="central"
						opacity={0.92}
						transform={`rotate(90 ${FIELD_LEN - ENDZONE / 2} ${MID_Y})`}
					>
						{(home?.name ?? "").toUpperCase()}
					</text>

					{/* Midfield logo, faint enough to be turf paint rather than a sticker. */}
					{midfieldLogo ? (
						<image
							href={midfieldLogo}
							x={FIELD_LEN / 2 - 7}
							y={MID_Y - 7}
							width={14}
							height={14}
							opacity={0.34}
							preserveAspectRatio="xMidYMid meet"
						/>
					) : null}

					{/* Yard lines every five, goal lines heavier. */}
					{range(21).map((i) => {
						const x = ENDZONE + i * 5;
						const goal = i === 0 || i === 20;
						return (
							<line
								key={i}
								x1={x}
								y1={0}
								x2={x}
								y2={FIELD_W}
								stroke="#fff"
								strokeWidth={goal ? 0.5 : 0.22}
								opacity={goal ? 0.95 : 0.75}
							/>
						);
					})}

					{/* Hash marks: one per yard, in two rows. */}
					{range(99).map((i) => {
						const x = ENDZONE + i + 1;
						if (x % 5 === ENDZONE % 5) {
							return null;
						}
						return (
							<g key={i} stroke="#fff" strokeWidth={0.16} opacity={0.62}>
								<line x1={x} y1={HASH_NEAR - 0.45} x2={x} y2={HASH_NEAR + 0.45} />
								<line x1={x} y1={HASH_FAR - 0.45} x2={x} y2={HASH_FAR + 0.45} />
								<line x1={x} y1={0} x2={x} y2={0.7} />
								<line x1={x} y1={FIELD_W - 0.7} x2={x} y2={FIELD_W} />
							</g>
						);
					})}

					{/* Yard numbers, both sidelines, upright to their own side. */}
					{range(9).map((i) => {
						const yard = (i + 1) * 10;
						const label = yard > 50 ? 100 - yard : yard;
						const x = ENDZONE + yard;
						return (
							<g key={i} fill="#fff" opacity={0.8} fontWeight={700}>
								<text
									x={x}
									y={NUMBER_Y_NEAR}
									fontSize={4.2}
									textAnchor="middle"
									dominantBaseline="central"
								>
									{label}
								</text>
								<text
									x={x}
									y={NUMBER_Y_FAR}
									fontSize={4.2}
									textAnchor="middle"
									dominantBaseline="central"
									transform={`rotate(180 ${x} ${NUMBER_Y_FAR})`}
								>
									{label}
								</text>
							</g>
						);
					})}

					{/* Sidelines and end lines. */}
					<rect
						x={0}
						y={0}
						width={FIELD_LEN}
						height={FIELD_W}
						fill="none"
						stroke="#fff"
						strokeWidth={0.5}
						opacity={0.95}
					/>

					{/* Pylons at the eight end zone corners. */}
					{[0, FIELD_LEN - ENDZONE, ENDZONE, FIELD_LEN].map((x, i) => (
						<g key={i} fill="#ff7300">
							<rect x={x - 0.35} y={-0.7} width={0.7} height={1.4} rx={0.2} />
							<rect
								x={x - 0.35}
								y={FIELD_W - 0.7}
								width={0.7}
								height={1.4}
								rx={0.2}
							/>
						</g>
					))}

					{/* Goalposts on the end lines. From directly overhead a goalpost is
					    its crossbar - 18'6" across - with an upright standing at each
					    end, so that is exactly what is drawn: a bar and two posts. A
					    kick has to be seen going through something. */}
					{[0, 1].map((i) => {
						const x = i === 0 ? 1 : FIELD_LEN - 1;
						return (
							<g key={i}>
								<line
									x1={x}
									y1={MID_Y - UPRIGHT_HALF_W}
									x2={x}
									y2={MID_Y + UPRIGHT_HALF_W}
									stroke="#ffd21f"
									strokeWidth={0.5}
								/>
								<circle
									cx={x}
									cy={MID_Y - UPRIGHT_HALF_W}
									r={0.7}
									fill="#ffd21f"
								/>
								<circle
									cx={x}
									cy={MID_Y + UPRIGHT_HALF_W}
									r={0.7}
									fill="#ffd21f"
								/>
							</g>
						);
					})}

					{/* The drive so far: where each earlier play died. */}
					{scene?.driveMarks?.map((x, i) => (
						<line
							key={i}
							x1={x}
							y1={MID_Y - 1.6}
							x2={x}
							y2={MID_Y + 1.6}
							stroke="#fff"
							strokeWidth={0.3}
							opacity={0.28}
						/>
					))}

					{/* THE TWO LINES A VIEWER ACTUALLY READS: blue for the line of
					    scrimmage, yellow for the first down, the way every broadcast
					    since 1998 has drawn them. */}
					{scene && scene.kind !== "kickoff" ? (
						<line
							x1={scene.losX}
							y1={0}
							x2={scene.losX}
							y2={FIELD_W}
							stroke="#2f7bff"
							strokeWidth={0.45}
							opacity={0.95}
						/>
					) : null}
					{scene?.firstDownX !== undefined ? (
						<line
							x1={scene.firstDownX}
							y1={0}
							x2={scene.firstDownX}
							y2={FIELD_W}
							stroke="#ffd21f"
							strokeWidth={0.45}
							opacity={0.95}
						/>
					) : null}

				</svg>

				{/* Bodies live in HTML above the SVG so they can carry facesjs
				    portraits and text without being crushed by the field's
				    non-uniform aspect scaling. */}
				<div
					className="position-absolute"
					style={{ inset: 0, pointerEvents: "none" }}
				>
					{bodies}
				</div>

				{/* THE BALL, on its own layer above the twenty-two. A football at true
				    scale on a 120-yard field is a few pixels, and behind a carrier it
				    is none - so it is drawn larger than life, outlined, and always on
				    top. Same viewBox as the field, so the geometry is identical. */}
				<svg
					viewBox={VIEW}
					preserveAspectRatio="none"
					style={{
						position: "absolute",
						inset: 0,
						width: "100%",
						height: "100%",
						pointerEvents: "none",
					}}
				>
					<circle
						ref={impactRef}
						cx={0}
						cy={0}
						r={1.6}
						fill="none"
						stroke="#fff"
						strokeWidth={0.45}
						opacity={0}
					/>
					<ellipse
						ref={ballShadowRef}
						cx={0}
						cy={0}
						rx={1.5}
						ry={0.9}
						fill="#000"
						opacity={0}
					/>
					<g ref={ballRef} opacity={0}>
						<ellipse
							rx={1.35}
							ry={0.82}
							fill="#8b4a1c"
							stroke="#2a1408"
							strokeWidth={0.22}
						/>
						<ellipse
							rx={1.35}
							ry={0.82}
							fill="none"
							stroke="#fff"
							strokeWidth={0.13}
							opacity={0.85}
						/>
						<line
							x1={-0.62}
							y1={0}
							x2={0.62}
							y2={0}
							stroke="#fff"
							strokeWidth={0.24}
						/>
						{[-0.42, -0.14, 0.14, 0.42].map((lx) => (
							<line
								key={lx}
								x1={lx}
								y1={-0.26}
								x2={lx}
								y2={0.26}
								stroke="#fff"
								strokeWidth={0.16}
							/>
						))}
					</g>
				</svg>

				{/* Down and distance, bottom left, the way a scorebug does it. */}
				{scene?.down ? (
					<div
						className="position-absolute"
						style={{
							left: "1.2%",
							bottom: "3%",
							background: "rgba(0,0,0,0.62)",
							color: "#fff",
							borderRadius: 4,
							padding: "1px 7px",
							fontSize: "clamp(9px, 1.35cqw, 15px)",
							fontWeight: 700,
							pointerEvents: "none",
						}}
					>
						{scene.down}
					</div>
				) : null}

				{/* The call, top left: what the offense is actually running. */}
				{scene?.playName ? (
					<div
						className="position-absolute"
						style={{
							left: "1.2%",
							top: "3%",
							background: "rgba(0,0,0,0.55)",
							color: "#fff",
							borderRadius: 4,
							padding: "1px 7px",
							fontSize: "clamp(8px, 1.25cqw, 14px)",
							fontWeight: 600,
							letterSpacing: 0.2,
							pointerEvents: "none",
						}}
					>
						{scene.playName}
					</div>
				) : null}

				{/* The drive so far, bottom right. */}
				{scene?.drive ? (
					<div
						className="position-absolute live-field-drive"
						style={{
							right: "1.2%",
							bottom: "3%",
							background: "rgba(0,0,0,0.62)",
							color: "#fff",
							borderRadius: 4,
							padding: "1px 7px",
							fontSize: "clamp(8px, 1.2cqw, 13px)",
							pointerEvents: "none",
						}}
					>
						{scene.drive}
					</div>
				) : null}

				{/* The play-by-play line, on the field. */}
				{scene?.text ? (
					<div
						className="position-absolute text-center"
						style={{
							left: "50%",
							bottom: "3%",
							transform: "translateX(-50%)",
							maxWidth: "62%",
							background: "rgba(0,0,0,0.62)",
							color: "#fff",
							borderRadius: 4,
							padding: "1px 9px",
							fontSize: "clamp(9px, 1.4cqw, 16px)",
							fontWeight: 500,
							pointerEvents: "none",
						}}
					>
						{scene.text}
						{scene.score ? (
							<div style={{ fontWeight: 700 }}>{scene.score}</div>
						) : null}
					</div>
				) : null}
			</div>
		</div>
	);
};

export default LiveField;
