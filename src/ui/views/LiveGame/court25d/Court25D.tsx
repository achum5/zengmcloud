import {
	useCallback,
	useEffect,
	useLayoutEffect,
	useMemo,
	useRef,
	useState,
	type ReactNode,
} from "react";
import { useLocal } from "../../../util/local.ts";
import { usePlayerFace, type PlayerFace } from "../../../util/playerFaces.ts";
import LiveCourt from "../LiveCourt.tsx";
import {
	benchPlane,
	FLOOR,
	LED_WALL,
	paintBench,
	paintStands,
	paintTable,
	paintWall,
	STANDS,
	TABLE_FRONT,
	TABLE_TOP,
	type Plane,
} from "./arena.ts";
import { makeCamera, planeTransform, type Camera } from "./camera.ts";
import {
	buildClocks,
	formatGameClock,
	gameClockAt,
	shotClockAt,
} from "./clock.ts";
import {
	compileCourt,
	snapForCursor,
	targetForCursor,
	type CourtPlayer,
} from "./director.ts";
import { headColors, loadHead, type HeadSprite } from "./faces.ts";
import { kitsFor, shade, type Look } from "./figure.ts";
import { COURT_H, COURT_W, type Side } from "./geometry.ts";
import { bodyOf, type Body } from "./poses.ts";
import { aimFor, drawFrame, momentAt } from "./scene.ts";

// THE 2.5D COURT: the game as a broadcast - the home team's own floor, the
// players with their faces, a camera that follows the ball - acting out the
// play-by-play, in place of the 2D court when this device has chosen it.
//
// The whole game is staged up front (see director.ts), so playback is a clock
// running along that timeline. The page's playback cursor (events consumed)
// says where the clock may run to: the moment the next line happens. When the
// clock gets there, it asks the page for that line (onReady) - so the
// animation, not a timer, sets the pace, and every line's text appears as its
// play happens on screen.

// The speed slider value that plays the timeline at 1x (the page's default).
const DEFAULT_SPEED = 7;
// A touch quicker than the timeline's own clock, so a game watched at the
// default speed takes about twenty minutes.
const BASE_RATE = 1.3;
// The court picture from LiveCourt, in px per foot. Big, so the lines stay
// sharp when the camera zooms in.
const COURT_PX = 16;
const COURT_PLANE: Plane = {
	key: "court",
	origin: { x: -5, y: -2.5, z: 0 },
	alongX: { x: 1 / COURT_PX, y: 0, z: 0 },
	alongY: { x: 0, y: 1 / COURT_PX, z: 0 },
	w: (COURT_W + 10) * COURT_PX,
	h: (COURT_H + 5) * COURT_PX,
};

const FaceLoader = ({
	pid,
	season,
	lid,
	onFace,
}: {
	pid: number;
	season: number | undefined;
	lid: number | undefined;
	onFace: (pid: number, face: PlayerFace | null) => void;
}) => {
	const face = usePlayerFace(pid, season, lid);
	useEffect(() => {
		if (face !== undefined) {
			onFace(pid, face);
		}
	}, [face, onFace, pid]);
	return null;
};

// A painted canvas, mounted as is.
const Painted = ({
	canvas,
	plane,
	setRef,
}: {
	canvas: HTMLCanvasElement;
	plane: Plane;
	setRef: (key: string, el: HTMLDivElement | null) => void;
}) => (
	<div
		ref={(el) => {
			setRef(plane.key, el);
			if (el && el.firstChild !== canvas) {
				canvas.style.display = "block";
				canvas.style.width = "100%";
				canvas.style.height = "100%";
				el.replaceChildren(canvas);
			}
		}}
		style={planeStyle(plane)}
	/>
);

const planeStyle = (plane: Plane) =>
	({
		position: "absolute",
		left: 0,
		top: 0,
		width: plane.w,
		height: plane.h,
		transformOrigin: "0 0",
		willChange: "transform",
		backfaceVisibility: "hidden",
		visibility: "hidden",
	}) as const;

type Props = {
	// The game's full play-by-play, never consumed.
	events: any[] | undefined;
	// How many of those the page has shown.
	cursor: number;
	boxScore: any;
	caption: ReactNode;
	paused: boolean;
	speed: number;
	// A multiplayer follower is stepped by the device in charge of simming, so
	// it never asks for the next line - it only keeps up.
	follower: boolean;
	onReady: () => void;
};

const Court25D = ({
	events,
	cursor,
	boxScore,
	caption,
	paused,
	speed,
	follower,
	onReady,
}: Props) => {
	const { lid, gender } = useLocal(["lid", "gender"]);
	const gid: number | undefined = boxScore?.gid;
	const season: number | undefined = boxScore?.season;
	const raw: any[] = Array.isArray(boxScore?.teams) ? boxScore.teams : [];
	// Display order, same as the 2D court: [away (attacks left), home].
	const away = raw[1];
	const home = raw[0];

	// The roster is fixed for the game - read it once.
	const roster = useMemo(() => {
		const out: (CourtPlayer & { name: string; jerseyNumber?: string })[] = [];
		const teams: [any, any] = [away, home];
		for (const t of [0, 1] as const) {
			for (const p of teams[t]?.players ?? []) {
				if (typeof p.pid === "number") {
					out.push({
						pid: p.pid,
						team: t,
						pos: p.pos,
						name: p.name,
						jerseyNumber: p.jerseyNumber,
					});
				}
			}
		}
		return out;
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);

	const timeline = useMemo(
		() =>
			events && events.length > 0
				? compileCourt({ events, players: roster, gid, gender })
				: undefined,
		[events, roster, gid, gender],
	);
	const clocks = useMemo(
		() => (timeline && events ? buildClocks(timeline, events) : undefined),
		[timeline, events],
	);

	const kits = useMemo(
		() => kitsFor(away?.colors, home?.colors),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);

	// The building, painted once a game.
	const paint = useMemo(() => {
		const a = {
			abbrev: away?.abbrev,
			name: away?.name,
			region: away?.region,
			colors: away?.colors,
		};
		const h = {
			abbrev: home?.abbrev,
			name: home?.name,
			region: home?.region,
			colors: home?.colors,
		};
		const table = paintTable(h, a);
		return {
			stands: paintStands(h, a, String(gid ?? 0)),
			wall: paintWall(h),
			tableTop: table.top,
			tableFront: table.front,
			bench0: paintBench(a),
			bench1: paintBench(h),
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);

	// Faces arrive asynchronously; until then a player has a plain head and an
	// average build.
	const faces = useRef(new Map<number, PlayerFace | null>());
	const heads = useRef(
		new Map<number, { sprite?: HeadSprite; skin?: string }>(),
	);
	const [facesVersion, setFacesVersion] = useState(0);
	const onFace = useCallback(
		(pid: number, face: PlayerFace | null) => {
			if (faces.current.get(pid) === face) {
				return;
			}
			faces.current.set(pid, face);
			setFacesVersion((v) => v + 1);
			const team = roster.find((p) => p.pid === pid)?.team;
			const colors = team === 0 ? away?.colors : home?.colors;
			void loadHead(face?.face, face?.imgURL, face?.colors ?? colors).then(
				(head) => {
					if (faces.current.get(pid) === face) {
						heads.current.set(pid, head);
						setFacesVersion((v) => v + 1);
					}
				},
			);
		},
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[roster],
	);

	const appearance = useMemo(() => {
		const looks = new Map<number, Look>();
		const bodies = new Map<number, Body>();
		for (const p of roster) {
			const f = faces.current.get(p.pid) ?? undefined;
			const head = heads.current.get(p.pid);
			const colors = headColors(f?.face);
			looks.set(p.pid, {
				kit: kits[p.team as Side],
				skin: head?.skin ?? colors.skin,
				hair: f?.imgURL ? "#1f1612" : colors.hair,
				jerseyNumber: f?.jerseyNumber ?? p.jerseyNumber ?? "",
				head: head?.sprite,
			});
			bodies.set(p.pid, bodyOf(f?.hgt, f?.weight));
		}
		return { looks, bodies };
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [roster, kits, facesVersion]);
	// Read by the animation loop, which outlives any one render.
	const looks = useRef(appearance.looks);
	const bodies = useRef(appearance.bodies);
	looks.current = appearance.looks;
	bodies.current = appearance.bodies;

	// The picture's size: 16:9, or 4:3 on a phone so the players stay big.
	const wrapRef = useRef<HTMLDivElement | null>(null);
	const canvasRef = useRef<HTMLCanvasElement | null>(null);
	const [size, setSize] = useState({ w: 640, h: 360 });
	useLayoutEffect(() => {
		const el = wrapRef.current;
		if (!el) {
			return;
		}
		const measure = () => {
			setSize({ w: el.clientWidth, h: el.clientHeight });
		};
		measure();
		const observer = new ResizeObserver(measure);
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, []);
	const narrow = size.w < 560;

	const planes = useRef(new Map<string, HTMLDivElement>());
	const setPlaneRef = useCallback((key: string, el: HTMLDivElement | null) => {
		if (el) {
			planes.current.set(key, el);
		} else {
			planes.current.delete(key);
		}
	}, []);
	const clockRef = useRef<HTMLSpanElement | null>(null);
	const shotRef = useRef<HTMLSpanElement | null>(null);

	const play = useRef({
		t: 0,
		camX: COURT_W / 2,
		camW: 60,
		snapCam: true,
		last: undefined as number | undefined,
		readyFor: -1,
		stepping: false,
		prevCursor: -1,
		prevPaused: paused,
		clockText: "",
		shotText: "",
	});

	// Follow the page's cursor: run on to the next line normally; cut straight
	// there on a rewind or a big jump ahead (fast-forward, joining late).
	useEffect(() => {
		if (!timeline) {
			return;
		}
		const s = play.current;
		const target = targetForCursor(timeline, cursor);
		if (s.prevCursor < 0) {
			s.t = snapForCursor(timeline, cursor);
			s.snapCam = true;
		} else if (cursor < s.prevCursor || target < s.t - 1) {
			s.t = snapForCursor(timeline, cursor);
			s.snapCam = true;
		} else if (cursor - s.prevCursor > 2 && target - s.t > 9000) {
			s.t = snapForCursor(timeline, cursor);
			s.snapCam = true;
		}
		if (paused && s.prevCursor >= 0 && cursor > s.prevCursor) {
			// "Next play" while paused: show that one play, then hold.
			s.stepping = true;
		}
		if (s.prevPaused && !paused) {
			s.readyFor = -1;
		}
		s.prevCursor = cursor;
		s.prevPaused = paused;
	}, [cursor, paused, timeline]);

	const homePad = home?.colors?.[0] ?? "#8c1d40";
	const warmups = useMemo(
		(): [string, string] => [
			shade(kits[0].jersey, -0.3),
			shade(kits[1].trim, -0.2),
		],
		[kits],
	);
	const rosterRef = useRef(roster);
	rosterRef.current = roster;
	const live = useRef({
		cursor,
		paused,
		speed,
		follower,
		onReady,
		timeline,
		clocks,
		size,
		narrow,
		homePad,
		warmups,
		eventsLength: events?.length ?? 0,
	});
	live.current = {
		cursor,
		paused,
		speed,
		follower,
		onReady,
		timeline,
		clocks,
		size,
		narrow,
		homePad,
		warmups,
		eventsLength: events?.length ?? 0,
	};

	useEffect(() => {
		const gloss = document.createElement("canvas");
		const glossCtx = gloss.getContext("2d");
		const lookOf = (pid: number) => looks.current.get(pid)!;
		const bodyOfPid = (pid: number) => bodies.current.get(pid) ?? bodyOf();
		const planeList: Plane[] = [
			STANDS,
			LED_WALL,
			FLOOR,
			COURT_PLANE,
			TABLE_TOP,
			TABLE_FRONT,
			benchPlane(0),
			benchPlane(1),
		];
		const shown = new Map<string, string | undefined>();

		const place = (cam: Camera) => {
			for (const pl of planeList) {
				const el = planes.current.get(pl.key);
				if (!el) {
					continue;
				}
				const tf = planeTransform(
					cam,
					pl.origin,
					pl.alongX,
					pl.alongY,
					pl.w,
					pl.h,
				);
				if (tf === shown.get(pl.key)) {
					continue;
				}
				shown.set(pl.key, tf);
				if (tf) {
					el.style.transform = tf;
					el.style.visibility = "visible";
				} else {
					el.style.visibility = "hidden";
				}
			}
		};

		const tick = (now: number, draw: boolean) => {
			const p = live.current;
			const tl = p.timeline;
			if (!tl) {
				return;
			}
			const s = play.current;
			const dt = s.last === undefined ? 0 : Math.min(100, now - s.last);
			s.last = now;
			const target = targetForCursor(tl, p.cursor);
			let rate = BASE_RATE * 1.2 ** (p.speed - DEFAULT_SPEED);
			if (p.follower) {
				// Behind the device in charge of simming: catch up, briskly.
				const lag = target - s.t;
				if (lag > 3000) {
					rate *= Math.min(6, 1 + (lag - 3000) / 2500);
				}
			}
			if ((!p.paused || s.stepping) && s.t < target) {
				s.t = Math.min(target, s.t + dt * rate);
			}
			if (s.t >= target) {
				s.stepping = false;
				if (
					!p.paused &&
					!p.follower &&
					p.cursor < p.eventsLength &&
					s.readyFor !== p.cursor
				) {
					s.readyFor = p.cursor;
					p.onReady();
				}
			}
			if (!draw) {
				return;
			}

			const canvas = canvasRef.current;
			const ctx = canvas?.getContext("2d");
			if (!canvas || !ctx || !glossCtx) {
				return;
			}
			const { w, h } = p.size;
			const dpr = Math.min(2, window.devicePixelRatio || 1);
			const cw = Math.round(w * dpr);
			const ch = Math.round(h * dpr);
			if (cw <= 0 || ch <= 0) {
				return;
			}
			if (canvas.width !== cw || canvas.height !== ch) {
				canvas.width = cw;
				canvas.height = ch;
			}

			const moment = momentAt(tl, s.t, rosterRef.current, bodyOfPid);
			const aim = aimFor(moment, p.narrow);
			if (s.snapCam) {
				s.camX = aim.x;
				s.camW = aim.width;
				s.snapCam = false;
			} else {
				const secs = (dt / 1000) * Math.min(4, Math.max(1, rate));
				s.camX += (aim.x - s.camX) * (1 - Math.exp(-secs * 2.6));
				s.camW += (aim.width - s.camW) * (1 - Math.exp(-secs * 1.5));
			}
			const cam = makeCamera({ x: s.camX, width: s.camW, y: aim.y }, w, h);
			place(cam);

			let shotText = "";
			let clockText = "";
			if (p.clocks) {
				const game = gameClockAt(p.clocks, s.t);
				const shot = shotClockAt(p.clocks, s.t);
				clockText = game === undefined ? "" : formatGameClock(game);
				shotText = shot === undefined ? "" : String(Math.ceil(shot - 1e-6));
			}
			drawFrame({
				ctx,
				glossCtx,
				cam,
				moment,
				tl,
				roster: rosterRef.current,
				bodyFor: bodyOfPid,
				lookFor: lookOf,
				padColor: p.homePad,
				warmups: p.warmups,
				shotClock: shotText,
				dpr,
			});
			if (clockText !== s.clockText && clockRef.current) {
				s.clockText = clockText;
				clockRef.current.textContent = clockText;
			}
			if (shotText !== s.shotText && shotRef.current) {
				s.shotText = shotText;
				shotRef.current.textContent = shotText;
				shotRef.current.style.visibility = shotText ? "visible" : "hidden";
			}
		};

		let raf = requestAnimationFrame(function frame(now) {
			tick(now, true);
			raf = requestAnimationFrame(frame);
		});
		// A background tab gets no animation frames. Keep the game moving anyway
		// (nothing to draw), or a hidden tab would hold up everyone watching.
		const interval = setInterval(() => {
			if (document.hidden) {
				tick(performance.now(), false);
			}
		}, 250);
		return () => {
			cancelAnimationFrame(raf);
			clearInterval(interval);
		};
	}, []);

	const awayPts = away?.pts ?? 0;
	const homePts = home?.pts ?? 0;
	const quarter = boxScore?.quarterShort ?? "";

	return (
		<div
			ref={wrapRef}
			className="mb-3"
			style={{
				position: "relative",
				width: "100%",
				aspectRatio: narrow ? "4 / 3" : "16 / 9",
				background: "#040406",
				borderRadius: 6,
				overflow: "hidden",
				containerType: "inline-size",
				isolation: "isolate",
			}}
		>
			{roster.map((p) => (
				<FaceLoader
					key={p.pid}
					pid={p.pid}
					season={season}
					lid={lid}
					onFace={onFace}
				/>
			))}
			<style>
				{
					".court25d-caption .text-body-secondary { color: #c9c3d3 !important; }"
				}
			</style>
			<div
				aria-hidden
				style={{
					position: "absolute",
					inset: 0,
					overflow: "hidden",
					pointerEvents: "none",
				}}
			>
				<Painted canvas={paint.stands} plane={STANDS} setRef={setPlaneRef} />
				<Painted canvas={paint.wall} plane={LED_WALL} setRef={setPlaneRef} />
				<div
					ref={(el) => {
						setPlaneRef(FLOOR.key, el);
					}}
					style={{
						...planeStyle(FLOOR),
						background: "linear-gradient(#17130f, #2b241d 30%, #2b241d)",
					}}
				/>
				<div
					ref={(el) => {
						setPlaneRef(COURT_PLANE.key, el);
					}}
					style={planeStyle(COURT_PLANE)}
				>
					<LiveCourt
						scene={undefined}
						teams={[away, home]}
						finals={!!boxScore?.finals}
						season={season}
						sceneMs={undefined}
					/>
				</div>
				<Painted
					canvas={paint.tableTop}
					plane={TABLE_TOP}
					setRef={setPlaneRef}
				/>
				<Painted
					canvas={paint.tableFront}
					plane={TABLE_FRONT}
					setRef={setPlaneRef}
				/>
				<Painted
					canvas={paint.bench0}
					plane={benchPlane(0)}
					setRef={setPlaneRef}
				/>
				<Painted
					canvas={paint.bench1}
					plane={benchPlane(1)}
					setRef={setPlaneRef}
				/>
			</div>
			<canvas
				ref={canvasRef}
				role="img"
				aria-label="Live court"
				style={{
					position: "absolute",
					inset: 0,
					width: "100%",
					height: "100%",
				}}
			/>
			<div
				style={{
					position: "absolute",
					left: "2cqw",
					top: "2cqw",
					display: "flex",
					alignItems: "stretch",
					fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
					fontWeight: 700,
					fontSize: "clamp(10px, 1.9cqw, 16px)",
					lineHeight: 1,
					borderRadius: 4,
					overflow: "hidden",
					boxShadow: "0 2px 8px rgba(0,0,0,.45)",
				}}
			>
				{[
					[away?.abbrev, awayPts, kits[0]],
					[home?.abbrev, homePts, kits[1]],
				].map(([abbrev, pts, kit]: any, i) => (
					<span
						key={i}
						style={{
							display: "flex",
							alignItems: "center",
							gap: "0.55em",
							padding: "0.5em 0.7em",
							background: i === 0 ? kit.jersey : kit.trim,
							color: "#fff",
							textShadow: "0 1px 1px rgba(0,0,0,.4)",
						}}
					>
						{abbrev}
						<span
							style={{
								fontVariantNumeric: "tabular-nums",
								fontSize: "1.15em",
							}}
						>
							{pts}
						</span>
					</span>
				))}
				<span
					style={{
						display: "flex",
						alignItems: "center",
						gap: "0.6em",
						padding: "0.5em 0.7em",
						background: "rgba(10, 10, 14, 0.9)",
						color: "#f4f4f4",
						fontVariantNumeric: "tabular-nums",
					}}
				>
					{quarter}
					<span ref={clockRef} />
					<span
						ref={shotRef}
						title="Shot clock"
						style={{ color: "#ffb547", minWidth: "1.3em" }}
					/>
				</span>
			</div>
			{caption ? (
				<div
					className="court25d-caption"
					style={{
						position: "absolute",
						left: "50%",
						bottom: "2.6cqw",
						transform: "translateX(-50%)",
						width: "max-content",
						maxWidth: "92%",
						textAlign: "center",
						fontSize: "clamp(10px, 1.85cqw, 16px)",
						lineHeight: 1.35,
						padding: "0.45em 1em",
						background: "rgba(8, 8, 12, 0.84)",
						color: "#f1ede6",
						borderLeft: `4px solid ${home?.colors?.[0] ?? "#888"}`,
						borderRadius: 3,
					}}
				>
					{caption}
				</div>
			) : null}
		</div>
	);
};

export default Court25D;
