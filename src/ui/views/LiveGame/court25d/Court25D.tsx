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
import {
	compileCourt,
	snapForCursor,
	targetForCursor,
	type CourtPlayer,
} from "./director.ts";
import { evalBall } from "./evaluate.ts";
import { VIEW_H, viewWidthFor, type Side } from "./geometry.ts";
import { bodyOf, type Body } from "./poses.ts";
import { buildArena, cameraTarget, drawFrame } from "./render.ts";
import { lookFor, SpriteCache, uniformsFor, type Look } from "./sprites.ts";

// THE 2.5D COURT: players acting out the play-by-play from a raised
// sideline camera, in place of the 2D court when this device has chosen it.
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

const lastNameTag = (name: string | undefined): string => {
	const parts = (name ?? "").trim().split(/\s+/);
	return (parts.length > 1 ? parts.slice(1).join(" ") : (parts[0] ?? ""))
		.toUpperCase()
		.replaceAll(/[^\d '.A-Z-]/g, "")
		.slice(0, 12);
};

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
				? compileCourt({
						events,
						players: roster,
						seed: String(gid ?? 0),
						dunkRate: gender === "female" ? 0.03 : 0.55,
					})
				: undefined,
		[events, roster, gid, gender],
	);

	const arena = useMemo(
		() =>
			buildArena(
				{ abbrev: away?.abbrev, name: away?.name, colors: away?.colors },
				{ abbrev: home?.abbrev, name: home?.name, colors: home?.colors },
			),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);
	const uniforms = useMemo(
		() => uniformsFor(away?.colors, home?.colors),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);

	// Faces arrive asynchronously; until then a player wears his uniform with a
	// stock skin tone and haircut, and an average build.
	const faces = useRef(new Map<number, PlayerFace | null>());
	const [facesVersion, setFacesVersion] = useState(0);
	const onFace = useCallback((pid: number, face: PlayerFace | null) => {
		if (faces.current.get(pid) !== face) {
			faces.current.set(pid, face);
			setFacesVersion((v) => v + 1);
		}
	}, []);

	const appearance = useMemo(() => {
		const nextLooks = new Map<number, Look>();
		const nextBodies = new Map<number, Body>();
		for (const p of roster) {
			const f = faces.current.get(p.pid) ?? undefined;
			nextLooks.set(
				p.pid,
				lookFor({
					pid: p.pid,
					face: f?.face,
					uniform: uniforms[p.team as Side],
					jerseyNumber: f?.jerseyNumber ?? p.jerseyNumber,
				}),
			);
			nextBodies.set(p.pid, bodyOf(f?.hgt, f?.weight));
		}
		return { looks: nextLooks, bodies: nextBodies };
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [roster, uniforms, facesVersion]);
	// Read by the animation loop, which outlives any one render.
	const looks = useRef(appearance.looks);
	const bodies = useRef(appearance.bodies);
	looks.current = appearance.looks;
	bodies.current = appearance.bodies;
	const tags = useMemo(
		() => new Map(roster.map((p) => [p.pid, lastNameTag(p.name)])),
		[roster],
	);

	// How much floor fits: a tighter camera on a narrow screen.
	const wrapRef = useRef<HTMLDivElement | null>(null);
	const canvasRef = useRef<HTMLCanvasElement | null>(null);
	const [viewW, setViewW] = useState(384);
	useLayoutEffect(() => {
		const el = wrapRef.current;
		if (!el) {
			return;
		}
		const measure = () => {
			setViewW(viewWidthFor(el.clientWidth));
		};
		measure();
		const observer = new ResizeObserver(measure);
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, []);

	const play = useRef({
		t: 0,
		camX: 47,
		snapCam: true,
		last: undefined as number | undefined,
		readyFor: -1,
		stepping: false,
		prevCursor: -1,
		prevPaused: paused,
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

	const live = useRef({
		cursor,
		paused,
		speed,
		follower,
		onReady,
		timeline,
		viewW,
		arena,
		eventsLength: events?.length ?? 0,
	});
	live.current = {
		cursor,
		paused,
		speed,
		follower,
		onReady,
		timeline,
		viewW,
		arena,
		eventsLength: events?.length ?? 0,
	};

	const sprites = useMemo(() => new SpriteCache(), []);

	useEffect(() => {
		const buffer = document.createElement("canvas");
		const lookOf = (pid: number) => looks.current.get(pid)!;
		const bodyOfPid = (pid: number) => bodies.current.get(pid) ?? bodyOf();
		const tagOf = (pid: number) => tags.get(pid) ?? "";

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
			const ctx = buffer.getContext("2d");
			if (!canvas || !ctx) {
				return;
			}
			if (buffer.width !== p.viewW || buffer.height !== VIEW_H) {
				buffer.width = p.viewW;
				buffer.height = VIEW_H;
				s.snapCam = true;
			}
			const ball = evalBall(tl, s.t, bodyOfPid);
			const camTarget = cameraTarget(tl, s.t, ball, p.viewW);
			s.camX = s.snapCam
				? camTarget
				: s.camX +
					(camTarget - s.camX) *
						(1 - Math.exp(-(dt / 1000) * 3.2 * Math.min(4, rate)));
			s.snapCam = false;
			drawFrame({
				ctx,
				viewW: p.viewW,
				camX: s.camX,
				t: s.t,
				tl,
				arena: p.arena,
				sprites,
				lookFor: lookOf,
				bodyFor: bodyOfPid,
				tagFor: tagOf,
			});

			const dpr = Math.min(3, window.devicePixelRatio || 1);
			const w = Math.round(canvas.clientWidth * dpr);
			const h = Math.round(canvas.clientHeight * dpr);
			if (w > 0 && h > 0) {
				if (canvas.width !== w || canvas.height !== h) {
					canvas.width = w;
					canvas.height = h;
				}
				const vctx = canvas.getContext("2d");
				if (vctx) {
					vctx.imageSmoothingEnabled = false;
					vctx.drawImage(buffer, 0, 0, w, h);
				}
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
	}, [sprites, tags]);

	const awayPts = away?.pts ?? 0;
	const homePts = home?.pts ?? 0;
	const clock =
		`${boxScore?.quarterShort ?? ""} ${boxScore?.time ?? ""}`.trim();

	return (
		<div
			ref={wrapRef}
			className="mb-3"
			style={{
				position: "relative",
				width: "100%",
				aspectRatio: `${viewW} / ${VIEW_H}`,
				background: "#07060a",
				borderRadius: 6,
				overflow: "hidden",
				containerType: "inline-size",
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
					".court25d-caption .text-body-secondary { color: #b9b1c6 !important; }"
				}
			</style>
			<canvas
				ref={canvasRef}
				role="img"
				aria-label="Live court"
				style={{
					position: "absolute",
					inset: 0,
					width: "100%",
					height: "100%",
					imageRendering: "pixelated",
				}}
			/>
			<div
				style={{
					position: "absolute",
					left: "1.8cqw",
					top: "1.8cqw",
					display: "flex",
					fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
					fontWeight: 700,
					fontSize: "clamp(9px, 2cqw, 15px)",
					lineHeight: 1,
					boxShadow: "0 2px 0 rgba(0,0,0,.45)",
				}}
			>
				{[
					[away?.abbrev, awayPts, uniforms[0]],
					[home?.abbrev, homePts, uniforms[1]],
				].map(([abbrev, pts, u]: any, i) => (
					<span
						key={i}
						style={{
							background: u.jersey,
							color: u.numberColor,
							padding: "0.45em 0.7em",
							display: "flex",
							gap: "0.6em",
						}}
					>
						{abbrev}
						<span style={{ fontVariantNumeric: "tabular-nums" }}>{pts}</span>
					</span>
				))}
				{clock ? (
					<span
						style={{
							background: "#0b0a0f",
							color: "#ffb547",
							padding: "0.45em 0.7em",
							fontVariantNumeric: "tabular-nums",
						}}
					>
						{clock}
					</span>
				) : null}
			</div>
			{caption ? (
				<div
					className="court25d-caption"
					style={{
						position: "absolute",
						left: "50%",
						bottom: "2.2cqw",
						transform: "translateX(-50%)",
						width: "max-content",
						maxWidth: "94%",
						textAlign: "center",
						fontSize: "clamp(10px, 1.9cqw, 15px)",
						lineHeight: 1.35,
						padding: "0.45em 0.9em",
						background: "rgba(9, 8, 13, 0.82)",
						color: "#ece6da",
						border: "1px solid rgba(255,255,255,0.14)",
					}}
				>
					{caption}
				</div>
			) : null}
		</div>
	);
};

export default Court25D;
