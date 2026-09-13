import { useEffect, useRef, useCallback } from "react";
import type { CourtScene, CourtTeam } from "./LiveCourt.tsx";
import { usePlayerFace, type PlayerFace } from "../../util/playerFaces.ts";
import { useLocal } from "../../util/local.ts";
import {
	createAdvancedPlan,
	sampleAdvancedPlan,
	type AdvancedFrame,
	type AdvancedPlan,
} from "./advancedCourtMotion.ts";
import { drawAdvancedCourt } from "./advancedCourtDrawing.ts";

const PlayerAppearance = ({
	pid,
	season,
	lid,
	onLoad,
}: {
	pid: number;
	season: number | undefined;
	lid: number | undefined;
	onLoad: (pid: number, face: PlayerFace) => void;
}) => {
	const face = usePlayerFace(pid, season, lid);
	useEffect(() => {
		if (face) {
			onLoad(pid, face);
		}
	}, [pid, face, onLoad]);
	return null;
};

// One canvas for all moving objects; the existing branded SVG floor stays on
// its own static layer. React updates at event boundaries, never per frame.
const AdvancedCourt = ({
	scene,
	teams,
	season,
	sceneMs,
	paused,
}: {
	scene: CourtScene | undefined;
	teams: [CourtTeam | undefined, CourtTeam | undefined];
	season: number | undefined;
	sceneMs: number | undefined;
	paused: boolean;
}) => {
	const { lid } = useLocal(["lid"]);
	const canvas = useRef<HTMLCanvasElement>(null);
	const appearances = useRef(new Map<number, PlayerFace>());
	const runtime = useRef<{
		plan?: AdvancedPlan;
		frame?: AdvancedFrame;
		progress: number;
		draw?: () => void;
	}>({ progress: 0 });
	const onLoad = useCallback((pid: number, face: PlayerFace) => {
		appearances.current.set(pid, face);
		runtime.current.draw?.();
	}, []);

	useEffect(() => {
		const element = canvas.current;
		const ctx = element?.getContext("2d");
		if (!element || !ctx) {
			return;
		}
		const state = runtime.current;
		const newScene = state.plan?.scene !== scene;
		if (!scene) {
			state.plan = undefined;
			state.frame = undefined;
			ctx.clearRect(0, 0, element.width, element.height);
			return;
		}
		if (newScene) {
			state.plan = createAdvancedPlan(scene, state.frame);
			// A paused step/seek displays its landing state. Pausing an existing
			// scene preserves its exact frame; resuming continues from that frame.
			state.progress = paused ? 1 : 0;
		}
		const plan = state.plan!;
		const duration = Math.max(16, scene.ms ?? sceneMs ?? 1100);
		let raf: number | undefined;
		let lastTime: number | undefined;
		const media = window.matchMedia("(prefers-reduced-motion: reduce)");
		const draw = () => {
			const width = element.getBoundingClientRect().width;
			if (width <= 0) {
				return;
			}
			const height = (width * 55) / 104;
			const dpr = Math.min(window.devicePixelRatio || 1, 2);
			if (
				element.width !== Math.round(width * dpr) ||
				element.height !== Math.round(height * dpr)
			) {
				element.width = Math.round(width * dpr);
				element.height = Math.round(height * dpr);
			}
			state.frame = sampleAdvancedPlan(
				plan,
				media.matches ? 1 : state.progress,
				// Keep full bodies readable on a phone without changing court positions.
				Math.max(1, Math.min(1.6, 5.5 / (width / 104))),
			);
			drawAdvancedCourt(
				ctx,
				state.frame,
				teams,
				appearances.current,
				width,
				height,
				dpr,
			);
		};
		state.draw = draw;
		const tick = (time: number) => {
			if (lastTime !== undefined && !document.hidden) {
				state.progress = Math.min(
					1,
					state.progress + (time - lastTime) / duration,
				);
			}
			lastTime = time;
			draw();
			if (!paused && !media.matches && !document.hidden && state.progress < 1) {
				raf = requestAnimationFrame(tick);
			}
		};
		const restart = () => {
			if (raf !== undefined) {
				cancelAnimationFrame(raf);
			}
			lastTime = undefined;
			draw();
			if (!paused && !media.matches && !document.hidden && state.progress < 1) {
				raf = requestAnimationFrame(tick);
			}
		};
		const observer = new ResizeObserver(draw);
		observer.observe(element);
		document.addEventListener("visibilitychange", restart);
		media.addEventListener("change", restart);
		restart();
		return () => {
			if (raf !== undefined) {
				cancelAnimationFrame(raf);
			}
			observer.disconnect();
			document.removeEventListener("visibilitychange", restart);
			media.removeEventListener("change", restart);
			state.draw = undefined;
		};
	}, [scene, sceneMs, paused, teams]);

	return (
		<>
			<canvas
				ref={canvas}
				data-testid="advanced-court"
				aria-label="Advanced 2D basketball court"
				role="img"
				style={{
					position: "absolute",
					top: 0,
					left: 0,
					width: "100%",
					aspectRatio: "104 / 55",
					pointerEvents: "none",
				}}
			/>
			{scene?.actors.map((actor) => (
				<PlayerAppearance
					key={actor.pid}
					pid={actor.pid}
					season={season}
					lid={lid}
					onLoad={onLoad}
				/>
			))}
		</>
	);
};

export default AdvancedCourt;
