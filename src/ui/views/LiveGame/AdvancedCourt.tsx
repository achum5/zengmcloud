import { useEffect, useRef } from "react";
import { BodyOnCourt, type CourtScene, type CourtTeam } from "./LiveCourt.tsx";
import { useLocal } from "../../util/local.ts";
import {
	actorTeam,
	createAdvancedPlan,
	sampleAdvancedPlan,
	type AdvancedFrame,
	type AdvancedPlan,
} from "./advancedCourtMotion.ts";
import { drawAdvancedCourt } from "./advancedCourtDrawing.ts";

// Basic player markers share the continuous timeline with the canvas ball.
// Update their transforms directly so faces do not rerender every frame.
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
	const markers = useRef(new Map<number, HTMLDivElement>());
	const runtime = useRef<{
		plan?: AdvancedPlan;
		frame?: AdvancedFrame;
		progress: number;
		draw?: () => void;
	}>({ progress: 0 });

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
				// Ball offsets stay readable at phone widths.
				Math.max(1, Math.min(1.6, 5.5 / (width / 104))),
			);
			for (const player of state.frame.players) {
				const marker = markers.current.get(player.pid);
				if (marker) {
					marker.style.transform = `translate3d(${((player.x + 5) / 104) * width}px, ${((player.y + 2.5) / 104) * width}px, 0)`;
				}
			}
			drawAdvancedCourt(
				ctx,
				state.frame,
				width,
				height,
				dpr,
				media.matches
					? []
					: [0.12, 0.09, 0.06, 0.03].map(
							(offset) =>
								sampleAdvancedPlan(
									plan,
									Math.max(0, state.progress - offset),
									Math.max(1, Math.min(1.6, 5.5 / (width / 104))),
								).ball,
						),
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
					zIndex: 6,
				}}
			/>
			{scene?.actors
				.filter(
					(actor, index, actors) =>
						actors.findIndex((a) => a.pid === actor.pid) === index,
				)
				.map((actor) => {
					const t = actorTeam(actor, scene);
					return (
						<div
							key={actor.pid}
							data-court-player={actor.pid}
							ref={(element) => {
								if (element) {
									markers.current.set(actor.pid, element);
								} else {
									markers.current.delete(actor.pid);
								}
							}}
							style={{
								position: "absolute",
								left: 0,
								top: 0,
								zIndex: actor.role === "onCourt" ? 2 : 4,
								pointerEvents: "none",
							}}
						>
							<BodyOnCourt
								actor={actor}
								tracked
								season={season}
								lid={lid}
								color={
									teams[t]?.colors?.[0] ?? (t === 0 ? "#fd7e14" : "#0d6efd")
								}
								background={actor.role === "onCourt"}
								nameAbove={
									scene.actors.some(
										(other) =>
											other.pid !== actor.pid &&
											other.role !== "onCourt" &&
											Math.abs(other.x - actor.x) < 9,
									)
										? actor.role !== "main"
										: actor.y > 41
								}
								size={undefined}
								sceneMs={0}
							/>
						</div>
					);
				})}
		</>
	);
};

export default AdvancedCourt;
