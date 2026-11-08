import type { RefObject } from "react";
import { STING_SLANT, STING_W } from "./broadcast.ts";

// WHAT IS LAID OVER THE PICTURE, any sport's (see broadcast.ts showWipe,
// dipOf): the vignette round its edges; the black it dips to at a cut and
// the white it flashes to between a replay's angles; the wipe into a
// replay and out of it, one band in each side's colors with its logo; the
// REPLAY tag; and, the game still being staged, that it is not stuck.
export const Overlays = ({
	dipRef,
	flashRef,
	stingRefs,
	replayRef,
	teams,
	logos,
	staging,
}: {
	dipRef: RefObject<HTMLDivElement | null>;
	flashRef: RefObject<HTMLDivElement | null>;
	stingRefs: RefObject<[HTMLDivElement | null, HTMLDivElement | null]>;
	replayRef: RefObject<HTMLDivElement | null>;
	// [visitors, home]
	teams: [any, any];
	logos: [string | undefined, string | undefined];
	staging: boolean;
}) => (
	<>
		<div
			aria-hidden
			style={{
				position: "absolute",
				inset: 0,
				pointerEvents: "none",
				background:
					"radial-gradient(ellipse at 50% 55%, transparent 58%, rgba(0,0,0,0.32) 100%)",
			}}
		/>
		<div
			ref={dipRef}
			aria-hidden
			style={{
				position: "absolute",
				inset: 0,
				background: "#000",
				opacity: 0,
				pointerEvents: "none",
			}}
		/>
		<div
			ref={flashRef}
			aria-hidden
			style={{
				position: "absolute",
				inset: 0,
				background: "#fff",
				opacity: 0,
				pointerEvents: "none",
			}}
		/>
		{teams.map((team, side) => {
			const logo = logos[side];
			const main = team?.colors?.[0] ?? "#8c1d40";
			const accent = team?.colors?.[1] ?? "#f2c14e";
			return (
				<div
					key={side}
					ref={(el) => {
						stingRefs.current[side as 0 | 1] = el;
					}}
					aria-hidden
					style={{
						display: "none",
						position: "absolute",
						top: 0,
						left: 0,
						width: `${STING_W}%`,
						height: "100%",
						alignItems: "center",
						justifyContent: "center",
						pointerEvents: "none",
						willChange: "transform",
						// (Lit from above, a little.)
						background: `linear-gradient(180deg, rgba(255,255,255,0.16), rgba(0,0,0,0.22)), linear-gradient(90deg, #fff 0 2%, ${accent} 2% 7%, ${main} 7% 93%, ${accent} 93% 98%, #fff 98% 100%)`,
					}}
				>
					{logo ? (
						<img
							alt=""
							src={logo}
							style={{
								height: "34%",
								maxWidth: "24%",
								objectFit: "contain",
								transform: `skewX(${STING_SLANT}deg)`,
								filter: "drop-shadow(0 0.4cqw 0.6cqw rgba(0,0,0,0.35))",
							}}
						/>
					) : null}
				</div>
			);
		})}
		<div
			ref={replayRef}
			style={{
				display: "none",
				position: "absolute",
				// (Up in the corner the page's controls are not in.)
				left: "2cqw",
				top: "2cqw",
				padding: "0.35em 0.7em",
				fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
				fontWeight: 800,
				fontSize: "clamp(10px, 1.7cqw, 14px)",
				letterSpacing: "0.08em",
				color: "#fff",
				background: "rgba(10, 10, 14, 0.85)",
				borderLeft: `3px solid ${teams[1]?.colors?.[0] ?? "#888"}`,
				borderRadius: 3,
				pointerEvents: "none",
			}}
		>
			REPLAY
		</div>
		{staging ? (
			<div
				className="spinner-border text-light"
				role="status"
				aria-label="Loading"
				style={{
					position: "absolute",
					left: "50%",
					top: "50%",
					marginLeft: "-1rem",
					marginTop: "-1rem",
					opacity: 0.7,
					pointerEvents: "none",
				}}
			/>
		) : null}
	</>
);
