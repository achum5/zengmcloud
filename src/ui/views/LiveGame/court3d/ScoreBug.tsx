import type { RefObject } from "react";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { lightness } from "./figure.ts";

// THE SCORE BUG: the bar along the bottom of the picture, the way the
// broadcasts carry it - each side's logo, abbreviation and score in its own
// colors, then the period, the game clock and the shot clock. Under each side,
// its timeouts left and its fouls this period, or BONUS once the other side has
// fouled enough to put it there; the side with the ball is marked.
//
// The clocks, the fouls and the ball change every frame and are written
// straight into the refs below by the court's own frame loop, not rendered.

export type BugTeam = {
	abbrev?: string;
	colors?: [string, string, string];
	imgURL?: string;
	imgURLSmall?: string;
	pts: number;
	// Timeouts left, if the game keeps them.
	timeouts?: number;
};

export type BugRefs = {
	clock: RefObject<HTMLSpanElement | null>;
	shot: RefObject<HTMLSpanElement | null>;
	// Per side, in the bug's order: [visitors, home].
	fouls: [RefObject<HTMLSpanElement | null>, RefObject<HTMLSpanElement | null>];
	ball: [RefObject<HTMLSpanElement | null>, RefObject<HTMLSpanElement | null>];
};

const FONT = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";
const MAX_TIMEOUTS = 7;

const Side = ({
	team,
	foulsRef,
	ballRef,
	totalTimeouts,
}: {
	team: BugTeam | undefined;
	foulsRef: BugRefs["fouls"][0];
	ballRef: BugRefs["ball"][0];
	totalTimeouts: number | undefined;
}) => {
	const main = team?.colors?.[0] ?? "#3a3a44";
	const accent = team?.colors?.[1] ?? "#9a9aa6";
	// White on a dark color, dark on a light one.
	const light = lightness(main) > 0.62;
	const ink = light ? "#111" : "#fff";
	const left = team?.timeouts;
	return (
		<div style={{ display: "flex", flexDirection: "column" }}>
			<div
				style={{
					display: "flex",
					alignItems: "center",
					height: "2.1em",
					background: main,
					color: ink,
					borderBottom: `0.16em solid ${accent}`,
				}}
			>
				<span
					style={{
						display: "flex",
						alignItems: "center",
						justifyContent: "center",
						alignSelf: "stretch",
						padding: "0 0.3em",
						background: "rgba(255, 255, 255, 0.92)",
					}}
				>
					<TeamLogoInline
						alt={team?.abbrev}
						imgURL={team?.imgURL}
						imgURLSmall={team?.imgURLSmall}
						includePlaceholderIfNoLogo
						style={{ height: "1.6em", width: "1.6em" }}
					/>
				</span>
				<span
					style={{
						display: "flex",
						alignItems: "center",
						gap: "0.3em",
						padding: "0 0.55em",
						minWidth: "3.6em",
						letterSpacing: "0.04em",
					}}
				>
					<span
						ref={ballRef}
						title="Possession"
						style={{
							width: 0,
							height: 0,
							borderTop: "0.3em solid transparent",
							borderBottom: "0.3em solid transparent",
							borderLeft: `0.42em solid ${ink}`,
							visibility: "hidden",
						}}
					/>
					{team?.abbrev ?? ""}
				</span>
				<span
					style={{
						display: "flex",
						alignItems: "center",
						justifyContent: "center",
						alignSelf: "stretch",
						minWidth: "2.2em",
						padding: "0 0.45em",
						fontSize: "1.25em",
						fontWeight: 800,
						fontVariantNumeric: "tabular-nums",
						background: light ? "rgba(0, 0, 0, 0.1)" : "rgba(0, 0, 0, 0.28)",
					}}
				>
					{team?.pts ?? 0}
				</span>
			</div>
			<div
				style={{
					display: "flex",
					alignItems: "center",
					justifyContent: "space-between",
					gap: "0.6em",
					height: "1.25em",
					padding: "0 0.45em",
					background: "rgba(10, 10, 14, 0.9)",
					color: "#d8d4de",
					fontSize: "max(7.5px, 0.72em)",
					letterSpacing: "0.06em",
				}}
			>
				<span
					title={
						left === undefined
							? undefined
							: `${left} timeout${left === 1 ? "" : "s"} left`
					}
					style={{ display: "flex", gap: "0.25em" }}
				>
					{left === undefined || totalTimeouts === undefined
						? null
						: Array.from(
								{ length: Math.min(MAX_TIMEOUTS, totalTimeouts) },
								(_, i) => (
									<span
										key={i}
										style={{
											width: "0.75em",
											height: "0.3em",
											borderRadius: 1,
											background:
												i < left ? "#f2c14e" : "rgba(255,255,255,.18)",
										}}
									/>
								),
							)}
				</span>
				<span ref={foulsRef} style={{ fontVariantNumeric: "tabular-nums" }} />
			</div>
		</div>
	);
};

export const ScoreBug = ({
	away,
	home,
	quarter,
	totalTimeouts,
	refs,
}: {
	away: BugTeam | undefined;
	home: BugTeam | undefined;
	quarter: string;
	// How many timeouts each side started the period's stretch with, for the
	// pips.
	totalTimeouts: number | undefined;
	refs: BugRefs;
}) => (
	<div
		style={{
			position: "absolute",
			left: "50%",
			bottom: "1.6cqw",
			transform: "translateX(-50%)",
			display: "flex",
			alignItems: "stretch",
			fontFamily: FONT,
			fontWeight: 700,
			fontSize: "clamp(10px, 1.75cqw, 16px)",
			lineHeight: 1,
			borderRadius: 4,
			overflow: "hidden",
			boxShadow: "0 3px 10px rgba(0,0,0,.5)",
			whiteSpace: "nowrap",
		}}
	>
		<Side
			team={away}
			foulsRef={refs.fouls[0]}
			ballRef={refs.ball[0]}
			totalTimeouts={totalTimeouts}
		/>
		<Side
			team={home}
			foulsRef={refs.fouls[1]}
			ballRef={refs.ball[1]}
			totalTimeouts={totalTimeouts}
		/>
		<div
			style={{
				display: "flex",
				alignItems: "center",
				gap: "0.7em",
				padding: "0 0.8em",
				background: "rgba(10, 10, 14, 0.94)",
				color: "#f4f4f4",
				fontVariantNumeric: "tabular-nums",
				fontSize: "1.1em",
			}}
		>
			<span style={{ color: "#bdb8c6", fontSize: "0.85em" }}>{quarter}</span>
			<span ref={refs.clock} style={{ minWidth: "2.6em" }} />
		</div>
		<span
			ref={refs.shot}
			title="Shot clock"
			style={{
				// Shown by the frame loop while the shot clock runs.
				display: "none",
				alignItems: "center",
				justifyContent: "center",
				minWidth: "1.9em",
				padding: "0 0.35em",
				background: "#1c1408",
				color: "#ffb547",
				fontVariantNumeric: "tabular-nums",
				fontSize: "1.1em",
			}}
		/>
	</div>
);
