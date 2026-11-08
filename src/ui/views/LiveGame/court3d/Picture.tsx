import type { ReactNode, RefObject } from "react";
import type { PlayerFace } from "../../../util/playerFaces.ts";
import type { ReplayLooks, ScoreBugStyle } from "../../../../common/types.ts";
import { helpers } from "../../../util/helpers.ts";
import { STARTING_NUM_TIMEOUTS } from "../../../../common/constants.ts";
import { FaceLoader } from "./FaceLoader.tsx";
import { ScoreBug, type BugRefs } from "./ScoreBug.tsx";
import { CustomScoreBug, type CustomBugTeam } from "./CustomScoreBug.tsx";

// THE PICTURE ON THE PAGE, any sport's (see Court3D, Field3D): the box it
// is shown in - 16:9, or 4:3 on a phone so the players stay big - with each
// player's face asked for out of sight (a replay has that night's already:
// see useFaces), and in it the canvas the frames draw on, what is laid over
// it (see Overlays) and the score bug.

export const PictureBox = ({
	wrapRef,
	narrow,
	roster,
	looksThen,
	season,
	lid,
	onFace,
	children,
}: {
	wrapRef: RefObject<HTMLDivElement | null>;
	narrow: boolean;
	roster: { pid: number }[];
	looksThen: ReplayLooks | undefined;
	season: number | undefined;
	lid: number | undefined;
	onFace: (pid: number, face: PlayerFace | null) => void;
	children: ReactNode;
}) => (
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
		{roster
			.filter((p) => !looksThen?.players[p.pid])
			.map((p) => (
				<FaceLoader
					key={p.pid}
					pid={p.pid}
					season={season}
					lid={lid}
					onFace={onFace}
				/>
			))}
		<style>
			{".court3d-caption .text-body-secondary { color: #c9c3d3 !important; }"}
		</style>
		{children}
	</div>
);

export const PictureCanvas = ({
	canvasRef,
	label,
}: {
	canvasRef: RefObject<HTMLCanvasElement | null>;
	label: string;
}) => (
	<canvas
		ref={canvasRef}
		role="img"
		aria-label={label}
		style={{
			position: "absolute",
			inset: 0,
			width: "100%",
			height: "100%",
			imageRendering: "pixelated",
		}}
	/>
);

// A side as the bug carries it: from its team (as the page has it), its
// points, and its box score team (its timeouts left, and its record where
// the game keeps one).
export const bugTeamOf = (
	t: any,
	pts: number,
	bt:
		| {
				timeouts?: number;
				won?: number;
				lost?: number;
				tied?: number;
				otl?: number;
		  }
		| undefined,
): CustomBugTeam | undefined =>
	t && {
		abbrev: t.abbrev,
		region: t.region,
		name: t.name,
		colors: t.colors,
		imgURL: t.imgURL,
		imgURLSmall: t.imgURLSmall,
		pts,
		timeouts: bt?.timeouts,
		record:
			typeof bt?.won === "number" && typeof bt?.lost === "number"
				? helpers.formatRecord({ ...bt, won: bt.won, lost: bt.lost })
				: undefined,
	};

// THE SCORE BUG: the league's own, if it has one (see ScoreBugStyle), or
// the standard one - its clocks and marks written by the frames (see
// writeBug), and a playoff game's round and series across its top
// (`series` - the standard one's). `bugRef`: where it is, for what is kept
// clear of it.
export const GameBug = ({
	bugRef,
	own,
	away,
	home,
	quarter,
	series,
	refs,
	hidden,
	shotTitle,
}: {
	bugRef: RefObject<HTMLDivElement | null>;
	own: ScoreBugStyle | null | undefined;
	away: CustomBugTeam | undefined;
	home: CustomBugTeam | undefined;
	quarter: string;
	series?: string;
	refs: BugRefs;
	hidden?: boolean;
	shotTitle?: string;
}) => {
	const bug = {
		hidden,
		away,
		home,
		quarter,
		totalTimeouts: STARTING_NUM_TIMEOUTS,
		refs,
	};
	return (
		<div ref={bugRef} style={{ display: "contents" }}>
			{own ? (
				<CustomScoreBug style={own} {...bug} />
			) : (
				<ScoreBug {...bug} series={series} shotTitle={shotTitle} />
			)}
		</div>
	);
};
