import type { FaceConfig } from "facesjs";
import type { Rig, Shot } from "./camera.ts";
import type { CrewRole } from "./crew.ts";
import type { Pt3 } from "./geometry.ts";
import type { HeadSprite } from "./faces.ts";
import type { Look } from "./figure.ts";
import type { IntroCardInfo } from "./IntroCard.tsx";
import type { Warmup } from "./pregame.ts";
import type { Body } from "./poses.ts";
import type { Moment } from "./scene.ts";

// A SCENE WITH NO GAME, AUTHORED - a draft pick walking out in his new
// team's cap (see draftWalk.ts): played by the court in place of the
// warm-ups, in the same building, from its own little timeline, its own
// camera and the clothes it puts its people in. It runs once and holds on
// its last moment.
export type Authored = {
	// The picture's timeline - the building, the lights - as the warm-ups
	// have theirs (see pregame.ts).
	show: Warmup;
	// How long it runs (ms).
	ms: number;
	// Everyone in it at t.
	moment: (t: number, bodyFor: (pid: number) => Body) => Moment;
	// Where the camera looks at t, and from where.
	aim: (t: number, narrow: boolean, m: Moment) => { shot: Shot; rig: Rig };
	// What a man in it wears, from how the game would have dressed him.
	dress?: (pid: number, look: Look) => Look;
	// His head drawn its own way (a cap on it), in place of the game's.
	head?: (
		pid: number,
		face: FaceConfig | undefined,
		colors: [string, string, string] | undefined,
	) => Promise<{ sprite?: HeadSprite; skin?: string }>;
	// Who of those working a game is there for it (see crew.ts) - everybody,
	// if unsaid.
	crew?: (role: CrewRole) => boolean;
	// Cameras going off at t, besides the photographers' own.
	flashes?: (t: number) => Pt3[];
	// The name card, up while the man in it is called (see IntroCard).
	card?: IntroCardInfo;
	// Run again from the top each time this changes.
	run?: number;
	onEnd?: () => void;
};
