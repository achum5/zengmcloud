import type { FaceConfig } from "facesjs";
import type { ReactNode } from "react";
import type { Rig, Shot } from "./camera.ts";
import type { CrewMember, CrewRole } from "./crew.ts";
import type { PlayerState } from "./evaluate.ts";
import type { Pt3 } from "./geometry.ts";
import type { HeadSprite } from "./faces.ts";
import type { Look } from "./figure.ts";
import type { IntroCardInfo } from "./IntroCard.tsx";
import type { Warmup } from "./pregame.ts";
import type { Body } from "./poses.ts";
import type { Moment, Venue } from "./scene.ts";

// A SCENE WITH NO GAME, AUTHORED - a draft pick called up onto the stage
// (see draftStage.ts): played by the court in place of the warm-ups, in the
// same building, from its own little timeline, its own camera and the clothes
// it puts its people in. It runs once and holds on its last moment.
export type Authored = {
	// The picture's timeline - the building, the lights - as the warm-ups
	// have theirs (see pregame.ts).
	show: Warmup;
	// How long it runs (ms).
	ms: number;
	// Everyone in it at t.
	moment: (t: number, bodyFor: (pid: number) => Body) => Moment;
	// Where the camera looks at t, and from where - `close`: in close on a
	// man, the building behind him out of focus.
	aim: (
		t: number,
		narrow: boolean,
		m: Moment,
	) => { shot: Shot; rig: Rig; close?: boolean };
	// What a man in it wears, from how the game would have dressed him.
	dress?: (pid: number, look: Look) => Look;
	// What somebody wears at t, from what they are dressed in (see dress):
	// the same look each time for the same, so the pictures of them kept by
	// it are.
	lookAt?: (pid: number, t: number, look: Look) => Look;
	// His head drawn its own way (a cap on it), in place of the game's.
	head?: (
		pid: number,
		face: FaceConfig | undefined,
		colors: [string, string, string] | undefined,
	) => Promise<{ sprite?: HeadSprite; skin?: string }>;
	// Who of those working a game is there for it (see crew.ts) - everybody,
	// if unsaid.
	crew?: (role: CrewRole) => boolean;
	// People of its own, drawn as those working a game are (their faces,
	// their clothes - pids below any of theirs), and where each is at t.
	cast?: CrewMember[];
	castAt?: (t: number) => PlayerState[];
	// Their heads all drawn (see Court3D): the scene can be shown.
	onCast?: () => void;
	// The floor dressed as it has it, not for a game (see scene.ts).
	venue?: Venue;
	// Cameras going off at t, besides the photographers' own.
	flashes?: (t: number) => Pt3[];
	// The name card, up while the man in it is called (see IntroCard).
	card?: IntroCardInfo;
	// Graphics of its own over the picture (a draft pick's broadcast - see
	// DraftBroadcast.tsx), in the picture's box. Kept the same element from
	// draw to draw: what it shows changes through onTick, not through this.
	overlay?: ReactNode;
	// Each frame, where the scene is (ms) - for the overlay to keep time with
	// it, paused and held at its end with it.
	onTick?: (t: number) => void;
	// Run again from the top each time this changes.
	run?: number;
	onEnd?: () => void;
};
