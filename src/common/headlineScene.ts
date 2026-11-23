// A HEADLINE'S OWN LITTLE SCENE in the 3D building (see HeadlineScene.tsx):
// what it shows. A draft pick walking out in his new team's cap; a team's
// press conference introducing who it traded for or signed (see the worker's
// pressConference.ts). A big night and a game winner each join the union as
// they are made.
export type HeadlineSceneSpec =
	| {
			type: "draftPick";
			pid: number;
	  }
	| {
			type: "pressConference";
			// The move, and the team holding it.
			eid: number;
			tid: number;
	  };

// Which scene it is, by what it says (not which object says it).
export const sceneKeyOf = (scene: HeadlineSceneSpec): string =>
	scene.type === "draftPick"
		? `draftPick:${scene.pid}`
		: `pressConference:${scene.eid}:${scene.tid}`;
