import { useEffect } from "react";
import { usePlayerFace, type PlayerFace } from "../../../util/playerFaces.ts";

// Loads one player's face and build, and hands it back once it has it (null:
// he has none to load). Rendered once per player, out of sight, by whichever
// 3D picture needs them.
export const FaceLoader = ({
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
