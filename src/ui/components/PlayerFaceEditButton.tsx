import { useState } from "react";
import { useLocal } from "../util/local.ts";
import { usePlayerFace } from "../util/playerFaces.ts";
import { PlayerFaceModal } from "./PlayerFaceModal.tsx";

// A tiny inline button next to a player's name that opens the cartoon-face
// editor for him: paste a faces.js config, watch it draw as you type, adjust
// any slot with a control, save straight to the player. Used on list pages
// where you'd otherwise have to walk into Customize Player one at a time.
export const PlayerFaceEditButton = ({
	firstName,
	lastName,
	pid,
	season,
}: {
	firstName: string;
	lastName: string;
	pid: number;
	season?: number;
}) => {
	const [show, setShow] = useState(false);
	const { lid } = useLocal(["lid"]);

	// The same batched, cached fetch every table already uses for the little face
	// beside the name, so opening the editor costs no extra round trip on a row
	// that's already drawn one.
	const faceData = usePlayerFace(show ? pid : undefined, season, lid);

	return (
		<>
			<button
				aria-label={`Edit face for ${firstName} ${lastName}`}
				className="btn btn-light-bordered btn-xs flex-shrink-0 ms-1"
				onClick={() => {
					setShow(true);
				}}
				title="Edit face"
				type="button"
			>
				<span className="glyphicon glyphicon-user" />
			</button>
			{show ? (
				<PlayerFaceModal
					colors={faceData?.colors}
					imgURL={faceData?.imgURL}
					initialFace={faceData?.face}
					jersey={faceData?.jersey}
					// Remount when the face arrives, so the editor opens on the player's
					// real config instead of the random one it starts with while the
					// fetch is in flight.
					key={faceData?.face ? "loaded" : "loading"}
					name={`${firstName} ${lastName}`}
					onHide={() => {
						setShow(false);
					}}
					pid={pid}
				/>
			) : null}
		</>
	);
};

export default PlayerFaceEditButton;
