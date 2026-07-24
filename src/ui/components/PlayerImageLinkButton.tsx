import { useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import { showNotification } from "../util/showNotification.ts";
import { updatePlayerFaceImage } from "../util/playerFaces.ts";

// A tiny inline button that replaces a player's image from a pasted URL,
// without leaving the page. Used next to names on list pages (Player Ratings,
// Draft Scouting).
export const PlayerImageLinkButton = ({
	firstName,
	lastName,
	pid,
}: {
	firstName: string;
	lastName: string;
	pid: number;
}) => {
	const [saving, setSaving] = useState(false);

	return (
		<button
			aria-label={`Replace image for ${firstName} ${lastName}`}
			className="btn btn-light-bordered btn-xs flex-shrink-0 ms-1"
			disabled={saving}
			onClick={async () => {
				const imgURL = window.prompt(
					`Paste a new image URL for ${firstName} ${lastName}:`,
				);
				if (imgURL === null) {
					return;
				}

				const trimmedImgURL = imgURL.trim();
				if (!trimmedImgURL) {
					showNotification({
						type: "error",
						text: "Enter an image URL.",
					});
					return;
				}

				setSaving(true);
				try {
					const savedImgURL = await toWorker("main", "updatePlayerImage", {
						pid,
						imgURL: trimmedImgURL,
					});
					updatePlayerFaceImage(pid, savedImgURL);
					showNotification({
						type: "success",
						text: `Image saved for ${firstName} ${lastName}.`,
					});
				} catch (error) {
					showNotification({
						type: "error",
						text: error.message,
					});
				} finally {
					setSaving(false);
				}
			}}
			title={
				saving ? "Saving player image…" : "Replace player image from a URL"
			}
			type="button"
		>
			<span className="glyphicon glyphicon-link" />
		</button>
	);
};

export default PlayerImageLinkButton;
