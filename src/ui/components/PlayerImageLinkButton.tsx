import { useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import { showNotification } from "../util/showNotification.ts";
import { updatePlayerFaceImage } from "../util/playerFaces.ts";

// A tiny inline button that sets a player's image from the URL on the
// clipboard in ONE click - no prompt, no dialog: copy a URL anywhere, then tap
// the button on the player's row. Used next to names on list pages (Player
// Ratings, Draft Scouting).
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
			aria-label={`Paste image URL for ${firstName} ${lastName}`}
			className="btn btn-light-bordered btn-xs flex-shrink-0 ms-1"
			disabled={saving}
			onClick={async () => {
				// The clipboard read is the FIRST thing here (no await before it) -
				// iOS Safari treats a read after any other await as outside the tap's
				// user-gesture and rejects it.
				let clipboardText: string;
				try {
					clipboardText = await navigator.clipboard.readText();
				} catch {
					showNotification({
						type: "error",
						text: "Couldn't read the clipboard — copy an image URL first.",
					});
					return;
				}

				const imgURL = clipboardText.trim();
				if (!imgURL) {
					showNotification({
						type: "error",
						text: "Clipboard is empty — copy an image URL first.",
					});
					return;
				}
				// With no confirmation step, guard against pasting whatever text
				// happened to be on the clipboard as a "URL".
				if (!/^(https?:\/\/|data:image\/)/i.test(imgURL)) {
					showNotification({
						type: "error",
						text: "The clipboard doesn't contain an image URL.",
					});
					return;
				}

				setSaving(true);
				try {
					const savedImgURL = await toWorker("main", "updatePlayerImage", {
						pid,
						imgURL,
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
				saving
					? "Saving player image…"
					: "Paste image URL from clipboard as this player's image"
			}
			type="button"
		>
			<span className="glyphicon glyphicon-link" />
		</button>
	);
};

export default PlayerImageLinkButton;
