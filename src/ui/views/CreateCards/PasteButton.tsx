import { useState } from "react";

// Reads the clipboard straight into a text field. Browsers that refuse
// programmatic clipboard reads (Firefox) throw, and there is nothing to do
// about it but say so and let the user press the keys.
export const PasteButton = ({
	onPaste,
	disabled,
}: {
	onPaste: (text: string) => void;
	disabled?: boolean;
}) => {
	const [blocked, setBlocked] = useState(false);

	return (
		<button
			type="button"
			className="btn btn-secondary btn-sm"
			disabled={disabled}
			title={
				blocked
					? "This browser blocks reading the clipboard - paste with the keyboard"
					: "Paste"
			}
			onClick={async () => {
				try {
					const text = await navigator.clipboard.readText();
					if (text.trim() !== "") {
						onPaste(text.trim());
					}
					setBlocked(false);
				} catch {
					setBlocked(true);
				}
			}}
		>
			Paste
		</button>
	);
};
