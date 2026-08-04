import { useState } from "react";

// Copy-to-clipboard with a confirmation that decays, matching the copy button
// in the player image generator.
export const CopyPromptButton = ({
	label,
	text,
}: {
	label: string;
	text: string;
}) => {
	const [copied, setCopied] = useState(false);

	return (
		<button
			type="button"
			className="btn btn-secondary"
			onClick={async () => {
				try {
					await navigator.clipboard.writeText(text);
					setCopied(true);
					setTimeout(() => {
						setCopied(false);
					}, 2000);
				} catch {
					setCopied(false);
				}
			}}
		>
			{copied ? "Copied!" : label}
		</button>
	);
};
