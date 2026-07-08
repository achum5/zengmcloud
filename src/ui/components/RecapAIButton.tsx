import type { CSSProperties } from "react";
import { useLocal } from "../util/local.ts";
import { openRecapAI, recapAIProvider } from "../util/recapAIProvider.ts";

// The "Claude"/"ChatGPT" button in the recap Copy → AI → Paste flow. Opens the
// chosen AI's native app if possible (else its web page), in a new tab so the
// BBGM tab is never navigated away. The provider is a per-device Global Setting.
export const RecapAIButton = ({ style }: { style?: CSSProperties }) => {
	const ai = recapAIProvider(useLocal(["recapAIProvider"]).recapAIProvider);

	return (
		<a
			className="btn btn-sm btn-light-bordered"
			style={style}
			href={ai.url}
			target="_blank"
			rel="noopener noreferrer"
			title={ai.title}
			onClick={(event) => {
				// Providers with a custom app scheme need JS to try the app first; the
				// href stays as a plain new-tab web fallback (no-JS / scheme failure).
				if (ai.appUrl) {
					event.preventDefault();
					openRecapAI(ai);
				}
			}}
		>
			{ai.label}
		</a>
	);
};

export default RecapAIButton;
