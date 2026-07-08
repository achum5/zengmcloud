import type { RecapAIProvider } from "../../common/types.ts";

// The AI site the recap "Copy → [AI] → Paste" buttons open. Chosen per-device in
// Global Settings; the copy/paste flow itself is identical for either.
export const RECAP_AI_PROVIDERS: Record<
	RecapAIProvider,
	{ label: string; url: string; title: string }
> = {
	claude: {
		label: "Claude",
		url: "https://claude.ai/new",
		title: "Open Claude in a new tab",
	},
	chatgpt: {
		label: "ChatGPT",
		url: "https://chatgpt.com/",
		title: "Open ChatGPT in a new tab",
	},
};

export const recapAIProvider = (provider: RecapAIProvider | undefined) =>
	RECAP_AI_PROVIDERS[provider ?? "claude"] ?? RECAP_AI_PROVIDERS.claude;
