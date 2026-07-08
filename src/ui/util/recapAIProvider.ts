import type { RecapAIProvider } from "../../common/types.ts";

// The AI the recap "Copy → [AI] → Paste" buttons open. Chosen per-device in
// Global Settings; the copy/paste flow itself is identical for either.
//
// `url` is the web page (also a universal link that opens the native app on
// mobile when the site has one configured - Claude does, so it needs nothing
// more). `appUrl`, when set, is a custom URL scheme that opens the app directly;
// ChatGPT has no working universal link for a new chat, so we try its
// `chatgpt://` scheme first and fall back to the web (see openRecapAI).
export const RECAP_AI_PROVIDERS: Record<
	RecapAIProvider,
	{ label: string; url: string; appUrl?: string; title: string }
> = {
	claude: {
		label: "Claude",
		url: "https://claude.ai/new",
		title: "Open Claude (app if installed)",
	},
	chatgpt: {
		label: "ChatGPT",
		url: "https://chatgpt.com/",
		appUrl: "chatgpt://",
		title: "Open ChatGPT (app if installed)",
	},
};

export const recapAIProvider = (provider: RecapAIProvider | undefined) =>
	RECAP_AI_PROVIDERS[provider ?? "claude"] ?? RECAP_AI_PROVIDERS.claude;

// Open the chosen AI, preferring its native app. For a provider whose web URL is
// itself an app-opening universal link (Claude), just open it in a new tab - the
// OS routes it to the app if installed, else the web. For one that needs a custom
// scheme (ChatGPT), open a throwaway tab (so the BBGM tab is never touched),
// point it at the app scheme, and - if the app doesn't take over within a beat -
// load the web version in that same tab instead.
export const openRecapAI = (ai: { url: string; appUrl?: string }) => {
	// No app scheme, or on desktop (where the scheme just prompts / falls flat and
	// people expect the web anyway): straight to the web in a new tab.
	if (!ai.appUrl || !window.mobile) {
		window.open(ai.url, "_blank", "noopener");
		return;
	}

	const tab = window.open("", "_blank");
	if (!tab) {
		// Popup blocked - best effort straight to the web.
		window.open(ai.url, "_blank", "noopener");
		return;
	}

	// If the app opens, the OS foregrounds it and this page goes hidden.
	let appeared = false;
	const onVisibility = () => {
		if (document.hidden) {
			appeared = true;
		}
	};
	document.addEventListener("visibilitychange", onVisibility);

	try {
		tab.location.href = ai.appUrl;
	} catch {
		// Some browsers reject an unknown scheme; treat as "no app".
	}

	window.setTimeout(() => {
		document.removeEventListener("visibilitychange", onVisibility);
		if (appeared) {
			// The app took over; drop the unused fallback tab.
			try {
				tab.close();
			} catch {
				// Ignore - can't always close a tab we opened.
			}
		} else {
			// No app answered - load the web version in the tab we already opened.
			try {
				tab.location.href = ai.url;
			} catch {
				window.open(ai.url, "_blank", "noopener");
			}
		}
	}, 1200);
};
