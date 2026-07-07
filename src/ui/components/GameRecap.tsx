import { useEffect, useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import { buildRecapPrompt, parseRecaps } from "../util/gameRecap.ts";

// The "Game Recap" workflow on the Daily Schedule, as three simple steps:
//   Copy (a prompt with every completed game's box score) → Claude (the native
//   app if installed, else claude.ai) → Paste (the AI's reply, filed as each
//   game's note).
// Deliberately NOT gated by the multiplayer "wheel": filing a recap is just a
// game note, which any device may write.
export const GameRecap = ({
	season,
	day,
	numCompleted,
}: {
	season: number;
	day: number;
	numCompleted: number;
}) => {
	// The prompt is built up-front (not on click) so the Copy tap can write to
	// the clipboard SYNCHRONOUSLY. iOS Safari rejects a clipboard write that
	// happens after an await - the user-gesture is considered gone by then, which
	// is why copying worked on desktop but failed on mobile.
	const [prompt, setPrompt] = useState<string | undefined>();
	const [loadFailed, setLoadFailed] = useState(false);

	const [copied, setCopied] = useState(false);
	const [pasted, setPasted] = useState(false);
	const [busy, setBusy] = useState(false);
	const [result, setResult] = useState<string | undefined>();
	const [manual, setManual] = useState<string | undefined>();
	const [copyFallback, setCopyFallback] = useState<string | undefined>();

	useEffect(() => {
		if (numCompleted === 0) {
			return;
		}
		let cancelled = false;
		setLoadFailed(false);
		(async () => {
			try {
				const games = await toWorker("main", "getDayGamesForRecap", {
					season,
					day,
				});
				if (cancelled) {
					return;
				}
				setPrompt(
					games && games.length > 0
						? buildRecapPrompt(games, `Day ${day}`)
						: undefined,
				);
			} catch (error) {
				if (!cancelled) {
					console.error("Failed to build recap prompt", error);
					setLoadFailed(true);
				}
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [season, day, numCompleted]);

	if (numCompleted === 0) {
		return null;
	}

	const copy = async () => {
		setResult(undefined);
		setCopyFallback(undefined);
		if (loadFailed || !prompt) {
			setResult(
				"Couldn't prepare this day's games — reload the page and retry.",
			);
			return;
		}
		// writeText is the FIRST thing here (no await before it), so the tap's
		// gesture is still valid on mobile.
		try {
			await navigator.clipboard.writeText(prompt);
			setCopied(true);
			globalThis.setTimeout(() => setCopied(false), 3000);
		} catch (error) {
			console.error("Clipboard write failed", error);
			// Last resort: show the prompt so it can be long-pressed / selected.
			setCopyFallback(prompt);
		}
	};

	const fileRecaps = async (text: string) => {
		setBusy(true);
		setResult(undefined);
		try {
			const recaps = parseRecaps(text);
			if (recaps.size === 0) {
				setResult(
					"Couldn't find any recaps in what was pasted — paste the AI's full reply (each recap keeps its <!--game:…--> marker).",
				);
				return;
			}
			for (const [gid, note] of recaps) {
				await toWorker("main", "setNote", {
					type: "game",
					gid,
					editedNote: note,
				});
			}
			setManual(undefined);
			// Confirm on the Paste button (no words), matching Copy.
			setPasted(true);
			globalThis.setTimeout(() => setPasted(false), 3000);
		} catch (error) {
			console.error("Failed to file recaps", error);
			setResult("Something went wrong filing the recaps.");
		} finally {
			setBusy(false);
		}
	};

	const paste = async () => {
		setResult(undefined);
		setCopyFallback(undefined);
		// readText first, for the same gesture reason as copy.
		try {
			const text = await navigator.clipboard.readText();
			if (text && text.trim() !== "") {
				await fileRecaps(text);
				return;
			}
		} catch {
			// Clipboard read blocked/unsupported - fall through to the manual box.
		}
		setManual("");
	};

	// Open the native Claude app (claude:// scheme, supported by the iOS/Android
	// and desktop apps) if it's installed; otherwise fall back to claude.ai in the
	// browser. When the app opens, this tab goes hidden and we cancel the web
	// fallback; if nothing takes over the scheme, we navigate to the website.
	const openClaude = (event: { preventDefault: () => void }) => {
		event.preventDefault();
		const appUrl = "claude://claude.ai/new";
		const webUrl = "https://claude.ai/new";

		let cancelled = false;
		const onVisibility = () => {
			if (document.hidden) {
				cancelled = true;
				globalThis.clearTimeout(timer);
				document.removeEventListener("visibilitychange", onVisibility);
			}
		};
		document.addEventListener("visibilitychange", onVisibility);

		const timer = globalThis.setTimeout(() => {
			if (!cancelled) {
				document.removeEventListener("visibilitychange", onVisibility);
				window.location.href = webUrl;
			}
		}, 1200);

		// Attempting an unregistered scheme just fails quietly (the timeout then
		// sends us to the website); when the app IS installed, the OS switches to
		// it and the visibility handler cancels the fallback.
		window.location.href = appUrl;
	};

	const arrow = <span className="text-body-secondary">›</span>;
	// Fixed width so swapping a label for a ✓ / spinner never resizes the button.
	const btnStyle = { width: 62 } as const;

	// Compact inline group meant to sit on the "More:" links row - no card, no
	// extra vertical space beyond the small buttons themselves.
	return (
		<div className="d-inline-flex flex-column">
			<div className="d-flex flex-wrap align-items-center gap-1">
				<button
					className={`btn btn-sm ${copied ? "btn-success" : "btn-primary"}`}
					style={btnStyle}
					disabled={busy}
					onClick={copy}
					title="Copy AI prompt"
				>
					{copied ? "✓" : "Copy"}
				</button>
				{arrow}
				<a
					className="btn btn-sm btn-light-bordered"
					style={btnStyle}
					href="https://claude.ai/new"
					target="_blank"
					rel="noopener noreferrer"
					onClick={openClaude}
					title="Open Claude"
				>
					Claude
				</a>
				{arrow}
				<button
					className={`btn btn-sm ${pasted ? "btn-success" : "btn-primary"}`}
					style={btnStyle}
					disabled={busy}
					onClick={paste}
					title="Paste AI reply"
				>
					{busy ? (
						<span
							className="spinner-border spinner-border-sm"
							role="status"
							aria-hidden="true"
						/>
					) : pasted ? (
						"✓"
					) : (
						"Paste"
					)}
				</button>
			</div>

			{copyFallback !== undefined ? (
				<textarea
					className="form-control mt-2"
					style={{ width: 340, maxWidth: "100%" }}
					rows={4}
					readOnly
					value={copyFallback}
					onFocus={(event) => event.target.select()}
				/>
			) : null}

			{manual !== undefined ? (
				<textarea
					className="form-control mt-2"
					style={{ width: 340, maxWidth: "100%" }}
					rows={6}
					placeholder="Paste the AI's reply here…"
					value={manual}
					onChange={(event) => setManual(event.target.value)}
					onPaste={(event) => {
						// File the moment content is pasted, so there's no extra tap.
						const text = event.clipboardData.getData("text");
						if (text && text.trim() !== "") {
							void fileRecaps(text);
						}
					}}
				/>
			) : null}

			{result ? (
				<div className="alert alert-warning mt-2 mb-0 py-2 small">{result}</div>
			) : null}
		</div>
	);
};

export default GameRecap;
