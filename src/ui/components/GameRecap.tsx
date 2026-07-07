import { useEffect, useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import { buildRecapPrompt, parseRecaps } from "../util/gameRecap.ts";

// The "Game Recap" workflow on the Daily Schedule, as three simple steps:
//   Copy (a prompt with every completed game's box score) → Claude (claude.ai)
//   → Paste (the AI's reply, filed as each game's note).
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
			let placed = 0;
			for (const [gid, note] of recaps) {
				await toWorker("main", "setNote", {
					type: "game",
					gid,
					editedNote: note,
				});
				placed += 1;
			}
			setManual(undefined);
			setResult(
				`Filed ${placed} recap${placed === 1 ? "" : "s"} — open a game's box score to read it.`,
			);
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

	const arrow = <span className="text-body-secondary">›</span>;

	return (
		<div className="card mb-3" style={{ maxWidth: 700 }}>
			<div className="card-body">
				<h2 className="card-title h5">Game Recap</h2>
				<p className="text-body-secondary small mb-3">
					AI write-ups for every completed game on this day.
				</p>

				<div className="d-flex flex-wrap align-items-center gap-2">
					<button className="btn btn-primary" disabled={busy} onClick={copy}>
						{copied ? "✓ Copied" : "Copy"}
					</button>
					{arrow}
					<a
						className="btn btn-light-bordered"
						href="https://claude.ai"
						target="_blank"
						rel="noopener noreferrer"
					>
						Claude
					</a>
					{arrow}
					<button className="btn btn-primary" disabled={busy} onClick={paste}>
						{busy ? "Pasting…" : "Paste"}
					</button>
				</div>

				{copyFallback !== undefined ? (
					<div className="mt-3">
						<div className="form-text mb-1">
							Couldn't copy automatically — select all and copy this:
						</div>
						<textarea
							className="form-control"
							rows={4}
							readOnly
							value={copyFallback}
							onFocus={(event) => event.target.select()}
						/>
					</div>
				) : null}

				{manual !== undefined ? (
					<div className="mt-3">
						<textarea
							className="form-control"
							rows={6}
							placeholder="Paste the AI's full reply here…"
							value={manual}
							onChange={(event) => setManual(event.target.value)}
						/>
						<button
							className="btn btn-primary btn-sm mt-2"
							disabled={busy || manual.trim() === ""}
							onClick={() => fileRecaps(manual)}
						>
							{busy ? "Filing…" : "File recaps"}
						</button>
					</div>
				) : null}

				{result ? (
					<div className="alert alert-info mt-3 mb-0">{result}</div>
				) : null}
			</div>
		</div>
	);
};

export default GameRecap;
