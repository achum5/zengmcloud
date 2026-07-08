import { useEffect, useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import {
	buildRetiredRecapPrompt,
	parseRetiredRecaps,
} from "../util/retiredRecap.ts";

// A Copy/Claude/Paste workflow for every player who retired in a season. The
// Copy button bakes each retiree's full career into one prompt; pasting the AI
// reply files a writeup onto each player's own note. Length of each writeup
// scales with the career (Hall of Famer vs. undrafted never-played).
export const RetiredRecap = ({ season }: { season: number }) => {
	const [prompt, setPrompt] = useState<string | undefined>();
	const [empty, setEmpty] = useState(false);
	const [loadFailed, setLoadFailed] = useState(false);

	const [copied, setCopied] = useState(false);
	const [pasted, setPasted] = useState(false);
	const [busy, setBusy] = useState(false);
	const [result, setResult] = useState<string | undefined>();
	const [manual, setManual] = useState<string | undefined>();
	const [copyFallback, setCopyFallback] = useState<string | undefined>();

	useEffect(() => {
		let cancelled = false;
		setLoadFailed(false);
		setEmpty(false);
		setPrompt(undefined);
		(async () => {
			try {
				const data = await toWorker(
					"main",
					"getRetiredPlayersForRecap",
					season,
				);
				if (cancelled) {
					return;
				}
				if (!data || data.players.length === 0) {
					setEmpty(true);
					return;
				}
				setPrompt(buildRetiredRecapPrompt(data));
			} catch (error) {
				if (!cancelled) {
					console.error("Failed to build retired-player prompt", error);
					setLoadFailed(true);
				}
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [season]);

	const copy = async () => {
		setResult(undefined);
		setCopyFallback(undefined);
		if (loadFailed || !prompt) {
			setResult(
				"Couldn't prepare this season's retirees — reload the page and retry.",
			);
			return;
		}
		try {
			await navigator.clipboard.writeText(prompt);
			setCopied(true);
			globalThis.setTimeout(() => setCopied(false), 3000);
		} catch (error) {
			console.error("Clipboard write failed", error);
			setCopyFallback(prompt);
		}
	};

	const fileRecaps = async (text: string) => {
		setBusy(true);
		setResult(undefined);
		try {
			const recaps = parseRetiredRecaps(text);
			if (recaps.size === 0) {
				setResult(
					"Couldn't find any writeups in what was pasted — paste the AI's full reply (each keeps its <!--player:…--> marker).",
				);
				return;
			}
			for (const [pid, note] of recaps) {
				await toWorker("main", "setNote", {
					type: "player",
					pid,
					editedNote: note,
				});
			}
			setManual(undefined);
			setPasted(true);
			globalThis.setTimeout(() => setPasted(false), 3000);
		} catch (error) {
			console.error("Failed to file retired writeups", error);
			setResult("Something went wrong filing the writeups.");
		} finally {
			setBusy(false);
		}
	};

	const paste = async () => {
		setResult(undefined);
		setCopyFallback(undefined);
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

	if (empty) {
		return (
			<p className="text-body-secondary mb-0">
				No players retired after this season.
			</p>
		);
	}

	const arrow = <span className="text-body-secondary">›</span>;
	const btnStyle = { width: 62 } as const;

	return (
		<div className="d-inline-flex flex-column">
			<div className="d-flex flex-wrap align-items-center gap-1">
				<button
					className={`btn btn-sm ${copied ? "btn-success" : "btn-primary"}`}
					style={btnStyle}
					disabled={busy}
					onClick={copy}
					title="Copy AI prompt (every retiree's career)"
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
					title="Open Claude in a new tab"
				>
					Claude
				</a>
				{arrow}
				<button
					className={`btn btn-sm ${pasted ? "btn-success" : "btn-primary"}`}
					style={btnStyle}
					disabled={busy}
					onClick={paste}
					title="Paste AI reply (files each player's note)"
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

export default RetiredRecap;
