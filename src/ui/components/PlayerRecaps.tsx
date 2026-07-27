import { useEffect, useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import {
	buildPlayerRecapPrompt,
	parsePlayerRecaps,
} from "../util/playerRecap.ts";
import { RecapAIButton } from "./RecapAIButton.tsx";
import type { RecapPlayerBatch } from "../../worker/util/getPlayerRecapData.ts";

// League-wide per-player season recaps, mirroring the Team Recaps flow on the
// same page: Copy → AI → Paste, filed into each player's own note under a
// [season] heading.
//
// The difference is scale: this covers every player in the league, which is far
// more than one prompt can hold, so it walks through BATCHES. The batch advances
// itself after a successful paste, so the loop is just Copy → AI → Paste,
// repeat, and the counter tells you how far along the season is.
export const PlayerRecaps = ({ season }: { season: number }) => {
	const [batchIndex, setBatchIndex] = useState(0);
	const [data, setData] = useState<RecapPlayerBatch | undefined>();
	const [prompt, setPrompt] = useState<string | undefined>();
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
		setPrompt(undefined);
		setData(undefined);
		(async () => {
			try {
				const batch = await toWorker("main", "getPlayerRecapData", {
					season,
					batchIndex,
				});
				if (cancelled) {
					return;
				}
				setData(batch);
				setPrompt(
					batch && batch.players.length > 0
						? buildPlayerRecapPrompt(batch)
						: undefined,
				);
			} catch (error) {
				if (!cancelled) {
					console.error("Failed to build player recap prompt", error);
					setLoadFailed(true);
				}
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [season, batchIndex]);

	const copy = async () => {
		setResult(undefined);
		setCopyFallback(undefined);
		if (loadFailed || !prompt) {
			setResult("Couldn't prepare this batch — reload the page and retry.");
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
			const recaps = parsePlayerRecaps(text);
			if (recaps.size === 0) {
				setResult(
					"Couldn't find any recaps in what was pasted — paste the AI's full reply (each recap keeps its <!--player:…--> marker).",
				);
				return;
			}

			const response = await toWorker("main", "filePlayerSeasonRecaps", {
				season,
				recaps: [...recaps].map(([pid, recap]) => ({
					pid,
					headline: recap.headline,
					text: recap.body,
				})),
			});

			setManual(undefined);
			setPasted(true);
			globalThis.setTimeout(() => setPasted(false), 3000);

			// Tell the user when the AI skipped players, rather than silently
			// advancing past them - a short reply is the main failure mode of a big
			// batch, and it's invisible otherwise.
			const expected = data?.players.length ?? 0;
			if (expected > 0 && recaps.size < expected) {
				setResult(
					`Filed ${response.filed} of ${expected} players — the AI's reply was short. Lower "AI Recap Max Players" in Global Settings, then re-copy this batch to fill the rest.`,
				);
			} else if (data && batchIndex + 1 < data.batchCount) {
				setBatchIndex(batchIndex + 1);
			}
		} catch (error) {
			console.error("Failed to file player recaps", error);
			setResult("Something went wrong filing the recaps.");
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

	const arrow = <span className="text-body-secondary">›</span>;
	const btnStyle = { width: 62 } as const;

	return (
		<div className="d-inline-flex flex-column">
			<div className="d-flex flex-wrap align-items-center gap-1">
				<button
					className={`btn btn-sm ${copied ? "btn-success" : "btn-primary"}`}
					style={btnStyle}
					disabled={busy || !prompt}
					onClick={copy}
					title="Copy AI prompt (this batch of players)"
				>
					{copied ? "✓" : "Copy"}
				</button>
				{arrow}
				<RecapAIButton style={btnStyle} />
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

			{data ? (
				<div className="d-flex align-items-center gap-2 mt-1 small text-body-secondary">
					<button
						className="btn btn-sm btn-link p-0 text-decoration-none"
						disabled={busy || batchIndex === 0}
						onClick={() => setBatchIndex(batchIndex - 1)}
						title="Previous batch"
					>
						‹
					</button>
					<span>
						Batch {data.batchIndex + 1}/{data.batchCount} ·{" "}
						{data.players.length} players
					</span>
					<button
						className="btn btn-sm btn-link p-0 text-decoration-none"
						disabled={busy || batchIndex + 1 >= data.batchCount}
						onClick={() => setBatchIndex(batchIndex + 1)}
						title="Next batch"
					>
						›
					</button>
					<span>
						· {data.alreadyWrittenTotal}/{data.totalPlayers} written
					</span>
				</div>
			) : null}

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

export default PlayerRecaps;
