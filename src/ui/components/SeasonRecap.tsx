import { useEffect, useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import {
	buildSeasonRecapPrompt,
	parseSeasonRecaps,
} from "../util/seasonRecap.ts";
import { RecapAIButton } from "./RecapAIButton.tsx";

// A league-wide "Team Recaps" workflow for a whole season, mirroring the Game
// Recap flow on the Daily Schedule:
//   Copy (a prompt with every team's season, franchise history, and the moves
//   that built it) → Claude (opens claude.ai in a new tab) → Paste
//   (the AI's reply, filed as each team's Team Season note).
// Best generated right after the playoffs finish, before the draft.
export const SeasonRecap = ({
	heading,
	season,
}: {
	heading: string;
	season: number;
}) => {
	// Built up-front so the Copy tap can write to the clipboard SYNCHRONOUSLY
	// (iOS Safari rejects a clipboard write that happens after an await).
	const [prompt, setPrompt] = useState<string | undefined>();
	const [progress, setProgress] = useState<
		{ written: number; total: number } | undefined
	>();
	const [loadFailed, setLoadFailed] = useState(false);
	const [reload, setReload] = useState(0);

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
		setProgress(undefined);
		(async () => {
			try {
				const data = await toWorker("main", "getSeasonRecapData", season);
				if (cancelled) {
					return;
				}
				setPrompt(
					data && data.teams.length > 0
						? buildSeasonRecapPrompt(data)
						: undefined,
				);
				setProgress(
					data
						? { written: data.alreadyWrittenTotal, total: data.teams.length }
						: undefined,
				);
			} catch (error) {
				if (!cancelled) {
					console.error("Failed to build season recap prompt", error);
					setLoadFailed(true);
				}
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [season, reload]);

	const copy = async () => {
		setResult(undefined);
		setCopyFallback(undefined);
		if (loadFailed || !prompt) {
			setResult(
				"Couldn't prepare this season's data — reload the page and retry.",
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
			const recaps = parseSeasonRecaps(text);
			if (recaps.size === 0) {
				setResult(
					"Couldn't find any recaps in what was pasted — paste the AI's full reply (each recap keeps its <!--team:…--> marker).",
				);
				return;
			}
			// One call for the whole batch, not one per team: in a shared league
			// every worker call waits on its own upload to the cloud.
			await toWorker("main", "fileTeamSeasonRecaps", {
				season,
				recaps: [...recaps].map(([tid, note]) => ({ tid, note })),
			});
			setManual(undefined);
			setPasted(true);
			globalThis.setTimeout(() => setPasted(false), 3000);
			// Re-count, so the section can take itself off the page once every team
			// has a note.
			setReload((prev) => prev + 1);
		} catch (error) {
			console.error("Failed to file season recaps", error);
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

	// Every team written means there is nothing left to do for this season, so
	// the section disappears - which is how you tell at a glance that the year
	// is finished.
	if (progress && progress.total > 0 && progress.written >= progress.total) {
		return null;
	}

	return (
		<div className="d-inline-flex flex-column">
			<h2 className="h5">{heading}</h2>
			<div className="d-flex flex-wrap align-items-center gap-1">
				<button
					className={`btn btn-sm ${copied ? "btn-success" : "btn-primary"}`}
					style={btnStyle}
					disabled={busy}
					onClick={copy}
					title="Copy AI prompt (every team's season)"
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
					title="Paste AI reply (files each team's note)"
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

			{progress ? (
				<div className="mt-1 small text-body-secondary">
					{progress.written}/{progress.total} written
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

export default SeasonRecap;
