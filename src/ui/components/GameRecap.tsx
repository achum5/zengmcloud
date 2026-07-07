import { useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import { buildRecapPrompt, parseRecaps } from "../util/gameRecap.ts";

// The "Game Recap" workflow on the Daily Schedule: copy a prompt with every
// completed game's box score, run it through an AI, then paste the AI's markdown
// reply back to file a recap (stored as each game's note) onto each game.
export const GameRecap = ({
	season,
	day,
	numCompleted,
}: {
	season: number;
	day: number;
	numCompleted: number;
}) => {
	const [copyState, setCopyState] = useState<
		"idle" | "copying" | "copied" | "error"
	>("idle");
	const [showPaste, setShowPaste] = useState(false);
	const [pasteText, setPasteText] = useState("");
	const [busy, setBusy] = useState(false);
	const [result, setResult] = useState<string | undefined>();

	if (numCompleted === 0) {
		return null;
	}

	const copyPrompt = async () => {
		setCopyState("copying");
		try {
			const games = await toWorker("main", "getDayGamesForRecap", {
				season,
				day,
			});
			if (!games || games.length === 0) {
				setCopyState("error");
				return;
			}
			const prompt = buildRecapPrompt(games, `Day ${day}`);
			await navigator.clipboard.writeText(prompt);
			setCopyState("copied");
			globalThis.setTimeout(() => setCopyState("idle"), 3000);
		} catch (error) {
			console.error("Failed to build/copy recap prompt", error);
			setCopyState("error");
		}
	};

	const placeRecaps = async () => {
		setBusy(true);
		setResult(undefined);
		try {
			const recaps = parseRecaps(pasteText);
			if (recaps.size === 0) {
				setResult(
					"Couldn't find any recaps. Paste the AI's full reply — each recap has to keep its <!--game:…--> marker.",
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
			setResult(
				`Filed ${placed} recap${placed === 1 ? "" : "s"} — open a game's box score to read it.`,
			);
			setPasteText("");
		} catch (error) {
			console.error("Failed to place recaps", error);
			setResult("Something went wrong filing the recaps.");
		} finally {
			setBusy(false);
		}
	};

	return (
		<div className="card mb-3" style={{ maxWidth: 700 }}>
			<div className="card-body">
				<h2 className="card-title h5">Game Recap</h2>
				<p className="text-body-secondary small mb-3">
					Generate an AI write-up for every completed game on this day.
				</p>

				<div className="d-flex flex-wrap gap-2 align-items-center">
					<button
						className="btn btn-primary"
						disabled={copyState === "copying"}
						onClick={copyPrompt}
					>
						{copyState === "copying"
							? "Building…"
							: copyState === "copied"
								? "✓ Prompt copied!"
								: `1. Copy AI Prompt (${numCompleted} game${
										numCompleted === 1 ? "" : "s"
									})`}
					</button>
					<button
						className="btn btn-light-bordered"
						onClick={() => setShowPaste((v) => !v)}
					>
						2. Paste recaps back
					</button>
				</div>

				<div className="form-text">
					Copy the prompt, paste it into Claude (or any AI), then paste its
					reply back here.
				</div>

				{copyState === "error" ? (
					<div className="alert alert-danger mt-2 mb-0">
						Couldn't build the prompt for this day.
					</div>
				) : null}

				{showPaste ? (
					<div className="mt-3">
						<textarea
							className="form-control"
							rows={6}
							placeholder="Paste the AI's full markdown reply here…"
							value={pasteText}
							onChange={(event) => setPasteText(event.target.value)}
						/>
						<button
							className="btn btn-primary btn-sm mt-2"
							disabled={busy || pasteText.trim() === ""}
							onClick={placeRecaps}
						>
							{busy ? "Filing…" : "File recaps to games"}
						</button>
						{result ? (
							<div className="alert alert-info mt-2 mb-0">{result}</div>
						) : null}
					</div>
				) : null}
			</div>
		</div>
	);
};

export default GameRecap;
