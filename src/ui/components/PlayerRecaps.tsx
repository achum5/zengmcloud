import { useEffect, useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import {
	buildPlayerRecapPrompt,
	parsePlayerRecaps,
	parseRecapSeason,
} from "../util/playerRecap.ts";
import { RecapAIButton } from "./RecapAIButton.tsx";
import type {
	RecapFilter,
	RecapPlayerBatch,
} from "../../worker/util/getPlayerRecapData.ts";

// Naming the players a reply dropped is the whole point of the message, so the
// list is generous before it gives up and counts.
const nameList = (players: { name: string }[]) => {
	const names = players.map((p) => p.name);
	if (names.length <= 8) {
		return names.join(", ");
	}
	return `${names.slice(0, 8).join(", ")} and ${names.length - 8} more`;
};

// League-wide per-player writeups, mirroring the Team Recaps flow on the same
// page: Copy → AI → Paste, filed into each player's own note under a [season]
// heading.
//
// The difference is scale: this covers every player in the league, which is far
// more than one prompt can hold, so it walks through BATCHES cut from whoever
// still has nothing written for the season. The batch rebuilds itself after
// every paste, so the loop is just Copy → AI → Paste until the counter reads
// N/N, and a reply that drops players simply hands them to the next batch.
export const PlayerRecaps = ({
	season,
	filter = "players",
	heading,
	undoOnly = false,
}: {
	season: number;
	// Which pass. "players" is season recaps for everyone who was in the league,
	// "draftPicks" is this season's own draft class written up after the draft,
	// and "prospects" is scouting reports on next year's class. Three separate
	// runs with their own prompts, because they are different jobs.
	filter?: RecapFilter;
	heading: string;
	// Never offer to WRITE here, only to undo. For a pass whose home is another
	// page, on a page that still has to be able to clean up after it.
	undoOnly?: boolean;
}) => {
	// Batches are cut from whoever is still unwritten, so the list shrinks as it
	// is worked through and every batch is real work. That means re-deriving
	// after each successful paste rather than stepping forward, and the whole
	// section vanishing once the season is done.
	const [batchIndex, setBatchIndex] = useState(0);
	const [reload, setReload] = useState(0);
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
					filter,
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
	}, [season, batchIndex, filter, reload]);

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
			if (recaps.length === 0) {
				setResult(
					"Couldn't find any recaps in what was pasted — paste the AI's full reply (each recap keeps its <!--player:…--> marker).",
				);
				return;
			}

			// Nothing about a reply written for another season looks wrong once it's
			// filed - it just attaches to the wrong year on every player in the
			// batch. So the season is stamped into the prompt and checked here.
			const stamped = parseRecapSeason(text);
			if (stamped === undefined) {
				setResult(
					`That reply has no season stamp, so it can't be checked against ${season}. Re-copy the prompt and run it again.`,
				);
				return;
			}
			if (stamped !== season) {
				setResult(
					`That reply was written for ${stamped}, not ${season}. Nothing was filed — go to the ${stamped} page to file it, or re-copy this season's prompt.`,
				);
				return;
			}

			const response = await toWorker("main", "filePlayerSeasonRecaps", {
				season,
				recaps: recaps.map((recap) => ({
					pid: recap.pid,
					kind: recap.kind,
					headline: recap.headline,
					text: recap.body,
				})),
			});

			setManual(undefined);
			setPasted(true);
			globalThis.setTimeout(() => setPasted(false), 3000);

			// A long reply that quietly stops forty players short is the main
			// failure mode of a big batch, and it used to be invisible: the count
			// mixed season recaps in with retirement writeups (so it could read
			// "filed 223 of 200"), and it never said WHO was missed.
			const batchPlayers = data?.players ?? [];
			const written = new Set(
				recaps.filter((x) => x.kind === "season").map((x) => x.pid),
			);
			const skipped = batchPlayers.filter((p) => !written.has(p.pid));

			// The worker cuts batches from whoever is still unwritten, so re-deriving
			// from the top rebuilds the batch out of exactly the players still
			// missing a recap - including the ones this reply dropped. When nobody
			// is left it returns nothing at all and the section disappears.
			setBatchIndex(0);
			setReload((prev) => prev + 1);

			if (response.wrongKind.length > 0) {
				setResult(
					`Filed ${response.filed}, but skipped ${response.wrongKind.length} retirement writeup${
						response.wrongKind.length === 1 ? "" : "s"
					} for players who didn't retire in ${season} — that reply was probably for a different season.`,
				);
			} else if (skipped.length > 0) {
				setResult(
					`The reply stopped ${skipped.length} short of the batch — no recap for ${nameList(skipped)}. This batch is now just the players still missing one. If it keeps happening, lower "AI Recap Max Players" in Global Settings.`,
				);
			}
		} catch (error) {
			console.error("Failed to file player recaps", error);
			setResult("Something went wrong filing the recaps.");
		} finally {
			setBusy(false);
		}
	};

	const noun =
		filter === "prospects"
			? "prospects"
			: filter === "draftPicks"
				? "draft picks"
				: "players";

	// Undo: strip this pass's writeups from the players it covers. The section
	// then removes itself, which is the confirmation.
	const removeFiled = async () => {
		if (!data) {
			return;
		}
		if (
			!window.confirm(
				`Remove the ${season} writeup from ${data.alreadyWrittenTotal} ${noun}? Anything else in their notes is kept.`,
			)
		) {
			return;
		}
		setBusy(true);
		setResult(undefined);
		try {
			await toWorker("main", "clearPlayerSeasonRecaps", { season, filter });
			setBatchIndex(0);
			setReload((prev) => prev + 1);
		} catch (error) {
			console.error("Failed to remove player recaps", error);
			setResult("Something went wrong removing the writeups.");
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

	// The Copy → AI → Paste loop, shown while this pass has work left AND is
	// allowed to run. Once everyone has a note it comes off the page — that's
	// the reminder switching itself off, and the way to tell at a glance that a
	// season is finished.
	const showNag =
		!undoOnly && !!data && data.players.length > 0 && !data.notYet;

	// The undo outlives the nag in one case only: a pass that cannot run yet but
	// already has writeups filed against it. That state is a mistake by
	// definition, and it is the only way back out of it.
	const showCleanup =
		!!data && data.alreadyWrittenTotal > 0 && (showNag || !!data.notYet);

	if (!data || (!showNag && !showCleanup)) {
		return null;
	}

	return (
		<div className="d-inline-flex flex-column">
			<h2 className="h5">{heading}</h2>
			{showNag ? (
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
			) : null}

			<div className="d-flex align-items-center gap-2 mt-1 small text-body-secondary">
				{showNag ? (
					<>
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
							{data.players.length} {noun}
						</span>
						<button
							className="btn btn-sm btn-link p-0 text-decoration-none"
							disabled={busy || batchIndex + 1 >= data.batchCount}
							onClick={() => setBatchIndex(batchIndex + 1)}
							title="Next batch"
						>
							›
						</button>
						<span>·</span>
					</>
				) : null}
				<span>
					{data.alreadyWrittenTotal}/{data.totalPlayers} written
				</span>
				{showCleanup ? (
					<button
						className="btn btn-sm btn-outline-danger py-0"
						disabled={busy}
						onClick={removeFiled}
						title={`Remove the ${season} writeup from the ${noun} who already have one`}
					>
						Remove
					</button>
				) : null}
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

export default PlayerRecaps;
