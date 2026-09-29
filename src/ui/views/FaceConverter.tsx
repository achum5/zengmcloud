import { useEffect, useMemo, useRef, useState } from "react";
import clsx from "clsx";
import type { FaceConfig } from "facesjs";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { showNotification } from "../util/showNotification.ts";
import { safeLocalStorage } from "../util/safeLocalStorage.ts";
import { confirm } from "../util/confirm.tsx";
import { updatePlayerFaceData } from "../util/playerFaces.ts";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import { Modal } from "../components/Modal.tsx";
import { FaceEditorControls } from "../components/PlayerFaceModal.tsx";
import {
	LABEL_H,
	PhotoReadError,
	TILE_H,
	TILE_W,
	buildFiles,
	buildSheet,
	copyImageToClipboard,
	saveBatchFiles,
	sheetLayout,
} from "../util/faceConverterImages.ts";
import {
	batchLabel,
	buildBatchPrompt,
	checkFace,
	parseFaceBatch,
	type BatchEntry,
} from "../../common/faceBatch.ts";
import { parseFaceJson } from "../../common/repairFaceJson.ts";
import type { FaceConverterPlayer } from "../../worker/api/index.ts";

// FACE CONVERTER: every player's photo into a faces.js face, a batch at a time.
//
//   1. Pick a draft class. The next batch is the first N players in it that
//      still have only a photo.
//   2. Copy the prompt and the photos (one labelled contact sheet, or labelled
//      files) into a chat AI.
//   3. Paste its reply. Every face in it is staged, not saved.
//   4. Review the staged faces one by one - photo beside face, every slot
//      editable - and approve each, which saves it to the player.
//
// Staged faces live in this browser's localStorage, per league, so a reload or
// a closed tab mid-review loses nothing.

type Staged = {
	text: string;
	warnings: string[];
};

type Source = "sheet" | "files";

const stagedKey = (lid: number) => `faceConverterStaged-${lid}`;
const yearKey = (lid: number) => `faceConverterYear-${lid}`;
const SIZE_KEY = "faceConverterBatchSize";
const SOURCE_KEY = "faceConverterSource";

const readJson = <T,>(key: string, fallback: T): T => {
	try {
		const raw = safeLocalStorage.getItem(key);
		return raw ? (JSON.parse(raw) as T) : fallback;
	} catch {
		return fallback;
	}
};

const writeJson = (key: string, value: unknown) => {
	try {
		safeLocalStorage.setItem(key, JSON.stringify(value));
	} catch {}
};

const statusOf = (
	p: FaceConverterPlayer,
	staged: Record<number, Staged>,
): "staged" | "done" | "todo" | "nophoto" => {
	if (staged[p.pid]) {
		return "staged";
	}
	if (p.converted) {
		return "done";
	}
	return p.photo ? "todo" : "nophoto";
};

const STATUS_BADGE = {
	staged: ["Review", "text-bg-warning"],
	done: ["Done", "text-bg-success"],
	todo: ["To do", "text-bg-secondary"],
	nophoto: ["No photo", "text-bg-light border"],
} as const;

const Tile = ({
	label,
	photo,
}: {
	label: string;
	photo: string | undefined;
}) => (
	// Colors are literal, not theme classes: the dark theme remaps "white", and
	// the prompt tells the AI to expect a white label strip under each photo.
	<div
		style={{
			backgroundColor: "#ffffff",
			border: "1px solid #999999",
			color: "#000000",
			height: TILE_H + LABEL_H,
			width: TILE_W,
		}}
	>
		<div
			className="d-flex align-items-center justify-content-center"
			style={{ width: TILE_W, height: TILE_H }}
		>
			{photo ? (
				<img
					alt=""
					src={photo}
					style={{ maxHeight: TILE_H, maxWidth: TILE_W }}
				/>
			) : null}
		</div>
		<div
			className="fw-bold text-truncate px-1"
			style={{ height: LABEL_H, lineHeight: `${LABEL_H}px`, fontSize: 14 }}
		>
			{label}
		</div>
	</div>
);

const Reviewer = ({
	onApprove,
	onClose,
	onDiscard,
	onNavigate,
	player,
	position,
	staged,
}: {
	onApprove: (face: FaceConfig) => Promise<void>;
	onClose: () => void;
	onDiscard: (() => void) | undefined;
	onNavigate: ((delta: number) => void) | undefined;
	player: FaceConverterPlayer;
	position: string | undefined;
	staged: Staged | undefined;
}) => {
	const [text, setText] = useState(
		() => staged?.text ?? JSON.stringify(player.face, undefined, 2),
	);
	const [saving, setSaving] = useState(false);
	const face = parseFaceJson(text) as FaceConfig | undefined;

	const approve = async () => {
		if (!face || saving) {
			return;
		}
		setSaving(true);
		try {
			await onApprove(checkFace(face as any).face);
		} finally {
			setSaving(false);
		}
	};

	// Ctrl+Enter approves. The listener reads the latest approve through a ref
	// so it can be registered once.
	const approveRef = useRef(approve);
	approveRef.current = approve;
	useEffect(() => {
		const onKey = (event: KeyboardEvent) => {
			if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
				event.preventDefault();
				void approveRef.current();
			}
		};
		window.addEventListener("keydown", onKey);
		return () => {
			window.removeEventListener("keydown", onKey);
		};
	}, []);

	return (
		<Modal onHide={onClose} scrollable show size="xl">
			<Modal.Header closeButton>
				<Modal.Title>
					{player.name}
					{position ? (
						<span className="text-body-secondary fs-6 ms-2">{position}</span>
					) : null}
				</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				<div className="row g-3">
					<div className="col-12 col-md-5">
						<div className="position-sticky d-flex gap-2" style={{ top: 0 }}>
							<div style={{ flex: "1 1 0", minWidth: 0 }}>
								{player.photo ? (
									<img
										alt=""
										src={player.photo}
										style={{ maxWidth: "100%", maxHeight: 320 }}
									/>
								) : null}
							</div>
							<div style={{ flex: "1 1 0", minWidth: 0, height: 320 }}>
								{face ? (
									<PlayerPicture
										colors={player.colors}
										face={face}
										jersey={player.jersey}
									/>
								) : null}
							</div>
						</div>
						{staged?.warnings.length ? (
							<div className="text-warning small mt-2">
								{staged.warnings.join(" · ")}
							</div>
						) : null}
					</div>
					<div className="col-12 col-md-7">
						<FaceEditorControls
							colors={player.colors}
							jersey={player.jersey}
							setText={setText}
							text={text}
						/>
					</div>
				</div>
			</Modal.Body>
			<Modal.Footer>
				{onDiscard ? (
					<button
						className="btn btn-outline-danger me-auto"
						onClick={onDiscard}
						type="button"
					>
						Discard
					</button>
				) : (
					<span className="me-auto" />
				)}
				{onNavigate ? (
					<>
						<button
							className="btn btn-secondary"
							onClick={() => {
								onNavigate(-1);
							}}
							type="button"
						>
							Back
						</button>
						<button
							className="btn btn-secondary"
							onClick={() => {
								onNavigate(1);
							}}
							type="button"
						>
							Skip
						</button>
					</>
				) : null}
				<button
					className="btn btn-primary"
					disabled={!face || saving}
					onClick={approve}
					title="Ctrl+Enter"
					type="button"
				>
					{staged ? "Approve" : "Save"}
				</button>
			</Modal.Footer>
		</Modal>
	);
};

const FaceConverter = () => {
	useTitleBar({ title: "Face Converter" });
	const { lid } = useLocal(["lid"]);

	const [classes, setClasses] = useState<{ year: number; count: number }[]>();
	const [year, setYear] = useState<number>();
	const [players, setPlayers] = useState<FaceConverterPlayer[]>();
	const [staged, setStaged] = useState<Record<number, Staged>>({});
	const [batchSize, setBatchSize] = useState(() =>
		readJson<number>(SIZE_KEY, 19),
	);
	const [source, setSource] = useState<Source>(() =>
		readJson<Source>(SOURCE_KEY, "sheet"),
	);
	const [reply, setReply] = useState("");
	const [replyResult, setReplyResult] = useState<string>();
	const [busy, setBusy] = useState<string>();
	// The on-page sheet is a small preview, until copying the photos fails and
	// it becomes the thing to screenshot - then it is shown at full size.
	const [fullSheet, setFullSheet] = useState(false);
	const [reviewing, setReviewing] = useState<{
		pid: number;
		queue: number[] | undefined;
	}>();

	// League-scoped state, loaded once the league is known.
	useEffect(() => {
		if (lid === undefined) {
			return;
		}
		setStaged(readJson(stagedKey(lid), {}));
		let cancelled = false;
		(async () => {
			const list = await toWorker("main", "faceConverterClasses", undefined);
			if (cancelled) {
				return;
			}
			setClasses(list);
			const saved = readJson<number | undefined>(yearKey(lid), undefined);
			setYear(list.some((c) => c.year === saved) ? saved : list[0]?.year);
		})();
		return () => {
			cancelled = true;
		};
	}, [lid]);

	useEffect(() => {
		if (year === undefined || lid === undefined) {
			return;
		}
		writeJson(yearKey(lid), year);
		setPlayers(undefined);
		setReplyResult(undefined);
		setFullSheet(false);
		let cancelled = false;
		(async () => {
			const list = await toWorker("main", "faceConverterClass", { year });
			if (!cancelled) {
				setPlayers(list);
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [year, lid]);

	// Saves in a loop (Approve all) each start from the previous save's result,
	// not from the render they were called in, so the latest map lives in a ref.
	const stagedRef = useRef(staged);
	stagedRef.current = staged;
	const saveStaged = (next: Record<number, Staged>) => {
		stagedRef.current = next;
		setStaged(next);
		if (lid !== undefined) {
			writeJson(stagedKey(lid), next);
		}
	};

	const byPid = useMemo(
		() => new Map((players ?? []).map((p) => [p.pid, p])),
		[players],
	);

	const counts = useMemo(() => {
		const out = { todo: 0, staged: 0, done: 0, nophoto: 0 };
		for (const p of players ?? []) {
			out[statusOf(p, staged)] += 1;
		}
		return out;
	}, [players, staged]);

	const size = Math.max(1, Math.min(200, Math.round(batchSize) || 19));
	const batch: (BatchEntry & { photo: string | undefined })[] = useMemo(
		() =>
			(players ?? [])
				.filter((p) => statusOf(p, staged) === "todo")
				.slice(0, size)
				.map((p, i) => ({
					n: i + 1,
					pid: p.pid,
					name: p.name,
					photo: p.photo,
				})),
		[players, staged, size],
	);

	const stagedHere = (players ?? []).filter((p) => staged[p.pid]);

	const yearIndex = classes?.findIndex((c) => c.year === year) ?? -1;

	const copyText = async (text: string) => {
		try {
			await navigator.clipboard.writeText(text);
			return true;
		} catch {
			showNotification({
				type: "error",
				text: "Couldn't write to the clipboard.",
			});
			return false;
		}
	};

	const photoError = () => {
		setFullSheet(true);
		showNotification({
			type: "error",
			text: "The photo host doesn't allow copying its images. Screenshot the sheet below instead.",
		});
	};

	const run = async (label: string, fn: () => Promise<void>) => {
		setBusy(label);
		try {
			await fn();
		} catch (error) {
			if (error instanceof PhotoReadError) {
				photoError();
			} else if ((error as Error).name !== "AbortError") {
				showNotification({ type: "error", text: (error as Error).message });
			}
		} finally {
			setBusy(undefined);
		}
	};

	const takeReply = (text: string) => {
		setReply(text);
		if (text.trim() === "" || !players) {
			setReplyResult(undefined);
			return;
		}
		const { faces, unknown } = parseFaceBatch(
			text,
			players.map((p) => p.pid),
		);
		if (faces.size === 0) {
			setReplyResult("No faces found in that reply.");
			return;
		}
		const next = { ...staged };
		for (const [pid, checked] of faces) {
			next[pid] = {
				text: JSON.stringify(checked.face, undefined, 2),
				warnings: checked.warnings,
			};
		}
		saveStaged(next);
		setReply("");

		const missing = batch.filter((entry) => !faces.has(entry.pid));
		const parts = [`Staged ${faces.size}.`];
		if (missing.length > 0 && missing.length < batch.length) {
			parts.push(
				`Missing: ${missing.map((entry) => `#${entry.n} ${entry.name}`).join(", ")}.`,
			);
		}
		if (unknown.length > 0) {
			parts.push(`Ignored unknown ids: ${unknown.join(", ")}.`);
		}
		setReplyResult(parts.join(" "));

		const queue = [...faces.keys()];
		setReviewing({ pid: queue[0]!, queue });
	};

	const approve = async (pid: number, face: FaceConfig) => {
		try {
			await toWorker("main", "updatePlayerFace", { pid, face });
		} catch (error) {
			showNotification({ type: "error", text: (error as Error).message });
			throw error;
		}
		updatePlayerFaceData(pid, face);
		setPlayers((list) =>
			list?.map((p) =>
				p.pid === pid ? { ...p, face, converted: p.converted || !!p.photo } : p,
			),
		);
		const next = { ...stagedRef.current };
		delete next[pid];
		saveStaged(next);
		return next;
	};

	const approveAll = async () => {
		const ok = await confirm(
			`Save all ${stagedHere.length} staged faces without reviewing them?`,
			{ okText: "Save all" },
		);
		if (!ok) {
			return;
		}
		await run("approveAll", async () => {
			let next = staged;
			for (const p of stagedHere) {
				const face = parseFaceJson(next[p.pid]!.text);
				if (face) {
					next = await approve(p.pid, checkFace(face as any).face);
				}
			}
		});
	};

	const layout = sheetLayout(batch.length);
	const current = reviewing ? byPid.get(reviewing.pid) : undefined;
	const queue = reviewing?.queue?.filter((pid) => staged[pid]);
	const queueIndex = queue && reviewing ? queue.indexOf(reviewing.pid) : -1;

	const moveInQueue = (delta: number) => {
		const remaining = queue;
		if (!remaining || remaining.length === 0) {
			setReviewing(undefined);
			return;
		}
		const at = Math.max(0, remaining.indexOf(reviewing!.pid));
		const nextPid =
			remaining[(at + delta + remaining.length) % remaining.length];
		setReviewing({ pid: nextPid!, queue: remaining });
	};

	if (!classes) {
		return <p>Loading...</p>;
	}
	if (classes.length === 0) {
		return <p>No players in this league.</p>;
	}

	return (
		<>
			<div className="d-flex flex-wrap align-items-end gap-3 mb-3">
				<div>
					<label className="form-label mb-1 small text-body-secondary">
						Draft class
					</label>
					<div className="input-group input-group-sm">
						<button
							className="btn btn-light-bordered"
							disabled={yearIndex <= 0}
							onClick={() => {
								setYear(classes[yearIndex - 1]!.year);
							}}
							type="button"
						>
							‹
						</button>
						<select
							className="form-select"
							onChange={(event) => {
								setYear(Number(event.target.value));
							}}
							value={year}
						>
							{classes.map((c) => (
								<option key={c.year} value={c.year}>
									{c.year} ({c.count})
								</option>
							))}
						</select>
						<button
							className="btn btn-light-bordered"
							disabled={yearIndex < 0 || yearIndex >= classes.length - 1}
							onClick={() => {
								setYear(classes[yearIndex + 1]!.year);
							}}
							type="button"
						>
							›
						</button>
					</div>
				</div>
				<div>
					<label
						className="form-label mb-1 small text-body-secondary"
						htmlFor="face-converter-size"
					>
						Batch size
					</label>
					<input
						className="form-control form-control-sm"
						id="face-converter-size"
						min={1}
						onChange={(event) => {
							const value = Number(event.target.value);
							setBatchSize(value);
							writeJson(SIZE_KEY, value);
						}}
						style={{ width: 80 }}
						type="number"
						value={batchSize}
					/>
				</div>
				<div>
					<label
						className="form-label mb-1 small text-body-secondary"
						htmlFor="face-converter-source"
					>
						Photos as
					</label>
					<select
						className="form-select form-select-sm"
						id="face-converter-source"
						onChange={(event) => {
							const value = event.target.value as Source;
							setSource(value);
							writeJson(SOURCE_KEY, value);
						}}
						value={source}
					>
						<option value="sheet">One image</option>
						<option value="files">Separate files</option>
					</select>
				</div>
				{players ? (
					<div className="small text-body-secondary ms-auto">
						{counts.todo} to do · {counts.staged} to review · {counts.done} done
						{counts.nophoto > 0 ? ` · ${counts.nophoto} without photo` : ""}
					</div>
				) : null}
			</div>

			{!players ? (
				<p>Loading...</p>
			) : (
				<>
					{batch.length > 0 ? (
						<div className="card mb-3">
							<div className="card-body">
								<div className="d-flex flex-wrap gap-2 mb-3">
									<button
										className="btn btn-primary"
										onClick={() => {
											void copyText(
												buildBatchPrompt(batch, source, layout.cols),
											).then((ok) => {
												if (ok) {
													showNotification({
														type: "success",
														text: "Prompt copied.",
													});
												}
											});
										}}
										type="button"
									>
										1. Copy prompt
									</button>
									{source === "sheet" ? (
										<button
											className="btn btn-primary"
											disabled={busy !== undefined}
											onClick={() =>
												run("sheet", async () => {
													const blob = await buildSheet(
														batch.map((entry) => ({
															label: batchLabel(entry),
															photo: entry.photo,
														})),
													);
													await copyImageToClipboard(blob);
													showNotification({
														type: "success",
														text: "Image copied.",
													});
												})
											}
											type="button"
										>
											{busy === "sheet" ? "Copying..." : "2. Copy image"}
										</button>
									) : (
										<button
											className="btn btn-primary"
											disabled={busy !== undefined}
											onClick={() =>
												run("files", async () => {
													const blobs = await buildFiles(
														batch.map((entry) => ({
															label: batchLabel(entry),
															photo: entry.photo,
														})),
													);
													const files = blobs.map((blob, i) => ({
														name: `${String(i + 1).padStart(2, "0")} ${batch[i]!.name.replaceAll(/["*/:<>?\\|]/g, "")}.png`,
														blob,
													}));
													files.push({
														name: "prompt.md",
														blob: new Blob(
															[buildBatchPrompt(batch, source, layout.cols)],
															{
																type: "text/markdown",
															},
														),
													});
													const where = await saveBatchFiles(files);
													showNotification({
														type: "success",
														text:
															where === "folder"
																? `Saved ${files.length} files.`
																: `Downloaded ${files.length} files.`,
													});
												})
											}
											title="The prompt is saved with them as prompt.md"
											type="button"
										>
											{busy === "files" ? "Saving..." : "2. Save photos"}
										</button>
									)}
								</div>

								<div className="mb-3">
									<div className="d-flex gap-2 mb-1">
										<span className="fw-bold">3. Paste the reply</span>
										<button
											className="btn btn-light-bordered btn-xs"
											onClick={async () => {
												try {
													takeReply(await navigator.clipboard.readText());
												} catch {
													showNotification({
														type: "error",
														text: "Couldn't read the clipboard.",
													});
												}
											}}
											type="button"
										>
											Paste
										</button>
									</div>
									<textarea
										className="form-control font-monospace"
										onChange={(event) => {
											takeReply(event.target.value);
										}}
										rows={2}
										spellCheck={false}
										style={{ fontSize: "0.8rem" }}
										value={reply}
									/>
									{replyResult ? (
										<div className="small mt-1">{replyResult}</div>
									) : null}
								</div>

								<div className="overflow-auto">
									<div
										className="d-grid"
										style={{
											gridTemplateColumns: `repeat(${layout.cols}, ${TILE_W}px)`,
											width: layout.cols * TILE_W,
											zoom: fullSheet ? 1 : 0.4,
										}}
									>
										{batch.map((entry) => (
											<Tile
												key={entry.pid}
												label={batchLabel(entry)}
												photo={entry.photo}
											/>
										))}
									</div>
								</div>
							</div>
						</div>
					) : null}

					{stagedHere.length > 0 ? (
						<div className="d-flex gap-2 mb-2">
							<button
								className="btn btn-success"
								onClick={() => {
									const pids = stagedHere.map((p) => p.pid);
									setReviewing({ pid: pids[0]!, queue: pids });
								}}
								type="button"
							>
								Review {stagedHere.length}
							</button>
							<button
								className="btn btn-light-bordered"
								disabled={busy !== undefined}
								onClick={approveAll}
								type="button"
							>
								Approve all
							</button>
						</div>
					) : null}

					<div className="d-flex flex-wrap gap-2">
						{players.map((p) => {
							const status = statusOf(p, staged);
							const stagedFace = staged[p.pid]
								? (parseFaceJson(staged[p.pid]!.text) as FaceConfig | undefined)
								: undefined;
							const face =
								stagedFace ?? (status === "done" ? p.face : undefined);
							const [badge, badgeClass] = STATUS_BADGE[status];
							return (
								<button
									className="btn btn-light-bordered p-1 text-start"
									key={p.pid}
									onClick={() => {
										setReviewing({ pid: p.pid, queue: undefined });
									}}
									style={{ width: 164 }}
									type="button"
								>
									<div className="d-flex gap-1" style={{ height: 90 }}>
										<div
											className="d-flex align-items-center justify-content-center"
											style={{ width: 90 }}
										>
											{p.photo ? (
												<img
													alt=""
													loading="lazy"
													src={p.photo}
													style={{ maxWidth: 90, maxHeight: 90 }}
												/>
											) : null}
										</div>
										<div style={{ width: 60 }}>
											{face ? (
												<PlayerPicture
													colors={p.colors}
													face={face}
													jersey={p.jersey}
													lazy
												/>
											) : null}
										</div>
									</div>
									<div className="small text-truncate">{p.name}</div>
									<span className={clsx("badge", badgeClass)}>{badge}</span>
									{staged[p.pid]?.warnings.length ? (
										<span
											className="badge text-bg-danger ms-1"
											title={staged[p.pid]!.warnings.join(" · ")}
										>
											!
										</span>
									) : null}
								</button>
							);
						})}
					</div>
				</>
			)}

			{current && reviewing ? (
				<Reviewer
					key={`${current.pid}-${staged[current.pid] ? "s" : "d"}`}
					onApprove={async (face) => {
						const next = await approve(current.pid, face);
						if (reviewing.queue) {
							const remaining = reviewing.queue.filter((pid) => next[pid]);
							if (remaining.length === 0) {
								setReviewing(undefined);
							} else {
								const at = reviewing.queue.indexOf(current.pid);
								const after = reviewing.queue
									.slice(at + 1)
									.find((pid) => next[pid]);
								setReviewing({
									pid: after ?? remaining[0]!,
									queue: remaining,
								});
							}
						} else {
							setReviewing(undefined);
						}
					}}
					onClose={() => {
						setReviewing(undefined);
					}}
					onDiscard={
						staged[current.pid]
							? () => {
									const next = { ...stagedRef.current };
									delete next[current.pid];
									saveStaged(next);
									const order = reviewing.queue ?? [];
									const remaining = order.filter((pid) => next[pid]);
									if (remaining.length === 0) {
										setReviewing(undefined);
										return;
									}
									// On to the player after the discarded one, not back to the
									// start of the queue.
									const at = order.indexOf(current.pid);
									const after = order.slice(at + 1).find((pid) => next[pid]);
									setReviewing({
										pid: after ?? remaining[0]!,
										queue: remaining,
									});
								}
							: undefined
					}
					onNavigate={
						queue && queue.length > 1
							? (delta) => {
									moveInQueue(delta);
								}
							: undefined
					}
					player={current}
					position={
						queue && queueIndex >= 0
							? `${queueIndex + 1} of ${queue.length}`
							: undefined
					}
					staged={staged[current.pid]}
				/>
			) : null}
		</>
	);
};

export default FaceConverter;
