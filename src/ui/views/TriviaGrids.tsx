import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import { Modal } from "../components/Modal.tsx";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import TriviaPlayerSelect, {
	type TriviaSearchPlayer,
} from "../components/TriviaPlayerSelect.tsx";
import {
	fetchTriviaCard,
	getCachedCard,
	type TriviaPlayerCard,
} from "../util/triviaPlayerCards.ts";
import { Confetti } from "./LiveGame/Confetti.tsx";
import type { View } from "../../common/types.ts";

// The Grids game (Immaculate Grid style): 9 cells, a shared pool of guesses,
// find any player in league history matching both the row and column criteria.
// A wrong guess burns a guess but leaves the cell open; each player can only
// be used once. Rarer correct answers score more.

type GridData = NonNullable<View<"triviaGrids">["data"]>;
type Criterion = GridData["grid"]["rows"][number];

type CriterionRef =
	| { kind: "team"; tid: number }
	| { kind: "career" | "season"; id: string };

type Catalog = {
	teams: { tid: number; label: string; count: number }[];
	achievements: {
		id: string;
		kind: "career" | "season";
		label: string;
		count: number;
	}[];
};

// A solved cell: the pid/name that filled it and the rarity points earned.
type CellState = {
	pid?: number;
	name?: string;
	points: number;
};

const emptyCells = (): CellState[] =>
	Array.from({ length: 9 }, () => ({ points: 0 }));

const GUESS_SETTING_KEY = "triviaGridsGuesses";
const STATS_KEY = "triviaGridsStats";

// Hints per grid, and what each successive hint on a cell costs that cell.
// Level 1 narrows the field, level 2 all but names someone - so level 2 keeps
// only a quarter of the points. Never zero: a hinted solve still beats a blank.
const HINTS_PER_GRID = 3;
const HINT_MULTIPLIER = [1, 0.5, 0.25];
const MIN_HINTED_POINTS = 5;

type Stats = {
	played: number;
	immaculate: number;
	best: number;
	totalScore: number;
};

const loadStats = (): Stats => {
	try {
		const raw = localStorage.getItem(STATS_KEY);
		if (raw) {
			const s = JSON.parse(raw);
			return {
				played: s.played ?? 0,
				immaculate: s.immaculate ?? 0,
				best: s.best ?? 0,
				totalScore: s.totalScore ?? 0,
			};
		}
	} catch {}
	return { played: 0, immaculate: 0, best: 0, totalScore: 0 };
};

// Rarity points (0-100, higher = more obscure) mapped to a display tier. Six
// tiers rather than four, so a lucky obvious answer and a genuinely deep cut
// don't land on the same color.
//
// The colors are the app's own `text-bg-*` set, ordered by how they actually
// render here rather than by Bootstrap's names: gray, cyan, green, yellow,
// orange, red. Note `primary` is orange in this theme, so it belongs near the
// hot end - putting it where its name suggests breaks the ramp.
// `cls` fills the solved cell and is the badge color wherever a tier is named.
const tierOf = (points: number): { label: string; cls: string } => {
	if (points >= 90) {
		return { label: "Mythic", cls: "text-bg-danger" };
	}
	if (points >= 75) {
		return { label: "Legendary", cls: "text-bg-primary" };
	}
	if (points >= 60) {
		return { label: "Epic", cls: "text-bg-warning" };
	}
	if (points >= 40) {
		return { label: "Rare", cls: "text-bg-success" };
	}
	if (points >= 20) {
		return { label: "Uncommon", cls: "text-bg-info" };
	}
	return { label: "Common", cls: "text-bg-secondary" };
};

const loadGuessSetting = (): number => {
	try {
		const raw = localStorage.getItem(GUESS_SETTING_KEY);
		if (raw === "Infinity") {
			return Infinity;
		}
		const n = Number.parseInt(raw ?? "");
		if ([6, 9, 12].includes(n)) {
			return n;
		}
	} catch {}
	return 9;
};

const refKey = (r: CriterionRef | undefined) =>
	r === undefined ? "" : r.kind === "team" ? `team-${r.tid}` : `ach-${r.id}`;

const criterionToRef = (c: Criterion): CriterionRef =>
	c.kind === "team" ? { kind: "team", tid: c.tid } : { kind: c.kind, id: c.id };

// "Michael Jordan" -> "M.J." for the strongest hint tier.
const initialsOf = (name: string) =>
	name
		.split(" ")
		.filter(Boolean)
		.map((w) => `${w[0]!.toUpperCase()}.`)
		.join("");

// --- Presentational pieces (module scope so they aren't remounted per render)

const CriterionLabel = ({
	c,
}: {
	c: { kind: string; tid?: number; label: string };
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	if (c.kind === "team" && c.tid !== undefined) {
		const t = teamInfoCache[c.tid];
		return (
			<div className="d-flex flex-column align-items-center justify-content-center gap-1 h-100 p-1">
				<TeamLogoInline
					imgURL={t?.imgURL}
					imgURLSmall={t?.imgURLSmall}
					size={44}
					includePlaceholderIfNoLogo
				/>
				<span
					className="text-center lh-sm"
					style={{ fontSize: "0.72rem", fontWeight: 500 }}
				>
					{c.label}
				</span>
			</div>
		);
	}
	return (
		<div
			className="d-flex align-items-center justify-content-center text-center fw-bold h-100 p-1 lh-sm"
			style={{ fontSize: "0.76rem" }}
		>
			{c.label}
		</div>
	);
};

const EditableHeader = ({
	header,
	onClick,
}: {
	header: { kind: string; tid?: number; label: string } | undefined;
	onClick: () => void;
}) => (
	<button
		type="button"
		className={`trivia-grid-head-edit ${header ? "" : "is-empty"}`}
		onClick={onClick}
	>
		<span className="trivia-edit-pencil" aria-hidden="true">
			✎
		</span>
		{header ? (
			<CriterionLabel c={header} />
		) : (
			<div className="text-body-secondary small p-2">Pick one</div>
		)}
	</button>
);

const AnswerFace = ({ pid }: { pid: number }) => {
	const [card, setCard] = useState<TriviaPlayerCard | undefined>(() =>
		getCachedCard(pid),
	);
	useEffect(() => {
		if (card) {
			return;
		}
		let stale = false;
		void fetchTriviaCard(pid).then((c) => {
			if (c && !stale) {
				setCard(c);
			}
		});
		return () => {
			stale = true;
		};
	}, [pid, card]);
	return (
		<div
			className="flex-shrink-0 overflow-hidden"
			style={{ width: 30, height: 40 }}
		>
			{card ? (
				<PlayerPicture
					face={card.face}
					imgURL={card.imgURL}
					colors={card.colors}
					jersey={card.jersey}
					lazy
				/>
			) : null}
		</div>
	);
};

const TriviaGrids = (props: View<"triviaGrids">) => {
	useTitleBar({ title: "Grids" });

	const [data, setData] = useState(props.data);
	const [cells, setCells] = useState<CellState[]>(emptyCells);
	const [usedPids, setUsedPids] = useState<Set<number>>(new Set());
	const [guessesUsed, setGuessesUsed] = useState(0);
	const [guessSetting, setGuessSetting] = useState(loadGuessSetting);
	const [gameMaxGuesses, setGameMaxGuesses] = useState(loadGuessSetting);
	const [activeCell, setActiveCell] = useState<number | undefined>();
	const [wrongGuess, setWrongGuess] = useState<string | undefined>();
	// Increments on each miss so the shake animation retriggers.
	const [wrongKey, setWrongKey] = useState(0);
	// Names already burned on each cell, so the same miss isn't repeated.
	const [missedByCell, setMissedByCell] = useState<Record<number, string[]>>(
		{},
	);
	const [gaveUp, setGaveUp] = useState(false);
	const [loadingNew, setLoadingNew] = useState(false);
	const [cards, setCards] = useState<Record<number, TriviaPlayerCard>>({});
	const [revealCell, setRevealCell] = useState<number | undefined>();
	const [revealLimit, setRevealLimit] = useState(24);
	const [stats, setStats] = useState<Stats>(loadStats);

	// Hints: a per-grid allowance, spent per cell, each level costing that cell
	// points rather than a guess.
	const [hintsLeft, setHintsLeft] = useState(HINTS_PER_GRID);
	// Per cell: how many hints have been spent, and WHICH player they describe.
	// The pid is pinned when the hint is bought - recomputing it per render
	// meant a hint could silently start describing someone else once its
	// original subject got used up on another cell.
	const [hints, setHints] = useState<
		Record<number, { level: number; pid?: number }>
	>({});

	// Inline editing: the board itself is the editor, seeded from whatever is
	// on screen, so "edit" means "tweak this grid" instead of "start over".
	const [editing, setEditing] = useState(false);
	const [editRefs, setEditRefs] = useState<(CriterionRef | undefined)[]>(
		Array.from({ length: 6 }, () => undefined),
	);
	const [editSlot, setEditSlot] = useState<number | undefined>();
	const [editPreview, setEditPreview] = useState<GridData | undefined>();
	const [editLoading, setEditLoading] = useState(false);
	const [catalog, setCatalog] = useState<Catalog | undefined>();

	const grid = data?.grid;
	// Memoized rather than defaulted inline: a fresh [] on every render would
	// re-run every memo/effect keyed on it (including the search box's face
	// fetching) on every keystroke.
	const searchList = useMemo(() => data?.searchList ?? [], [data]);

	const byPid = useMemo(() => {
		const m = new Map<number, TriviaSearchPlayer>();
		for (const p of searchList) {
			m.set(p.pid, p);
		}
		return m;
	}, [searchList]);

	const correctCount = cells.filter((c) => c.pid !== undefined).length;
	const score = cells.reduce((sum, c) => sum + c.points, 0);
	const guessesLeft = gameMaxGuesses - guessesUsed;
	const done =
		grid !== undefined && (gaveUp || correctCount === 9 || guessesLeft <= 0);
	const immaculate = correctCount === 9;

	// Record each finished game into local stats exactly once.
	const recordedRef = useRef(false);
	useEffect(() => {
		if (!done || recordedRef.current) {
			return;
		}
		recordedRef.current = true;
		setStats((prev) => {
			const next: Stats = {
				played: prev.played + 1,
				immaculate: prev.immaculate + (immaculate ? 1 : 0),
				best: Math.max(prev.best, score),
				totalScore: prev.totalScore + score,
			};
			try {
				localStorage.setItem(STATS_KEY, JSON.stringify(next));
			} catch {}
			return next;
		});
	}, [done, immaculate, score]);

	const resetGame = (fresh: GridData) => {
		setData(fresh);
		setCells(emptyCells());
		setUsedPids(new Set());
		setGuessesUsed(0);
		setGameMaxGuesses(guessSetting);
		setActiveCell(undefined);
		setWrongGuess(undefined);
		setMissedByCell({});
		setGaveUp(false);
		setCards({});
		setRevealCell(undefined);
		setHintsLeft(HINTS_PER_GRID);
		setHints({});
		recordedRef.current = false;
	};

	const newGrid = async () => {
		setLoadingNew(true);
		try {
			const fresh = await toWorker("main", "triviaNewGrid", undefined);
			if (fresh) {
				resetGame(fresh);
			}
		} finally {
			setLoadingNew(false);
		}
	};

	const setGuesses = (value: string) => {
		const n = value === "Infinity" ? Infinity : Number.parseInt(value);
		setGuessSetting(n);
		try {
			localStorage.setItem(GUESS_SETTING_KEY, String(n));
		} catch {}
	};

	// The team a cell belongs to (for jersey colors on the solved-cell face).
	const cellTid = (cellIndex: number): number | undefined => {
		if (!grid) {
			return undefined;
		}
		const row = grid.rows[Math.floor(cellIndex / 3)]!;
		const col = grid.cols[cellIndex % 3]!;
		if (row.kind === "team") {
			return row.tid;
		}
		if (col.kind === "team") {
			return col.tid;
		}
		return undefined;
	};

	const handleGuess = (cellIndex: number, guess: TriviaSearchPlayer) => {
		if (!grid || usedPids.has(guess.pid)) {
			return;
		}
		const cell = grid.cells[cellIndex]!;
		const correct = cell.pids.includes(guess.pid);
		setUsedPids((prev) => new Set(prev).add(guess.pid));
		setGuessesUsed((prev) => prev + 1);
		if (correct) {
			// Hints are paid for out of the cell's score, not out of guesses.
			const multiplier = HINT_MULTIPLIER[hints[cellIndex]?.level ?? 0] ?? 1;
			const base = cell.rarity[guess.pid] ?? 10;
			const points =
				multiplier === 1
					? base
					: Math.max(MIN_HINTED_POINTS, Math.round(base * multiplier));
			setCells((prev) =>
				prev.map((c, i) =>
					i === cellIndex ? { pid: guess.pid, name: guess.name, points } : c,
				),
			);
			setWrongGuess(undefined);
			setActiveCell(undefined);
			const tid = cellTid(cellIndex);
			void fetchTriviaCard(guess.pid, tid).then((card) => {
				if (card) {
					setCards((prev) => ({ ...prev, [guess.pid]: card }));
				}
			});
		} else {
			// Cell stays open - the burned guess is the price. Keep the modal up
			// for another try unless that was the last guess.
			setWrongGuess(guess.name);
			setWrongKey((k) => k + 1);
			setMissedByCell((prev) => ({
				...prev,
				[cellIndex]: [...(prev[cellIndex] ?? []), guess.name],
			}));
			if (gameMaxGuesses - (guessesUsed + 1) <= 0) {
				setActiveCell(undefined);
			}
		}
	};

	// --- Hints ---------------------------------------------------------------

	// The player a hint describes: the most common qualifying answer that is
	// still available. Describing the EASIEST answer is the point - a hint
	// should open the cell up, not send you hunting an obscure one.
	const hintTargetPid = (cellIndex: number): number | undefined => {
		if (!grid) {
			return undefined;
		}
		const cell = grid.cells[cellIndex]!;
		let bestPid: number | undefined;
		let bestRarity = Infinity;
		for (const pid of cell.pids) {
			if (usedPids.has(pid)) {
				continue;
			}
			const r = cell.rarity[pid] ?? 100;
			if (r < bestRarity) {
				bestRarity = r;
				bestPid = pid;
			}
		}
		return bestPid;
	};

	const useHint = () => {
		if (activeCell === undefined || hintsLeft <= 0) {
			return;
		}
		const existing = hints[activeCell];
		const level = existing?.level ?? 0;
		if (level >= HINT_MULTIPLIER.length - 1) {
			return;
		}
		setHintsLeft((h) => h - 1);
		setHints((prev) => ({
			...prev,
			[activeCell]: {
				level: level + 1,
				// Keep whoever the first hint picked, so levels stack on one player.
				pid: existing?.pid ?? hintTargetPid(activeCell),
			},
		}));
	};

	const hintText = (cellIndex: number): string[] => {
		const hint = hints[cellIndex];
		if (!hint || hint.level === 0 || !grid) {
			return [];
		}
		const cell = grid.cells[cellIndex]!;
		const out = [
			`${cell.pids.length} player${cell.pids.length === 1 ? "" : "s"} qualify.`,
		];
		const p = hint.pid !== undefined ? byPid.get(hint.pid) : undefined;
		if (p) {
			out.push(
				`One of them is a ${p.pos ? `${p.pos} ` : ""}who played ${p.years}.`,
			);
			if (hint.level >= 2) {
				out.push(`Their initials are ${initialsOf(p.name)}`);
			}
		}
		return out;
	};

	// --- Inline editing ------------------------------------------------------

	const loadCatalog = async () => {
		if (catalog) {
			return;
		}
		const c = await toWorker("main", "triviaGridCatalog", undefined);
		if (c) {
			setCatalog(c);
		}
	};

	const startEdit = () => {
		if (!grid) {
			return;
		}
		setEditRefs([
			...grid.rows.map(criterionToRef),
			...grid.cols.map(criterionToRef),
		]);
		setEditPreview(undefined);
		setEditing(true);
		void loadCatalog();
	};

	// Label/count for a ref, from the catalog. Falls back to the label on the
	// grid being edited so headers never blank out while the catalog loads.
	const refInfo = useMemo(() => {
		const m = new Map<string, { label: string; count: number }>();
		if (catalog) {
			for (const t of catalog.teams) {
				m.set(`team-${t.tid}`, { label: t.label, count: t.count });
			}
			for (const a of catalog.achievements) {
				m.set(`ach-${a.id}`, { label: a.label, count: a.count });
			}
		}
		return m;
	}, [catalog]);

	const fallbackLabels = useMemo(() => {
		const m = new Map<string, string>();
		if (grid) {
			for (const c of [...grid.rows, ...grid.cols]) {
				m.set(refKey(criterionToRef(c)), c.label);
			}
		}
		return m;
	}, [grid]);

	const labelForRef = (r: CriterionRef | undefined) => {
		if (!r) {
			return undefined;
		}
		const key = refKey(r);
		return refInfo.get(key)?.label ?? fallbackLabels.get(key);
	};

	// Re-validate whenever all six slots are filled.
	const editComplete = editRefs.every((r) => r !== undefined);
	useEffect(() => {
		if (!editing || !editComplete) {
			setEditPreview(undefined);
			return;
		}
		let stale = false;
		setEditLoading(true);
		void toWorker("main", "triviaCustomGrid", {
			rows: editRefs.slice(0, 3) as CriterionRef[],
			cols: editRefs.slice(3, 6) as CriterionRef[],
		})
			.then((result) => {
				if (!stale) {
					setEditPreview(result);
				}
			})
			.finally(() => {
				if (!stale) {
					setEditLoading(false);
				}
			});
		return () => {
			stale = true;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [editing, editComplete, JSON.stringify(editRefs)]);

	const editPlayable =
		editPreview !== undefined &&
		editPreview.grid.cells.every((c) => c.pids.length > 0);

	const playEdited = () => {
		if (editPreview) {
			resetGame(editPreview);
			setEditing(false);
		}
	};

	// Shuffle pulls a fresh RANDOM grid's criteria into the editor rather than
	// picking six at random itself - the generator only emits solvable sets, so
	// this always lands on a playable starting point to tweak.
	const shuffleEdit = async () => {
		setEditLoading(true);
		try {
			const fresh = await toWorker("main", "triviaNewGrid", undefined);
			if (fresh) {
				setEditRefs([
					...fresh.grid.rows.map(criterionToRef),
					...fresh.grid.cols.map(criterionToRef),
				]);
			}
		} finally {
			setEditLoading(false);
		}
	};

	// --- Render -------------------------------------------------------------

	if (!grid) {
		return (
			<>
				<p>
					Not enough league history to build a grid yet. Come back after a few
					seasons.
				</p>
				<button
					className="btn btn-primary"
					disabled={loadingNew}
					onClick={newGrid}
				>
					Try again
				</button>
			</>
		);
	}

	const activeRow =
		activeCell !== undefined
			? grid.rows[Math.floor(activeCell / 3)]
			: undefined;
	const activeCol =
		activeCell !== undefined ? grid.cols[activeCell % 3] : undefined;

	const reveal = revealCell !== undefined ? grid.cells[revealCell] : undefined;
	const revealSorted = reveal
		? [...reveal.pids].sort(
				(a, b) => (reveal.rarity[a] ?? 0) - (reveal.rarity[b] ?? 0),
			)
		: [];

	// While editing, the headers come from the draft rather than the live grid.
	const headerFor = (slot: number) => {
		const r = editRefs[slot];
		const label = labelForRef(r);
		if (!r || !label) {
			return undefined;
		}
		return r.kind === "team"
			? { kind: "team", tid: r.tid, label }
			: { kind: r.kind, label };
	};

	const activeHints = activeCell !== undefined ? hintText(activeCell) : [];
	const activeHintLevel =
		activeCell !== undefined ? (hints[activeCell]?.level ?? 0) : 0;
	const activeMissed =
		activeCell !== undefined ? (missedByCell[activeCell] ?? []) : [];

	return (
		<>
			<div className="d-flex flex-wrap align-items-center gap-2 mb-3">
				<div className="trivia-tile">
					<div className="trivia-tile-value">{score}</div>
					<div className="trivia-tile-label">Score</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">{correctCount}/9</div>
					<div className="trivia-tile-label">Solved</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">
						{gameMaxGuesses === Infinity ? "∞" : Math.max(0, guessesLeft)}
					</div>
					<div className="trivia-tile-label">Guesses</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">{hintsLeft}</div>
					<div className="trivia-tile-label">Hints</div>
				</div>
				<div className="d-flex flex-wrap align-items-center gap-2 ms-auto">
					{editing ? null : (
						<>
							{!done ? (
								<button
									className="btn btn-sm btn-light-bordered"
									onClick={() => setGaveUp(true)}
								>
									Give up
								</button>
							) : null}
							<button
								className="btn btn-sm btn-primary"
								disabled={loadingNew}
								onClick={newGrid}
							>
								{loadingNew ? "Generating…" : "New grid"}
							</button>
							<button
								className="btn btn-sm btn-light-bordered"
								onClick={startEdit}
								title="Edit this grid's rows and columns"
							>
								Edit
							</button>
							<select
								className="form-select form-select-sm w-auto"
								title="Guesses per grid (applies to the next grid)"
								value={String(guessSetting)}
								onChange={(e) => setGuesses(e.target.value)}
							>
								<option value="6">6 guesses</option>
								<option value="9">9 guesses</option>
								<option value="12">12 guesses</option>
								<option value="Infinity">Unlimited</option>
							</select>
						</>
					)}
				</div>
			</div>

			<div className="trivia-grid-board mb-2" style={{ maxWidth: 640 }}>
				<div
					className="trivia-grid-inner"
					style={{
						// Fractional columns so the inner grid always fills the board -
						// fixed max widths left a strip of the container showing.
						gridTemplateColumns: "minmax(76px, 108px) repeat(3, 1fr)",
					}}
				>
					<div className="trivia-grid-head flex-column">
						{editing ? (
							<div className="text-body-secondary small text-center p-1 lh-sm">
								Tap a header to change it
							</div>
						) : (
							<>
								<div className="h3 mb-0">
									{gameMaxGuesses === Infinity ? "∞" : Math.max(0, guessesLeft)}
								</div>
								<div
									className="text-body-secondary"
									style={{ fontSize: "0.7rem" }}
								>
									guesses
								</div>
							</>
						)}
					</div>

					{[0, 1, 2].map((i) => {
						const slot = 3 + i;
						const header = editing ? headerFor(slot) : grid.cols[i]!;
						if (!editing) {
							return (
								<div key={i} className="trivia-grid-head">
									<CriterionLabel c={header as Criterion} />
								</div>
							);
						}
						return (
							<EditableHeader
								key={i}
								header={header}
								onClick={() => setEditSlot(slot)}
							/>
						);
					})}

					{[0, 1, 2].map((r) => (
						<Fragment key={r}>
							{editing ? (
								<EditableHeader
									header={headerFor(r)}
									onClick={() => setEditSlot(r)}
								/>
							) : (
								<div className="trivia-grid-head">
									<CriterionLabel c={grid.rows[r]!} />
								</div>
							)}

							{[0, 1, 2].map((cIdx) => {
								const i = r * 3 + cIdx;

								// Editing: every cell is a live "is this playable" readout.
								if (editing) {
									const count = editPreview?.grid.cells[i]?.pids.length;
									return (
										<div key={cIdx} className="trivia-grid-count">
											{count === undefined ? (
												<span className="text-body-secondary small">
													{editLoading ? "…" : "—"}
												</span>
											) : (
												<>
													<span
														className={`fw-bold ${count === 0 ? "text-danger" : ""}`}
													>
														{count}
													</span>
													<span
														className="text-body-secondary"
														style={{ fontSize: "0.7rem" }}
													>
														{count === 0 ? "none" : "answers"}
													</span>
												</>
											)}
										</div>
									);
								}

								const cell = cells[i]!;
								const solved = cell.pid !== undefined;
								if (solved) {
									const card = cards[cell.pid!];
									const tier = tierOf(cell.points);
									return (
										<div
											key={cIdx}
											className={`position-relative overflow-hidden trivia-pop trivia-flash-green ${tier.cls}`}
											style={{
												aspectRatio: "1 / 1",
												cursor: done ? "pointer" : undefined,
											}}
											onClick={
												done
													? () => {
															setRevealCell(i);
															setRevealLimit(24);
														}
													: undefined
											}
										>
											<div
												className="position-absolute d-flex justify-content-center"
												style={{ inset: "0 0 18px 0" }}
											>
												{card ? (
													<div style={{ height: "100%", aspectRatio: "2 / 3" }}>
														<PlayerPicture
															face={card.face}
															imgURL={card.imgURL}
															colors={card.colors}
															jersey={card.jersey}
															lazy
														/>
													</div>
												) : null}
											</div>
											<span
												// The chip sits on top of the tier-colored cell, so it
												// needs its own neutral background for contrast.
												className="badge text-bg-dark position-absolute top-0 end-0 m-1"
												title={`${tier.label} pick`}
											>
												+{cell.points}
											</span>
											<div
												className="position-absolute bottom-0 start-0 end-0 text-center text-truncate px-1 text-white"
												style={{
													background: "rgba(0,0,0,0.6)",
													fontSize: "0.7rem",
													lineHeight: "18px",
													height: 18,
												}}
											>
												{cell.name}
											</div>
										</div>
									);
								}
								return (
									<button
										key={cIdx}
										className={`btn p-0 trivia-cell trivia-grid-cell ${
											activeCell === i
												? "btn-primary"
												: done
													? "btn-light-bordered border-danger"
													: "btn-light-bordered"
										}`}
										style={{ aspectRatio: "1 / 1" }}
										onClick={() => {
											if (done) {
												setRevealCell(i);
												setRevealLimit(24);
											} else {
												setActiveCell(i);
												setWrongGuess(undefined);
											}
										}}
									>
										{done ? (
											<span className="small">
												<span className="d-block fw-bold">
													{grid.cells[i]!.pids.length}
												</span>
												<span className="text-body-secondary">answers</span>
											</span>
										) : (hints[i]?.level ?? 0) > 0 ? (
											<span
												className="text-body-secondary h4 mb-0"
												title="Hint used"
											>
												💡
											</span>
										) : (
											<span className="text-body-secondary h4 mb-0">+</span>
										)}
									</button>
								);
							})}
						</Fragment>
					))}
				</div>
			</div>

			{editing ? (
				<div
					className="d-flex flex-wrap align-items-center gap-2 mb-3"
					style={{ maxWidth: 640 }}
				>
					<button
						className="btn btn-primary"
						disabled={!editPlayable || editLoading}
						onClick={playEdited}
						title={
							editPlayable
								? undefined
								: "Every cell needs at least one qualifying player"
						}
					>
						Play this grid
					</button>
					<button
						className="btn btn-light-bordered"
						disabled={editLoading}
						onClick={shuffleEdit}
					>
						Shuffle
					</button>
					<button
						className="btn btn-light-bordered ms-auto"
						onClick={() => setEditing(false)}
					>
						Cancel
					</button>
					{editComplete && !editLoading && !editPlayable ? (
						<div className="text-danger small w-100">
							A red cell has no qualifying player — change one of its criteria.
						</div>
					) : null}
				</div>
			) : done ? (
				<>
					{immaculate ? <Confetti /> : null}
					<div className="card trivia-rise mb-3" style={{ maxWidth: 640 }}>
						<div className="card-body d-flex flex-wrap align-items-center gap-3">
							<div style={{ fontSize: "1.3rem", lineHeight: 1.15 }}>
								{[0, 1, 2].map((r) => (
									<div key={r}>
										{[0, 1, 2].map((c) =>
											cells[r * 3 + c]!.pid !== undefined ? "🟩" : "⬛",
										)}
									</div>
								))}
							</div>
							<div className="flex-grow-1">
								<div className="h4 mb-1">
									{immaculate
										? "Immaculate! 🏆"
										: correctCount >= 7
											? "So close!"
											: correctCount >= 4
												? "Solid board."
												: "Tough grid."}
								</div>
								<div className="mb-1">
									<span className="fw-bold">{score}</span> points ·{" "}
									{correctCount}/9 solved
									{score > 0 && score >= stats.best ? (
										<span className="badge text-bg-warning ms-2">
											New best!
										</span>
									) : null}
								</div>
								<div className="d-flex flex-wrap gap-1">
									{cells
										.filter((c) => c.pid !== undefined)
										.map((c, i) => {
											const tier = tierOf(c.points);
											return (
												<span key={i} className={`badge ${tier.cls}`}>
													{tier.label} +{c.points}
												</span>
											);
										})}
								</div>
								<div className="text-body-secondary small mt-1">
									Tap any cell to see its answers.
								</div>
							</div>
							<button
								className="btn btn-primary"
								disabled={loadingNew}
								onClick={newGrid}
							>
								{loadingNew ? "Generating…" : "Play again"}
							</button>
						</div>
					</div>
				</>
			) : (
				<p className="text-body-secondary small mb-3">
					{searchList.length.toLocaleString()} players in the pool. Each player
					can be used once.
				</p>
			)}

			{editing ? null : (
				<div className="d-flex flex-wrap gap-2">
					<div className="trivia-tile">
						<div className="trivia-tile-value">{stats.played}</div>
						<div className="trivia-tile-label">Played</div>
					</div>
					<div className="trivia-tile">
						<div className="trivia-tile-value">{stats.immaculate}</div>
						<div className="trivia-tile-label">Immaculate</div>
					</div>
					<div className="trivia-tile">
						<div className="trivia-tile-value">{stats.best}</div>
						<div className="trivia-tile-label">Best</div>
					</div>
					<div className="trivia-tile">
						<div className="trivia-tile-value">
							{stats.played > 0
								? Math.round(stats.totalScore / stats.played)
								: 0}
						</div>
						<div className="trivia-tile-label">Avg score</div>
					</div>
				</div>
			)}

			{/* Guess modal */}
			<Modal
				show={activeCell !== undefined && !done}
				onHide={() => setActiveCell(undefined)}
			>
				<Modal.Header closeButton>
					<Modal.Title className="fs-6">
						{activeRow?.label} × {activeCol?.label}
					</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					<TriviaPlayerSelect
						players={searchList.filter((p) => !usedPids.has(p.pid))}
						onSelect={(p) => {
							if (activeCell !== undefined) {
								handleGuess(activeCell, p);
							}
						}}
						autoFocus
					/>
					{wrongGuess !== undefined ? (
						<div
							key={wrongKey}
							className="text-danger fw-bold small mt-2 trivia-shake"
						>
							✗ {wrongGuess} — not a match.
						</div>
					) : null}
					{activeHints.length > 0 ? (
						<div className="alert alert-secondary py-2 px-3 small mt-2 mb-0">
							{activeHints.map((h, i) => (
								<div key={i}>💡 {h}</div>
							))}
						</div>
					) : null}
					{activeMissed.length > 0 ? (
						<div className="text-body-secondary small mt-2">
							Already tried here: {activeMissed.join(", ")}
						</div>
					) : null}
					<div className="d-flex align-items-center gap-2 mt-3">
						<button
							className="btn btn-sm btn-light-bordered"
							disabled={
								hintsLeft <= 0 || activeHintLevel >= HINT_MULTIPLIER.length - 1
							}
							onClick={useHint}
							title={
								activeHintLevel >= HINT_MULTIPLIER.length - 1
									? "No more hints for this cell"
									: `Costs this cell ${Math.round((1 - (HINT_MULTIPLIER[activeHintLevel + 1] ?? 0)) * 100)}% of its points`
							}
						>
							💡 Hint ({hintsLeft})
						</button>
						<div className="text-body-secondary small ms-auto">
							{gameMaxGuesses === Infinity
								? "Unlimited guesses"
								: `${Math.max(0, guessesLeft)} guess${guessesLeft === 1 ? "" : "es"} left`}
						</div>
					</div>
				</Modal.Body>
			</Modal>

			{/* Answers reveal modal (after the game ends) */}
			<Modal
				show={revealCell !== undefined}
				onHide={() => setRevealCell(undefined)}
			>
				<Modal.Header closeButton>
					<Modal.Title className="fs-6">
						{revealCell !== undefined
							? `${grid.rows[Math.floor(revealCell / 3)]!.label} × ${grid.cols[revealCell % 3]!.label}`
							: ""}
					</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					<div className="text-body-secondary small mb-2">
						{revealSorted.length} qualifying player
						{revealSorted.length === 1 ? "" : "s"}, most common first
					</div>
					<div className="border rounded overflow-hidden">
						{revealSorted.slice(0, revealLimit).map((pid) => {
							const p = byPid.get(pid);
							const mine =
								revealCell !== undefined && cells[revealCell]!.pid === pid;
							const pts = reveal?.rarity[pid];
							const tier = pts === undefined ? undefined : tierOf(pts);
							return (
								<div
									key={pid}
									className={`trivia-answer-row ${mine ? "is-mine" : ""}`}
								>
									<AnswerFace pid={pid} />
									<div className="flex-grow-1" style={{ minWidth: 0 }}>
										<div className="text-truncate">
											{p?.name ?? "???"}
											{mine ? (
												<span className="badge text-bg-success ms-2">
													Your pick
												</span>
											) : null}
										</div>
										<div className="small text-body-secondary">
											{p?.pos ? `${p.pos} · ` : ""}
											{p?.years}
										</div>
									</div>
									{tier ? (
										<span className={`badge flex-shrink-0 ${tier.cls}`}>
											{tier.label} +{pts}
										</span>
									) : null}
								</div>
							);
						})}
					</div>
					{revealSorted.length > revealLimit ? (
						<button
							className="btn btn-sm btn-light-bordered mt-2"
							onClick={() => setRevealLimit((n) => n + 24)}
						>
							Show {Math.min(24, revealSorted.length - revealLimit)} more
						</button>
					) : null}
				</Modal.Body>
			</Modal>

			{/* Criterion picker (inline edit) */}
			<Modal
				show={editSlot !== undefined}
				onHide={() => setEditSlot(undefined)}
			>
				<Modal.Header closeButton>
					<Modal.Title className="fs-6">
						{editSlot !== undefined && editSlot < 3
							? `Row ${editSlot + 1}`
							: `Column ${(editSlot ?? 3) - 2}`}
					</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					<CriterionPicker
						catalog={catalog}
						current={editSlot !== undefined ? editRefs[editSlot] : undefined}
						taken={
							new Set(
								editRefs.filter((_, i) => i !== editSlot).map((r) => refKey(r)),
							)
						}
						onPick={(ref) => {
							const slot = editSlot;
							if (slot === undefined) {
								return;
							}
							setEditRefs((prev) => prev.map((r, i) => (i === slot ? ref : r)));
							setEditSlot(undefined);
						}}
					/>
				</Modal.Body>
			</Modal>
		</>
	);
};

// The criterion list for one row/column slot. A flat searchable list rather
// than a dropdown: picking is the whole interaction here, and a search box
// beats scrolling a few hundred achievements.
const CriterionPicker = ({
	catalog,
	current,
	taken,
	onPick,
}: {
	catalog: Catalog | undefined;
	current: CriterionRef | undefined;
	taken: Set<string>;
	onPick: (ref: CriterionRef) => void;
}) => {
	const [query, setQuery] = useState("");

	const groups = useMemo(() => {
		if (!catalog) {
			return [];
		}
		const q = query.trim().toLowerCase();
		const match = (label: string) => !q || label.toLowerCase().includes(q);
		type Option = {
			key: string;
			label: string;
			count: number;
			ref: CriterionRef;
		};
		const teams: Option[] = catalog.teams
			.filter((t) => match(t.label))
			.map((t) => ({
				key: `team-${t.tid}`,
				label: t.label,
				count: t.count,
				ref: { kind: "team", tid: t.tid },
			}));
		const career: Option[] = [];
		const season: Option[] = [];
		for (const a of catalog.achievements) {
			if (!match(a.label)) {
				continue;
			}
			(a.kind === "career" ? career : season).push({
				key: `ach-${a.id}`,
				label: a.label,
				count: a.count,
				ref: { kind: a.kind, id: a.id },
			});
		}
		return [
			{ label: "Teams", options: teams },
			{ label: "Career", options: career },
			{ label: "Season & awards", options: season },
		].filter((g) => g.options.length > 0);
	}, [catalog, query]);

	if (!catalog) {
		return <div className="text-body-secondary">Loading…</div>;
	}

	const currentKey = refKey(current);

	return (
		<>
			<input
				className="form-control mb-2"
				type="text"
				value={query}
				placeholder="Search criteria…"
				autoComplete="off"
				onChange={(e) => setQuery(e.target.value)}
			/>
			<div style={{ maxHeight: "55vh", overflowY: "auto" }}>
				{groups.length === 0 ? (
					<div className="text-body-secondary small">No match.</div>
				) : null}
				{groups.map((g) => (
					<div key={g.label} className="mb-3">
						<div className="text-body-secondary small fw-bold mb-1">
							{g.label}
						</div>
						<div className="d-flex flex-wrap gap-1">
							{g.options.map((o) => {
								const isCurrent = o.key === currentKey;
								const isTaken = taken.has(o.key);
								return (
									<button
										key={o.key}
										type="button"
										className={`btn btn-sm ${
											isCurrent ? "btn-primary" : "btn-light-bordered"
										}`}
										disabled={isTaken}
										title={
											isTaken
												? "Already used on this grid"
												: `${o.count} qualifying players`
										}
										onClick={() => onPick(o.ref)}
									>
										{o.label}{" "}
										<span className="text-body-secondary">({o.count})</span>
									</button>
								);
							})}
						</div>
					</div>
				))}
			</div>
		</>
	);
};

export default TriviaGrids;
