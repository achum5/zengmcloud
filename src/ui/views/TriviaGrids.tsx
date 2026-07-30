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
import { buildHintOptions } from "../util/triviaHint.ts";
import { TriviaHistoryModal } from "../components/TriviaHistoryModal.tsx";
import { TriviaPlayerModal } from "../components/TriviaPlayerModal.tsx";
import { TriviaSquares } from "../components/TriviaSquares.tsx";
import {
	addHistoryEntry,
	countPerfect,
	loadHistory,
	summarize,
	type TriviaReplay,
} from "../util/triviaHistory.ts";
import { shareHistory } from "../util/triviaHistorySync.ts";
import { shareOrCopy } from "../util/triviaShare.ts";
import { loadProgress, saveProgress } from "../util/triviaProgress.ts";
import { tierOf } from "../util/triviaTiers.ts";
import {
	decodeGridCode,
	encodeGridCode,
	type GridCodeRef,
} from "../../common/triviaGridCode.ts";
import { decadeLabel, statLabel } from "../../common/triviaCriteriaLabels.ts";
import { Confetti } from "./LiveGame/Confetti.tsx";
import type { View } from "../../common/types.ts";

// The Grids game (Immaculate Grid style): 9 cells, a shared pool of guesses,
// find any player in league history matching both the row and column criteria.
// A wrong guess burns a guess but leaves the cell open; each player can only
// be used once. Rarer correct answers score more.

type GridData = NonNullable<View<"triviaGrids">["data"]>;
type Criterion = GridData["grid"]["rows"][number];

// `stat` and `decade` carry their own threshold, which is what lets a header be
// edited to any number ("1+ PPG", "100+ PPG", "20 or fewer PPG") rather than
// picked from a fixed list.
type StatOp = "gte" | "lte";
type DecadeMode = "debut" | "played";

type CriterionRef =
	| { kind: "team"; tid: number }
	| { kind: "career" | "season"; id: string }
	| { kind: "stat"; spec: string; op: StatOp; value: number }
	| { kind: "decade"; mode: DecadeMode; decade: number };

type StatSpec = {
	id: string;
	label: string;
	unit: string;
	scope: "career" | "season";
	decimals: number;
	defaultValue: number;
	step: number;
};

type Catalog = {
	teams: { tid: number; label: string; count: number }[];
	achievements: {
		id: string;
		kind: "career" | "season";
		label: string;
		count: number;
	}[];
	statSpecs: StatSpec[];
	decades: number[];
};

// A solved cell: the pid/name that filled it and the rarity points earned.
type CellState = {
	pid?: number;
	name?: string;
	points: number;
};

const emptyCells = (): CellState[] =>
	Array.from({ length: 9 }, () => ({ points: 0 }));

// A grid in progress, as it survives a reload. The board itself is stored
// rather than re-derived from a code: rebuilding it would mean an async worker
// round trip before anything could be drawn, and the grid the player was
// looking at would flash in late.
type SavedGrid = {
	grid: GridData["grid"];
	cells: CellState[];
	usedPids: number[];
	guessesUsed: number;
	gameMaxGuesses: number | "Infinity";
	missedByCell: Record<number, string[]>;
	gaveUp: boolean;
	hinted: number[];
	// Optional so a save written before hint picks were one-shot still loads.
	failed?: number[];
	// Ditto, for saves written while a wrong guess still burned the player
	// across the whole board.
	missedPidsByCell?: Record<number, number[]>;
};

// Was a restored board already over? A finished game has already been written
// to the history, so resuming one must not record it a second time.
const savedIsDone = (saved: SavedGrid): boolean => {
	const solved = saved.cells.filter((c) => c.pid !== undefined).length;
	const max =
		saved.gameMaxGuesses === "Infinity" ? Infinity : saved.gameMaxGuesses;
	return (
		saved.gaveUp ||
		solved + (saved.failed?.length ?? 0) === 9 ||
		max - saved.guessesUsed <= 0
	);
};

const GUESS_SETTING_KEY = "triviaGridsGuesses";
const HINT_SETTING_KEY = "triviaGridsHintMode";

// Hint mode is a difficulty switch, not a consumable. With it on, tapping a
// cell deals six faces - one of which fits - so every cell is a multiple-choice
// question instead of a blank search box. It costs nothing to turn on and there
// is no budget to ration; the price is on the scoreboard, where a hinted cell
// is worth a quarter (never zero: a hinted solve still beats a blank cell), so
// a hinted board and an unhinted one stay comparable.
const HINT_POINT_MULTIPLIER = 0.25;
const MIN_HINTED_POINTS = 5;

const loadHintMode = (): boolean => {
	try {
		return localStorage.getItem(HINT_SETTING_KEY) === "1";
	} catch {
		return false;
	}
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

const refKey = (r: CriterionRef | undefined): string => {
	if (r === undefined) {
		return "";
	}
	switch (r.kind) {
		case "team":
			return `team-${r.tid}`;
		case "stat":
			return `stat-${r.spec}-${r.op}-${r.value}`;
		case "decade":
			return `decade-${r.mode}-${r.decade}`;
		default:
			return `ach-${r.id}`;
	}
};

const fmtStatValue = (value: number, decimals: number) =>
	decimals > 0
		? String(Number(value.toFixed(decimals)))
		: String(Math.round(value));

const criterionToRef = (c: Criterion): CriterionRef =>
	c.kind === "team" ? { kind: "team", tid: c.tid } : { kind: c.kind, id: c.id };

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

// A header while editing. For a plain criterion the whole tile opens the
// picker; for a PARAMETRIC one (a stat threshold or a decade) the controls are
// right here in the tile, so changing "30+ PPG" to "1+ PPG" is one keystroke
// rather than a trip through a modal.
const EditableHeader = ({
	header,
	criterionRef,
	spec,
	decades,
	onClick,
	onChange,
}: {
	header: { kind: string; tid?: number; label: string } | undefined;
	criterionRef: CriterionRef | undefined;
	spec: StatSpec | undefined;
	decades: number[];
	onClick: () => void;
	onChange: (ref: CriterionRef) => void;
}) => {
	if (criterionRef?.kind === "stat" && spec) {
		return (
			<div className="trivia-grid-head-edit is-inline">
				<button
					type="button"
					className="trivia-edit-swap"
					title="Change which stat this is"
					onClick={onClick}
				>
					{spec.unit}
					{spec.scope === "season" && spec.id !== "season-gp"
						? " (Season)"
						: ""}
				</button>
				<div className="d-flex gap-1 px-1 pb-1">
					<select
						className="form-select form-select-sm trivia-edit-op"
						title="Greater or fewer"
						value={criterionRef.op}
						onChange={(event) =>
							onChange({
								...criterionRef,
								op: event.target.value as StatOp,
							})
						}
					>
						<option value="gte">≥</option>
						<option value="lte">≤</option>
					</select>
					<input
						type="number"
						className="form-control form-control-sm trivia-edit-value"
						// No max: an absurd threshold just makes a dead cell, which the
						// board already reports, and capping it would rule out the
						// deliberately silly grids that are half the fun.
						min={0}
						step={spec.step}
						value={fmtStatValue(criterionRef.value, spec.decimals)}
						onChange={(event) => {
							const raw = Number.parseFloat(event.target.value);
							onChange({
								...criterionRef,
								value: Number.isFinite(raw) && raw >= 0 ? raw : 0,
							});
						}}
					/>
				</div>
			</div>
		);
	}

	if (criterionRef?.kind === "decade") {
		return (
			<div className="trivia-grid-head-edit is-inline">
				<button
					type="button"
					className="trivia-edit-swap"
					title="Change this criterion"
					onClick={onClick}
				>
					{criterionRef.mode === "debut" ? "Debuted in" : "Played in"}
				</button>
				<div className="px-1 pb-1">
					<select
						className="form-select form-select-sm"
						title="Decade"
						value={String(criterionRef.decade)}
						onChange={(event) =>
							onChange({
								...criterionRef,
								decade: Number.parseInt(event.target.value),
							})
						}
					>
						{decades.map((d) => (
							<option key={d} value={d}>
								{d}s
							</option>
						))}
					</select>
				</div>
			</div>
		);
	}

	return (
		<button
			type="button"
			className={`trivia-grid-head-edit ${header ? "" : "is-empty"}`}
			onClick={onClick}
		>
			{header ? (
				<CriterionLabel c={header} />
			) : (
				<div className="text-body-secondary small p-2">Pick one</div>
			)}
		</button>
	);
};

const HintFace = ({ pid }: { pid: number }) => {
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
		<span className="trivia-hint-face">
			{card ? (
				<PlayerPicture
					face={card.face}
					imgURL={card.imgURL}
					colors={card.colors}
					jersey={card.jersey}
					lazy
				/>
			) : null}
		</span>
	);
};

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

	const { lid } = useLocal(["lid"]);

	// An unfinished board from a previous visit. Read once, before any state
	// exists, so the restore seeds the initial state instead of racing the fresh
	// random grid the view always loads.
	const [restored] = useState(() => loadProgress<SavedGrid>("grids", lid));

	const [data, setData] = useState(() =>
		restored && props.data
			? // The saved board with the CURRENT search list: it's the same league
				// either way, and the pool is far too big to be worth storing.
				{ grid: restored.grid, searchList: props.data.searchList }
			: props.data,
	);
	const [cells, setCells] = useState<CellState[]>(
		() => restored?.cells ?? emptyCells(),
	);
	const [usedPids, setUsedPids] = useState<Set<number>>(
		() => new Set(restored?.usedPids ?? []),
	);
	const [guessesUsed, setGuessesUsed] = useState(
		() => restored?.guessesUsed ?? 0,
	);
	const [guessSetting, setGuessSetting] = useState(loadGuessSetting);
	const [gameMaxGuesses, setGameMaxGuesses] = useState(() => {
		if (restored === undefined) {
			return loadGuessSetting();
		}
		// Infinity does not survive JSON, so it travels as a sentinel.
		return restored.gameMaxGuesses === "Infinity"
			? Infinity
			: restored.gameMaxGuesses;
	});
	const [activeCell, setActiveCell] = useState<number | undefined>();
	const [wrongGuess, setWrongGuess] = useState<string | undefined>();
	// Increments on each miss so the shake animation retriggers.
	const [wrongKey, setWrongKey] = useState(0);
	// Names already burned on each cell, so the same miss isn't repeated.
	const [missedByCell, setMissedByCell] = useState<Record<number, string[]>>(
		() => restored?.missedByCell ?? {},
	);
	// The same misses as pids, which is what the search box needs to drop them.
	// Per CELL, not per board: a wrong guess is wrong for that pairing only, and
	// the player may well be the right answer somewhere else on the grid.
	const [missedPidsByCell, setMissedPidsByCell] = useState<
		Record<number, number[]>
	>(() => restored?.missedPidsByCell ?? {});
	const [gaveUp, setGaveUp] = useState(() => restored?.gaveUp ?? false);
	// Cells burned by a wrong pick in hint mode. Six faces with one right answer
	// is a single question, so getting it wrong closes the cell rather than
	// letting you work through the other five.
	const [failedCells, setFailedCells] = useState<Set<number>>(
		() => new Set(restored?.failed ?? []),
	);
	const [loadingNew, setLoadingNew] = useState(false);
	const [cards, setCards] = useState<Record<number, TriviaPlayerCard>>({});
	const [revealCell, setRevealCell] = useState<number | undefined>();
	const [revealLimit, setRevealLimit] = useState(24);
	const [history, setHistory] = useState(() => loadHistory("grids"));
	const [showHistory, setShowHistory] = useState(false);
	const [showShare, setShowShare] = useState(false);
	const [shared, setShared] = useState<string | undefined>();
	const [codeInput, setCodeInput] = useState("");
	const [codeError, setCodeError] = useState<string | undefined>();
	// The player card, opened by tapping anyone you've already found.
	const [profilePid, setProfilePid] = useState<number | undefined>();

	// Hint mode. `hinted` records which cells were played with help (so they
	// score less for the rest of the game, however they're eventually solved),
	// `hintShuffle` lets a cell be re-dealt, and `hintPicked` is the face that
	// was chosen - a hint is ONE guess, so choosing is the end of that cell.
	const [hintMode, setHintMode] = useState(loadHintMode);
	const [hinted, setHinted] = useState<Set<number>>(
		() => new Set(restored?.hinted ?? []),
	);
	const [hintCell, setHintCell] = useState<number | undefined>();
	const [hintShuffle, setHintShuffle] = useState<Record<number, number>>({});
	const [hintPicked, setHintPicked] = useState<number | undefined>();
	// usedPids frozen at the moment the hand was dealt. Without this, a wrong
	// pick lands in usedPids, the options recompute, and the whole hand re-deals
	// under the cursor - so crossing an option out would be meaningless.
	const [hintUsed, setHintUsed] = useState<Set<number>>(new Set());

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
	// A failed cell can never be solved, so a board with nothing left open is
	// over even with guesses in hand.
	const done =
		grid !== undefined &&
		(gaveUp || correctCount + failedCells.size === 9 || guessesLeft <= 0);
	const immaculate = correctCount === 9;

	const summary = summarize(history);
	const best = summary.best;
	const immaculateCount = countPerfect(history);

	// This board as a code someone else can paste in. Derived from the criteria
	// on screen, so an edited or replayed grid shares correctly too.
	const gridCode = useMemo(
		() =>
			grid
				? encodeGridCode(
						grid.rows.map((c) => criterionToRef(c) as GridCodeRef),
						grid.cols.map((c) => criterionToRef(c) as GridCodeRef),
					)
				: "",
		[grid],
	);

	// Persist the board on every move. Cheap enough to do on each change (one
	// JSON write of a board), and it means a reload, an accidental tap through
	// to another page, or iOS reclaiming the tab all come back to the same grid.
	useEffect(() => {
		if (!grid) {
			return;
		}
		saveProgress("grids", lid, {
			grid,
			cells,
			usedPids: [...usedPids],
			guessesUsed,
			gameMaxGuesses: gameMaxGuesses === Infinity ? "Infinity" : gameMaxGuesses,
			missedByCell,
			missedPidsByCell,
			gaveUp,
			hinted: [...hinted],
			failed: [...failedCells],
		} satisfies SavedGrid);
	}, [
		grid,
		lid,
		cells,
		usedPids,
		guessesUsed,
		gameMaxGuesses,
		missedByCell,
		missedPidsByCell,
		gaveUp,
		hinted,
		failedCells,
	]);

	// Faces for a restored board. handleGuess fetches these as cells are solved,
	// so without this a resumed grid comes back with empty picture frames.
	useEffect(() => {
		for (const cell of cells) {
			if (cell.pid !== undefined && !cards[cell.pid]) {
				const pid = cell.pid;
				void fetchTriviaCard(pid).then((card) => {
					if (card) {
						setCards((prev) => (prev[pid] ? prev : { ...prev, [pid]: card }));
					}
				});
			}
		}
		// Only when the solved set changes - `cards` is what this fills in, and
		// depending on it would re-run the effect on every fetch it completes.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [cells]);

	// Record each finished game into the history exactly once. The label is the
	// board itself, so the history search finds "every grid with the Knicks on
	// it" - which is the thing you'd actually want to look up.
	const recordedRef = useRef(restored !== undefined && savedIsDone(restored));
	useEffect(() => {
		if (!done || recordedRef.current || !grid) {
			return;
		}
		recordedRef.current = true;
		const team = [...grid.rows, ...grid.cols].find((c) => c.kind === "team");
		const next = addHistoryEntry("grids", {
			score,
			label: `${grid.rows.map((c) => c.label).join(" / ")} × ${grid.cols
				.map((c) => c.label)
				.join(" / ")}`,
			detail: `${correctCount}/9 solved${hinted.size > 0 ? ` · ${hinted.size} hinted` : ""}`,
			tid: team?.tid,
			colors: team?.colors,
			progress: { done: correctCount, total: 9 },
			// The squares, so anyone in the room can see the shape of the board
			// without seeing an answer on it.
			cells: cells.map((c) => (c.pid === undefined ? null : c.points)),
			replay: { kind: "grid", code: gridCode },
		});
		setHistory(next);
		shareHistory("grids", next);
	}, [done, grid, score, correctCount, hinted, cells, gridCode]);

	const resetGame = (fresh: GridData) => {
		setData(fresh);
		setCells(emptyCells());
		setUsedPids(new Set());
		setGuessesUsed(0);
		setGameMaxGuesses(guessSetting);
		setActiveCell(undefined);
		setWrongGuess(undefined);
		setMissedByCell({});
		setMissedPidsByCell({});
		setGaveUp(false);
		setCards({});
		setRevealCell(undefined);
		setShared(undefined);
		setHinted(new Set());
		setHintCell(undefined);
		setHintShuffle({});
		setHintPicked(undefined);
		setHintUsed(new Set());
		setFailedCells(new Set());
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

	// Load a board from a code. Invalid codes and codes whose criteria don't
	// exist in THIS league both land here as "no grid" - the message says the
	// same thing either way, because from the player's side they are the same
	// problem.
	const playCode = async (code: string) => {
		const decoded = decodeGridCode(code);
		if (!decoded) {
			setCodeError("That doesn't look like a grid code.");
			return;
		}
		setCodeError(undefined);
		setLoadingNew(true);
		try {
			const fresh = await toWorker("main", "triviaCustomGrid", {
				rows: decoded.rows as any,
				cols: decoded.cols as any,
			});
			if (fresh && fresh.grid.cells.every((c) => c.pids.length > 0)) {
				resetGame(fresh);
				setShowShare(false);
				setCodeInput("");
			} else {
				setCodeError("That grid doesn't work in this league.");
			}
		} finally {
			setLoadingNew(false);
		}
	};

	const replay = (r: TriviaReplay) => {
		if (r.kind === "grid") {
			void playCode(r.code);
		}
	};

	const toggleHintMode = (on: boolean) => {
		setHintMode(on);
		try {
			localStorage.setItem(HINT_SETTING_KEY, on ? "1" : "0");
		} catch {}
	};

	const copyCode = async () => {
		const result = await shareOrCopy(gridCode);
		setShared(
			result === "copied"
				? "Copied"
				: result === "shared"
					? "Shared"
					: "Copy failed",
		);
		// A button permanently reading "Copied" stops looking like a button you
		// can press again.
		setTimeout(() => setShared(undefined), 2500);
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
		// One player per BOARD applies to answers, not to attempts. Burning a
		// wrong guess out of the whole grid meant missing with LeBron on
		// Celtics x Lakers also locked him out of Lakers x Heat, where he's the
		// obvious answer.
		if (correct) {
			setUsedPids((prev) => new Set(prev).add(guess.pid));
		}
		setGuessesUsed((prev) => prev + 1);
		if (correct) {
			// Hints are paid for out of the cell's score, not out of guesses.
			const multiplier = hinted.has(cellIndex) ? HINT_POINT_MULTIPLIER : 1;
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
			setMissedPidsByCell((prev) => ({
				...prev,
				[cellIndex]: [...(prev[cellIndex] ?? []), guess.pid],
			}));
			if (gameMaxGuesses - (guessesUsed + 1) <= 0) {
				setActiveCell(undefined);
			}
		}
	};

	// --- Hints ---------------------------------------------------------------

	const popByPid = useMemo(() => {
		const m = new Map<number, number>();
		for (const p of searchList) {
			m.set(p.pid, p.pop ?? 0);
		}
		return m;
	}, [searchList]);

	// One cell's hand. Pulled out of the memo so opening a hint can look at what
	// it would deal BEFORE charging for it.
	const dealHint = (
		cellIndex: number,
		used: Set<number>,
		shuffleCount: number,
	) => {
		if (!grid) {
			return [];
		}
		const r = Math.floor(cellIndex / 3);
		const c = cellIndex % 3;
		return buildHintOptions({
			cellPids: grid.cells[cellIndex]!.pids,
			rarity: grid.cells[cellIndex]!.rarity,
			rowPids: grid.rowPids?.[r] ?? [],
			colPids: grid.colPids?.[c] ?? [],
			usedPids: used,
			popByPid,
			seed: `${cellIndex}|${shuffleCount}|${searchList.length}`,
		});
	};

	// The faces for the open hint. Recomputed from the seed, so it survives
	// re-renders and only changes when the cell or the shuffle count does.
	const hintOptions = useMemo(() => {
		if (hintCell === undefined) {
			return [];
		}
		return dealHint(hintCell, hintUsed, hintShuffle[hintCell] ?? 0);
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [hintCell, grid, hintUsed, popByPid, hintShuffle, searchList.length]);

	// Deal a cell's faces. Marking the cell hinted is what costs points, and it
	// sticks for the rest of the game: seeing the shortlist can't be un-seen, so
	// typing the answer afterwards still scores as a hinted solve.
	//
	// A cell with nothing left to find has no hint to give, so it isn't charged
	// for one.
	const openHintFor = (cellIndex: number) => {
		const snapshot = new Set(usedPids);
		const options = dealHint(cellIndex, snapshot, hintShuffle[cellIndex] ?? 0);
		if (options.length > 0) {
			setHinted((prev) => new Set(prev).add(cellIndex));
		}
		setHintPicked(undefined);
		setHintUsed(snapshot);
		setHintCell(cellIndex);
		setActiveCell(undefined);
	};

	// Picking a face. Six faces with exactly one right answer is a single
	// question, so it gets a single answer: right fills the cell, wrong closes
	// it. Being able to work through the other five made hint mode a free
	// solve for the price of a few guesses.
	const pickHint = (pid: number, correct: boolean) => {
		if (hintCell === undefined || hintPicked !== undefined) {
			return;
		}
		const p = byPid.get(pid);
		if (!p) {
			return;
		}
		setHintPicked(pid);
		handleGuess(hintCell, p);
		if (correct) {
			setHintCell(undefined);
		} else {
			// The modal stays up so the answer can be seen, but the cell is done.
			setFailedCells((prev) => new Set(prev).add(hintCell));
		}
	};

	const reshuffleHint = () => {
		if (hintCell === undefined || hintPicked !== undefined) {
			return;
		}
		setHintShuffle((prev) => ({
			...prev,
			[hintCell]: (prev[hintCell] ?? 0) + 1,
		}));
		// Re-deal against what is ACTUALLY used now, so a shuffle drops the
		// players burned since the hand was dealt.
		setHintUsed(new Set(usedPids));
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

	const specById = (id: string) =>
		catalog?.statSpecs.find((sp) => sp.id === id);

	// Parametric refs format their own label from the live threshold, so the
	// header updates as you type instead of waiting on the worker's response.
	const labelForRef = (r: CriterionRef | undefined) => {
		if (!r) {
			return undefined;
		}
		if (r.kind === "stat") {
			const spec = specById(r.spec);
			return spec ? statLabel(spec, r.op, r.value) : undefined;
		}
		if (r.kind === "decade") {
			return decadeLabel(r.mode, r.decade);
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
		// await, not .then: toWorker's declared return nests one promise deeper
		// than it resolves, so a .then callback hands back a Promise-typed value
		// and setEditPreview was being passed the wrong shape.
		void (async () => {
			try {
				const result = await toWorker("main", "triviaCustomGrid", {
					rows: editRefs.slice(0, 3) as CriterionRef[],
					cols: editRefs.slice(3, 6) as CriterionRef[],
				});
				if (!stale) {
					setEditPreview(result);
				}
			} finally {
				if (!stale) {
					setEditLoading(false);
				}
			}
		})();
		return () => {
			stale = true;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [editing, editComplete, JSON.stringify(editRefs)]);

	const specForSlot = (slot: number) => {
		const r = editRefs[slot];
		return r?.kind === "stat" ? specById(r.spec) : undefined;
	};

	const setSlotRef = (slot: number, ref: CriterionRef) => {
		setEditRefs((prev) => prev.map((r, i) => (i === slot ? ref : r)));
	};

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

	const activeMissed =
		activeCell !== undefined ? (missedByCell[activeCell] ?? []) : [];
	const activeMissedPids = new Set(
		activeCell !== undefined ? (missedPidsByCell[activeCell] ?? []) : [],
	);

	return (
		<>
			{editing ? null : (
				<div className="trivia-toolbar mb-3">
					{done ? null : (
						<button
							className="btn btn-sm btn-light-bordered"
							onClick={() => setGaveUp(true)}
						>
							Give up
						</button>
					)}
					<button
						className="btn btn-sm btn-primary"
						disabled={loadingNew}
						onClick={newGrid}
					>
						{loadingNew ? "Generating" : "New grid"}
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
					<button
						className="btn btn-sm btn-light-bordered"
						onClick={() => setShowHistory(true)}
					>
						History
					</button>
					<div className="trivia-toolbar-score ms-auto">
						{correctCount}/9 solved
					</div>
				</div>
			)}

			<div className="trivia-layout">
				<div className="trivia-board-col">
					<div className="trivia-grid-board mb-2">
						<div
							className="trivia-grid-inner"
							style={{
								// Fractional columns so the inner grid always fills the board -
								// fixed max widths left a strip of the container showing. The
								// minmax(0, ...) lets a column shrink below its header's
								// min-content width; without it a long criterion ("2nd Round
								// Pick") pushes the board past the edge of a phone screen.
								gridTemplateColumns:
									"minmax(64px, 108px) repeat(3, minmax(0, 1fr))",
							}}
						>
							<div className="trivia-grid-head flex-column">
								{editing ? (
									<div className="text-body-secondary small text-center p-1 lh-sm">
										Tap a header to change it
									</div>
								) : (
									<>
										<div
											className="text-body-secondary"
											style={{ fontSize: "0.7rem" }}
										>
											Score
										</div>
										<div className="h3 mb-0">{score}</div>
										<div
											className="text-body-secondary"
											style={{ fontSize: "0.7rem" }}
											title="Guesses left"
										>
											{gameMaxGuesses === Infinity
												? "∞"
												: Math.max(0, guessesLeft)}{" "}
											left
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
										criterionRef={editRefs[slot]}
										spec={specForSlot(slot)}
										decades={catalog?.decades ?? []}
										onClick={() => setEditSlot(slot)}
										onChange={(ref) => setSlotRef(slot, ref)}
									/>
								);
							})}

							{[0, 1, 2].map((r) => (
								<Fragment key={r}>
									{editing ? (
										<EditableHeader
											header={headerFor(r)}
											criterionRef={editRefs[r]}
											spec={specForSlot(r)}
											decades={catalog?.decades ?? []}
											onClick={() => setEditSlot(r)}
											onChange={(ref) => setSlotRef(r, ref)}
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
											// The cell itself stays neutral: flooding it with the tier
											// color drowned the face and made nine solved cells read as
											// nine unrelated blocks of paint. The rarity lives in one
											// bar and one number instead, which is both quieter and
											// easier to compare across the board.
											return (
												<button
													key={cIdx}
													type="button"
													className="trivia-grid-solved trivia-pop"
													title={`${cell.name} · ${tier.label}`}
													onClick={() => setProfilePid(cell.pid)}
												>
													<span className="trivia-grid-solved-face">
														{card ? (
															<PlayerPicture
																face={card.face}
																imgURL={card.imgURL}
																colors={card.colors}
																jersey={card.jersey}
																lazy
															/>
														) : null}
													</span>
													<span
														className="trivia-grid-rarity"
														style={{ color: tier.color }}
													>
														{cell.points}
													</span>
													<span className="trivia-grid-solved-name">
														{cell.name}
													</span>
													<span
														className="trivia-grid-rarity-bar"
														style={{
															width: `${Math.max(4, Math.min(100, cell.points))}%`,
															background: tier.color,
														}}
													/>
												</button>
											);
										}
										// A cell burned by a wrong hint pick is out of play. It
										// can still be opened once the game ends, to see what fit.
										const failed = failedCells.has(i);
										return (
											<button
												key={cIdx}
												className={`btn p-0 trivia-cell trivia-grid-cell ${
													activeCell === i
														? "btn-primary"
														: failed || done
															? "btn-light-bordered border-danger"
															: "btn-light-bordered"
												}`}
												style={{ aspectRatio: "1 / 1" }}
												disabled={failed && !done}
												onClick={() => {
													if (done) {
														setRevealCell(i);
														setRevealLimit(24);
													} else if (hintMode) {
														setWrongGuess(undefined);
														openHintFor(i);
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
												) : failed ? (
													<span className="h4 mb-0 text-danger">&times;</span>
												) : (
													<span
														className={`h4 mb-0 ${hinted.has(i) ? "text-warning" : "text-body-secondary"}`}
														title={hinted.has(i) ? "Hint used" : undefined}
													>
														+
													</span>
												)}
											</button>
										);
									})}
								</Fragment>
							))}
						</div>
					</div>

					{editing ? null : (
						<div className="trivia-board-actions">
							<button
								className="btn btn-light-bordered"
								onClick={() => {
									setCodeError(undefined);
									setShowShare(true);
								}}
							>
								Share grid
							</button>
							<label
								className="trivia-hint-toggle"
								title="Turn every cell into a six-face multiple choice. Hinted cells score a quarter."
							>
								<span className="form-switch d-inline-flex">
									<input
										type="checkbox"
										className="form-check-input m-0"
										role="switch"
										checked={hintMode}
										onChange={(event) => toggleHintMode(event.target.checked)}
									/>
								</span>
								Hint
							</label>
							<button
								className="btn btn-light-bordered ms-auto"
								onClick={startEdit}
								title="Build your own rows and columns"
							>
								Custom
							</button>
						</div>
					)}

					{editing ? (
						<div className="d-flex flex-wrap align-items-center gap-2">
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
									A red cell has no qualifying player — change one of its
									criteria.
								</div>
							) : null}
						</div>
					) : null}
				</div>

				<div className="trivia-side-col">
					{done ? (
						<>
							{immaculate ? <Confetti /> : null}
							<div className="card trivia-rise mb-3">
								<div className="card-body d-flex flex-wrap align-items-center gap-3">
									<TriviaSquares
										size={18}
										cells={cells.map((c) =>
											c.pid === undefined ? null : c.points,
										)}
									/>
									<div className="flex-grow-1">
										<div className="h4 mb-1">
											{immaculate
												? "Immaculate"
												: correctCount >= 7
													? "So close!"
													: correctCount >= 4
														? "Solid board."
														: "Tough grid."}
										</div>
										<div className="mb-1">
											<span className="fw-bold">{score}</span> points ·{" "}
											{correctCount}/9 solved
											{score > 0 && score >= best ? (
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
														<span key={i} className={`badge ${tier.badge}`}>
															{tier.label} +{c.points}
														</span>
													);
												})}
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
					) : null}

					{editing ? null : (
						<div className="d-flex flex-wrap gap-2">
							<div className="trivia-tile">
								<div className="trivia-tile-value">{summary.played}</div>
								<div className="trivia-tile-label">Played</div>
							</div>
							<div className="trivia-tile">
								<div className="trivia-tile-value">{immaculateCount}</div>
								<div className="trivia-tile-label">Immaculate</div>
							</div>
							<div className="trivia-tile">
								<div className="trivia-tile-value">{summary.best}</div>
								<div className="trivia-tile-label">Best</div>
							</div>
							<div className="trivia-tile">
								<div className="trivia-tile-value">{summary.average}</div>
								<div className="trivia-tile-label">Avg score</div>
							</div>
						</div>
					)}
				</div>
			</div>

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
						players={searchList.filter(
							(p) => !usedPids.has(p.pid) && !activeMissedPids.has(p.pid),
						)}
						onSelect={(p) => {
							if (activeCell !== undefined) {
								handleGuess(activeCell, p);
							}
						}}
						// The modal exists only to take a guess, and it was opened by a
						// deliberate tap on a cell. Landing in the search box is what
						// anyone opening it wants, on a keyboard or a phone.
						// eslint-disable-next-line jsx-a11y/no-autofocus
						autoFocus
					/>
					{wrongGuess !== undefined ? (
						<div
							key={wrongKey}
							className="text-danger fw-bold small mt-2 trivia-shake"
						>
							{wrongGuess} — not a match.
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
							onClick={() => {
								if (activeCell !== undefined) {
									openHintFor(activeCell);
								}
							}}
							title={`Show six players, one of which fits. Worth ${Math.round(HINT_POINT_MULTIPLIER * 100)}% points`}
						>
							Hint
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
								<button
									type="button"
									key={pid}
									className={`trivia-answer-row ${mine ? "is-mine" : ""}`}
									onClick={() => setProfilePid(pid)}
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
										<span className={`badge flex-shrink-0 ${tier.badge}`}>
											{tier.label} +{pts}
										</span>
									) : null}
								</button>
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

			{/* Hint mode: six faces, one of which fits the cell. The two criteria
			    are shown as they appear on the board rather than as a text title -
			    a logo is what you actually recognise a cell by. */}
			<Modal
				show={hintCell !== undefined}
				onHide={() => setHintCell(undefined)}
				size="lg"
			>
				<Modal.Header closeButton>
					<Modal.Title className="fs-5">Hint mode</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					{hintCell !== undefined ? (
						<div className="trivia-hint-criteria mb-3">
							<div className="trivia-hint-criterion">
								<CriterionLabel c={grid.rows[Math.floor(hintCell / 3)]!} />
							</div>
							<div className="trivia-hint-times" aria-hidden="true">
								×
							</div>
							<div className="trivia-hint-criterion">
								<CriterionLabel c={grid.cols[hintCell % 3]!} />
							</div>
						</div>
					) : null}
					{hintOptions.length === 0 ? (
						<div className="text-body-secondary">
							No players are left for this cell.
						</div>
					) : (
						<>
							<div className="trivia-hint-grid">
								{hintOptions.map((option) => {
									const p = byPid.get(option.pid);
									// After a pick the hand becomes a result: the face that
									// was chosen and the one that was right are both marked,
									// which is the only way to learn anything from a miss.
									const answered = hintPicked !== undefined;
									const picked = hintPicked === option.pid;
									const reveal = answered
										? option.correct
											? "is-right"
											: picked
												? "is-wrong"
												: "is-dimmed"
										: "";
									return (
										<button
											key={option.pid}
											type="button"
											className={`trivia-hint-option ${reveal}`}
											disabled={answered}
											onClick={() => pickHint(option.pid, option.correct)}
										>
											<HintFace pid={option.pid} />
											<span className="trivia-hint-name text-truncate">
												{p?.name ?? "???"}
											</span>
										</button>
									);
								})}
							</div>
							<div className="d-flex align-items-center gap-2 mt-3">
								{hintPicked === undefined ? (
									<>
										<div className="fw-bold">
											Choose the correct player — one guess
										</div>
										<button
											className="btn btn-sm btn-light-bordered ms-auto"
											onClick={reshuffleHint}
										>
											Shuffle
										</button>
									</>
								) : (
									<>
										<div
											className={`fw-bold ${
												hintOptions.find((o) => o.pid === hintPicked)?.correct
													? "text-success"
													: "text-danger"
											}`}
										>
											{hintOptions.find((o) => o.pid === hintPicked)?.correct
												? "Correct"
												: "Wrong — this cell is closed"}
										</div>
										<button
											className="btn btn-sm btn-primary ms-auto"
											onClick={() => setHintCell(undefined)}
										>
											Close
										</button>
									</>
								)}
							</div>
						</>
					)}
				</Modal.Body>
			</Modal>

			<TriviaHistoryModal
				game="grids"
				show={showHistory}
				onHide={() => setShowHistory(false)}
				onReplay={replay}
			/>

			<TriviaPlayerModal
				pid={profilePid}
				onHide={() => setProfilePid(undefined)}
			/>

			{/* Share: a grid IS its six criteria, so a code carrying them is
			    enough for someone else to play the identical board. */}
			<Modal show={showShare} onHide={() => setShowShare(false)}>
				<Modal.Header closeButton>
					<Modal.Title className="fs-5">Share grid</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					<div className="text-body-secondary small mb-1">This grid's code</div>
					<div className="trivia-code mb-2">{gridCode}</div>
					<button className="btn btn-primary mb-4" onClick={copyCode}>
						{shared ?? "Copy code"}
					</button>

					<div className="text-body-secondary small mb-1">Play a code</div>
					<div className="d-flex gap-2">
						<input
							className="form-control"
							type="text"
							value={codeInput}
							placeholder="Paste a code"
							autoComplete="off"
							spellCheck={false}
							onChange={(event) => {
								setCodeInput(event.target.value);
								setCodeError(undefined);
							}}
						/>
						<button
							className="btn btn-light-bordered flex-shrink-0"
							disabled={loadingNew || codeInput.trim() === ""}
							onClick={() => playCode(codeInput)}
						>
							Play
						</button>
					</div>
					{codeError ? (
						<div className="text-danger small mt-2">{codeError}</div>
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
	onPick,
}: {
	catalog: Catalog | undefined;
	current: CriterionRef | undefined;
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
		// Parametric entries seed at their default threshold; the number is then
		// edited in place on the board rather than here.
		const stats: Option[] = catalog.statSpecs
			.filter((sp) => match(sp.label))
			.map((sp) => ({
				key: `stat-${sp.id}`,
				label: `${sp.label} (editable)`,
				count: -1,
				ref: { kind: "stat", spec: sp.id, op: "gte", value: sp.defaultValue },
			}));
		const decadeOptions: Option[] = [];
		for (const mode of ["debut", "played"] as DecadeMode[]) {
			for (const d of catalog.decades) {
				const label = decadeLabel(mode, d);
				if (match(label)) {
					decadeOptions.push({
						key: `decade-${mode}-${d}`,
						label,
						count: -1,
						ref: { kind: "decade", mode, decade: d },
					});
				}
			}
		}
		return [
			{ label: "Teams", options: teams },
			{ label: "Stat thresholds", options: stats },
			{ label: "Decades", options: decadeOptions },
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
								// Deliberately NOT disabled when already used elsewhere on the
								// board - an all-Celtics grid is a perfectly good thing to want.
								const isCurrent = o.key === currentKey;
								return (
									<button
										key={o.key}
										type="button"
										className={`btn btn-sm ${
											isCurrent ? "btn-primary" : "btn-light-bordered"
										}`}
										title={
											o.count >= 0
												? `${o.count} qualifying players`
												: "Set the number on the board"
										}
										onClick={() => onPick(o.ref)}
									>
										{o.label}
										{o.count >= 0 ? (
											<span className="text-body-secondary"> ({o.count})</span>
										) : null}
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
