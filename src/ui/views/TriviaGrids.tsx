import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import { Modal } from "../components/Modal.tsx";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import SelectMultiple from "../components/SelectMultiple/index.tsx";
import TriviaPlayerSelect, {
	type TriviaSearchPlayer,
} from "../components/TriviaPlayerSelect.tsx";
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

type PlayerCard = {
	pid: number;
	face?: any;
	imgURL?: string;
	colors?: [string, string, string];
	jersey?: string;
};

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

// Rarity points (10-100, higher = more obscure) mapped to a display tier.
const tierOf = (
	points: number,
): { label: string; badge: string } => {
	if (points >= 90) {
		return { label: "Legendary", badge: "text-bg-warning" };
	}
	if (points >= 65) {
		return { label: "Rare", badge: "text-bg-info" };
	}
	if (points >= 35) {
		return { label: "Solid", badge: "text-bg-success" };
	}
	return { label: "Common", badge: "text-bg-secondary" };
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

const TriviaGrids = (props: View<"triviaGrids">) => {
	useTitleBar({ title: "Grids" });

	const { teamInfoCache } = useLocal(["teamInfoCache"]);

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
	const [gaveUp, setGaveUp] = useState(false);
	const [loadingNew, setLoadingNew] = useState(false);
	const [cards, setCards] = useState<Record<number, PlayerCard>>({});
	const [revealCell, setRevealCell] = useState<number | undefined>();
	const [stats, setStats] = useState<Stats>(loadStats);

	// Custom grid builder
	const [builderOpen, setBuilderOpen] = useState(false);
	const [catalog, setCatalog] = useState<Catalog | undefined>();
	const [builderRefs, setBuilderRefs] = useState<(CriterionRef | undefined)[]>(
		Array.from({ length: 6 }, () => undefined),
	);
	const [builderPreview, setBuilderPreview] = useState<GridData | undefined>();
	const [builderLoading, setBuilderLoading] = useState(false);

	const grid = data?.grid;
	const searchList = data?.searchList ?? [];

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
		setGaveUp(false);
		setCards({});
		setRevealCell(undefined);
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
			setCells((prev) =>
				prev.map((c, i) =>
					i === cellIndex
						? {
								pid: guess.pid,
								name: guess.name,
								points: cell.rarity[guess.pid] ?? 10,
							}
						: c,
				),
			);
			setWrongGuess(undefined);
			setActiveCell(undefined);
			const tid = cellTid(cellIndex);
			void toWorker("main", "triviaPlayerCard", { pid: guess.pid, tid }).then(
				(card) => {
					if (card) {
						setCards((prev) => ({ ...prev, [guess.pid]: card }));
					}
				},
			);
		} else {
			// Cell stays open - the burned guess is the price. Keep the modal up
			// for another try unless that was the last guess.
			setWrongGuess(guess.name);
			setWrongKey((k) => k + 1);
			if (gameMaxGuesses - (guessesUsed + 1) <= 0) {
				setActiveCell(undefined);
			}
		}
	};

	const openBuilder = async () => {
		setBuilderOpen(true);
		if (!catalog) {
			const c = await toWorker("main", "triviaGridCatalog", undefined);
			if (c) {
				setCatalog(c);
			}
		}
	};

	// Re-validate the custom grid whenever all six slots are picked.
	const builderComplete = builderRefs.every((r) => r !== undefined);
	useEffect(() => {
		if (!builderOpen || !builderComplete) {
			setBuilderPreview(undefined);
			return;
		}
		let stale = false;
		setBuilderLoading(true);
		void toWorker("main", "triviaCustomGrid", {
			rows: builderRefs.slice(0, 3) as CriterionRef[],
			cols: builderRefs.slice(3, 6) as CriterionRef[],
		})
			.then((result) => {
				if (!stale) {
					setBuilderPreview(result);
				}
			})
			.finally(() => {
				if (!stale) {
					setBuilderLoading(false);
				}
			});
		return () => {
			stale = true;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [builderOpen, JSON.stringify(builderRefs)]);

	const builderPlayable =
		builderPreview !== undefined &&
		builderPreview.grid.cells.every((c) => c.pids.length > 0);

	const playCustom = () => {
		if (builderPreview) {
			resetGame(builderPreview);
			setBuilderOpen(false);
		}
	};

	const CriterionHeader = ({ c }: { c: Criterion }) => {
		if (c.kind === "team") {
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

	// --- Builder helpers ----------------------------------------------------

	type BuilderOption = {
		key: string;
		label: string;
		ref: CriterionRef;
		[k: string]: unknown;
	};

	const refKey = (r: CriterionRef | undefined) =>
		r === undefined ? "" : r.kind === "team" ? `team-${r.tid}` : `ach-${r.id}`;

	const builderOptions = useMemo(() => {
		if (!catalog) {
			return [];
		}
		const teams: BuilderOption[] = catalog.teams.map((t) => ({
			key: `team-${t.tid}`,
			label: `${t.label} (${t.count})`,
			ref: { kind: "team", tid: t.tid },
		}));
		const career: BuilderOption[] = [];
		const season: BuilderOption[] = [];
		for (const a of catalog.achievements) {
			const opt: BuilderOption = {
				key: `ach-${a.id}`,
				label: `${a.label} (${a.count})`,
				ref: { kind: a.kind, id: a.id },
			};
			(a.kind === "career" ? career : season).push(opt);
		}
		return [
			{ label: "Teams", options: teams },
			{ label: "Career", options: career },
			{ label: "Season/Awards", options: season },
		];
	}, [catalog]);

	const builderSlot = (slot: number, label: string) => {
		const current = builderRefs[slot];
		const currentKey = refKey(current);
		const takenKeys = new Set(
			builderRefs.filter((_, i) => i !== slot).map(refKey),
		);
		const options = builderOptions.map((group) => ({
			label: group.label,
			options: group.options.filter((o) => !takenKeys.has(o.key)),
		}));
		const flat = builderOptions.flatMap((g) => g.options);
		const value = flat.find((o) => o.key === currentKey) ?? null;
		return (
			<div key={slot}>
				<label className="form-label small mb-1">{label}</label>
				<SelectMultiple<BuilderOption>
					value={value}
					options={options}
					onChange={(o) => {
						setBuilderRefs((prev) =>
							prev.map((r, i) => (i === slot ? (o?.ref ?? undefined) : r)),
						);
					}}
					getOptionLabel={(o) => o.label}
					getOptionValue={(o) => o.key}
					loading={catalog === undefined}
				/>
			</div>
		);
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
		activeCell !== undefined ? grid.rows[Math.floor(activeCell / 3)] : undefined;
	const activeCol = activeCell !== undefined ? grid.cols[activeCell % 3] : undefined;

	const reveal = revealCell !== undefined ? grid.cells[revealCell] : undefined;
	const revealSorted = reveal
		? [...reveal.pids].sort(
				(a, b) => (reveal.rarity[a] ?? 0) - (reveal.rarity[b] ?? 0),
			)
		: [];

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
				<div className="d-flex flex-wrap align-items-center gap-2 ms-auto">
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
						onClick={openBuilder}
					>
						Custom grid
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
				</div>
			</div>

			<div
				className="mb-2"
				style={{
					display: "grid",
					gridTemplateColumns: "minmax(76px, 108px) repeat(3, minmax(92px, 150px))",
					gap: 6,
					maxWidth: 640,
				}}
			>
				<div className="d-flex flex-column align-items-center justify-content-center rounded bg-body-secondary">
					<div className="h3 mb-0">
						{gameMaxGuesses === Infinity ? "∞" : Math.max(0, guessesLeft)}
					</div>
					<div className="text-body-secondary" style={{ fontSize: "0.7rem" }}>
						guesses
					</div>
				</div>
				{grid.cols.map((c, i) => (
					<div key={i} className="rounded bg-body-secondary">
						<CriterionHeader c={c} />
					</div>
				))}

				{grid.rows.map((row, r) => (
					<Fragment key={r}>
						<div className="rounded bg-body-secondary">
							<CriterionHeader c={row} />
						</div>
						{grid.cols.map((_, cIdx) => {
							const i = r * 3 + cIdx;
							const cell = cells[i]!;
							const solved = cell.pid !== undefined;
							if (solved) {
								const card = cards[cell.pid!];
								const tier = tierOf(cell.points);
								return (
									<div
										key={cIdx}
										className="position-relative rounded overflow-hidden border border-success trivia-pop trivia-flash-green"
										style={{ aspectRatio: "1 / 1", cursor: done ? "pointer" : undefined }}
										onClick={done ? () => setRevealCell(i) : undefined}
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
											className={`badge ${tier.badge} position-absolute top-0 end-0 m-1`}
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
									className={`btn p-0 rounded trivia-cell ${
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
									) : (
										<span className="text-body-secondary h4 mb-0">+</span>
									)}
								</button>
							);
						})}
					</Fragment>
				))}
			</div>

			{done ? (
				<>
					{immaculate ? <Confetti /> : null}
					<div
						className="card trivia-rise mb-3"
						style={{ maxWidth: 640 }}
					>
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
												<span key={i} className={`badge ${tier.badge}`}>
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
						{stats.played > 0 ? Math.round(stats.totalScore / stats.played) : 0}
					</div>
					<div className="trivia-tile-label">Avg score</div>
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
					<div className="text-body-secondary small mt-2">
						{gameMaxGuesses === Infinity
							? "Unlimited guesses"
							: `${Math.max(0, guessesLeft)} guess${guessesLeft === 1 ? "" : "es"} left`}
					</div>
				</Modal.Body>
			</Modal>

			{/* Answers reveal modal (after the game ends) */}
			<Modal show={revealCell !== undefined} onHide={() => setRevealCell(undefined)}>
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
						{revealSorted.length === 1 ? "" : "s"}
					</div>
					<table className="table table-sm mb-0">
						<thead>
							<tr>
								<th>Player</th>
								<th className="text-end">Rarity</th>
							</tr>
						</thead>
						<tbody>
							{revealSorted.slice(0, 30).map((pid) => {
								const p = byPid.get(pid);
								const mine =
									revealCell !== undefined && cells[revealCell]!.pid === pid;
								return (
									<tr key={pid} className={mine ? "table-success" : undefined}>
										<td>
											{p?.name ?? "???"}{" "}
											<span className="text-body-secondary small">
												{p?.years}
											</span>
											{mine ? (
												<span className="badge text-bg-success ms-1">
													Your pick
												</span>
											) : null}
										</td>
										<td className="text-end">
											{(() => {
												const pts = reveal?.rarity[pid];
												if (pts === undefined) {
													return "—";
												}
												const tier = tierOf(pts);
												return (
													<span className={`badge ${tier.badge}`}>
														{tier.label} +{pts}
													</span>
												);
											})()}
										</td>
									</tr>
								);
							})}
						</tbody>
					</table>
					{revealSorted.length > 30 ? (
						<div className="text-body-secondary small mt-1">
							+{revealSorted.length - 30} more
						</div>
					) : null}
				</Modal.Body>
			</Modal>

			{/* Custom grid builder */}
			<Modal show={builderOpen} onHide={() => setBuilderOpen(false)}>
				<Modal.Header closeButton>
					<Modal.Title className="fs-6">Custom grid</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					<div className="row g-2">
						<div className="col-6 d-flex flex-column gap-2">
							{builderSlot(0, "Row 1")}
							{builderSlot(1, "Row 2")}
							{builderSlot(2, "Row 3")}
						</div>
						<div className="col-6 d-flex flex-column gap-2">
							{builderSlot(3, "Column 1")}
							{builderSlot(4, "Column 2")}
							{builderSlot(5, "Column 3")}
						</div>
					</div>
					{builderComplete ? (
						<div className="mt-3">
							{builderLoading ? (
								<div className="text-body-secondary small">Checking…</div>
							) : builderPreview ? (
								<div
									style={{
										display: "grid",
										gridTemplateColumns: "repeat(3, 64px)",
										gap: 4,
									}}
								>
									{builderPreview.grid.cells.map((c, i) => (
										<div
											key={i}
											className={`rounded text-center small py-2 ${
												c.pids.length === 0
													? "bg-danger-subtle text-danger fw-bold"
													: "bg-success-subtle"
											}`}
											title="Qualifying players"
										>
											{c.pids.length}
										</div>
									))}
								</div>
							) : null}
						</div>
					) : null}
				</Modal.Body>
				<Modal.Footer>
					<button
						className="btn btn-secondary"
						onClick={() => {
							setBuilderRefs(Array.from({ length: 6 }, () => undefined));
						}}
					>
						Reset
					</button>
					<button
						className="btn btn-primary"
						disabled={!builderPlayable || builderLoading}
						onClick={playCustom}
					>
						Play
					</button>
				</Modal.Footer>
			</Modal>
		</>
	);
};

export default TriviaGrids;
