import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import TriviaPlayerSelect, {
	type TriviaSearchPlayer,
} from "../components/TriviaPlayerSelect.tsx";
import type { View } from "../../common/types.ts";

// The Grids game (Immaculate Grid style): 9 cells, one guess each, find any
// player in league history matching both the row and column criteria. Rarer
// correct answers score more.

type GridData = NonNullable<View<"triviaGrids">["data"]>;
type Criterion = GridData["grid"]["rows"][number];

type CellState = {
	guessed: boolean;
	correct: boolean;
	autoFilled: boolean;
	name?: string;
	points: number;
};

const emptyCells = (): CellState[] =>
	Array.from({ length: 9 }, () => ({
		guessed: false,
		correct: false,
		autoFilled: false,
		points: 0,
	}));

const TriviaGrids = (props: View<"triviaGrids">) => {
	useTitleBar({ title: "Grids" });

	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	const [data, setData] = useState(props.data);
	const [cells, setCells] = useState<CellState[]>(emptyCells);
	const [usedPids, setUsedPids] = useState<Set<number>>(new Set());
	const [activeCell, setActiveCell] = useState<number | undefined>();
	const [gaveUp, setGaveUp] = useState(false);
	const [loadingNew, setLoadingNew] = useState(false);

	const newGrid = async () => {
		setLoadingNew(true);
		try {
			const fresh = await toWorker("main", "triviaNewGrid", undefined);
			if (fresh) {
				setData(fresh);
				setCells(emptyCells());
				setUsedPids(new Set());
				setActiveCell(undefined);
				setGaveUp(false);
			}
		} finally {
			setLoadingNew(false);
		}
	};

	if (!data) {
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

	const { grid, searchList } = data;

	const CriterionHeader = ({ c }: { c: Criterion }) => {
		if (c.kind === "team") {
			const t = teamInfoCache[c.tid];
			return (
				<div className="d-flex flex-column align-items-center gap-1 p-1">
					<TeamLogoInline
						imgURL={t?.imgURL}
						imgURLSmall={t?.imgURLSmall}
						size={40}
						includePlaceholderIfNoLogo
					/>
					<span className="small text-center">{c.label}</span>
				</div>
			);
		}
		return <div className="small fw-bold text-center p-1">{c.label}</div>;
	};

	const done = gaveUp || cells.every((c) => c.guessed);
	const score = cells.reduce((sum, c) => sum + c.points, 0);
	const correctCount = cells.filter((c) => c.correct).length;
	const guessesLeft = cells.filter((c) => !c.guessed).length;

	const handleGuess = (cellIndex: number, guess: TriviaSearchPlayer) => {
		if (usedPids.has(guess.pid)) {
			return;
		}
		const cell = grid.cells[cellIndex]!;
		const correct = cell.pids.includes(guess.pid);
		setCells((prev) =>
			prev.map((c, i) =>
				i === cellIndex
					? {
							guessed: true,
							correct,
							autoFilled: false,
							name: guess.name,
							points: correct ? (cell.rarity[guess.pid] ?? 10) : 0,
						}
					: c,
			),
		);
		setUsedPids((prev) => new Set(prev).add(guess.pid));
		setActiveCell(undefined);
	};

	// Reveal every unguessed cell with its most common qualifying player.
	const giveUp = () => {
		const used = new Set(usedPids);
		setCells((prev) =>
			prev.map((c, i) => {
				if (c.guessed) {
					return c;
				}
				const cell = grid.cells[i]!;
				const available = cell.pids
					.filter((pid) => !used.has(pid))
					.sort((a, b) => (cell.rarity[a] ?? 100) - (cell.rarity[b] ?? 100));
				const pid = available[0] ?? cell.pids[0]!;
				used.add(pid);
				const name =
					searchList.find((p) => p.pid === pid)?.name ?? "???";
				return {
					guessed: true,
					correct: false,
					autoFilled: true,
					name,
					points: 0,
				};
			}),
		);
		setUsedPids(used);
		setActiveCell(undefined);
		setGaveUp(true);
	};

	return (
		<>
			<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
				<div>
					<span className="text-body-secondary small">Score</span>
					<div className="h4 mb-0">{score}</div>
				</div>
				<div>
					<span className="text-body-secondary small">Correct</span>
					<div className="h4 mb-0">{correctCount}/9</div>
				</div>
				{!done ? (
					<button className="btn btn-light-bordered" onClick={giveUp}>
						Give up
					</button>
				) : null}
				<button
					className="btn btn-primary"
					disabled={loadingNew}
					onClick={newGrid}
				>
					{loadingNew ? "Generating…" : "New grid"}
				</button>
			</div>

			{activeCell !== undefined && !done ? (
				<div className="mb-3" style={{ maxWidth: 480 }}>
					<div className="fw-bold mb-1">
						{grid.rows[Math.floor(activeCell / 3)]!.label} ×{" "}
						{grid.cols[activeCell % 3]!.label}
					</div>
					<TriviaPlayerSelect
						players={searchList.filter((p) => !usedPids.has(p.pid))}
						onSelect={(p) => handleGuess(activeCell, p)}
					/>
				</div>
			) : !done ? (
				<p className="text-body-secondary">
					Tap a cell, then find a player who matches both its row and column.
					One guess per cell — each player can only be used once.
				</p>
			) : (
				<p className="fw-bold">
					Final score: {score} ({correctCount}/9 correct). Rarer answers score
					more.
				</p>
			)}

			<div className="table-responsive">
				<table
					className="table table-bordered align-middle mb-3"
					style={{ maxWidth: 560 }}
				>
					<thead>
						<tr>
							<th style={{ width: 130 }} />
							{grid.cols.map((c, i) => (
								<th key={i} className="text-center" style={{ width: 130 }}>
									<CriterionHeader c={c} />
								</th>
							))}
						</tr>
					</thead>
					<tbody>
						{grid.rows.map((row, r) => (
							<tr key={r}>
								<th className="text-center">
									<CriterionHeader c={row} />
								</th>
								{grid.cols.map((_, c) => {
									const i = r * 3 + c;
									const cell = cells[i]!;
									return (
										<td key={c} className="p-1" style={{ height: 90 }}>
											{cell.guessed ? (
												<div
													className={`h-100 d-flex flex-column align-items-center justify-content-center rounded p-1 text-center ${
														cell.correct
															? "bg-success-subtle"
															: cell.autoFilled
																? "bg-body-secondary"
																: "bg-danger-subtle"
													}`}
												>
													<span className="small fw-medium">{cell.name}</span>
													{cell.correct ? (
														<span className="badge text-bg-success mt-1">
															+{cell.points}
														</span>
													) : cell.autoFilled ? (
														<span className="text-body-secondary small">
															revealed
														</span>
													) : (
														<span className="badge text-bg-danger mt-1">✗</span>
													)}
												</div>
											) : (
												<button
													className={`btn w-100 h-100 ${activeCell === i ? "btn-primary" : "btn-light-bordered"}`}
													disabled={done}
													onClick={() =>
														setActiveCell(activeCell === i ? undefined : i)
													}
												>
													{activeCell === i ? "…" : ""}
												</button>
											)}
										</td>
									);
								})}
							</tr>
						))}
					</tbody>
				</table>
			</div>

			{!done ? (
				<p className="text-body-secondary small">
					{guessesLeft} cell{guessesLeft === 1 ? "" : "s"} left
				</p>
			) : null}
		</>
	);
};

export default TriviaGrids;
