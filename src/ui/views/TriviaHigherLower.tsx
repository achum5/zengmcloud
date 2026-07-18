import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { safeLocalStorage } from "../util/safeLocalStorage.ts";
import type { View } from "../../common/types.ts";

// Higher or Lower: pick a stat category, then keep calling whether the next
// player's number is higher or lower than the current one. One miss ends the
// run; high scores are kept per category on this device.

type HLPlayer = View<"triviaHigherLower">["players"][number];

type Category = {
	key: string;
	label: string;
	group: string;
	// Draft position: a LOWER number is the better player, but the question is
	// still just "is their number higher or lower", so no special logic.
};

const CATEGORIES: Category[] = [
	{ key: "careerPts", label: "Career Points", group: "Career Totals" },
	{ key: "careerTrb", label: "Career Rebounds", group: "Career Totals" },
	{ key: "careerAst", label: "Career Assists", group: "Career Totals" },
	{ key: "careerStl", label: "Career Steals", group: "Career Totals" },
	{ key: "careerBlk", label: "Career Blocks", group: "Career Totals" },
	{ key: "careerTp", label: "Career Threes", group: "Career Totals" },
	{ key: "careerGp", label: "Career Games", group: "Career Totals" },
	{ key: "careerMin", label: "Career Minutes", group: "Career Totals" },
	{ key: "seasons", label: "Seasons Played", group: "Career Totals" },
	{ key: "ppg", label: "Career PPG", group: "Career Averages" },
	{ key: "rpg", label: "Career RPG", group: "Career Averages" },
	{ key: "apg", label: "Career APG", group: "Career Averages" },
	{ key: "spg", label: "Career SPG", group: "Career Averages" },
	{ key: "bpg", label: "Career BPG", group: "Career Averages" },
	{ key: "fgPct", label: "Career FG%", group: "Career Averages" },
	{ key: "ftPct", label: "Career FT%", group: "Career Averages" },
	{ key: "tpPct", label: "Career 3P%", group: "Career Averages" },
	{ key: "bestPpg", label: "Best Season PPG", group: "Season Bests" },
	{ key: "bestRpg", label: "Best Season RPG", group: "Season Bests" },
	{ key: "bestApg", label: "Best Season APG", group: "Season Bests" },
	{ key: "highPts", label: "Career-High Points", group: "Game Highs" },
	{ key: "highTrb", label: "Career-High Rebounds", group: "Game Highs" },
	{ key: "highAst", label: "Career-High Assists", group: "Game Highs" },
	{ key: "draftPick", label: "Draft Position", group: "Draft" },
];

const highScoreKey = (category: string) => `bbgmTriviaHol-${category}`;

const TriviaHigherLower = ({ players }: View<"triviaHigherLower">) => {
	useTitleBar({ title: "Higher or Lower" });

	const [category, setCategory] = useState<Category | undefined>();
	const [pool, setPool] = useState<HLPlayer[]>([]);
	const [left, setLeft] = useState<HLPlayer | undefined>();
	const [right, setRight] = useState<HLPlayer | undefined>();
	const [streak, setStreak] = useState(0);
	const [gameOver, setGameOver] = useState(false);
	const [lastReveal, setLastReveal] = useState<string | undefined>();

	const valueOf = (p: HLPlayer): number =>
		p.values[category!.key] as number;

	const draw = (from: HLPlayer[]): [HLPlayer, HLPlayer[]] => {
		const i = Math.floor(Math.random() * from.length);
		const picked = from[i]!;
		const rest = from.slice(0, i).concat(from.slice(i + 1));
		return [picked, rest];
	};

	const startGame = (cat: Category) => {
		const eligible = players.filter(
			(p) => typeof p.values[cat.key] === "number",
		);
		if (eligible.length < 5) {
			return;
		}
		setCategory(cat);
		let rest = eligible;
		let a: HLPlayer;
		let b: HLPlayer;
		[a, rest] = draw(rest);
		[b, rest] = draw(rest);
		setLeft(a);
		setRight(b);
		setPool(rest);
		setStreak(0);
		setGameOver(false);
		setLastReveal(undefined);
	};

	const highScore = category
		? Number(safeLocalStorage.getItem(highScoreKey(category.key)) ?? "0")
		: 0;

	const answer = (higher: boolean) => {
		if (!category || !left || !right || gameOver) {
			return;
		}
		const lv = valueOf(left);
		const rv = valueOf(right);
		// Ties count as correct either way.
		const correct = rv === lv || (higher ? rv > lv : rv < lv);
		setLastReveal(`${right.name}: ${rv.toLocaleString()}`);
		if (!correct) {
			setGameOver(true);
			const best = Number(
				safeLocalStorage.getItem(highScoreKey(category.key)) ?? "0",
			);
			if (streak > best) {
				safeLocalStorage.setItem(highScoreKey(category.key), String(streak));
			}
			return;
		}
		const newStreak = streak + 1;
		setStreak(newStreak);
		if (pool.length === 0) {
			setGameOver(true);
			const best = Number(
				safeLocalStorage.getItem(highScoreKey(category.key)) ?? "0",
			);
			if (newStreak > best) {
				safeLocalStorage.setItem(
					highScoreKey(category.key),
					String(newStreak),
				);
			}
			return;
		}
		const [next, rest] = draw(pool);
		setLeft(right);
		setRight(next);
		setPool(rest);
	};

	if (!category) {
		const groups = [...new Set(CATEGORIES.map((c) => c.group))];
		return (
			<>
				<p>Pick a category, then call each matchup. One miss ends the run.</p>
				{groups.map((group) => (
					<div key={group} className="mb-3">
						<h3 className="h5">{group}</h3>
						<div className="d-flex flex-wrap gap-2">
							{CATEGORIES.filter((c) => c.group === group).map((c) => {
								const eligible = players.filter(
									(p) => typeof p.values[c.key] === "number",
								).length;
								const best = Number(
									safeLocalStorage.getItem(highScoreKey(c.key)) ?? "0",
								);
								return (
									<button
										key={c.key}
										className="btn btn-light-bordered"
										disabled={eligible < 5}
										onClick={() => startGame(c)}
									>
										{c.label}
										{best > 0 ? (
											<span className="badge text-bg-secondary ms-2">
												best {best}
											</span>
										) : null}
									</button>
								);
							})}
						</div>
					</div>
				))}
			</>
		);
	}

	return (
		<>
			<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
				<div>
					<span className="text-body-secondary small">{category.label}</span>
					<div className="h4 mb-0">Streak: {streak}</div>
				</div>
				<div>
					<span className="text-body-secondary small">Best</span>
					<div className="h4 mb-0">{Math.max(highScore, streak)}</div>
				</div>
				<button
					className="btn btn-light-bordered ms-auto"
					onClick={() => setCategory(undefined)}
				>
					Change category
				</button>
			</div>

			{left && right ? (
				<div className="row" style={{ maxWidth: 640 }}>
					<div className="col-6">
						<div className="card h-100">
							<div className="card-body text-center">
								<div className="fw-bold">{left.name}</div>
								<div className="text-body-secondary small">{left.years}</div>
								<div className="h3 mt-2">
									{valueOf(left).toLocaleString()}
								</div>
							</div>
						</div>
					</div>
					<div className="col-6">
						<div className="card h-100">
							<div className="card-body text-center">
								<div className="fw-bold">{right.name}</div>
								<div className="text-body-secondary small">{right.years}</div>
								{gameOver ? (
									<div className="h3 mt-2">
										{valueOf(right).toLocaleString()}
									</div>
								) : (
									<>
										<div className="h3 mt-2">?</div>
										<div className="d-flex gap-2 justify-content-center">
											<button
												className="btn btn-success"
												onClick={() => answer(true)}
											>
												Higher
											</button>
											<button
												className="btn btn-danger"
												onClick={() => answer(false)}
											>
												Lower
											</button>
										</div>
									</>
								)}
							</div>
						</div>
					</div>
				</div>
			) : null}

			{gameOver ? (
				<div className="mt-3">
					{lastReveal ? <p className="mb-2">{lastReveal}</p> : null}
					<p className="fw-bold">
						Run over — streak of {streak}.
						{streak > highScore ? " New best!" : ""}
					</p>
					<button
						className="btn btn-primary me-2"
						onClick={() => startGame(category)}
					>
						Play again
					</button>
					<button
						className="btn btn-light-bordered"
						onClick={() => setCategory(undefined)}
					>
						Change category
					</button>
				</div>
			) : null}
		</>
	);
};

export default TriviaHigherLower;
