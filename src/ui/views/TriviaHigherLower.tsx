import { useEffect, useRef, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { safeLocalStorage } from "../util/safeLocalStorage.ts";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import type { View } from "../../common/types.ts";

// Higher or Lower: pick a stat category, then keep calling whether the mystery
// player's number is higher or lower than the one on the board. One miss ends
// the run; best streaks are kept per category on this device. Faces are pulled
// on demand so each matchup shows the real player.

type HLPlayer = View<"triviaHigherLower">["players"][number];

type PlayerCard = {
	pid: number;
	face?: any;
	imgURL?: string;
	colors?: [string, string, string];
	jersey?: string;
};

type Category = {
	key: string;
	label: string;
	group: string;
	// How the value reads (a rate/percent/plain count) so the reveal formats it.
	fmt?: "pct" | "rate";
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
	{ key: "ppg", label: "Career PPG", group: "Career Averages", fmt: "rate" },
	{ key: "rpg", label: "Career RPG", group: "Career Averages", fmt: "rate" },
	{ key: "apg", label: "Career APG", group: "Career Averages", fmt: "rate" },
	{ key: "spg", label: "Career SPG", group: "Career Averages", fmt: "rate" },
	{ key: "bpg", label: "Career BPG", group: "Career Averages", fmt: "rate" },
	{ key: "fgPct", label: "Career FG%", group: "Career Averages", fmt: "pct" },
	{ key: "ftPct", label: "Career FT%", group: "Career Averages", fmt: "pct" },
	{ key: "tpPct", label: "Career 3P%", group: "Career Averages", fmt: "pct" },
	{ key: "bestPpg", label: "Best Season PPG", group: "Season Bests", fmt: "rate" },
	{ key: "bestRpg", label: "Best Season RPG", group: "Season Bests", fmt: "rate" },
	{ key: "bestApg", label: "Best Season APG", group: "Season Bests", fmt: "rate" },
	{ key: "highPts", label: "Career-High Points", group: "Game Highs" },
	{ key: "highTrb", label: "Career-High Rebounds", group: "Game Highs" },
	{ key: "highAst", label: "Career-High Assists", group: "Game Highs" },
	{ key: "draftPick", label: "Draft Position", group: "Draft" },
];

const highScoreKey = (category: string) => `bbgmTriviaHol-${category}`;
const getBest = (key: string) =>
	Number(safeLocalStorage.getItem(highScoreKey(key)) ?? "0");
const setBest = (key: string, v: number) =>
	safeLocalStorage.setItem(highScoreKey(key), String(v));

const fmtValue = (v: number, fmt?: Category["fmt"]) => {
	if (fmt === "pct") {
		return `${v.toFixed(1)}%`;
	}
	if (fmt === "rate") {
		return v.toFixed(1);
	}
	return Math.round(v).toLocaleString();
};

// A face + name card. `hiddenValue` masks the stat until the guess is revealed.
const cardCache = new Map<number, PlayerCard>();
const inFlight = new Map<number, Promise<PlayerCard | undefined>>();
const fetchCard = (pid: number): Promise<PlayerCard | undefined> => {
	const cached = cardCache.get(pid);
	if (cached) {
		return Promise.resolve(cached);
	}
	const existing = inFlight.get(pid);
	if (existing) {
		return existing;
	}
	const p = toWorker("main", "triviaPlayerCard", { pid }).then((card) => {
		if (card) {
			cardCache.set(pid, card as PlayerCard);
		}
		inFlight.delete(pid);
		return card as PlayerCard | undefined;
	});
	inFlight.set(pid, p);
	return p;
};

const TriviaHigherLower = ({ players }: View<"triviaHigherLower">) => {
	useTitleBar({ title: "Higher or Lower" });

	const [category, setCategory] = useState<Category | undefined>();
	const [pool, setPool] = useState<HLPlayer[]>([]);
	const [left, setLeft] = useState<HLPlayer | undefined>();
	const [right, setRight] = useState<HLPlayer | undefined>();
	const [streak, setStreak] = useState(0);
	const [gameOver, setGameOver] = useState(false);
	// "asking" -> awaiting guess; "revealing" -> counting up the answer.
	const [phase, setPhase] = useState<"asking" | "revealing">("asking");
	const [guessedHigher, setGuessedHigher] = useState<boolean | undefined>();
	const [wasCorrect, setWasCorrect] = useState<boolean | undefined>();
	const [revealNum, setRevealNum] = useState(0);
	const [cards, setCards] = useState<Record<number, PlayerCard>>({});
	const [best, setBestState] = useState(0);
	const [played, setPlayed] = useState(0);

	const valueOf = (p: HLPlayer): number => p.values[category!.key] as number;

	const rememberCard = async (pid: number) => {
		const card = await fetchCard(pid);
		if (card) {
			setCards((prev) => (prev[pid] ? prev : { ...prev, [pid]: card }));
		}
	};

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
		setBestState(getBest(cat.key));
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
		setPhase("asking");
		setGuessedHigher(undefined);
		setWasCorrect(undefined);
		void rememberCard(a.pid);
		void rememberCard(b.pid);
	};

	// Count-up animation for the reveal.
	const rafRef = useRef<number | undefined>(undefined);
	const runCountUp = (target: number) => {
		if (rafRef.current) {
			cancelAnimationFrame(rafRef.current);
		}
		const durationMs = 650;
		let startTs: number | undefined;
		const step = (ts: number) => {
			if (startTs === undefined) {
				startTs = ts;
			}
			const t = Math.min(1, (ts - startTs) / durationMs);
			// Ease-out so it decelerates into the final number.
			const eased = 1 - (1 - t) ** 3;
			setRevealNum(target * eased);
			if (t < 1) {
				rafRef.current = requestAnimationFrame(step);
			} else {
				setRevealNum(target);
			}
		};
		rafRef.current = requestAnimationFrame(step);
	};

	useEffect(() => {
		return () => {
			if (rafRef.current) {
				cancelAnimationFrame(rafRef.current);
			}
		};
	}, []);

	const finishRun = (finalStreak: number) => {
		setGameOver(true);
		setPlayed((p) => p + 1);
		if (category && finalStreak > getBest(category.key)) {
			setBest(category.key, finalStreak);
			setBestState(finalStreak);
		}
	};

	const answer = (higher: boolean) => {
		if (!category || !left || !right || phase !== "asking" || gameOver) {
			return;
		}
		const lv = valueOf(left);
		const rv = valueOf(right);
		// Ties count as correct either way.
		const correct = rv === lv || (higher ? rv > lv : rv < lv);
		setGuessedHigher(higher);
		setWasCorrect(correct);
		setPhase("revealing");
		runCountUp(rv);

		// Hold on the reveal, then advance or end.
		window.setTimeout(() => {
			if (!correct) {
				finishRun(streak);
				return;
			}
			const newStreak = streak + 1;
			setStreak(newStreak);
			if (pool.length === 0) {
				finishRun(newStreak);
				return;
			}
			const [next, rest] = draw(pool);
			setLeft(right);
			setRight(next);
			setPool(rest);
			setPhase("asking");
			setGuessedHigher(undefined);
			setWasCorrect(undefined);
			void rememberCard(next.pid);
		}, 1100);
	};

	// --- Category picker ----------------------------------------------------
	if (!category) {
		const groups = [...new Set(CATEGORIES.map((c) => c.group))];
		return (
			<>
				<p className="text-body-secondary">
					Pick a category, then call whether the mystery player's number is
					higher or lower. One miss ends the run.
				</p>
				{groups.map((group) => (
					<div key={group} className="mb-3">
						<h3 className="h6 text-body-secondary">{group}</h3>
						<div className="d-flex flex-wrap gap-2">
							{CATEGORIES.filter((c) => c.group === group).map((c) => {
								const eligible = players.filter(
									(p) => typeof p.values[c.key] === "number",
								).length;
								const b = getBest(c.key);
								return (
									<button
										key={c.key}
										className="btn btn-light-bordered"
										disabled={eligible < 5}
										onClick={() => startGame(c)}
									>
										{c.label}
										{b > 0 ? (
											<span className="badge text-bg-warning ms-2">
												best {b}
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

	const Face = ({ p }: { p: HLPlayer }) => {
		const card = cards[p.pid];
		return (
			<div
				className="mx-auto"
				style={{ height: 130, aspectRatio: "2 / 3", maxWidth: "100%" }}
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

	const revealing = phase === "revealing";

	return (
		<>
			<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
				<div>
					<span className="text-body-secondary small">{category.label}</span>
					<div className="h3 mb-0">
						🔥 {streak}
						{streak >= 5 && !gameOver ? (
							<span className="badge text-bg-danger ms-2 align-middle">
								hot
							</span>
						) : null}
					</div>
				</div>
				<div>
					<span className="text-body-secondary small">Best</span>
					<div className="h4 mb-0">{Math.max(best, streak)}</div>
				</div>
				<button
					className="btn btn-light-bordered ms-auto"
					onClick={() => setCategory(undefined)}
				>
					Change category
				</button>
			</div>

			{left && right ? (
				<div
					className="d-flex align-items-stretch gap-2 gap-md-3"
					style={{ maxWidth: 560 }}
				>
					{/* Known player */}
					<div className="card flex-fill">
						<div className="card-body text-center p-2 p-md-3">
							<Face p={left} />
							<div className="fw-bold mt-2">{left.name}</div>
							<div className="text-body-secondary small">{left.years}</div>
							<div className="h2 mt-2 mb-0">
								{fmtValue(valueOf(left), category.fmt)}
							</div>
							<div className="text-body-secondary small">{category.label}</div>
						</div>
					</div>

					{/* VS */}
					<div className="d-flex align-items-center">
						<span className="badge rounded-pill text-bg-secondary">VS</span>
					</div>

					{/* Mystery player */}
					<div
						className={`card flex-fill ${
							revealing || gameOver
								? wasCorrect
									? "border-success"
									: "border-danger"
								: ""
						}`}
					>
						<div className="card-body text-center p-2 p-md-3">
							<Face p={right} />
							<div className="fw-bold mt-2">{right.name}</div>
							<div className="text-body-secondary small">{right.years}</div>
							{revealing || gameOver ? (
								<>
									<div
										className={`h2 mt-2 mb-0 ${
											wasCorrect ? "text-success" : "text-danger"
										}`}
									>
										{fmtValue(revealNum, category.fmt)}
									</div>
									<div className="text-body-secondary small">
										{wasCorrect ? "✓ correct" : "✗ wrong"}
									</div>
								</>
							) : (
								<>
									<div className="h2 mt-2 mb-0">?</div>
									<div className="text-body-secondary small mb-2">
										{category.label}
									</div>
									<div className="d-flex flex-column gap-2">
										<button
											className="btn btn-success"
											onClick={() => answer(true)}
										>
											▲ Higher
										</button>
										<button
											className="btn btn-danger"
											onClick={() => answer(false)}
										>
											▼ Lower
										</button>
									</div>
								</>
							)}
						</div>
					</div>
				</div>
			) : null}

			{gameOver ? (
				<div className="mt-3" style={{ maxWidth: 560 }}>
					<div className="alert alert-secondary">
						<div className="fw-bold h5 mb-1">
							Run over — streak of {streak}.
							{streak >= best && streak > 0 ? " 🏆 New best!" : ""}
						</div>
						<div className="text-body-secondary small">
							{guessedHigher !== undefined && right && left
								? `You said ${guessedHigher ? "higher" : "lower"}: ${
										right.name
									} had ${fmtValue(valueOf(right), category.fmt)} vs ${
										left.name
									}'s ${fmtValue(valueOf(left), category.fmt)}.`
								: null}
						</div>
					</div>
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

			{played > 0 ? (
				<div className="text-body-secondary small mt-3">
					{category.label} · best streak {best} · {played} run
					{played === 1 ? "" : "s"} this visit
				</div>
			) : null}
		</>
	);
};

export default TriviaHigherLower;
