import { useEffect, useRef, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { safeLocalStorage } from "../util/safeLocalStorage.ts";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import { TriviaPlayerModal } from "../components/TriviaPlayerModal.tsx";
import { useLocal } from "../util/local.ts";
import {
	clearProgress,
	loadProgress,
	saveProgress,
} from "../util/triviaProgress.ts";
import { Confetti } from "./LiveGame/Confetti.tsx";
import type { View } from "../../common/types.ts";

// Higher or Lower: pick a stat category, then keep calling whether the mystery
// player's number is higher or lower than the one on the board. Three lives per
// run; best streaks are kept per category on this device. Faces are pulled on
// demand so each matchup shows the real player.

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
	{
		key: "bestPpg",
		label: "Best Season PPG",
		group: "Season Bests",
		fmt: "rate",
	},
	{
		key: "bestRpg",
		label: "Best Season RPG",
		group: "Season Bests",
		fmt: "rate",
	},
	{
		key: "bestApg",
		label: "Best Season APG",
		group: "Season Bests",
		fmt: "rate",
	},
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

const draw = (from: HLPlayer[]): [HLPlayer, HLPlayer[]] => {
	const i = Math.floor(Math.random() * from.length);
	const picked = from[i]!;
	const rest = from.slice(0, i).concat(from.slice(i + 1));
	return [picked, rest];
};

// Pick the NEXT opponent with difficulty that scales to the current streak.
// A purely random draw is what makes a higher/lower game boring: most pairs are
// blowouts you answer without thinking, and the ones that end your run are
// random rather than earned. Instead, the deeper the streak the tighter the gap
// we aim for - early rounds stay forgiving, later ones become genuine coin-flips
// you have to actually know the league to win.
const drawOpponent = (
	from: HLPlayer[],
	against: HLPlayer,
	atStreak: number,
	value: (p: HLPlayer) => number,
): [HLPlayer, HLPlayer[]] => {
	if (from.length <= 1) {
		return draw(from);
	}
	const anchor = value(against);
	// Target gap as a share of the anchor value: ~60% at streak 0, tightening
	// toward ~5% by streak 12 and holding there.
	const t = Math.min(1, atStreak / 12);
	const targetShare = 0.6 - 0.55 * t;
	const target = Math.abs(anchor) * targetShare;

	// Sample a bounded candidate set rather than sorting the whole pool every
	// round - the pool can be thousands of players and this runs per guess.
	const SAMPLE = 60;
	let bestIdx = -1;
	let bestScore = Infinity;
	for (let n = 0; n < SAMPLE; n += 1) {
		const idx = Math.floor(Math.random() * from.length);
		const cand = from[idx]!;
		const gap = Math.abs(value(cand) - anchor);
		// Prefer the candidate whose gap is closest to the target gap. Ties in
		// value are allowed (they count as correct either way), but a pile of
		// exact ties would be dull, so nudge away from a literal 0 gap.
		const score = Math.abs(gap - target) + (gap === 0 ? target * 0.5 : 0);
		if (score < bestScore) {
			bestScore = score;
			bestIdx = idx;
		}
	}
	if (bestIdx < 0) {
		return draw(from);
	}
	const picked = from[bestIdx]!;
	const rest = from.slice(0, bestIdx).concat(from.slice(bestIdx + 1));
	return [picked, rest];
};

// Points for a correct call: a coin-flip margin is worth far more than an
// obvious blowout, so score reflects how hard the read was rather than just how
// many you have answered. Streak adds a rising multiplier on top.
const pointsFor = (a: number, b: number, atStreak: number): number => {
	const scale = Math.max(1, Math.abs(a), Math.abs(b));
	const closeness = 1 - Math.min(1, Math.abs(a - b) / scale);
	const base = 10 + Math.round(90 * closeness ** 2);
	const multiplier = 1 + Math.min(2, atStreak * 0.1);
	return Math.round(base * multiplier);
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
	// await, not .then: toWorker's declared return nests one promise deeper than
	// it resolves, so a .then callback gets a value typed as a Promise and the
	// casts that used to paper over it were lying about the shape.
	const p = (async () => {
		const card = await toWorker("main", "triviaPlayerCard", { pid });
		if (card) {
			cardCache.set(pid, card);
		}
		inFlight.delete(pid);
		return card;
	})();
	inFlight.set(pid, p);
	return p;
};

// Declared at module scope, NOT inside the view. A component created during
// render is a new type every render, so React tore down and rebuilt the face -
// and PlayerPicture redraws its canvas on mount, so every guess flickered both
// portraits.
const Face = ({ card }: { card: PlayerCard | undefined }) => (
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

// A run in progress. Players are stored as pids and looked back up in the
// pool, which is the same league data either way and far smaller to write.
type SavedRun = {
	categoryKey: string;
	poolPids: number[];
	leftPid?: number;
	rightPid?: number;
	streak: number;
	lives: number;
	score: number;
	closest?: { a: string; av: number; b: string; bv: number };
	gameOver: boolean;
	guessedHigher?: boolean;
	wasCorrect?: boolean;
	revealNum: number;
};

const TriviaHigherLower = ({ players }: View<"triviaHigherLower">) => {
	useTitleBar({ title: "Higher or Lower" });

	const { lid } = useLocal(["lid"]);
	const [restored] = useState(() => loadProgress<SavedRun>("higherLower", lid));
	// Resolving pids back to players needs the pool, so this is done once here
	// rather than inside a dozen separate initializers.
	const [seed] = useState(() => {
		if (!restored) {
			return undefined;
		}
		const cat = CATEGORIES.find((c) => c.key === restored.categoryKey);
		if (!cat) {
			return undefined;
		}
		const byPid = new Map(players.map((p) => [p.pid, p]));
		const left = restored.leftPid ? byPid.get(restored.leftPid) : undefined;
		const right = restored.rightPid ? byPid.get(restored.rightPid) : undefined;
		// A run whose two players are gone is not resumable - the whole question
		// on screen was those two.
		if (!left || !right) {
			return undefined;
		}
		return {
			...restored,
			category: cat,
			pool: restored.poolPids
				.map((pid) => byPid.get(pid))
				.filter((p): p is HLPlayer => p !== undefined),
			left,
			right,
		};
	});

	const [category, setCategory] = useState<Category | undefined>(
		() => seed?.category,
	);
	const [pool, setPool] = useState<HLPlayer[]>(() => seed?.pool ?? []);
	const [left, setLeft] = useState<HLPlayer | undefined>(() => seed?.left);
	const [right, setRight] = useState<HLPlayer | undefined>(() => seed?.right);
	const [streak, setStreak] = useState(() => seed?.streak ?? 0);
	// Lives, so one unlucky coin-flip doesn't end an otherwise great run. This is
	// the single biggest fun change: runs last long enough to build a streak, and
	// losing feels like it took three real mistakes rather than one.
	const [lives, setLives] = useState(() => seed?.lives ?? 3);
	const [score, setScore] = useState(() => seed?.score ?? 0);
	const [lastPoints, setLastPoints] = useState(0);
	// The tightest call survived this run, for the end-of-run summary.
	const [closest, setClosest] = useState<
		{ a: string; av: number; b: string; bv: number } | undefined
	>(() => seed?.closest);
	const [gameOver, setGameOver] = useState(() => seed?.gameOver ?? false);
	// "asking" -> awaiting guess; "revealing" -> counting up the answer.
	const [phase, setPhase] = useState<"asking" | "revealing">("asking");
	// Both players on screen are named, so either one opens their card.
	const [profilePid, setProfilePid] = useState<number | undefined>();
	const [guessedHigher, setGuessedHigher] = useState<boolean | undefined>(
		() => seed?.guessedHigher,
	);
	const [wasCorrect, setWasCorrect] = useState<boolean | undefined>(
		() => seed?.wasCorrect,
	);
	const [revealNum, setRevealNum] = useState(() => seed?.revealNum ?? 0);
	const [cards, setCards] = useState<Record<number, PlayerCard>>({});
	const [best, setBestState] = useState(0);
	const [played, setPlayed] = useState(0);
	const [newBest, setNewBest] = useState(false);

	// Persist between questions, not during one. The reveal advances on a timer
	// that a reload would never fire, so a run saved mid-reveal would resume
	// stuck; saving only while awaiting an answer costs at most the one question
	// you were part-way through.
	useEffect(() => {
		if (!category || !left || !right) {
			return;
		}
		if (phase !== "asking" && !gameOver) {
			return;
		}
		saveProgress("higherLower", lid, {
			categoryKey: category.key,
			poolPids: pool.map((p) => p.pid),
			leftPid: left.pid,
			rightPid: right.pid,
			streak,
			lives,
			score,
			closest,
			gameOver,
			guessedHigher,
			wasCorrect,
			revealNum,
		} satisfies SavedRun);
	}, [
		lid,
		category,
		pool,
		left,
		right,
		streak,
		lives,
		score,
		closest,
		gameOver,
		phase,
		guessedHigher,
		wasCorrect,
		revealNum,
	]);

	const valueOf = (p: HLPlayer): number => p.values[category!.key] as number;

	const rememberCard = async (pid: number) => {
		const card = await fetchCard(pid);
		if (card) {
			setCards((prev) => (prev[pid] ? prev : { ...prev, [pid]: card }));
		}
	};

	// Faces for a resumed pair - they're normally fetched as each player is
	// dealt, so a restored run would come back with empty frames.
	useEffect(() => {
		if (seed) {
			void rememberCard(seed.left.pid);
			void rememberCard(seed.right.pid);
		}
		// Once, on mount.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, []);

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
		setLives(3);
		setScore(0);
		setLastPoints(0);
		setClosest(undefined);
		setGameOver(false);
		setNewBest(false);
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
			setNewBest(finalStreak > 0);
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

		// Track the tightest call of the run for the summary, whichever way it went.
		const scale = Math.max(1, Math.abs(lv), Math.abs(rv));
		if (
			closest === undefined ||
			Math.abs(rv - lv) / scale <
				Math.abs(closest.bv - closest.av) /
					Math.max(1, Math.abs(closest.av), Math.abs(closest.bv))
		) {
			setClosest({ a: left.name, av: lv, b: right.name, bv: rv });
		}

		const gained = correct ? pointsFor(lv, rv, streak) : 0;
		setLastPoints(gained);
		if (correct) {
			setScore((v) => v + gained);
		}

		// Hold on the reveal, then advance or end.
		window.setTimeout(() => {
			// A miss costs a life instead of ending the run outright; the run only
			// ends when they're all gone.
			const livesLeft = correct ? lives : lives - 1;
			if (!correct) {
				setLives(livesLeft);
				if (livesLeft <= 0) {
					finishRun(streak);
					return;
				}
			}
			// Only a correct call extends the streak - a survived miss keeps the run
			// alive but resets the multiplier, so lives are a cushion, not a freebie.
			const newStreak = correct ? streak + 1 : 0;
			setStreak(newStreak);
			if (pool.length === 0) {
				finishRun(newStreak);
				return;
			}
			// The next opponent is chosen against the card that stays on screen, at
			// the difficulty the new streak has earned.
			const [next, rest] = drawOpponent(pool, right, newStreak, valueOf);
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
										className="trivia-chip"
										style={{ minWidth: 150 }}
										disabled={eligible < 5}
										onClick={() => startGame(c)}
									>
										<div className="fw-bold">{c.label}</div>
										<div className="d-flex align-items-center gap-2 small text-body-secondary">
											{eligible.toLocaleString()} players
											{b > 0 ? (
												<span className="badge text-bg-warning">best {b}</span>
											) : null}
										</div>
									</button>
								);
							})}
						</div>
					</div>
				))}
			</>
		);
	}

	const revealing = phase === "revealing";

	return (
		<>
			<TriviaPlayerModal
				pid={profilePid}
				onHide={() => setProfilePid(undefined)}
			/>
			<div className="d-flex flex-wrap align-items-center gap-2 mb-3">
				<div className="trivia-tile">
					<div
						className={`trivia-tile-value ${streak >= 5 ? "text-warning" : ""}`}
					>
						{streak}
					</div>
					<div className="trivia-tile-label">Streak</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">{Math.max(best, streak)}</div>
					<div className="trivia-tile-label">Best</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">
						{score.toLocaleString()}
						{lastPoints > 0 && revealing ? (
							<span
								className="text-success ms-1"
								style={{ fontSize: "0.8rem" }}
							>
								+{lastPoints}
							</span>
						) : null}
					</div>
					<div className="trivia-tile-label">Score</div>
				</div>
				<div className="trivia-tile">
					<div className="trivia-tile-value">{Math.max(0, lives)}</div>
					<div className="trivia-tile-label">Lives</div>
				</div>
				<div className="trivia-tile d-none d-sm-block">
					<div className="trivia-tile-value" style={{ fontSize: "0.95rem" }}>
						{category.label}
					</div>
					<div className="trivia-tile-label">Category</div>
				</div>
				<button
					className="btn btn-sm btn-light-bordered ms-auto"
					onClick={() => {
						clearProgress("higherLower");
						setCategory(undefined);
					}}
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
					<div className="card flex-fill" key={`l-${left.pid}`}>
						<div className="card-body text-center p-2 p-md-3">
							<Face card={cards[left.pid]} />
							<button
								type="button"
								className="btn btn-link fw-bold mt-2 p-0"
								onClick={() => setProfilePid(left.pid)}
							>
								{left.name}
							</button>
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
						key={`r-${right.pid}`}
						className={`card flex-fill trivia-slide-in ${
							revealing || gameOver
								? wasCorrect
									? "border-success trivia-flash-green"
									: "border-danger trivia-shake"
								: ""
						}`}
					>
						<div className="card-body text-center p-2 p-md-3">
							<Face card={cards[right.pid]} />
							{/* His page carries the very number you're being asked to
							    guess, so it stays shut until the reveal. */}
							{revealing || gameOver ? (
								<button
									type="button"
									className="btn btn-link fw-bold mt-2 p-0"
									onClick={() => setProfilePid(right.pid)}
								>
									{right.name}
								</button>
							) : (
								<div className="fw-bold mt-2">{right.name}</div>
							)}
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
									<div
										className={`small fw-bold ${
											wasCorrect ? "text-success" : "text-danger"
										}`}
									>
										You said {guessedHigher ? "higher" : "lower"} —{" "}
										{wasCorrect ? "correct" : "wrong"}
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
			) : null}

			{gameOver ? (
				<>
					{newBest ? <Confetti /> : null}
					<div className="card trivia-rise mt-3" style={{ maxWidth: 560 }}>
						<div className="card-body d-flex flex-wrap align-items-center gap-3">
							<div className="trivia-tile">
								<div className="trivia-tile-value">{streak}</div>
								<div className="trivia-tile-label">Final streak</div>
							</div>
							<div className="trivia-tile">
								<div className="trivia-tile-value">
									{score.toLocaleString()}
								</div>
								<div className="trivia-tile-label">Score</div>
							</div>
							<div className="flex-grow-1">
								<div className="h5 mb-1">
									{newBest
										? "New best"
										: streak >= 10
											? "Great run!"
											: streak >= 5
												? "Nice run."
												: "Run over."}
								</div>
								<div className="text-body-secondary small">
									{guessedHigher !== undefined && right && left
										? `${right.name}: ${fmtValue(valueOf(right), category.fmt)} vs ${
												left.name
											}: ${fmtValue(valueOf(left), category.fmt)}.`
										: null}
								</div>
								{closest ? (
									<div className="text-body-secondary small mt-1">
										Closest call: {closest.a}{" "}
										{fmtValue(closest.av, category.fmt)} vs {closest.b}{" "}
										{fmtValue(closest.bv, category.fmt)}
									</div>
								) : null}
							</div>
							<div className="d-flex flex-column gap-2">
								<button
									className="btn btn-primary"
									onClick={() => startGame(category)}
								>
									Play again
								</button>
								<button
									className="btn btn-light-bordered"
									onClick={() => setCategory(undefined)}
								>
									Categories
								</button>
							</div>
						</div>
					</div>
				</>
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
