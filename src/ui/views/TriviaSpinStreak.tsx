import clsx from "clsx";
import {
	useCallback,
	useEffect,
	useLayoutEffect,
	useRef,
	useState,
	type ReactNode,
} from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { safeLocalStorage } from "../util/safeLocalStorage.ts";
import { CountryFlag } from "../components/CountryFlag.tsx";
import { TriviaPlayerModal } from "../components/TriviaPlayerModal.tsx";
import { Confetti } from "./LiveGame/Confetti.tsx";
import type { View } from "../../common/types.ts";
import type {
	SpinDifficulty,
	SpinOption,
	SpinQuestion,
} from "../../worker/core/trivia/spinStreak.ts";

// Spin Streak: three reels land on a season, a player and a category, then you
// pick the right answer from three tiles before the clock runs out. One miss
// ends the run. Questions come from the worker one spin at a time; the next one
// is fetched while you're answering the current one, so the reels never wait.

const DIFFICULTIES: {
	key: SpinDifficulty;
	label: string;
	seconds: number;
	desc: string;
}[] = [
	{ key: "easy", label: "Easy", seconds: 15, desc: "Stars · 15s" },
	{ key: "normal", label: "Normal", seconds: 10, desc: "Regulars · 10s" },
	{ key: "hard", label: "Hard", seconds: 6, desc: "Deep cuts · 6s" },
];

const REEL_MS = [950, 1300, 1650];
const SPIN_MS = REEL_MS[2]! + 150;
const ITEM_H = 58;
const VIEW_H = 150;

const bestKey = (d: SpinDifficulty) => `bbgmSpinStreak-${d}`;
const getBest = (d: SpinDifficulty) =>
	Number(safeLocalStorage.getItem(bestKey(d)) ?? "0") || 0;
const setBest = (d: SpinDifficulty, v: number) =>
	safeLocalStorage.setItem(bestKey(d), String(v));

// --- Reels -------------------------------------------------------------------

// One reel: a strip of items that slides from its first item (what was showing
// before) to its last (the new result). Remounting per spin restarts it.
const Reel = <T,>({
	items,
	render,
	duration,
	spinKey,
}: {
	items: T[];
	render: (item: T) => ReactNode;
	duration: number;
	spinKey: string;
}) => {
	const [go, setGo] = useState(false);
	useLayoutEffect(() => {
		setGo(false);
		let inner = 0;
		const outer = requestAnimationFrame(() => {
			inner = requestAnimationFrame(() => setGo(true));
		});
		return () => {
			cancelAnimationFrame(outer);
			cancelAnimationFrame(inner);
		};
	}, [spinKey]);

	const idx = go ? items.length - 1 : 0;
	const y = (VIEW_H - ITEM_H) / 2 - idx * ITEM_H;

	return (
		<div className="spin-reel" style={{ height: VIEW_H }}>
			<div
				className={clsx(
					"spin-reel-strip",
					go && items.length > 1 && "is-spinning",
				)}
				style={{
					transform: `translateY(${y}px)`,
					transition: go
						? `transform ${duration}ms cubic-bezier(0.2, 0.75, 0.25, 1.06)`
						: "none",
					animationDuration: `${duration}ms`,
				}}
			>
				{items.map((item, i) => (
					<div className="spin-reel-item" key={i} style={{ height: ITEM_H }}>
						{render(item)}
					</div>
				))}
			</div>
			<div className="spin-reel-window" style={{ height: ITEM_H }} />
		</div>
	);
};

const SeasonItem = ({ season }: { season: number | undefined }) => (
	<span className="spin-reel-big">{season ?? "—"}</span>
);

const PlayerItem = ({
	player,
}: {
	player: { firstName: string; lastName: string } | undefined;
}) =>
	player ? (
		<span className="d-flex flex-column align-items-center lh-1">
			<span className="spin-reel-small">{player.firstName}</span>
			<span className="spin-reel-name">{player.lastName}</span>
		</span>
	) : (
		<span className="spin-reel-big">—</span>
	);

const CategoryItem = ({ label }: { label: string | undefined }) => (
	<span className="spin-reel-cat">{label ?? "—"}</span>
);

// Decoys repeated to the strip length, previous result first, new result last.
const strip = <T,>(prev: T | undefined, decoys: T[], final: T, len: number) => {
	const out: (T | undefined)[] = [prev];
	for (let i = 0; out.length < len - 1; i++) {
		out.push(decoys.length > 0 ? decoys[i % decoys.length] : final);
	}
	out.push(final);
	return out;
};

// --- Answer tiles ------------------------------------------------------------

const monogram = (name: string) => {
	const words = name
		.replace(/[^\d &'A-Za-z-]/g, " ")
		.split(/\s+/)
		.filter(
			(w) => w !== "" && !/^(of|the|at|and|&|university|college)$/i.test(w),
		);
	const letters = words.map((w) => w[0]!.toUpperCase()).join("");
	return (letters || name.slice(0, 2).toUpperCase()).slice(0, 3);
};

const hue = (s: string) => {
	let h = 0;
	for (let i = 0; i < s.length; i++) {
		h = (h * 31 + s.charCodeAt(i)) % 360;
	}
	return h;
};

const OptionFace = ({ option }: { option: SpinOption }) => {
	switch (option.kind) {
		case "team": {
			const t = option.team!;
			return (
				<>
					<div
						className="spin-team-block"
						style={{ backgroundColor: t.colors[0], color: t.colors[1] }}
					>
						<span
							className={clsx(
								"spin-team-abbrev",
								t.abbrev.length > 3 && "is-long",
							)}
						>
							{t.abbrev}
						</span>
					</div>
					<div className="spin-option-sub">{option.sub}</div>
				</>
			);
		}
		case "flag":
			return (
				<>
					<CountryFlag
						className="spin-flag"
						country={option.country ?? option.label}
					/>
					<div className="spin-option-label">{option.label}</div>
				</>
			);
		case "college":
			return (
				<>
					<div
						className="spin-monogram"
						style={{
							backgroundColor: `hsl(${hue(option.label)} 55% 38%)`,
						}}
					>
						{monogram(option.label)}
					</div>
					<div className="spin-option-label">{option.label}</div>
				</>
			);
		case "player":
			return (
				<div className="d-flex flex-column align-items-center lh-1">
					{option.sub ? (
						<span className="spin-reel-small">{option.sub}</span>
					) : null}
					<span className="spin-option-name">{option.label}</span>
				</div>
			);
		case "number":
			return (
				<>
					<div
						className={clsx(
							"spin-option-number",
							option.label.length > 5 && "is-long",
						)}
					>
						{option.label}
					</div>
					{option.sub ? (
						<div className="spin-option-sub">{option.sub}</div>
					) : null}
				</>
			);
		default:
			return <div className="spin-option-text">{option.label}</div>;
	}
};

const answerText = (o: SpinOption | undefined) => {
	if (!o) {
		return "";
	}
	if (o.kind === "player") {
		return o.sub ? `${o.sub} ${o.label}` : o.label;
	}
	if (o.kind === "team") {
		return o.sub ?? o.label;
	}
	if (o.sub) {
		return o.kind === "flag" ? `${o.label}, ${o.sub}` : `${o.label} (${o.sub})`;
	}
	return o.label;
};

// --- Page --------------------------------------------------------------------

type Phase = "idle" | "loading" | "spinning" | "asking" | "revealed" | "over";

const TriviaSpinStreak = ({ numPlayers }: View<"triviaSpinStreak">) => {
	useTitleBar({ title: "Spin Streak" });

	const [difficulty, setDifficulty] = useState<SpinDifficulty>(() => {
		const saved = safeLocalStorage.getItem("bbgmSpinStreakDifficulty");
		return saved === "easy" || saved === "hard" ? saved : "normal";
	});
	const [phase, setPhase] = useState<Phase>("idle");
	const [question, setQuestion] = useState<SpinQuestion | undefined>();
	const [prev, setPrev] = useState<SpinQuestion | undefined>();
	const [streak, setStreak] = useState(0);
	const [best, setBestState] = useState(() => getBest(difficulty));
	const [picked, setPicked] = useState<number | undefined>();
	const [newBest, setNewBest] = useState(false);
	const [error, setError] = useState(false);
	const [copied, setCopied] = useState(false);
	const [confetti, setConfetti] = useState(false);
	const [profilePid, setProfilePid] = useState<number | undefined>();

	const seconds = DIFFICULTIES.find((d) => d.key === difficulty)!.seconds;

	// Timers and the prefetched next question live in refs: they're touched from
	// timeouts that would otherwise read stale state.
	const timers = useRef<number[]>([]);
	const clockRef = useRef<number | undefined>(undefined);
	const recentPids = useRef<number[]>([]);
	const nextRef = useRef<Promise<SpinQuestion | undefined> | undefined>(
		undefined,
	);
	const runId = useRef(0);

	const later = (fn: () => void, ms: number) => {
		timers.current.push(window.setTimeout(fn, ms));
	};
	const clearTimers = () => {
		for (const t of timers.current) {
			window.clearTimeout(t);
		}
		timers.current = [];
		if (clockRef.current !== undefined) {
			window.clearTimeout(clockRef.current);
			clockRef.current = undefined;
		}
	};
	useEffect(() => clearTimers, []);

	const fetchQuestion = async (
		atStreak: number,
		lastCategory: string | undefined,
	) => {
		try {
			return await toWorker("main", "triviaSpinQuestion", {
				difficulty,
				streak: atStreak,
				recentPids: recentPids.current,
				lastCategory,
			});
		} catch (error_) {
			console.error(error_);
			return undefined;
		}
	};

	// Land the reels on a question, then open the answers.
	const spinTo = (
		q: SpinQuestion | undefined,
		from: SpinQuestion | undefined,
	) => {
		if (!q) {
			setError(true);
			setPhase("idle");
			return;
		}
		recentPids.current = [q.pid, ...recentPids.current].slice(0, 40);
		setPrev(from);
		setQuestion(q);
		setPicked(undefined);
		setPhase("spinning");
		const id = runId.current;
		later(() => {
			if (runId.current !== id) {
				return;
			}
			setPhase("asking");
		}, SPIN_MS);
	};

	const start = async () => {
		clearTimers();
		runId.current += 1;
		safeLocalStorage.setItem("bbgmSpinStreakDifficulty", difficulty);
		setStreak(0);
		setNewBest(false);
		setConfetti(false);
		setError(false);
		setCopied(false);
		setBestState(getBest(difficulty));
		recentPids.current = [];
		nextRef.current = undefined;
		setPhase("loading");
		const id = runId.current;
		const q = await fetchQuestion(0, undefined);
		if (runId.current !== id) {
			return;
		}
		spinTo(q, question);
	};

	const answer = useCallback(
		(i: number) => {
			if (phase !== "asking" || !question) {
				return;
			}
			clearTimers();
			setPicked(i);
			setPhase("revealed");
			const id = runId.current;
			if (i === question.answer) {
				const nextStreak = streak + 1;
				setStreak(nextStreak);
				if (nextStreak > best) {
					setBestState(nextStreak);
				}
				const next =
					nextRef.current ?? fetchQuestion(nextStreak, question.category);
				nextRef.current = undefined;
				later(async () => {
					const q = await next;
					if (runId.current !== id) {
						return;
					}
					spinTo(q, question);
				}, 850);
			} else {
				nextRef.current = undefined;
				later(() => {
					if (runId.current !== id) {
						return;
					}
					if (streak > getBest(difficulty)) {
						setBest(difficulty, streak);
						setNewBest(true);
						// A few seconds of confetti, cleared on its own so it never sits
						// over the Spin again button.
						setConfetti(true);
						later(() => setConfetti(false), 3500);
					}
					setPhase("over");
				}, 1300);
			}
		},
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[phase, question, streak, best, difficulty],
	);

	// The shot clock, and the next question fetched while this one is up.
	useEffect(() => {
		if (phase !== "asking" || !question) {
			return;
		}
		nextRef.current = fetchQuestion(streak + 1, question.category);
		clockRef.current = window.setTimeout(() => answer(-1), seconds * 1000);
		return () => {
			if (clockRef.current !== undefined) {
				window.clearTimeout(clockRef.current);
				clockRef.current = undefined;
			}
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [phase, question]);

	// 1/2/3 to answer, Enter to spin. The listener reads the latest handler
	// through a ref so it's attached once.
	const keyHandler = useRef<(e: KeyboardEvent) => void>(() => {});
	keyHandler.current = (e: KeyboardEvent) => {
		if (
			e.target instanceof HTMLElement &&
			(e.target.isContentEditable ||
				["INPUT", "TEXTAREA", "SELECT"].includes(e.target.tagName))
		) {
			return;
		}
		if (phase === "asking" && ["1", "2", "3"].includes(e.key)) {
			answer(Number(e.key) - 1);
		} else if ((phase === "idle" || phase === "over") && e.key === "Enter") {
			void start();
		}
	};
	useEffect(() => {
		const onKey = (e: KeyboardEvent) => keyHandler.current(e);
		window.addEventListener("keydown", onKey);
		return () => window.removeEventListener("keydown", onKey);
	}, []);

	const shareText = () => {
		if (!question) {
			return "";
		}
		const lines = [
			`🏀 ${streak} CORRECT`,
			`🎯 LOST ON ${question.season}`,
			`🎯 ${`${question.firstName} ${question.lastName}`.toUpperCase()}`,
			`🎯 ${question.categoryLabel.toUpperCase()}`,
		];
		return lines.join("\n");
	};

	const copy = async () => {
		try {
			await navigator.clipboard.writeText(shareText());
			setCopied(true);
		} catch {
			setCopied(false);
		}
	};

	const showOptions =
		phase === "asking" || phase === "revealed" || phase === "over";
	const revealed = phase === "revealed" || phase === "over";
	const q = question;
	const spinKey = q?.id ?? "none";

	// --- Start screen ---------------------------------------------------------
	const picker = (
		<div className="d-flex flex-wrap gap-2 mb-3">
			{DIFFICULTIES.map((d) => {
				const b = getBest(d.key);
				return (
					<button
						key={d.key}
						type="button"
						className={clsx(
							"trivia-chip",
							difficulty === d.key && "spin-chip-active",
						)}
						style={{ minWidth: 140 }}
						onClick={() => {
							setDifficulty(d.key);
							setBestState(getBest(d.key));
						}}
					>
						<div className="fw-bold">{d.label}</div>
						<div className="d-flex align-items-center gap-2 small text-body-secondary">
							{d.desc}
							{b > 0 ? (
								<span className="badge text-bg-warning">best {b}</span>
							) : null}
						</div>
					</button>
				);
			})}
		</div>
	);

	if (numPlayers < 10) {
		return (
			<p className="text-body-secondary">
				Not enough league history yet. Play a season and come back.
			</p>
		);
	}

	return (
		<>
			<TriviaPlayerModal
				pid={profilePid}
				onHide={() => setProfilePid(undefined)}
			/>

			{phase === "idle" || phase === "loading" ? picker : null}

			<div className="spin-machine">
				<div className="spin-header">
					<span className="spin-title">Spin Streak</span>
					<span
						key={streak}
						className={clsx("spin-streak", streak > 0 && "trivia-pop")}
					>
						{streak}
					</span>
					<span className="spin-best">Best {Math.max(best, streak)}</span>
				</div>

				<div className="spin-reels">
					<Reel
						spinKey={spinKey}
						duration={REEL_MS[0]!}
						items={
							q
								? strip(prev?.season, q.decoys.seasons, q.season, 10)
								: [undefined]
						}
						render={(s) => <SeasonItem season={s} />}
					/>
					<Reel
						spinKey={spinKey}
						duration={REEL_MS[1]!}
						items={
							q
								? strip(
										prev
											? { firstName: prev.firstName, lastName: prev.lastName }
											: undefined,
										q.decoys.players,
										{ firstName: q.firstName, lastName: q.lastName },
										14,
									)
								: [undefined]
						}
						render={(p) => <PlayerItem player={p} />}
					/>
					<Reel
						spinKey={spinKey}
						duration={REEL_MS[2]!}
						items={
							q
								? strip(
										prev?.categoryLabel,
										q.decoys.categories,
										q.categoryLabel,
										18,
									)
								: [undefined]
						}
						render={(c) => <CategoryItem label={c} />}
					/>
				</div>

				<div className="spin-timer">
					<div
						key={spinKey}
						className={clsx(
							"spin-timer-fill",
							(phase === "asking" || revealed) && "is-running",
							revealed && "is-paused",
						)}
						style={{ animationDuration: `${seconds}s` }}
					/>
				</div>

				<div className="spin-options">
					{[0, 1, 2].map((i) => {
						const option = q?.options[i];
						const isAnswer = q !== undefined && i === q.answer;
						const isPicked = picked === i;
						return (
							<button
								key={`${spinKey}-${i}`}
								type="button"
								className={clsx(
									"spin-option",
									showOptions && option && "is-shown",
									revealed && isAnswer && "is-right",
									revealed && isPicked && !isAnswer && "is-wrong trivia-shake",
									revealed && !isAnswer && !isPicked && "is-dimmed",
								)}
								style={{ animationDelay: `${i * 70}ms` }}
								disabled={phase !== "asking"}
								onClick={() => answer(i)}
							>
								{showOptions && option ? (
									<>
										<OptionFace option={option} />
										{revealed && isAnswer ? (
											<span className="spin-badge is-right">✓</span>
										) : null}
										{revealed && isPicked && !isAnswer ? (
											<span className="spin-badge is-wrong">✕</span>
										) : null}
									</>
								) : null}
							</button>
						);
					})}
				</div>

				{phase === "revealed" && picked === -1 ? (
					<div className="text-danger fw-bold text-center mt-2">Time!</div>
				) : null}

				{phase === "idle" || phase === "loading" ? (
					<div className="spin-overlay">
						<button
							type="button"
							className="btn btn-success btn-lg px-5 fw-bold"
							disabled={phase === "loading"}
							onClick={() => void start()}
						>
							{phase === "loading" ? "Loading…" : "Spin"}
						</button>
						{error ? (
							<div className="small text-danger mt-2">
								Couldn't build a question. Try again.
							</div>
						) : null}
					</div>
				) : null}
			</div>

			{phase === "over" && q ? (
				<>
					{confetti ? <Confetti /> : null}
					<div className="card trivia-rise mt-3 spin-summary">
						<div className="card-body">
							<div className="h5 mb-2">
								{newBest
									? "New best!"
									: streak >= 15
										? "Great run!"
										: streak >= 5
											? "Nice run."
											: "Run over."}
							</div>
							<div className="spin-summary-lines">
								<div>🏀 {streak} correct</div>
								<div>🎯 Lost on {q.season}</div>
								<div>
									🎯{" "}
									<button
										type="button"
										className="btn btn-link p-0 align-baseline"
										onClick={() => setProfilePid(q.pid)}
									>
										{q.firstName} {q.lastName}
									</button>
								</div>
								<div>
									🎯 {q.categoryLabel}:{" "}
									<span className="fw-bold">
										{answerText(q.options[q.answer])}
									</span>
								</div>
							</div>
							<div className="d-flex flex-wrap gap-2 mt-3">
								<button
									type="button"
									className="btn btn-success fw-bold"
									onClick={() => void start()}
								>
									Spin again
								</button>
								<button
									type="button"
									className="btn btn-light-bordered"
									onClick={() => void copy()}
								>
									{copied ? "Copied" : "Copy result"}
								</button>
								<button
									type="button"
									className="btn btn-light-bordered"
									onClick={() => {
										clearTimers();
										runId.current += 1;
										setPhase("idle");
									}}
								>
									Difficulty
								</button>
							</div>
						</div>
					</div>
				</>
			) : null}
		</>
	);
};

export default TriviaSpinStreak;
