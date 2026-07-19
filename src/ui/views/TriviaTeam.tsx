import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { useCountUp } from "../util/useCountUp.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import TriviaPlayerSelect, {
	type TriviaSearchPlayer,
} from "../components/TriviaPlayerSelect.tsx";
import { Confetti } from "./LiveGame/Confetti.tsx";
import type { View } from "../../common/types.ts";

// Team Trivia: a random team-season - name the roster (bonus points without
// hints), pick the team's stat leaders, guess the win total, and how their
// season ended. Flows through staged rounds with a graded finale.

const LEADER_STATS = [
	["pts", "points", "ppg", "PPG"],
	["trb", "rebounds", "rpg", "RPG"],
	["ast", "assists", "apg", "APG"],
	["stl", "steals", "spg", "SPG"],
	["blk", "blocks", "bpg", "BPG"],
] as const;

type Phase =
	| "guess"
	| "hint"
	| { leader: number } // index into LEADER_STATS
	| "wins"
	| "playoffs"
	| "done";

// The ordered stage rail shown across the top.
const STAGES = [
	{ key: "name", label: "Name the roster" },
	{ key: "leaders", label: "Stat leaders" },
	{ key: "wins", label: "Win total" },
	{ key: "playoffs", label: "Season end" },
	{ key: "done", label: "Results" },
] as const;

const stageIndexOf = (phase: Phase): number => {
	if (phase === "guess" || phase === "hint") {
		return 0;
	}
	if (typeof phase === "object") {
		return 1;
	}
	if (phase === "wins") {
		return 2;
	}
	if (phase === "playoffs") {
		return 3;
	}
	return 4;
};

const gradeFor = (pct: number): { letter: string; color: string } => {
	if (pct >= 0.9) {
		return { letter: "S", color: "text-warning" };
	}
	if (pct >= 0.75) {
		return { letter: "A", color: "text-success" };
	}
	if (pct >= 0.6) {
		return { letter: "B", color: "text-success" };
	}
	if (pct >= 0.45) {
		return { letter: "C", color: "text-info" };
	}
	if (pct >= 0.3) {
		return { letter: "D", color: "text-danger" };
	}
	return { letter: "F", color: "text-danger" };
};

// Counts up the actual win total when it's revealed.
const WinsReveal = ({
	actual,
	correct,
}: {
	actual: number;
	correct: boolean;
}) => {
	const n = useCountUp(actual, 900);
	return (
		<span className={`h3 mb-0 ${correct ? "text-success" : "text-danger"}`}>
			{Math.round(n)}
		</span>
	);
};

const TriviaTeam = (props: View<"triviaTeam">) => {
	useTitleBar({ title: "Team Trivia" });

	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	const [round, setRound] = useState(props.round);
	const [phase, setPhase] = useState<Phase>("guess");
	const [revealed, setRevealed] = useState<Set<number>>(new Set());
	const [score, setScore] = useState(0);
	const [lastGain, setLastGain] = useState<
		{ key: number; amount: number } | undefined
	>();
	const [leaderResult, setLeaderResult] = useState<
		Record<number, { pickedPid: number; correct: boolean }>
	>({});
	const [winsGuess, setWinsGuess] = useState(0);
	const [winsResult, setWinsResult] = useState<boolean | undefined>();
	const [playoffPick, setPlayoffPick] = useState<number | undefined>();
	const [loadingNew, setLoadingNew] = useState(false);

	const gain = (amount: number) => {
		setScore((s) => s + amount);
		setLastGain((prev) => ({ key: (prev?.key ?? 0) + 1, amount }));
	};

	const newRound = async () => {
		setLoadingNew(true);
		try {
			const fresh = await toWorker("main", "triviaNewTeamRound", undefined);
			if (fresh) {
				setRound(fresh);
				setPhase("guess");
				setRevealed(new Set());
				setScore(0);
				setLastGain(undefined);
				setLeaderResult({});
				setWinsGuess(Math.floor(fresh.wins.games / 2));
				setWinsResult(undefined);
				setPlayoffPick(undefined);
			}
		} finally {
			setLoadingNew(false);
		}
	};

	if (!round) {
		return (
			<>
				<p>
					No complete team seasons to quiz on yet. Come back after a season
					finishes.
				</p>
				<button
					className="btn btn-primary"
					disabled={loadingNew}
					onClick={newRound}
				>
					Try again
				</button>
			</>
		);
	}

	const t = teamInfoCache[round.team.tid];
	const inLeaderPhase = typeof phase === "object";
	const rosterVisible =
		inLeaderPhase || phase === "wins" || phase === "playoffs" || phase === "done";
	const stageIndex = stageIndexOf(phase);
	const stages = round.playoffs
		? STAGES
		: STAGES.filter((s) => s.key !== "playoffs");

	const handleNameGuess = (p: TriviaSearchPlayer) => {
		const hit = round.roster.find((r) => r.pid === p.pid);
		if (!hit || revealed.has(hit.pid)) {
			return;
		}
		setRevealed((prev) => new Set(prev).add(hit.pid));
		gain(phase === "guess" ? 15 : 10);
	};

	const handleLeaderPick = (leaderIndex: number, pid: number) => {
		if (leaderResult[leaderIndex]) {
			return;
		}
		const statKey = LEADER_STATS[leaderIndex]![0];
		const correct = round.leaders[statKey] === pid;
		setLeaderResult((prev) => ({
			...prev,
			[leaderIndex]: { pickedPid: pid, correct },
		}));
		if (correct) {
			gain(10);
		}
	};

	const nextLeader = (leaderIndex: number) => {
		if (leaderIndex + 1 < LEADER_STATS.length) {
			setPhase({ leader: leaderIndex + 1 });
		} else {
			setWinsGuess(Math.floor(round.wins.games / 2));
			setPhase("wins");
		}
	};

	const winsTolerance = Math.max(1, Math.round(round.wins.window / 2));

	const submitWins = () => {
		const correct = Math.abs(winsGuess - round.wins.actual) <= winsTolerance;
		setWinsResult(correct);
		if (correct) {
			gain(10);
		}
	};

	const pickPlayoff = (i: number) => {
		if (playoffPick !== undefined || !round.playoffs) {
			return;
		}
		setPlayoffPick(i);
		if (i === round.playoffs.answerIndex) {
			gain(10);
		}
	};

	// Grade math for the finale.
	const maxScore =
		15 * round.roster.length +
		10 * LEADER_STATS.length +
		10 +
		(round.playoffs ? 10 : 0);
	const leadersCorrect = Object.values(leaderResult).filter(
		(r) => r.correct,
	).length;
	const grade = gradeFor(score / maxScore);

	const rosterTable = (
		<div className="table-responsive">
			<table
				className="table table-striped table-borderless table-sm align-middle mb-3"
				style={{ maxWidth: 640 }}
			>
				<thead>
					<tr>
						<th>Player</th>
						<th>Pos</th>
						<th className="text-end">Age</th>
						<th className="text-end">GP</th>
						<th className="text-end">PPG</th>
						<th className="text-end">RPG</th>
						<th className="text-end">APG</th>
					</tr>
				</thead>
				<tbody>
					{round.roster.map((p) => {
						const shown = rosterVisible || revealed.has(p.pid);
						const showHints = phase !== "guess" || revealed.has(p.pid);
						const clickable =
							inLeaderPhase && !leaderResult[(phase as { leader: number }).leader];
						const leaderIndex = inLeaderPhase
							? (phase as { leader: number }).leader
							: undefined;
						const result =
							leaderIndex !== undefined ? leaderResult[leaderIndex] : undefined;
						const isAnswer =
							leaderIndex !== undefined &&
							result !== undefined &&
							round.leaders[LEADER_STATS[leaderIndex]![0]] === p.pid;
						const isPicked = result?.pickedPid === p.pid;
						return (
							<tr
								key={p.pid}
								className={
									isAnswer
										? "table-success"
										: isPicked && !result.correct
											? "table-danger"
											: revealed.has(p.pid) && !rosterVisible
												? "table-success"
												: undefined
								}
								style={clickable ? { cursor: "pointer" } : undefined}
								onClick={
									clickable && leaderIndex !== undefined
										? () => handleLeaderPick(leaderIndex, p.pid)
										: undefined
								}
							>
								<td>
									{shown ? (
										<span
											className={
												revealed.has(p.pid) && !rosterVisible
													? "d-inline-block trivia-pop"
													: undefined
											}
										>
											{p.jerseyNumber !== undefined ? (
												<span className="text-body-secondary me-1">
													#{p.jerseyNumber}
												</span>
											) : null}
											{p.name}
											{rosterVisible && !revealed.has(p.pid) ? (
												<span
													className="text-body-secondary small ms-1"
													title="Not named"
												>
													·
												</span>
											) : null}
										</span>
									) : (
										<span className="text-body-secondary">— hidden —</span>
									)}
								</td>
								<td>{showHints ? p.pos : "?"}</td>
								<td className="text-end">{showHints ? p.age : "?"}</td>
								<td className="text-end">{showHints ? p.gp : "?"}</td>
								<td className="text-end">{showHints ? p.ppg : "?"}</td>
								<td className="text-end">{showHints ? p.rpg : "?"}</td>
								<td className="text-end">{showHints ? p.apg : "?"}</td>
							</tr>
						);
					})}
				</tbody>
			</table>
		</div>
	);

	return (
		<>
			{/* Hero header */}
			<div className="d-flex flex-wrap align-items-center gap-3 mb-2">
				<TeamLogoInline
					imgURL={t?.imgURL}
					imgURLSmall={t?.imgURLSmall}
					size={56}
					includePlaceholderIfNoLogo
				/>
				<div>
					<div className="h4 mb-0">
						{round.season} {round.team.label}
					</div>
					<div className="text-body-secondary small">
						How well do you know this team?
					</div>
				</div>
				<div className="d-flex align-items-center gap-2 ms-auto">
					<div className="trivia-tile position-relative">
						<div className="trivia-tile-value">{score}</div>
						<div className="trivia-tile-label">Score</div>
						{lastGain ? (
							<span
								key={lastGain.key}
								className="badge text-bg-success position-absolute top-0 start-100 translate-middle trivia-rise"
							>
								+{lastGain.amount}
							</span>
						) : null}
					</div>
					<button
						className="btn btn-sm btn-primary"
						disabled={loadingNew}
						onClick={newRound}
					>
						{loadingNew ? "Loading…" : "New team"}
					</button>
				</div>
			</div>

			{/* Stage rail */}
			<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
				{stages.map((s, i) => {
					const realIndex = STAGES.findIndex((x) => x.key === s.key);
					const active = realIndex === stageIndex;
					const doneStage = realIndex < stageIndex;
					return (
						<div
							key={s.key}
							className={`trivia-step ${active ? "active" : ""} ${doneStage ? "done" : ""}`}
						>
							<span>{doneStage ? "✓" : `${i + 1}.`}</span>
							{s.label}
						</div>
					);
				})}
			</div>

			{phase === "guess" || phase === "hint" ? (
				<>
					<div className="d-flex align-items-center gap-2 mb-2" style={{ maxWidth: 480 }}>
						<div className="progress flex-grow-1" style={{ height: 8 }}>
							<div
								className="progress-bar bg-success"
								style={{
									width: `${(revealed.size / round.roster.length) * 100}%`,
								}}
							/>
						</div>
						<span className="small text-body-secondary">
							{revealed.size}/{round.roster.length}
						</span>
					</div>
					<p className="mb-2">
						Name this roster —{" "}
						{phase === "guess" ? (
							<span className="fw-bold">15 points each</span>
						) : (
							<span>
								with hints, <span className="fw-bold">10 points each</span>
							</span>
						)}
					</p>
					<div className="mb-2" style={{ maxWidth: 480 }}>
						<TriviaPlayerSelect
							players={round.searchList.filter((p) => !revealed.has(p.pid))}
							onSelect={handleNameGuess}
						/>
					</div>
					<div className="d-flex gap-2 mb-3">
						{phase === "guess" ? (
							<button
								className="btn btn-light-bordered"
								onClick={() => setPhase("hint")}
							>
								Show hints (10 pts each)
							</button>
						) : null}
						<button
							className="btn btn-light-bordered"
							onClick={() => setPhase({ leader: 0 })}
						>
							Continue to stat leaders
						</button>
					</div>
				</>
			) : null}

			{inLeaderPhase ? (
				<div className="mb-2">
					{(() => {
						const leaderIndex = (phase as { leader: number }).leader;
						const [statKey, statName, perGameKey, perGameLabel] =
							LEADER_STATS[leaderIndex]!;
						const result = leaderResult[leaderIndex];
						const leaderPid = round.leaders[statKey];
						const leader = round.roster.find((p) => p.pid === leaderPid);
						return (
							<>
								<div className="d-flex flex-wrap align-items-center gap-2 mb-2">
									{LEADER_STATS.map(([, name], i) => {
										const r = leaderResult[i];
										return (
											<span
												key={name}
												className={`badge ${
													i === leaderIndex
														? "text-bg-primary"
														: r
															? r.correct
																? "text-bg-success"
																: "text-bg-danger"
															: "text-bg-secondary"
												}`}
											>
												{r ? (r.correct ? "✓ " : "✗ ") : ""}
												{name}
											</span>
										);
									})}
								</div>
								<p className="fw-bold mb-2">
									Who led the team in {statName}? Tap the player. (10 pts)
								</p>
								{result ? (
									<div className="mb-2 trivia-rise">
										<span
											className={`badge ${result.correct ? "text-bg-success" : "text-bg-danger"} me-2`}
										>
											{result.correct ? "Correct!" : "Not quite"}
										</span>
										{leader ? (
											<span>
												{leader.name} led with{" "}
												<span className="fw-bold">
													{leader[perGameKey]} {perGameLabel}
												</span>
												.
											</span>
										) : null}
										<button
											className="btn btn-sm btn-primary ms-3"
											onClick={() => nextLeader(leaderIndex)}
										>
											{leaderIndex + 1 < LEADER_STATS.length
												? "Next"
												: "Continue"}
										</button>
									</div>
								) : null}
							</>
						);
					})()}
				</div>
			) : null}

			{phase === "wins" ? (
				<div className="mb-3" style={{ maxWidth: 480 }}>
					<p className="fw-bold mb-2">
						How many of their {round.wins.games} games did they win? (within ±
						{winsTolerance}, 10 pts)
					</p>
					<div className="d-flex align-items-center gap-3">
						<input
							type="range"
							className="form-range"
							min={0}
							max={round.wins.games}
							value={winsGuess}
							disabled={winsResult !== undefined}
							onChange={(e) => setWinsGuess(Number(e.target.value))}
						/>
						<span className="h4 mb-0" style={{ minWidth: 48 }}>
							{winsGuess}
						</span>
					</div>
					<div className="d-flex justify-content-between text-body-secondary small mb-2">
						<span>0</span>
						<span>{Math.floor(round.wins.games / 2)}</span>
						<span>{round.wins.games}</span>
					</div>
					{winsResult === undefined ? (
						<button className="btn btn-primary" onClick={submitWins}>
							Lock it in
						</button>
					) : (
						<div className="trivia-rise">
							<span
								className={`badge ${winsResult ? "text-bg-success" : "text-bg-danger"} me-2`}
							>
								{winsResult ? "Correct!" : "Missed"}
							</span>
							They won{" "}
							<WinsReveal actual={round.wins.actual} correct={winsResult} /> (you
							said {winsGuess}).
							<button
								className="btn btn-primary ms-3"
								onClick={() => setPhase(round.playoffs ? "playoffs" : "done")}
							>
								Continue
							</button>
						</div>
					)}
				</div>
			) : null}

			{phase === "playoffs" && round.playoffs ? (
				<div className="mb-3" style={{ maxWidth: 480 }}>
					<p className="fw-bold mb-2">How did their season end? (10 pts)</p>
					<div className="d-flex flex-column gap-2">
						{round.playoffs.options.map((option, i) => {
							const picked = playoffPick !== undefined;
							const isAnswer = i === round.playoffs!.answerIndex;
							return (
								<button
									key={i}
									className={`btn text-start ${
										picked && isAnswer
											? "btn-success trivia-pop"
											: picked && playoffPick === i
												? "btn-danger trivia-shake"
												: "btn-light-bordered"
									}`}
									disabled={picked && playoffPick !== i && !isAnswer}
									onClick={() => pickPlayoff(i)}
								>
									{option}
								</button>
							);
						})}
					</div>
					{playoffPick !== undefined ? (
						<button
							className="btn btn-primary mt-2"
							onClick={() => setPhase("done")}
						>
							Finish
						</button>
					) : null}
				</div>
			) : null}

			{phase === "done" ? (
				<>
					{grade.letter === "S" ? <Confetti /> : null}
					<div className="card trivia-rise mb-3" style={{ maxWidth: 640 }}>
						<div className="card-body d-flex flex-wrap align-items-center gap-4">
							<div className={`trivia-grade ${grade.color}`}>{grade.letter}</div>
							<div className="flex-grow-1">
								<div className="h4 mb-1">
									{score} / {maxScore} points
								</div>
								<div className="small">
									Named {revealed.size}/{round.roster.length} players · Leaders{" "}
									{leadersCorrect}/{LEADER_STATS.length} ·{" "}
									{winsResult ? "Nailed the win total" : "Missed the win total"}
									{round.playoffs
										? playoffPick === round.playoffs.answerIndex
											? " · Called the finish"
											: " · Missed the finish"
										: ""}
								</div>
							</div>
							<button
								className="btn btn-primary"
								disabled={loadingNew}
								onClick={newRound}
							>
								{loadingNew ? "Loading…" : "Play again"}
							</button>
						</div>
					</div>
				</>
			) : null}

			{rosterTable}
		</>
	);
};

export default TriviaTeam;
