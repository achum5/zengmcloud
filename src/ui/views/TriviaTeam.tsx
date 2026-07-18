import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import TriviaPlayerSelect, {
	type TriviaSearchPlayer,
} from "../components/TriviaPlayerSelect.tsx";
import type { View } from "../../common/types.ts";

// Team Trivia: a random team-season - name the roster (bonus points without
// hints), pick the team's stat leaders, guess the win total, and how their
// season ended.

const LEADER_STATS = [
	["pts", "points"],
	["trb", "rebounds"],
	["ast", "assists"],
	["stl", "steals"],
	["blk", "blocks"],
] as const;

type Phase =
	| "guess"
	| "hint"
	| { leader: number } // index into LEADER_STATS
	| "wins"
	| "playoffs"
	| "done";

const TriviaTeam = (props: View<"triviaTeam">) => {
	useTitleBar({ title: "Team Trivia" });

	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	const [round, setRound] = useState(props.round);
	const [phase, setPhase] = useState<Phase>("guess");
	const [revealed, setRevealed] = useState<Set<number>>(new Set());
	const [score, setScore] = useState(0);
	const [leaderResult, setLeaderResult] = useState<
		Record<number, { pickedPid: number; correct: boolean }>
	>({});
	const [winsGuess, setWinsGuess] = useState(0);
	const [winsResult, setWinsResult] = useState<boolean | undefined>();
	const [playoffPick, setPlayoffPick] = useState<number | undefined>();
	const [loadingNew, setLoadingNew] = useState(false);

	const newRound = async () => {
		setLoadingNew(true);
		try {
			const fresh = await toWorker("main", "triviaNewTeamRound", undefined);
			if (fresh) {
				setRound(fresh);
				setPhase("guess");
				setRevealed(new Set());
				setScore(0);
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
	const allRevealed = revealed.size >= round.roster.length;
	const inLeaderPhase = typeof phase === "object";
	const rosterVisible = inLeaderPhase || phase === "wins" || phase === "playoffs" || phase === "done";

	const handleNameGuess = (p: TriviaSearchPlayer) => {
		const hit = round.roster.find((r) => r.pid === p.pid);
		if (!hit || revealed.has(hit.pid)) {
			return;
		}
		setRevealed((prev) => new Set(prev).add(hit.pid));
		setScore((s) => s + (phase === "guess" ? 15 : 10));
	};

	const advanceFromNaming = () => {
		if (phase === "guess") {
			setPhase("hint");
		} else {
			setPhase({ leader: 0 });
		}
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
			setScore((s) => s + 10);
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
			setScore((s) => s + 10);
		}
	};

	const pickPlayoff = (i: number) => {
		if (playoffPick !== undefined || !round.playoffs) {
			return;
		}
		setPlayoffPick(i);
		if (i === round.playoffs.answerIndex) {
			setScore((s) => s + 10);
		}
	};

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
										<>
											{p.jerseyNumber !== undefined ? (
												<span className="text-body-secondary me-1">
													#{p.jerseyNumber}
												</span>
											) : null}
											{p.name}
										</>
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
			<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
				<TeamLogoInline
					imgURL={t?.imgURL}
					imgURLSmall={t?.imgURLSmall}
					size={40}
					includePlaceholderIfNoLogo
				/>
				<div>
					<div className="fw-bold">
						{round.season} {round.team.label}
					</div>
					<div className="text-body-secondary small">
						Score: <span className="fw-bold">{score}</span>
					</div>
				</div>
				<button
					className="btn btn-primary ms-auto"
					disabled={loadingNew}
					onClick={newRound}
				>
					{loadingNew ? "Loading…" : "New team"}
				</button>
			</div>

			{phase === "guess" || phase === "hint" ? (
				<>
					<p>
						Name this roster ({revealed.size}/{round.roster.length}
						{phase === "guess"
							? ", 15 points each"
							: ", with hints — 10 points each"}
						)
					</p>
					<div className="mb-3" style={{ maxWidth: 480 }}>
						<TriviaPlayerSelect
							players={round.searchList.filter((p) => !revealed.has(p.pid))}
							onSelect={handleNameGuess}
						/>
					</div>
					<button
						className="btn btn-light-bordered mb-3"
						onClick={advanceFromNaming}
						disabled={allRevealed && phase === "guess" ? false : undefined}
					>
						{phase === "guess" ? "Show hints" : "Continue to stat leaders"}
					</button>
				</>
			) : null}

			{inLeaderPhase ? (
				<div className="mb-2">
					{(() => {
						const leaderIndex = (phase as { leader: number }).leader;
						const result = leaderResult[leaderIndex];
						return (
							<>
								<p className="fw-bold mb-2">
									Who led the team in {LEADER_STATS[leaderIndex]![1]}? (10
									points — tap the player)
								</p>
								{result ? (
									<button
										className="btn btn-primary mb-2"
										onClick={() => nextLeader(leaderIndex)}
									>
										{result.correct ? "Correct! " : "Not quite. "}
										{leaderIndex + 1 < LEADER_STATS.length
											? "Next"
											: "Continue"}
									</button>
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
						{winsTolerance}, 10 points)
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
					{winsResult === undefined ? (
						<button className="btn btn-primary mt-2" onClick={submitWins}>
							Lock it in
						</button>
					) : (
						<div className="mt-2">
							<span
								className={`badge ${winsResult ? "text-bg-success" : "text-bg-danger"} me-2`}
							>
								{winsResult ? "Correct!" : "Missed"}
							</span>
							They won {round.wins.actual}.
							<button
								className="btn btn-primary ms-3"
								onClick={() =>
									setPhase(round.playoffs ? "playoffs" : "done")
								}
							>
								Continue
							</button>
						</div>
					)}
				</div>
			) : null}

			{phase === "playoffs" && round.playoffs ? (
				<div className="mb-3" style={{ maxWidth: 480 }}>
					<p className="fw-bold mb-2">How did their season end? (10 points)</p>
					<div className="d-flex flex-column gap-2">
						{round.playoffs.options.map((option, i) => {
							const picked = playoffPick !== undefined;
							const isAnswer = i === round.playoffs!.answerIndex;
							return (
								<button
									key={i}
									className={`btn text-start ${
										picked && isAnswer
											? "btn-success"
											: picked && playoffPick === i
												? "btn-danger"
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
				<p className="fw-bold">
					Final score: {score}. Named {revealed.size}/{round.roster.length}{" "}
					players.
				</p>
			) : null}

			{rosterTable}
		</>
	);
};

export default TriviaTeam;
