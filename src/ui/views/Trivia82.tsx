import { useState } from "react";
import type { View } from "../../common/types.ts";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { ActionButton } from "../components/ActionButton.tsx";
import type {
	EightyTwoZeroMatchup,
	EightyTwoZeroOption,
	EightyTwoZeroPosition,
} from "../../worker/core/trivia/eightyTwoZero.ts";
import type { EightyTwoZeroResult } from "../../worker/core/trivia/eightyTwoZeroSim.ts";

// 82-0, played on your own league.
//
// Five rounds, one per position. Each round rolls a franchise and an era, and
// you take the best player you can find who suited up there and fits the slot.
// Then the five play a full season against the actual teams in your file, in
// BBGM's own engine, and you find out how close to 82-0 they get.
//
// The whole draft lives in this component. The worker deals the board once and
// answers "who is available for this round", and nothing is written to the
// league - it's a game about your league, not a change to it.

const POSITIONS: EightyTwoZeroPosition[] = ["PG", "SG", "SF", "PF", "C"];

const MODES = [
	{
		key: "classic",
		title: "Classic",
		description: "Every candidate's numbers are on the table.",
	},
	{
		key: "hoopIQ",
		title: "Hoop IQ",
		description: "No stats. Draft on what you remember.",
	},
	{
		key: "daily",
		title: "Daily",
		description: "One fixed set of five rounds a day. No rerolls.",
	},
	{
		key: "pick",
		title: "Pick Your Own",
		description: "Choose the franchise and era yourself each round.",
	},
] as const;

type Mode = (typeof MODES)[number]["key"];

const REROLLS = 3;

type Pick = {
	position: EightyTwoZeroPosition;
	option: EightyTwoZeroOption;
	tid: number;
	eraLabel: string;
};

// Deterministic per league-day, so the Daily is the same board every time it's
// opened and reloading can't reroll a round you didn't like.
const mulberry32 = (seed: number) => {
	let a = seed;
	return () => {
		a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};

const hashString = (value: string) => {
	let h = 2166136261;
	for (let i = 0; i < value.length; i++) {
		h ^= value.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	return h >>> 0;
};

const perGame = (total: number, gp: number) =>
	gp > 0 ? (Math.round((10 * total) / gp) / 10).toFixed(1) : "0.0";

const Trivia82 = ({ data, season }: View<"trivia82">) => {
	useTitleBar({ title: "82-0" });

	const [mode, setMode] = useState<Mode | undefined>();
	const [picks, setPicks] = useState<Pick[]>([]);
	const [matchup, setMatchup] = useState<EightyTwoZeroMatchup | undefined>();
	const [options, setOptions] = useState<EightyTwoZeroOption[] | undefined>();
	const [rerollsLeft, setRerollsLeft] = useState(REROLLS);
	const [rolling, setRolling] = useState(false);
	const [simulating, setSimulating] = useState(false);
	const [result, setResult] = useState<EightyTwoZeroResult | undefined>();
	const [error, setError] = useState<string | undefined>();

	if (!data) {
		return (
			<p>
				There isn't enough league history yet to draft from. Play a season and
				come back.
			</p>
		);
	}

	const teamByTid = new Map(data.teams.map((t) => [t.tid, t]));
	const eraByStart = new Map(data.eras.map((era) => [era.start, era]));
	const round = picks.length;
	const position = POSITIONS[round];
	const takenPids = picks.map((pick) => pick.option.pid);

	const describe = (m: EightyTwoZeroMatchup) => {
		const t = teamByTid.get(m.tid);
		const era = eraByStart.get(m.eraStart);
		return {
			team: t ? `${t.region} ${t.name}` : "?",
			abbrev: t?.abbrev ?? "",
			era: era?.label ?? String(m.eraStart),
		};
	};

	const loadOptions = async (
		m: EightyTwoZeroMatchup,
		pos: EightyTwoZeroPosition,
	) => {
		const rows = await toWorker("main", "trivia82Options", {
			tid: m.tid,
			eraStart: m.eraStart,
			position: pos,
			excludePids: takenPids,
		});
		return rows;
	};

	// Roll a franchise and an era for this round. Daily uses a seed built from
	// the date and the round, so everyone's fifth round is everyone's fifth
	// round; every other mode rolls freely.
	const roll = async (chosen?: EightyTwoZeroMatchup) => {
		if (!position) {
			return;
		}
		setError(undefined);
		setRolling(true);
		try {
			const board = data.matchups[position];
			if (board.length === 0) {
				setError(`No franchise in this league has ever had a ${position}.`);
				return;
			}

			let next = chosen;
			if (!next) {
				const rand =
					mode === "daily"
						? mulberry32(
								hashString(
									`${season}|${new Date().toISOString().slice(0, 10)}|${round}`,
								),
							)
						: Math.random;
				// A matchup whose only eligible player is already drafted is a dead
				// round, so keep rolling until one has somebody left.
				for (let attempt = 0; attempt < 30; attempt++) {
					const candidate = board[Math.floor(rand() * board.length)]!;
					const rows = await loadOptions(candidate, position);
					if (rows.length > 0) {
						setMatchup(candidate);
						setOptions(rows);
						return;
					}
					next = candidate;
				}
				setError("Couldn't find a matchup with anyone left to draft.");
				return;
			}

			const rows = await loadOptions(next, position);
			setMatchup(next);
			setOptions(rows);
			if (rows.length === 0) {
				setError("Nobody there fits this position. Try another.");
			}
		} catch (error_) {
			console.error("82-0 roll failed", error_);
			setError("Something went wrong dealing that round.");
		} finally {
			setRolling(false);
		}
	};

	const start = async (chosen: Mode) => {
		setMode(chosen);
		setPicks([]);
		setResult(undefined);
		setMatchup(undefined);
		setOptions(undefined);
		setRerollsLeft(chosen === "daily" ? 0 : REROLLS);
	};

	const draft = async (option: EightyTwoZeroOption) => {
		if (!matchup || !position) {
			return;
		}
		const { era } = describe(matchup);
		const next = [
			...picks,
			{ position, option, tid: matchup.tid, eraLabel: era },
		];
		setPicks(next);
		setMatchup(undefined);
		setOptions(undefined);
	};

	const simulate = async () => {
		setSimulating(true);
		setError(undefined);
		try {
			const out = await toWorker(
				"main",
				"trivia82Simulate",
				picks.map((pick) => ({
					pid: pick.option.pid,
					season: pick.option.season,
				})),
			);
			setResult(out);
		} catch (error_) {
			console.error("82-0 simulation failed", error_);
			setError("The season wouldn't sim. Try again.");
		} finally {
			setSimulating(false);
		}
	};

	if (mode === undefined) {
		return (
			<>
				<div className="d-flex flex-wrap gap-2">
					{MODES.map((row) => (
						<button
							key={row.key}
							type="button"
							className="btn btn-light-bordered text-start"
							style={{ width: 220 }}
							onClick={() => start(row.key)}
						>
							<div className="fw-bold">{row.title}</div>
							<div className="small text-body-secondary">{row.description}</div>
						</button>
					))}
				</div>
			</>
		);
	}

	const hideStats = mode === "hoopIQ";

	return (
		<>
			<div className="d-flex flex-wrap align-items-center gap-2 mb-3">
				<button
					type="button"
					className="btn btn-light-bordered btn-sm"
					onClick={() => setMode(undefined)}
				>
					← Modes
				</button>
				<span className="fw-bold">
					{MODES.find((row) => row.key === mode)?.title}
				</span>
				{result ? null : (
					<span className="text-body-secondary">
						Round {Math.min(round + 1, POSITIONS.length)} of {POSITIONS.length}
					</span>
				)}
			</div>

			<div className="d-flex flex-wrap gap-2 mb-3">
				{POSITIONS.map((pos, i) => {
					const pick = picks[i];
					return (
						<div
							key={pos}
							className={`border rounded p-2 ${
								i === round && !result ? "border-primary" : ""
							}`}
							style={{ width: 190 }}
						>
							<div className="fw-bold">{pos}</div>
							{pick ? (
								<>
									<div>
										<a href={helpers.leagueUrl(["player", pick.option.pid])}>
											{pick.option.name}
										</a>
									</div>
									<div className="small text-body-secondary">
										{pick.option.season} {teamByTid.get(pick.tid)?.abbrev ?? ""}
									</div>
								</>
							) : (
								<div className="small text-body-secondary">—</div>
							)}
						</div>
					);
				})}
			</div>

			{error ? <div className="alert alert-warning py-2">{error}</div> : null}

			{result ? (
				<Result
					result={result}
					onAgain={() => start(mode)}
					teamByTid={teamByTid}
					picks={picks}
				/>
			) : round >= POSITIONS.length ? (
				<ActionButton
					processing={simulating}
					processingText="Playing the season"
					onClick={simulate}
					size="lg"
				>
					Play the season
				</ActionButton>
			) : (
				<Round
					mode={mode}
					position={position!}
					matchup={matchup}
					options={options}
					rolling={rolling}
					hideStats={hideStats}
					rerollsLeft={rerollsLeft}
					describe={describe}
					teams={data.teams}
					eras={data.eras}
					board={data.matchups[position!]}
					onRoll={roll}
					onReroll={() => {
						setRerollsLeft(rerollsLeft - 1);
						void roll();
					}}
					onDraft={draft}
				/>
			)}
		</>
	);
};

const Round = ({
	mode,
	position,
	matchup,
	options,
	rolling,
	hideStats,
	rerollsLeft,
	describe,
	teams,
	eras,
	board,
	onRoll,
	onReroll,
	onDraft,
}: {
	mode: Mode;
	position: EightyTwoZeroPosition;
	matchup: EightyTwoZeroMatchup | undefined;
	options: EightyTwoZeroOption[] | undefined;
	rolling: boolean;
	hideStats: boolean;
	rerollsLeft: number;
	describe: (m: EightyTwoZeroMatchup) => {
		team: string;
		abbrev: string;
		era: string;
	};
	teams: NonNullable<View<"trivia82">["data"]>["teams"];
	eras: NonNullable<View<"trivia82">["data"]>["eras"];
	board: EightyTwoZeroMatchup[];
	onRoll: (chosen?: EightyTwoZeroMatchup) => Promise<void>;
	onReroll: () => void;
	onDraft: (option: EightyTwoZeroOption) => void;
}) => {
	const [tid, setTid] = useState<number | undefined>();
	const [eraStart, setEraStart] = useState<number | undefined>();

	if (!matchup) {
		if (mode === "pick") {
			// Only offer combinations that can actually field this position, so a
			// hand-picked round is never a dead end either.
			const tids = [...new Set(board.map((m) => m.tid))];
			const erasFor = board.filter((m) => m.tid === tid).map((m) => m.eraStart);
			return (
				<div className="d-flex flex-wrap align-items-end gap-2">
					<div>
						<div className="small text-body-secondary">Franchise</div>
						<select
							className="form-select"
							style={{ width: 240 }}
							value={tid ?? ""}
							onChange={(event) => {
								setTid(Number(event.target.value));
								setEraStart(undefined);
							}}
						>
							<option value="">Choose a team…</option>
							{tids.map((x) => {
								const t = teams.find((row) => row.tid === x);
								return (
									<option key={x} value={x}>
										{t ? `${t.region} ${t.name}` : x}
									</option>
								);
							})}
						</select>
					</div>
					<div>
						<div className="small text-body-secondary">Era</div>
						<select
							className="form-select"
							style={{ width: 160 }}
							value={eraStart ?? ""}
							disabled={tid === undefined}
							onChange={(event) => setEraStart(Number(event.target.value))}
						>
							<option value="">Choose an era…</option>
							{erasFor.map((start) => (
								<option key={start} value={start}>
									{eras.find((era) => era.start === start)?.label ?? start}
								</option>
							))}
						</select>
					</div>
					<ActionButton
						processing={rolling}
						disabled={tid === undefined || eraStart === undefined}
						onClick={() => onRoll({ tid: tid!, eraStart: eraStart! })}
					>
						Go
					</ActionButton>
				</div>
			);
		}

		return (
			<ActionButton
				processing={rolling}
				processingText="Rolling"
				onClick={() => onRoll()}
				size="lg"
			>
				Roll for {position}
			</ActionButton>
		);
	}

	const info = describe(matchup);

	return (
		<>
			<div className="mb-3">
				<div className="h4 mb-0">
					{info.team} · {info.era}
				</div>
				<div className="text-body-secondary">
					Take a {position} who played here.
				</div>
			</div>

			{mode === "daily" || rerollsLeft <= 0 ? null : (
				<button
					type="button"
					className="btn btn-light-bordered btn-sm mb-3"
					disabled={rolling}
					onClick={onReroll}
				>
					Reroll ({rerollsLeft} left)
				</button>
			)}

			<div className="table-responsive">
				<table className="table table-striped table-sm table-hover">
					<thead>
						<tr>
							<th>Player</th>
							<th>Season</th>
							<th>Pos</th>
							{hideStats ? null : (
								<>
									<th>G</th>
									<th>MP</th>
									<th>PTS</th>
									<th>TRB</th>
									<th>AST</th>
									<th>STL</th>
									<th>BLK</th>
								</>
							)}
							<th />
						</tr>
					</thead>
					<tbody>
						{(options ?? []).map((option) => (
							<tr key={option.pid}>
								<td>{option.name}</td>
								<td>{option.season}</td>
								<td>{option.pos}</td>
								{hideStats ? null : (
									<>
										<td>{option.gp}</td>
										<td>{perGame(option.min, option.gp)}</td>
										<td>{perGame(option.pts, option.gp)}</td>
										<td>{perGame(option.trb, option.gp)}</td>
										<td>{perGame(option.ast, option.gp)}</td>
										<td>{perGame(option.stl, option.gp)}</td>
										<td>{perGame(option.blk, option.gp)}</td>
									</>
								)}
								<td>
									<button
										type="button"
										className="btn btn-primary btn-sm"
										onClick={() => onDraft(option)}
									>
										Draft
									</button>
								</td>
							</tr>
						))}
					</tbody>
				</table>
			</div>
		</>
	);
};

const Result = ({
	result,
	onAgain,
	teamByTid,
	picks,
}: {
	result: EightyTwoZeroResult;
	onAgain: () => void;
	teamByTid: Map<number, { abbrev: string }>;
	picks: Pick[];
}) => {
	const perfect = result.lost === 0;

	return (
		<>
			<div className="mb-3">
				<div className="display-4">
					{result.won}-{result.lost}
				</div>
				<div className="text-body-secondary">
					{perfect
						? "Perfect season."
						: `${helpers.roundStat(result.ptsFor, "pts")} scored, ${helpers.roundStat(
								result.ptsAgainst,
								"pts",
							)} allowed per game.`}
				</div>
				{result.best ? (
					<div className="small text-body-secondary">
						Biggest win: {result.best.pts > 0 ? "+" : ""}
						{result.best.pts} vs {result.best.opponent}
						{result.worst
							? ` · Worst night: ${result.worst.pts > 0 ? "+" : ""}${result.worst.pts} vs ${result.worst.opponent}`
							: null}
					</div>
				) : null}
			</div>

			<div className="table-responsive mb-3">
				<table className="table table-striped table-sm">
					<thead>
						<tr>
							<th>Pos</th>
							<th>Player</th>
							<th>From</th>
							<th>MP</th>
							<th>PTS</th>
							<th>TRB</th>
							<th>AST</th>
							<th>STL</th>
							<th>BLK</th>
							<th>TOV</th>
						</tr>
					</thead>
					<tbody>
						{result.players.map((line, i) => (
							<tr key={line.pid}>
								<td>{picks[i]?.position}</td>
								<td>
									<a href={helpers.leagueUrl(["player", line.pid])}>
										{line.name}
									</a>
								</td>
								<td>
									{line.season}{" "}
									{picks[i] ? (teamByTid.get(picks[i]!.tid)?.abbrev ?? "") : ""}
								</td>
								<td>{perGame(line.min, line.gp)}</td>
								<td>{perGame(line.pts, line.gp)}</td>
								<td>{perGame(line.trb, line.gp)}</td>
								<td>{perGame(line.ast, line.gp)}</td>
								<td>{perGame(line.stl, line.gp)}</td>
								<td>{perGame(line.blk, line.gp)}</td>
								<td>{perGame(line.tov, line.gp)}</td>
							</tr>
						))}
					</tbody>
				</table>
			</div>

			<button type="button" className="btn btn-primary" onClick={onAgain}>
				Draft another
			</button>
		</>
	);
};

export default Trivia82;
