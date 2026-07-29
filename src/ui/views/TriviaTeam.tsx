import { useEffect, useMemo, useRef, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import { useCountUp } from "../util/useCountUp.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import { PlayerPicture } from "../components/PlayerPicture.tsx";
import { Modal } from "../components/Modal.tsx";
import TriviaPlayerSelect, {
	type TriviaSearchPlayer,
} from "../components/TriviaPlayerSelect.tsx";
import { TriviaHistoryModal } from "../components/TriviaHistoryModal.tsx";
import { TriviaPlayerModal } from "../components/TriviaPlayerModal.tsx";
import { JerseyNumber } from "../components/JerseyNumber.tsx";
import {
	primeTriviaFaces,
	type TriviaPlayerCard,
} from "../util/triviaPlayerCards.ts";
import {
	addHistoryEntry,
	loadHistory,
	summarize,
	type TriviaReplay,
} from "../util/triviaHistory.ts";
import { shareHistory } from "../util/triviaHistorySync.ts";
import { loadProgress, saveProgress } from "../util/triviaProgress.ts";
import { Confetti } from "./LiveGame/Confetti.tsx";
import { DEFAULT_TEAM_COLORS } from "../../common/constants.ts";
import type { View } from "../../common/types.ts";

// Team Trivia: pick a team-season - or take a random one - and name the roster
// off a grid of face cards, then pick the stat leaders, guess the win total,
// and call how the season ended.
//
// The naming round is the game: every card shows the position and jersey number
// its player wore, and nothing else, so you are recognising a squad rather than
// reading a list. Names are never in the DOM until they're earned - a redacted
// bar stands in - because a blurred name is still a name to anyone who selects
// the page.

type Round = NonNullable<View<"triviaTeam">["round"]>;
type Catalog = NonNullable<View<"triviaTeam">["catalog"]>;

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

// A quiz in progress, as it survives a reload. Only the team-season is stored,
// not the roster: (season, tid) names exactly one candidate, so the round is
// rebuilt identically from two numbers rather than a payload.
type SavedTeam = {
	season: number;
	tid: number;
	phase: Phase;
	revealed: number[];
	score: number;
	leaderResult: Record<number, { pickedPid: number; correct: boolean }>;
	winsGuess: number;
	winsResult?: boolean;
	playoffPick?: number;
};

const SETTINGS_KEY = "triviaTeamSettings";

type Settings = {
	minSeason?: number;
	maxSeason?: number;
	// Restrict random draws to one franchise.
	tid?: number;
};

const loadSettings = (): Settings => {
	try {
		const raw = localStorage.getItem(SETTINGS_KEY);
		if (raw) {
			const s = JSON.parse(raw);
			return {
				minSeason: typeof s.minSeason === "number" ? s.minSeason : undefined,
				maxSeason: typeof s.maxSeason === "number" ? s.maxSeason : undefined,
				tid: typeof s.tid === "number" ? s.tid : undefined,
			};
		}
	} catch {}
	return {};
};

// A name stands in as one block per word, roughly as wide as the word it hides.
// It gives the same "there is a name here, about this long" cue a blur does
// without putting the answer on the page.
const redact = (name: string) =>
	name
		.split(" ")
		.filter(Boolean)
		.map((w) => "█".repeat(Math.max(2, Math.min(9, w.length))))
		.join(" ");

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

// Year range + team filter for random draws. A modal rather than a popover: it
// has to work the same on a phone, where a popover anchored to a toolbar button
// has nowhere to go.
const SettingsModal = ({
	show,
	onHide,
	catalog,
	settings,
	onChange,
}: {
	show: boolean;
	onHide: () => void;
	catalog: Catalog | undefined;
	settings: Settings;
	onChange: (settings: Settings) => void;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const min = catalog?.minSeason ?? 0;
	const max = catalog?.maxSeason ?? 0;
	const from = settings.minSeason ?? min;
	const to = settings.maxSeason ?? max;

	// Dragging one handle past the other pushes the other along rather than
	// producing an empty range.
	const setFrom = (value: number) => {
		const next = Math.max(min, Math.min(max, value));
		onChange({ ...settings, minSeason: next, maxSeason: Math.max(next, to) });
	};
	const setTo = (value: number) => {
		const next = Math.max(min, Math.min(max, value));
		onChange({ ...settings, maxSeason: next, minSeason: Math.min(next, from) });
	};

	const teamOptions = useMemo(() => {
		const tids = new Set<number>();
		for (const c of catalog?.candidates ?? []) {
			tids.add(c.tid);
		}
		return [...tids]
			.map((tid) => ({
				tid,
				label:
					`${teamInfoCache[tid]?.region ?? ""} ${teamInfoCache[tid]?.name ?? tid}`.trim(),
			}))
			.sort((a, b) => a.label.localeCompare(b.label));
	}, [catalog, teamInfoCache]);

	return (
		<Modal show={show} onHide={onHide}>
			<Modal.Header closeButton>
				<Modal.Title className="fs-5">Random game settings</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				<div className="fw-bold">Year range</div>
				<div className="text-body-secondary small mb-2">
					Which seasons a random team-season is drawn from
				</div>
				<div className="d-flex gap-3 mb-2">
					<label className="flex-grow-1">
						<span className="small text-body-secondary">From</span>
						<input
							className="form-control"
							type="number"
							min={min}
							max={max}
							value={from}
							onChange={(event) => setFrom(Number(event.target.value))}
						/>
					</label>
					<label className="flex-grow-1">
						<span className="small text-body-secondary">To</span>
						<input
							className="form-control"
							type="number"
							min={min}
							max={max}
							value={to}
							onChange={(event) => setTo(Number(event.target.value))}
						/>
					</label>
				</div>
				<input
					className="form-range"
					type="range"
					min={min}
					max={max}
					value={from}
					aria-label="Earliest season"
					onChange={(event) => setFrom(Number(event.target.value))}
				/>
				<input
					className="form-range"
					type="range"
					min={min}
					max={max}
					value={to}
					aria-label="Latest season"
					onChange={(event) => setTo(Number(event.target.value))}
				/>

				<hr />

				<div className="fw-bold">Team filter</div>
				<div className="text-body-secondary small mb-2">
					Draw random games from one team only
				</div>
				<select
					className="form-select"
					value={settings.tid === undefined ? "" : String(settings.tid)}
					onChange={(event) =>
						onChange({
							...settings,
							tid:
								event.target.value === ""
									? undefined
									: Number(event.target.value),
						})
					}
				>
					<option value="">All teams</option>
					{teamOptions.map((t) => (
						<option key={t.tid} value={t.tid}>
							{t.label}
						</option>
					))}
				</select>

				<button
					className="btn btn-light-bordered mt-3"
					onClick={() => onChange({})}
				>
					Reset
				</button>
			</Modal.Body>
		</Modal>
	);
};

const TriviaTeam = (props: View<"triviaTeam">) => {
	useTitleBar({ title: "Team Trivia" });

	const { teamInfoCache, lid } = useLocal(["teamInfoCache", "lid"]);

	// An unfinished quiz from a previous visit. Read before any state exists so
	// it seeds the initial values rather than racing the random round the view
	// always loads.
	const [restored] = useState(() => loadProgress<SavedTeam>("team", lid));
	const needsRound =
		restored !== undefined &&
		(restored.season !== props.round?.season ||
			restored.tid !== props.round?.team.tid);

	const [round, setRound] = useState(props.round);
	// The random round the view loaded is the wrong team; hold the page until
	// the saved one arrives rather than flashing someone else's logo.
	const [restoring, setRestoring] = useState(needsRound);
	const [catalog, setCatalog] = useState(props.catalog);
	const [phase, setPhase] = useState<Phase>(() => restored?.phase ?? "guess");
	const [revealed, setRevealed] = useState<Set<number>>(
		() => new Set(restored?.revealed ?? []),
	);
	const [score, setScore] = useState(() => restored?.score ?? 0);
	const [lastGain, setLastGain] = useState<
		{ key: number; amount: number } | undefined
	>();
	const [missKey, setMissKey] = useState(0);
	const [miss, setMiss] = useState<string | undefined>();
	const [leaderResult, setLeaderResult] = useState<
		Record<number, { pickedPid: number; correct: boolean }>
	>(() => restored?.leaderResult ?? {});
	const [winsGuess, setWinsGuess] = useState(() => restored?.winsGuess ?? 0);
	const [winsResult, setWinsResult] = useState<boolean | undefined>(
		() => restored?.winsResult,
	);
	const [playoffPick, setPlayoffPick] = useState<number | undefined>(
		() => restored?.playoffPick,
	);
	const [loadingNew, setLoadingNew] = useState(false);
	const [cards, setCards] = useState<Record<number, TriviaPlayerCard>>({});
	const [settings, setSettings] = useState<Settings>(loadSettings);
	const [showSettings, setShowSettings] = useState(false);
	const [showHistory, setShowHistory] = useState(false);
	const [history, setHistory] = useState(() => loadHistory("team"));
	// The player card, opened by tapping anyone whose name is showing.
	const [profilePid, setProfilePid] = useState<number | undefined>();

	const gain = (amount: number) => {
		setScore((s) => s + amount);
		setLastGain((prev) => ({ key: (prev?.key ?? 0) + 1, amount }));
	};

	const saveSettings = (next: Settings) => {
		setSettings(next);
		try {
			localStorage.setItem(SETTINGS_KEY, JSON.stringify(next));
		} catch {}
	};

	// Fetch the saved team-season. (season, tid) narrows to exactly one
	// candidate, so this rebuilds the identical round.
	useEffect(() => {
		if (!restoring || !restored) {
			return;
		}
		let stale = false;
		// Awaited rather than chained: toWorker's declared return nests one
		// promise deeper than it resolves, so `.then` hands back a Promise-typed
		// value while `await` collapses it.
		void (async () => {
			try {
				const fresh = await toWorker("main", "triviaNewTeamRound", {
					season: restored.season,
					tid: restored.tid,
				});
				if (!stale && fresh) {
					setRound(fresh);
				}
			} finally {
				if (!stale) {
					setRestoring(false);
				}
			}
		})();
		return () => {
			stale = true;
		};
		// Once, on mount - `restored` never changes.
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, []);

	// Persist on every move, so a reload or a trip to another page comes back to
	// the same quiz at the same stage.
	useEffect(() => {
		if (!round || restoring) {
			return;
		}
		saveProgress("team", lid, {
			season: round.season,
			tid: round.team.tid,
			phase,
			revealed: [...revealed],
			score,
			leaderResult,
			winsGuess,
			winsResult,
			playoffPick,
		} satisfies SavedTeam);
	}, [
		round,
		restoring,
		lid,
		phase,
		revealed,
		score,
		leaderResult,
		winsGuess,
		winsResult,
		playoffPick,
	]);

	// Faces for the whole roster in one call, dressed in this team's colors.
	useEffect(() => {
		if (!round) {
			return;
		}
		let stale = false;
		setCards({});
		void primeTriviaFaces(
			round.roster.map((p) => p.pid),
			round.team,
		).then((next) => {
			if (!stale) {
				setCards(next);
			}
		});
		return () => {
			stale = true;
		};
	}, [round]);

	const startRound = (fresh: Round) => {
		setRound(fresh);
		setPhase("guess");
		setRevealed(new Set());
		setScore(0);
		setLastGain(undefined);
		setMiss(undefined);
		setLeaderResult({});
		setWinsGuess(Math.floor(fresh.wins.games / 2));
		setWinsResult(undefined);
		setPlayoffPick(undefined);
	};

	const newRound = async (
		overrides: { season?: number; tid?: number } = {},
	) => {
		setLoadingNew(true);
		try {
			const fresh = await toWorker("main", "triviaNewTeamRound", {
				minSeason: settings.minSeason,
				maxSeason: settings.maxSeason,
				tid: settings.tid,
				...overrides,
			});
			if (fresh) {
				startRound(fresh);
			}
		} finally {
			setLoadingNew(false);
		}
	};

	// Record the finished game exactly once, when the results stage is reached.
	const recordedRef = useRef(restored?.phase === "done");
	useEffect(() => {
		if (phase !== "done") {
			recordedRef.current = false;
			return;
		}
		if (recordedRef.current || !round) {
			return;
		}
		recordedRef.current = true;
		const max =
			15 * round.roster.length +
			10 * LEADER_STATS.length +
			10 +
			(round.playoffs ? 10 : 0);
		const next = addHistoryEntry("team", {
			score,
			label: `${round.season} ${round.team.label}`,
			detail: `${revealed.size}/${round.roster.length} named · Grade ${gradeFor(score / max).letter}`,
			tid: round.team.tid,
			season: round.season,
			colors: round.team.colors,
			progress: { done: revealed.size, total: round.roster.length },
			replay: { kind: "team", season: round.season, tid: round.team.tid },
		});
		setHistory(next);
		shareHistory("team", next);
	}, [phase, round, score, revealed]);

	// The catalog only arrives with the first view render; if that failed (an
	// empty league at load), fetch it once the player asks for anything.
	const ensureCatalog = async () => {
		if (catalog) {
			return;
		}
		const c = await toWorker("main", "triviaTeamCatalog", undefined);
		if (c) {
			setCatalog(c);
		}
	};

	// Which seasons THIS team can be quizzed on, and which teams existed in THIS
	// season - so neither dropdown can offer a combination that doesn't exist.
	// Both are keyed off what's on screen rather than off the settings filter:
	// the dropdowns change the current game, the filter only shapes random draws.
	const seasonOptions = useMemo(() => {
		const seasons = new Set<number>();
		for (const c of catalog?.candidates ?? []) {
			if (round === undefined || c.tid === round.team.tid) {
				seasons.add(c.season);
			}
		}
		return [...seasons].sort((a, b) => b - a);
	}, [catalog, round]);

	const teamOptionsForSeason = useMemo(() => {
		const tids = new Set<number>();
		for (const c of catalog?.candidates ?? []) {
			if (round === undefined || c.season === round.season) {
				tids.add(c.tid);
			}
		}
		return [...tids]
			.map((tid) => ({
				tid,
				label:
					`${teamInfoCache[tid]?.region ?? ""} ${teamInfoCache[tid]?.name ?? tid}`.trim(),
			}))
			.sort((a, b) => a.label.localeCompare(b.label));
	}, [catalog, round, teamInfoCache]);

	if (restoring) {
		return <p className="text-body-secondary">Loading</p>;
	}

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
					onClick={() => newRound()}
				>
					Try again
				</button>
			</>
		);
	}

	const t = teamInfoCache[round.team.tid];
	const inLeaderPhase = typeof phase === "object";
	const namingRound = phase === "guess" || phase === "hint";
	const rosterVisible = !namingRound;
	const stageIndex = stageIndexOf(phase);
	const stages = round.playoffs
		? STAGES
		: STAGES.filter((s) => s.key !== "playoffs");

	const handleNameGuess = (p: TriviaSearchPlayer) => {
		const hit = round.roster.find((r) => r.pid === p.pid);
		if (!hit || revealed.has(hit.pid)) {
			// Naming someone who never played here is the interesting kind of wrong
			// - say so, rather than swallowing the guess.
			setMiss(p.name);
			setMissKey((k) => k + 1);
			return;
		}
		setMiss(undefined);
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

	const leaderIndex = inLeaderPhase
		? (phase as { leader: number }).leader
		: undefined;
	const leaderAnswered =
		leaderIndex !== undefined && leaderResult[leaderIndex] !== undefined;

	const summary = summarize(history);

	const cardGrid = (
		<div className="trivia-roster-grid mb-3">
			{round.roster.map((p) => {
				const shown = rosterVisible || revealed.has(p.pid);
				const justNamed = revealed.has(p.pid) && namingRound;
				const showStats =
					!namingRound || phase === "hint" || revealed.has(p.pid);
				const result =
					leaderIndex === undefined ? undefined : leaderResult[leaderIndex];
				const isAnswer =
					leaderIndex !== undefined &&
					result !== undefined &&
					round.leaders[LEADER_STATS[leaderIndex]![0]] === p.pid;
				const isPickedWrong =
					result !== undefined && result.pickedPid === p.pid && !result.correct;
				const clickable = inLeaderPhase && !leaderAnswered;
				const card = cards[p.pid];
				return (
					<button
						key={p.pid}
						type="button"
						// Once a name is showing, the card opens that player's page. In
						// the stat-leader round it picks instead, which takes priority -
						// that's the question being asked right now.
						disabled={!clickable && !shown}
						className={`trivia-roster-card ${justNamed ? "is-named trivia-pop" : ""} ${
							isAnswer ? "is-answer" : ""
						} ${isPickedWrong ? "is-wrong trivia-shake" : ""} ${
							clickable || shown ? "is-clickable" : ""
						}`}
						onClick={
							clickable && leaderIndex !== undefined
								? () => handleLeaderPick(leaderIndex, p.pid)
								: shown
									? () => setProfilePid(p.pid)
									: undefined
						}
					>
						<span className="trivia-roster-pos">{p.pos}</span>
						{p.jerseyNumber !== undefined ? (
							<span className="trivia-roster-jersey">
								<JerseyNumber
									number={p.jerseyNumber}
									start={round.season}
									end={round.season}
									t={{
										colors: round.team.colors ?? DEFAULT_TEAM_COLORS,
										name: round.team.label,
										region: "",
									}}
								/>
							</span>
						) : null}
						<span className="trivia-roster-face">
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
							className={`trivia-roster-name ${shown ? "" : "is-hidden"}`}
							aria-label={shown ? undefined : "Not named yet"}
						>
							{shown ? p.name : redact(p.name)}
						</span>
						{showStats ? (
							<span className="trivia-roster-stats">
								{p.gp} GP · {p.ppg}/{p.rpg}/{p.apg}
							</span>
						) : null}
					</button>
				);
			})}
		</div>
	);

	return (
		<>
			{/* Toolbar: what you're playing, and how to change it */}
			<div className="trivia-toolbar mb-3">
				<button
					className="btn btn-sm btn-light-bordered"
					onClick={() => {
						void ensureCatalog();
						setShowSettings(true);
					}}
				>
					Settings
				</button>
				<button
					className="btn btn-sm btn-light-bordered"
					disabled={loadingNew}
					title="Random team-season"
					onClick={() => newRound()}
				>
					Shuffle
				</button>
				<select
					className="form-select form-select-sm w-auto"
					title="Season"
					disabled={loadingNew || seasonOptions.length === 0}
					value={String(round.season)}
					onChange={(event) => {
						void newRound({
							season: Number(event.target.value),
							tid: round.team.tid,
						});
					}}
				>
					{seasonOptions.includes(round.season) ? null : (
						<option value={String(round.season)}>{round.season}</option>
					)}
					{seasonOptions.map((s) => (
						<option key={s} value={s}>
							{s}
						</option>
					))}
				</select>
				<TeamLogoInline
					imgURL={t?.imgURL}
					imgURLSmall={t?.imgURLSmall}
					size={28}
					includePlaceholderIfNoLogo
				/>
				<select
					className="form-select form-select-sm w-auto"
					title="Team"
					disabled={loadingNew || teamOptionsForSeason.length === 0}
					value={String(round.team.tid)}
					onChange={(event) => {
						void newRound({
							season: round.season,
							tid: Number(event.target.value),
						});
					}}
				>
					{teamOptionsForSeason.some((o) => o.tid === round.team.tid) ? null : (
						<option value={String(round.team.tid)}>{round.team.label}</option>
					)}
					{teamOptionsForSeason.map((o) => (
						<option key={o.tid} value={o.tid}>
							{o.label}
						</option>
					))}
				</select>
				<button
					className="btn btn-sm btn-light-bordered"
					onClick={() => setShowHistory(true)}
				>
					History
				</button>
				<div className="trivia-toolbar-score ms-auto position-relative">
					Score: <span className="fw-bold">{score}</span>
					{lastGain ? (
						<span
							key={lastGain.key}
							className="badge text-bg-success position-absolute top-0 start-100 translate-middle trivia-rise"
						>
							+{lastGain.amount}
						</span>
					) : null}
				</div>
			</div>

			{/* Hero header */}
			<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
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
				</div>
				{namingRound ? (
					<button
						className="btn btn-sm btn-light-bordered ms-auto"
						onClick={() => {
							setPhase({ leader: 0 });
						}}
						title="Reveal the roster and move on"
					>
						Give up
					</button>
				) : null}
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

			{namingRound ? (
				<div className="d-flex align-items-center gap-2 mb-3">
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
					{phase === "guess" ? (
						<button
							className="btn btn-sm btn-light-bordered"
							onClick={() => setPhase("hint")}
						>
							Show hints
						</button>
					) : null}
					<button
						className="btn btn-sm btn-primary"
						onClick={() => setPhase({ leader: 0 })}
					>
						Continue
					</button>
				</div>
			) : null}

			{inLeaderPhase && leaderIndex !== undefined
				? (() => {
						const [statKey, statName, perGameKey, perGameLabel] =
							LEADER_STATS[leaderIndex]!;
						const result = leaderResult[leaderIndex];
						const leaderPid = round.leaders[statKey];
						const leader = round.roster.find((p) => p.pid === leaderPid);
						return (
							<div className="mb-3">
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
									Who led the team in {statName}? Tap a card. (10 pts)
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
							</div>
						);
					})()
				: null}

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
							<WinsReveal actual={round.wins.actual} correct={winsResult} />{" "}
							(you said {winsGuess}).
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
							<div className={`trivia-grade ${grade.color}`}>
								{grade.letter}
							</div>
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
								{summary.played > 1 ? (
									<div className="text-body-secondary small mt-1">
										{summary.played} games played · best {summary.best} ·
										average {summary.average}
									</div>
								) : null}
							</div>
							<button
								className="btn btn-primary"
								disabled={loadingNew}
								onClick={() => newRound()}
							>
								{loadingNew ? "Loading…" : "Play again"}
							</button>
						</div>
					</div>
				</>
			) : null}

			{cardGrid}

			{/* The guess bar rides the bottom of the screen through the naming
			    round, so it stays in reach however far down the grid you are. */}
			{namingRound ? (
				<div className="trivia-guess-bar">
					{miss !== undefined ? (
						<div
							key={missKey}
							className="text-danger small fw-bold mb-1 trivia-shake"
						>
							✗ {miss} — not on this team.
						</div>
					) : null}
					<TriviaPlayerSelect
						players={round.searchList.filter((p) => !revealed.has(p.pid))}
						onSelect={handleNameGuess}
						resultsAbove
						submitTitle="Guess"
						placeholder={`Guess a player correctly for ${phase === "guess" ? 15 : 10} points…`}
					/>
				</div>
			) : null}

			<SettingsModal
				show={showSettings}
				onHide={() => setShowSettings(false)}
				catalog={catalog}
				settings={settings}
				onChange={saveSettings}
			/>

			<TriviaHistoryModal
				game="team"
				show={showHistory}
				onHide={() => setShowHistory(false)}
				onReplay={(r: TriviaReplay) => {
					if (r.kind === "team") {
						void newRound({ season: r.season, tid: r.tid });
					}
				}}
			/>

			<TriviaPlayerModal
				pid={profilePid}
				onHide={() => setProfilePid(undefined)}
			/>
		</>
	);
};

export default TriviaTeam;
