import clsx from "clsx";
import {
	Component,
	type ChangeEvent,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
	type ReactNode,
	memo,
	type MutableRefObject,
} from "react";
import { TeamLogoInline } from "../../components/TeamLogoInline.tsx";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import { helpers } from "../../util/helpers.ts";
import { toWorker } from "../../util/toWorker.ts";
import { useLocal } from "../../util/local.ts";
import type { View } from "../../../common/types.ts";
import { bySport, isSport } from "../../../common/sportFunctions.ts";
import useLocalStorageState from "use-local-storage-state";
import { DEFAULT_SPORT_STATE as DEFAULT_SPORT_STATE_BASEBALL } from "../../util/processLiveGameEvents.baseball.tsx";
import { DEFAULT_SPORT_STATE as DEFAULT_SPORT_STATE_FOOTBALL } from "../../util/processLiveGameEvents.football.tsx";
import { processLiveGameEvents } from "../../util/processLiveGameEvents.ts";
import {
	BoxScoreWrapper,
	HeadlineScoreLive,
} from "../../components/BoxScoreWrapper.tsx";
import { useIsStuck } from "../../hooks/useIsStuck.ts";
import { useBlocker } from "../../hooks/useBlocker.ts";
import {
	PlayPauseNext,
	type FastForward,
} from "../../components/PlayPauseNext.tsx";
import { Confetti } from "./Confetti.tsx";
import { BoxScoreRow } from "../../components/BoxScoreRow.tsx";
import { getPeriodName } from "../../../common/getPeriodName.ts";
import LiveCourt, {
	courtActionFromEventType,
	rimXFor,
	scorerTableRow,
	synthHeaveSpot,
	synthPlaySpot,
	synthReboundSpot,
	synthShotSpot,
	zoneLabel,
	type CourtActor,
	type CourtDot,
	type CourtScene,
	type CourtZone,
} from "./LiveCourt.tsx";

type PlayerRowProps = {
	exhibition?: boolean;
	forceUpdate?: boolean;
	i: number;
	liveGameInProgress?: boolean;
	p: any;
	season: number;
};

class PlayerRow extends Component<PlayerRowProps> {
	prevInGame: boolean | undefined;

	// Can't just switch to hooks and React.memo because p is mutated, so there is no way to access the previous value of inGame in the memo callback function
	override shouldComponentUpdate(nextProps: PlayerRowProps) {
		return bySport({
			baseball: true,
			basketball: !!(
				this.prevInGame ||
				nextProps.p.inGame ||
				nextProps.forceUpdate
			),
			football: true,
			hockey: !!(
				this.prevInGame ||
				nextProps.p.inGame ||
				nextProps.p.inPenaltyBox ||
				nextProps.forceUpdate
			),
		});
	}

	override render() {
		const { p, ...props } = this.props;

		// Needed for shouldComponentUpdate because state is mutated so we need to explicitly store the last value
		this.prevInGame = p.inGame;

		const classes = bySport({
			baseball: undefined,
			basketball: clsx({
				"table-warning": p.inGame,
			}),
			football: undefined,
			hockey: clsx({
				"table-warning": p.inGame,
				"table-danger": p.inPenaltyBox,
			}),
		});

		return <BoxScoreRow className={classes} p={p} {...props} />;
	}
}

const onLiveSimOver = () => {
	// Send to worker, rather than doing `localActions.update({ liveGameInProgress: false });`, so it works in all tabs
	toWorker("main", "onLiveSimOver", undefined);
};

const getSeconds = (time: string | undefined) => {
	if (!time) {
		return 0;
	}

	const parts = time.split(":").map((x) => Number.parseInt(x));
	if (parts.length === 0) {
		return 0;
	}
	if (parts.length === 1) {
		// Seconds only being displayed
		return Number.parseFloat(time);
	}
	const [min, sec] = parts as [number, number];
	return min * 60 + sec;
};

const DEFAULT_SPORT_STATE = bySport<any>({
	baseball: DEFAULT_SPORT_STATE_BASEBALL,
	basketball: undefined,
	football: DEFAULT_SPORT_STATE_FOOTBALL,
	hockey: undefined,
});

type PlayByPlayEntryInfo = {
	key: number;
	score: ReactNode | undefined;
	scoreDiff: number;
	scoreType: string | undefined;
	outs: number | undefined;
	t: 0 | 1 | undefined;
	text: ReactNode;
	textOnly: boolean;
	time: string;
};

const PlayByPlayEntry = memo(
	({ boxScore, entry }: { boxScore: any; entry: PlayByPlayEntryInfo }) => {
		let scoreBlock = null;
		if (entry.score) {
			if (isSport("basketball")) {
				scoreBlock = entry.score;
			} else {
				scoreBlock = (
					<>
						<span
							className={`fw-bold ${
								entry.scoreDiff >= 0 &&
								(!isSport("football") || entry.scoreType !== "Safety")
									? "text-success"
									: "text-danger"
							}`}
						>
							{bySport({
								baseball: boxScore.shootout
									? "Home run!"
									: `${entry.scoreDiff} ${helpers.plural(
											"run scores",
											entry.scoreDiff,
											"runs score",
										)}!`,
								basketball: "",
								football: boxScore.shootout
									? "It's good!"
									: `${entry.scoreType ?? "???"}!`,
								hockey: "Goal!",
							})}
						</span>{" "}
						{entry.score}
					</>
				);
			}
		}

		return (
			<div className="d-flex">
				{entry.t !== undefined ? (
					<TeamLogoInline
						alt={boxScore.teams[entry.t].abbrev}
						className={clsx("flex-shrink-0", {
							// If there is a time line, then add some margin to the top, looks better.
							// If it's just score and no time, then that's football, and no margin looks more consistent. So don't check score here.
							"mt-1": !entry.textOnly && entry.time,
						})}
						imgURL={boxScore.teams[entry.t].imgURL}
						imgURLSmall={boxScore.teams[entry.t].imgURLSmall}
						includePlaceholderIfNoLogo
					/>
				) : null}
				<div
					className={clsx(
						"flex-grow-1 align-self-center me-2",
						entry.textOnly ? "fw-bold" : undefined,
						entry.t !== undefined ? "ms-2" : undefined,
					)}
				>
					{!entry.textOnly ? (
						<div className="d-flex">
							{entry.time ? (
								<div className="text-body-secondary me-auto">{entry.time}</div>
							) : null}
							{isSport("basketball") ? scoreBlock : null}
						</div>
					) : null}
					{isSport("hockey") ? scoreBlock : null}
					{entry.text}
					{!isSport("basketball") && !isSport("hockey") ? (
						<div>{scoreBlock}</div>
					) : null}
					{entry.outs !== undefined ? (
						<div className="fw-bold text-danger">
							{entry.outs} {helpers.plural("out", entry.outs)}
						</div>
					) : null}
				</div>
			</div>
		);
	},
	() => true,
);

const PlayByPlay = ({
	boxScore,
	entries,
	playByPlayDivRef,
}: {
	boxScore: any;
	entries: PlayByPlayEntryInfo[];
	playByPlayDivRef: MutableRefObject<HTMLDivElement | null>;
}) => {
	useEffect(() => {
		const setPlayByPlayDivHeight = () => {
			if (playByPlayDivRef.current) {
				// Keep in sync with .live-game-affix
				if (window.matchMedia("(min-width:768px)").matches) {
					playByPlayDivRef.current.style.height = `${
						window.innerHeight - 113
					}px`;
				} else if (playByPlayDivRef.current.style.height !== "") {
					playByPlayDivRef.current.style.removeProperty("height");
				}
			}
		};

		// Keep height of plays list equal to window
		setPlayByPlayDivHeight();
		window.addEventListener("optimizedResize", setPlayByPlayDivHeight);

		return () => {
			window.removeEventListener("optimizedResize", setPlayByPlayDivHeight);
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, []);

	return (
		<div
			className="live-game-playbyplay d-flex flex-column gap-3"
			ref={playByPlayDivRef}
			style={{
				scrollMarginTop: 174,
			}}
		>
			{entries.map((entry) => (
				<PlayByPlayEntry key={entry.key} boxScore={boxScore} entry={entry} />
			))}
		</div>
	);
};

const DEFAULT_SPEED = 7;

const speedToMs = (speed: number) => {
	return 4000 / 1.2 ** speed;
};

const getNavigateWarning = (
	exhibition: boolean | undefined,
	replay: boolean | undefined,
) => {
	if (replay) {
		// A saved replay can always be re-watched, so there's nothing to lose by
		// navigating away.
		return "";
	}
	return exhibition
		? "If you navigate away from this page, you won't be able to see this box score again."
		: "If you navigate away from this page, you won't be able to see these play-by-play results again. The results of this game are already final, though.";
};

export const LiveGame = (props: View<"liveGame">) => {
	const [paused, setPaused] = useState(false);
	const pausedRef = useRef(paused);
	const [speed, setSpeed] = useLocalStorageState("live-game-speed", {
		defaultValue: String(DEFAULT_SPEED),
	});
	const speedRef = useRef(Number.parseInt(speed));
	const [playIndex, setPlayIndex] = useState(-1);
	const [started, setStarted] = useState(false);
	const [confetti, setConfetti] = useState<{
		colors?: [string, string, string];
		display: boolean;
	}>({
		display: false,
	});

	const boxScore = useRef<any>(
		props.initialBoxScore ? props.initialBoxScore : {},
	);

	const overtimes = useRef(0);
	const playByPlayDiv = useRef<HTMLDivElement | null>(null);
	const quarters = useRef([]);
	const possessionChange = useRef<boolean | undefined>(undefined);
	const componentIsMounted = useRef(false);
	const events = useRef<any[] | undefined>(undefined);
	const sportState = useRef(
		DEFAULT_SPORT_STATE ? { ...DEFAULT_SPORT_STATE } : undefined,
	);

	const playByPlayEntries = useRef<PlayByPlayEntryInfo[]>([]);

	// Live court graphic (basketball): the scene currently playing on the
	// court, the accumulated shot-chart dots, and the in-flight attempt (a
	// block event only names the blocker, so the shooter/spot are remembered
	// from the attempt so the players don't teleport between the two).
	const courtScene = useRef<CourtScene | undefined>(undefined);
	const courtDots = useRef<CourtDot[]>([]);
	const courtSceneCount = useRef(0);
	const lastFga = useRef<
		| {
				pid: number;
				zone: CourtZone;
				t: 0 | 1;
				spot: { x: number; y: number };
		  }
		| undefined
	>(undefined);

	const playerByPid = (pid: number): any => {
		for (const t of boxScore.current.teams ?? []) {
			for (const p of t.players ?? []) {
				if (p.pid === pid) {
					return p;
				}
			}
		}
		return undefined;
	};

	const playerNameByPid = (pid: number): string =>
		playerByPid(pid)?.name ?? "???";

	const pushScene = (scene: Omit<CourtScene, "key">) => {
		courtSceneCount.current += 1;
		courtScene.current = { key: courtSceneCount.current, ...scene };
	};

	const scoreLine = (): string => {
		const [a, h] = boxScore.current.teams ?? [];
		return `${a?.abbrev ?? ""} ${a?.pts ?? 0}–${h?.pts ?? 0} ${h?.abbrev ?? ""}`;
	};

	// Turn the play-by-play event behind the current line into a court scene:
	// who's involved, where they stand, what the ball does, and the play text
	// right there on the floor. Locations are synthesized (the sim has no real
	// coordinates), but every actor/outcome is the genuine play.
	const handleCourtEvent = (event: any, text: ReactNode, scoreDiff = 0) => {
		if (
			!event ||
			typeof event.type !== "string" ||
			typeof event.t !== "number"
		) {
			return;
		}
		const rawT: 0 | 1 = event.t === 0 ? 0 : 1;
		// Box score display order swaps the raw team index.
		const displayT: 0 | 1 = rawT === 0 ? 1 : 0;

		// A three put up with the clock nearly expired is a last-second heave -
		// shown from way out (around half court) rather than a normal shot spot.
		const isHeaveNow = () => getSeconds(boxScore.current.time) <= 1.5;

		// A scored basket shows the running score under the play text, flanked by
		// both team logos: [away] 102 - 99 [home].
		let scoreNode: ReactNode | undefined;
		if (scoreDiff > 0 && Array.isArray(boxScore.current.teams)) {
			const a = boxScore.current.teams[0];
			const h = boxScore.current.teams[1];
			scoreNode = (
				<div
					style={{
						display: "flex",
						alignItems: "center",
						justifyContent: "center",
						gap: 5,
					}}
				>
					<TeamLogoInline
						imgURL={a?.imgURL}
						imgURLSmall={a?.imgURLSmall}
						includePlaceholderIfNoLogo
						size={16}
					/>
					<span style={{ fontWeight: 700 }}>
						{a?.pts ?? 0} - {h?.pts ?? 0}
					</span>
					<TeamLogoInline
						imgURL={h?.imgURL}
						imgURLSmall={h?.imgURLSmall}
						includePlaceholderIfNoLogo
						size={16}
					/>
				</div>
			);
		}

		const action = courtActionFromEventType(event.type);
		if (action) {
			if (action.kind === "attempt") {
				if (typeof event.pid !== "number") {
					return;
				}
				const spot =
					action.zone === "three" && isHeaveNow()
						? synthHeaveSpot(displayT)
						: synthShotSpot(displayT, action.zone);
				lastFga.current = {
					pid: event.pid,
					zone: action.zone,
					t: displayT,
					spot,
				};
				// Just the shooter on an attempt - a defender only appears if the
				// shot is actually blocked (handled on the result below).
				pushScene({
					kind: "attempt",
					t: displayT,
					actors: [
						{
							pid: event.pid,
							name: playerNameByPid(event.pid),
							x: spot.x,
							y: spot.y,
							role: "main",
						},
					],
					text,
				});
				return;
			}

			// Result. A block event's t/pid are the BLOCKER's; the shooter comes
			// from the attempt we remembered.
			const shooterT: 0 | 1 = action.blocked ? rawT : displayT;
			const shooterPid = action.blocked
				? (lastFga.current?.pid ?? undefined)
				: event.pid;
			if (typeof shooterPid !== "number") {
				return;
			}
			const reuse =
				lastFga.current && lastFga.current.pid === shooterPid
					? lastFga.current
					: undefined;
			// Free throws are ALWAYS taken from the line - keyed off THIS event's
			// zone, never the reused field-goal zone (otherwise a made basket +
			// and-one FT reused the drive's spot and the FT drifted off the line).
			const isFt = action.zone === "ft";
			const zone: CourtZone = isFt ? "ft" : (reuse?.zone ?? action.zone);
			// Field goals reuse the attempt spot so the shooter doesn't teleport
			// between attempt and result; free throws snap to the line.
			const spot = isFt
				? synthShotSpot(shooterT, "ft")
				: (reuse?.spot ??
					(zone === "three" && isHeaveNow()
						? synthHeaveSpot(shooterT)
						: synthShotSpot(shooterT, zone)));

			const actors: CourtActor[] = [
				{
					pid: shooterPid,
					name: playerNameByPid(shooterPid),
					x: spot.x,
					y: spot.y,
					role: "main",
				},
			];
			if (action.blocked && typeof event.pid === "number") {
				// The blocker rises up next to the shooter, toward the rim.
				const toward = rimXFor(shooterT) > spot.x ? 1 : -1;
				actors.push({
					pid: event.pid,
					name: playerNameByPid(event.pid),
					x: spot.x + toward * 3,
					y: spot.y,
					role: "defender",
				});
			}

			pushScene({
				kind: action.blocked ? "block" : action.made ? "make" : "miss",
				t: shooterT,
				actors,
				text,
				score: scoreNode,
				ballFrom: spot,
				rimX: rimXFor(shooterT),
			});

			// Field goals leave a shot-chart dot; free throws would just bury the
			// chart in identical dots at the line.
			if (zone !== "ft") {
				courtDots.current.push({
					key: courtSceneCount.current,
					x: spot.x,
					y: spot.y,
					made: action.made,
					t: shooterT,
					title: `${playerNameByPid(shooterPid)} — ${
						action.blocked ? "blocked" : action.made ? "made" : "missed"
					} ${zoneLabel(zone)} · ${boxScore.current.quarterShort ?? ""} ${
						boxScore.current.time ?? ""
					} · ${scoreLine()}`,
				});
			}
			return;
		}

		// Non-shot plays.
		const type = event.type as string;
		if (type === "tov" && typeof event.pid === "number") {
			const spot = synthPlaySpot(displayT);
			pushScene({
				kind: "tov",
				t: displayT,
				actors: [
					{
						pid: event.pid,
						name: playerNameByPid(event.pid),
						x: spot.x,
						y: spot.y,
						role: "main",
					},
				],
				text,
			});
		} else if (
			type === "stl" &&
			typeof event.pid === "number" &&
			typeof event.pidTov === "number"
		) {
			// The victim is on the OTHER team; the play happens in their frontcourt.
			const victimT: 0 | 1 = displayT === 0 ? 1 : 0;
			const spot = synthPlaySpot(victimT);
			pushScene({
				kind: "stl",
				t: displayT,
				actors: [
					{
						pid: event.pid,
						name: playerNameByPid(event.pid),
						x: spot.x,
						y: spot.y,
						role: "main",
					},
					{
						pid: event.pidTov,
						name: playerNameByPid(event.pidTov),
						x: spot.x + (rimXFor(victimT) > spot.x ? -4 : 4),
						y: spot.y,
						role: "victim",
					},
				],
				text,
			});
		} else if (
			(type === "orb" || type === "drb") &&
			typeof event.pid === "number"
		) {
			// Rebounds happen at the rim that was just shot at: the rebounder's own
			// attacking rim for an offensive board, the opposite for a defensive one.
			const rimT: 0 | 1 = type === "orb" ? displayT : displayT === 0 ? 1 : 0;
			const spot = synthReboundSpot(rimT);
			pushScene({
				kind: "reb",
				t: displayT,
				actors: [
					{
						pid: event.pid,
						name: playerNameByPid(event.pid),
						x: spot.x,
						y: spot.y,
						role: "main",
					},
				],
				text,
				// The ball comes off the rim into the rebounder's hands.
				ballFrom: { x: rimXFor(rimT), y: 25 },
				ballTo: spot,
			});
		} else if (type === "sub" && Array.isArray(event.pids)) {
			// Subs check in at the scorer's table: a single cluster at center court
			// along the near sideline, ins and outs side by side.
			const inPids = (event.pids as number[]).slice(0, 3);
			const outPids = ((event.pidsOff as number[]) ?? []).slice(0, 3);
			const all: { pid: number; role: "in" | "out" }[] = [
				...inPids.map((pid) => ({ pid, role: "in" as const })),
				...outPids.map((pid) => ({ pid, role: "out" as const })),
			];
			const spots = scorerTableRow(all.length);
			const actors: CourtActor[] = all.map((a, i) => ({
				pid: a.pid,
				name: playerNameByPid(a.pid),
				x: spots[i]!.x,
				y: spots[i]!.y,
				role: a.role,
			}));
			if (actors.length > 0) {
				pushScene({ kind: "sub", t: displayT, actors, text });
			}
		} else if (type.startsWith("pf") && typeof event.pid === "number") {
			// A foul: the fouler swipes at the fouled player, who gets rocked. The
			// victim (pidShooting) is known for shooting fouls; otherwise use the
			// last shooter as a stand-in so there's still someone to swipe at.
			const offT: 0 | 1 = displayT === 0 ? 1 : 0;
			const spot = lastFga.current?.spot ?? synthPlaySpot(offT);
			const victimPid =
				typeof event.pidShooting === "number"
					? event.pidShooting
					: lastFga.current?.pid;
			const toward = rimXFor(offT) > spot.x ? -1 : 1;
			const actors: CourtActor[] = [];
			if (typeof victimPid === "number") {
				actors.push({
					pid: victimPid,
					name: playerNameByPid(victimPid),
					x: spot.x,
					y: spot.y,
					role: "victim",
				});
			}
			actors.push({
				pid: event.pid,
				name: playerNameByPid(event.pid),
				x: spot.x + toward * 3.5,
				y: spot.y,
				role: "main",
			});
			pushScene({ kind: "foul", t: displayT, actors, text });
		} else if (type === "jumpBall" && typeof event.pid === "number") {
			// Opening tip: both jumpers rise at center court. event.pid is the
			// winner (on displayT); event.pid2 the loser. The winner taps the ball
			// back behind them - away from the rim they attack.
			const winnerT = displayT;
			const cx = (rimXFor(0) + rimXFor(1)) / 2;
			const attackDir = rimXFor(winnerT) > cx ? 1 : -1;
			const actors: CourtActor[] = [
				{
					pid: event.pid,
					name: playerNameByPid(event.pid),
					x: cx + attackDir * 2.5,
					y: 25,
					role: "main",
				},
			];
			if (typeof event.pid2 === "number") {
				actors.push({
					pid: event.pid2,
					name: playerNameByPid(event.pid2),
					x: cx - attackDir * 2.5,
					y: 25,
					role: "defender",
				});
			}
			pushScene({
				kind: "jump",
				t: winnerT,
				actors,
				text,
				ballFrom: { x: cx, y: 24 },
				ballTo: { x: cx - attackDir * 16, y: 25 },
			});
		} else if (type === "injury" && typeof event.pid === "number") {
			const spot = synthPlaySpot(displayT);
			pushScene({
				kind: "other",
				t: displayT,
				actors: [
					{
						pid: event.pid,
						name: playerNameByPid(event.pid),
						x: spot.x,
						y: spot.y,
						role: "main",
					},
				],
				text,
			});
		}
	};

	// Multiplayer live-sim broadcast. On a follower, playback is driven ENTIRELY by
	// the simmer's cursor (no own timer) and the page is locked; on the broadcaster
	// we additionally heartbeat our cursor to the room. In single-player both are
	// false and nothing below changes.
	const { mpLiveBroadcast } = useLocal(["mpLiveBroadcast"]);
	const isFollower =
		!!mpLiveBroadcast?.active && !mpLiveBroadcast.isBroadcaster;
	const isBroadcaster =
		!!mpLiveBroadcast?.active && mpLiveBroadcast.isBroadcaster;
	const followerRef = useRef(isFollower);
	followerRef.current = isFollower;
	// Number of events we started with, so cursor = initial - remaining tells us
	// how far the simmer (or we) have played.
	const initialEventCount = useRef(0);

	const isReplay = !!boxScore.current.replay;
	const navigateWarning = getNavigateWarning(
		boxScore.current.exhibition,
		isReplay,
	);

	const { setDirty } = useBlocker({
		message: navigateWarning,
		// A saved replay can be re-watched anytime, so don't block navigation.
		initialDirty: !isReplay,
		// A follower is locked in until the simmer ends the broadcast.
		hardBlock: isFollower,
	});

	// Make sure to call setPlayIndex after calling this! Can't be done inside because React is not always smart enough to batch renders
	const processToNextPause = useCallback(
		(force?: boolean): number => {
			if (
				!componentIsMounted.current ||
				(pausedRef.current && !force) ||
				!events.current
			) {
				return 0;
			}

			const startSeconds = getSeconds(boxScore.current.time);

			const shootout = !!boxScore.current.shootout;
			const ptsKey = shootout ? "sPts" : "pts";
			if (!Array.isArray(boxScore.current.teams)) {
				console.error("[live-game-debug] missing boxScore teams", {
					boxScore: boxScore.current,
					boxScoreKeys:
						boxScore.current && typeof boxScore.current === "object"
							? Object.keys(boxScore.current)
							: undefined,
					eventCount: events.current.length,
					nextEvent: events.current[0],
					playIndex,
					isFollower: followerRef.current,
					mpLiveBroadcast,
				});
				return 0;
			}

			// Save here since it is mutated in processLiveGameEvents
			const prevOuts = sportState.current?.outs;
			const prevPts =
				boxScore.current.teams[0][ptsKey] + boxScore.current.teams[1][ptsKey];

			const output = processLiveGameEvents({
				boxScore: boxScore.current,
				events: events.current,
				overtimes: overtimes.current,
				quarters: quarters.current,
				sportState: sportState.current,
			});
			const text = output.text;
			const currentPts =
				boxScore.current.teams[0][ptsKey] + boxScore.current.teams[1][ptsKey];
			const scoreDiff = currentPts - prevPts;

			if (isSport("basketball")) {
				handleCourtEvent((output as any).event, text, scoreDiff);
			}

			overtimes.current = output.overtimes;
			quarters.current = output.quarters;
			possessionChange.current = output.possessionChange;
			sportState.current = output.sportState;

			if (text !== undefined) {
				let outs;
				if (isSport("baseball") && output.sportState.outs > prevOuts) {
					outs = output.sportState.outs;
				}

				// For baseball, always show logo of the batting team, since t is not always sent in output (or maybe never sent)
				const t = isSport("baseball") ? sportState.current.o : output.t;

				let score;
				let scoreType;
				if (scoreDiff !== 0) {
					// Swap team for safety
					const scoreT =
						isSport("football") &&
						sportState.current.plays.at(-1)?.scoreInfo?.type === "SF"
							? t === 0
								? 1
								: 0
							: t;

					score =
						scoreT === 0 ? (
							<>
								<b>{boxScore.current.teams[0][ptsKey]}</b>-
								<span className="text-body-secondary">
									{boxScore.current.teams[1][ptsKey]}
								</span>
							</>
						) : scoreT === 1 ? (
							<>
								<span className="text-body-secondary">
									{boxScore.current.teams[0][ptsKey]}
								</span>
								-<b>{boxScore.current.teams[1][ptsKey]}</b>
							</>
						) : undefined;

					if (isSport("football")) {
						// If no score type, then it must be a penalty overturning a score
						scoreType =
							sportState.current.plays.at(-1)?.scoreInfo?.long ??
							"Penalty overturned score";
					}
				}

				let time;
				// Baseball has no time, football it's displayed with down/distance before play. In both cases, skip showing time for individual entries.
				if (
					bySport({
						baseball: false,
						basketball: true,
						football: false,
						hockey: true,
					})
				) {
					if (shootout && t !== undefined) {
						time = `Attempt ${boxScore.current.teams[t].sAtt}`;
					} else if (
						isSport("basketball") &&
						boxScore.current.elamTarget !== undefined
					) {
						time = `Target: ${boxScore.current.elamTarget}`;
					} else {
						time = boxScore.current.time;
					}
				}

				playByPlayEntries.current.unshift({
					key: playByPlayEntries.current.length,
					score,
					scoreDiff,
					scoreType,
					outs,
					text,
					textOnly: output.textOnly,
					t,
					time,
				});
			}

			if (events.current && events.current.length > 0) {
				// A follower never self-schedules: its playback is stepped only by the
				// simmer's cursor (see the follower effect below), so it can't run ahead.
				if (!pausedRef.current && !followerRef.current) {
					setTimeout(() => {
						processToNextPause();
						setPlayIndex((prev) => prev + 1);
					}, speedToMs(speedRef.current));
				}
			} else {
				boxScore.current.time = "0:00";
				boxScore.current.gameOver = true;
				boxScore.current.possession = undefined;

				// Update team records with result of game
				// Keep in sync with liveGame.ts
				if (!boxScore.current.exhibition) {
					for (const t of boxScore.current.teams) {
						if (boxScore.current.playoffs) {
							if (t.playoffs) {
								if (boxScore.current.won.tid === t.tid) {
									t.playoffs.won += 1;

									if (props.confetti) {
										setConfetti({
											display: true,
											colors: t.colors,
										});
									}
								} else if (boxScore.current.lost.tid === t.tid) {
									t.playoffs.lost += 1;
								}
							}
						} else {
							if (
								boxScore.current.won.pts === boxScore.current.lost.pts &&
								boxScore.current.won.sPts === boxScore.current.lost.sPts
							) {
								// Tied!
								if (t.tied !== undefined) {
									t.tied += 1;
								}
							} else if (boxScore.current.won.tid === t.tid) {
								t.won += 1;
							} else if (boxScore.current.lost.tid === t.tid) {
								if (boxScore.current.overtimes > 0 && props.otl) {
									t.otl += 1;
								} else {
									t.lost += 1;
								}
							}
						}
					}
				}

				// Clearing dirty is what "the game is over" means - it releases the
				// navigation block (the silent follower trap included), same as a
				// normal live sim. So a viewer is trapped only while the game is live.
				if (!boxScore.current.exhibition) {
					setDirty(false);
				}
				onLiveSimOver();
			}

			const endSeconds = getSeconds(boxScore.current.time);

			// This is negative when rolling over to a new quarter
			const elapsedSeconds = startSeconds - endSeconds;
			return elapsedSeconds;
		},
		[props.confetti, props.otl, setDirty],
	);

	useEffect(() => {
		componentIsMounted.current = true;

		return () => {
			componentIsMounted.current = false;
			onLiveSimOver();
		};
	}, []);

	const startLiveGame = useCallback(
		(events2: any[]) => {
			events.current = events2;
			setTimeout(() => {
				processToNextPause();
				setPlayIndex((prev) => prev + 1);
			}, speedToMs(DEFAULT_SPEED));
		},
		[processToNextPause],
	);

	useEffect(() => {
		if (props.events && !started) {
			boxScore.current = props.initialBoxScore;
			initialEventCount.current = props.events.length;
			setStarted(true);
			if (followerRef.current) {
				// Follower: load the events but DON'T start the local timer - the
				// cursor effect below steps playback to match the simmer.
				events.current = props.events.slice();
			} else {
				startLiveGame(props.events.slice());
			}
		}
	}, [props.events, props.initialBoxScore, started, startLiveGame]);

	// Follower lockstep: whenever the simmer's cursor advances, step our own
	// playback forward to the same position (fast-forwarding through any gap, e.g.
	// when we first join mid-game). Pure catch-up - it never runs ahead of the
	// simmer, so we always show exactly what they've shown.
	const followerCursor = isFollower ? (mpLiveBroadcast?.cursor ?? 0) : 0;
	useEffect(() => {
		if (!isFollower || !started || !events.current) {
			return;
		}
		let steps = 0;
		while (
			events.current.length > 0 &&
			initialEventCount.current - events.current.length < followerCursor
		) {
			processToNextPause(true);
			steps += 1;
		}
		if (steps > 0) {
			setPlayIndex((prev) => prev + steps);
		}
	}, [followerCursor, isFollower, started, processToNextPause]);

	// Broadcaster heartbeat: report our playback position to the room so followers
	// stay in lockstep, and end the broadcast when we leave the page. Writes only
	// on a real change, plus a slow keep-alive so the follower lease never lapses
	// while we sit paused / on the final box score.
	const lastBroadcastSent = useRef<
		| { cursor: number; paused: boolean; gameOver: boolean; at: number }
		| undefined
	>(undefined);
	useEffect(() => {
		if (!isBroadcaster || !started) {
			return;
		}
		const sendHeartbeat = () => {
			if (!events.current) {
				return;
			}
			const cursor = initialEventCount.current - events.current.length;
			const paused = pausedRef.current;
			const gameOver = !!boxScore.current.gameOver;
			const last = lastBroadcastSent.current;
			const now = Date.now();
			const changed =
				!last ||
				last.cursor !== cursor ||
				last.paused !== paused ||
				last.gameOver !== gameOver;
			// Re-stamp the lease at least every few seconds even when nothing moved.
			const stale = !last || now - last.at > 4000;
			if (!changed && !stale) {
				return;
			}
			lastBroadcastSent.current = { cursor, paused, gameOver, at: now };
			void toWorker("main", "updateLiveBroadcast", {
				cursor,
				paused,
				speed: speedRef.current,
				gameOver,
			});
		};

		const interval = setInterval(sendHeartbeat, 400);
		sendHeartbeat();

		return () => {
			clearInterval(interval);
			lastBroadcastSent.current = undefined;
			// Leaving the live game ends the broadcast, unlocking every follower.
			void toWorker("main", "endLiveBroadcast", undefined);
		};
	}, [isBroadcaster, started]);

	const handleSpeedChange = (event: ChangeEvent<HTMLInputElement>) => {
		const speed = event.target.value;
		setSpeed(speed);
		speedRef.current = Number.parseInt(speed);
	};

	const handlePause = useCallback(() => {
		setPaused(true);
		pausedRef.current = true;
	}, []);

	const handlePlay = useCallback(() => {
		setPaused(false);

		// Without pausedRef check, this was a race condition and could lead to incorrect post-game records (counting as 2 or more wins)
		if (pausedRef.current) {
			pausedRef.current = false;
			processToNextPause();
		}

		setPlayIndex((prev) => prev + 1);
	}, [processToNextPause]);

	const handleNextPlay = useCallback(() => {
		processToNextPause(true);
		setPlayIndex((prev) => prev + 1);
	}, [processToNextPause]);

	const fastForwardMenuItems = useMemo(() => {
		// Plays up to `cutoffs` seconds, or until end of quarter
		const playSeconds = (cutoff: number) => {
			let seconds = 0;
			let numPlays = 0;

			// Stop at shootout, unless we're already in a shootout
			const initialShootout = boxScore.current.shootout;

			while (
				seconds < cutoff &&
				!boxScore.current.gameOver &&
				(initialShootout || !boxScore.current.shootout)
			) {
				const elapsedSeconds = processToNextPause(true);
				numPlays += 1;
				if (elapsedSeconds > 0) {
					seconds += elapsedSeconds;
				} else if (elapsedSeconds < 0) {
					// End of quarter, always stop
					break;
				}
			}
			setPlayIndex((prev) => prev + numPlays);
		};

		const playUntilLastTwoMinutes = () => {
			// quarters.current.length can be 0 early in the game
			const initialQuarter = Math.max(1, quarters.current.length);

			const quartersToPlay =
				initialQuarter >= boxScore.current.numPeriods
					? 0
					: boxScore.current.numPeriods - initialQuarter;
			for (let i = 0; i < quartersToPlay; i++) {
				playSeconds(Infinity);
			}

			const currentSeconds = getSeconds(boxScore.current.time);
			const targetSeconds = 125; // 2 minutes plus 5 seconds, cause can't always be exact
			const secoundsToPlay = currentSeconds - targetSeconds;
			if (secoundsToPlay > 0) {
				playSeconds(secoundsToPlay);
			}
		};

		const playUntilElamEnding = () => {
			let numPlays = 0;
			while (
				boxScore.current.elamTarget === undefined &&
				!boxScore.current.gameOver
			) {
				processToNextPause(true);
				numPlays += 1;
			}
			setPlayIndex((prev) => prev + numPlays);
		};

		const playUntilNextScore = () => {
			const initialPts =
				boxScore.current.teams[0].pts + boxScore.current.teams[1].pts;
			let currentPts = initialPts;
			let numPlays = 0;
			while (
				initialPts === currentPts &&
				!boxScore.current.gameOver &&
				!boxScore.current.shootout
			) {
				processToNextPause(true);
				currentPts =
					boxScore.current.teams[0].pts + boxScore.current.teams[1].pts;
				numPlays += 1;
			}
			setPlayIndex((prev) => prev + numPlays);
		};

		const playUntilChangeOfPossession = () => {
			let numPlays = 0;

			// If currently on one, play through it
			if (possessionChange.current) {
				while (possessionChange.current && !boxScore.current.gameOver) {
					processToNextPause(true);
					numPlays += 1;
				}
			}

			// Find next one
			while (!possessionChange.current && !boxScore.current.gameOver) {
				processToNextPause(true);
				numPlays += 1;
			}

			setPlayIndex((prev) => prev + numPlays);
		};

		// elamTarget check is because clock is set to Infinity in Elam ending, so we can't skip ahead minutes
		let skipMinutes =
			isSport("baseball") ||
			boxScore.current.elamTarget !== undefined ||
			boxScore.current.shootout
				? []
				: [
						{
							minutes: 1,
							keyboardShortcut: "o",
						},
						{
							minutes: helpers.bound(
								Math.round(props.quarterLength / 4),
								1,
								Infinity,
							),
							keyboardShortcut: "t",
						},
						{
							minutes: helpers.bound(
								Math.round(props.quarterLength / 2),
								1,
								Infinity,
							),
							keyboardShortcut: "s",
						},
					];

		// Dedupe
		const skipMinutesValues = new Set();
		skipMinutes = skipMinutes.filter(({ minutes }) => {
			if (skipMinutesValues.has(minutes)) {
				return false;
			}

			skipMinutesValues.add(minutes);
			return true;
		});

		const getNumSidesSoFar = () =>
			boxScore.current.teams === undefined
				? 0
				: boxScore.current.teams[0].ptsQtrs.length +
					boxScore.current.teams[1].ptsQtrs.length;

		const menuItems: FastForward[] = [
			...skipMinutes.map(
				({ minutes, keyboardShortcut }) =>
					({
						label: `${minutes} ${helpers.plural("minute", minutes)}`,
						keyboardShortcut,
						onClick: () => {
							playSeconds(60 * minutes);
						},
					}) as FastForward,
			),
			...(isSport("baseball")
				? !boxScore.current.shootout
					? ([
							{
								label: "Next batter",
								keyboardShortcut: "o",
								onClick: () => {
									let numPlays = 0;

									const initialBatter = sportState.current?.batterPid;
									while (!boxScore.current.gameOver) {
										processToNextPause(true);
										numPlays += 1;

										const currentBatter = sportState.current?.batterPid;
										if (
											currentBatter !== undefined &&
											currentBatter >= 0 &&
											initialBatter !== currentBatter
										) {
											break;
										}
									}

									setPlayIndex((prev) => prev + numPlays);
								},
							},
							{
								label: "Next baserunner",
								keyboardShortcut: "t",
								onClick: () => {
									const sportStateBaseball =
										sportState.current as typeof DEFAULT_SPORT_STATE_BASEBALL;
									const initialBases = sportStateBaseball.bases ?? [];
									const initialBaserunners = new Set(
										initialBases.filter((pid) => pid !== undefined),
									);

									const initialHR =
										boxScore.current.teams[0].hr + boxScore.current.teams[1].hr;

									let numPlays = 0;

									while (!boxScore.current.gameOver) {
										processToNextPause(true);
										numPlays += 1;

										// Any new baserunner -> stop
										const baserunners = (sportStateBaseball.bases ?? []).filter(
											(pid) => pid !== undefined,
										);
										if (baserunners.length === 0) {
											// Handle case where it's a new inning and the same guy gets on base
											initialBaserunners.clear();
										}
										if (
											baserunners.some((pid) => !initialBaserunners.has(pid))
										) {
											break;
										}

										// Home run counts as new baserunner
										const currentHR =
											boxScore.current.teams[0].hr +
											boxScore.current.teams[1].hr;
										if (initialHR !== currentHR) {
											break;
										}
									}

									setPlayIndex((prev) => prev + numPlays);
								},
							},
							{
								label: "Side is retired",
								keyboardShortcut: "c",
								onClick: () => {
									let numPlays = 0;

									const numSidesSoFar = getNumSidesSoFar();
									while (
										!boxScore.current.gameOver &&
										!boxScore.current.shootout
									) {
										processToNextPause(true);
										numPlays += 1;

										if (numSidesSoFar !== getNumSidesSoFar()) {
											break;
										}
									}

									setPlayIndex((prev) => prev + numPlays);
								},
							},
							{
								label: "End of inning",
								keyboardShortcut: "q",
								onClick: () => {
									let numPlays = 0;

									const numSidesSoFar = getNumSidesSoFar();
									while (
										!boxScore.current.gameOver &&
										!boxScore.current.shootout
									) {
										processToNextPause(true);
										numPlays += 1;

										const newNum = getNumSidesSoFar();
										if (numSidesSoFar !== newNum && newNum % 2 === 1) {
											break;
										}
									}

									setPlayIndex((prev) => prev + numPlays);
								},
							},
							...(getNumSidesSoFar() <= (boxScore.current.numPeriods - 1) * 2
								? [
										{
											label: `${helpers.ordinal(boxScore.current.numPeriods)} inning`,
											keyboardShortcut: "u",
											onClick: () => {
												let numPlays = 0;

												while (
													getNumSidesSoFar() <=
														(boxScore.current.numPeriods - 1) * 2 &&
													!boxScore.current.gameOver
												) {
													processToNextPause(true);
													numPlays += 1;
												}

												setPlayIndex((prev) => prev + numPlays);
											},
										},
									]
								: []),
						] as FastForward[])
					: ([
							{
								label: "End of shootout",
								keyboardShortcut: "q",
								onClick: () => {
									playSeconds(Infinity);
								},
							},
						] as FastForward[])
				: ([
						{
							label: `End of ${
								boxScore.current.elamTarget !== undefined
									? "game"
									: boxScore.current.shootout
										? "shootout"
										: boxScore.current.overtime
											? "period"
											: getPeriodName(boxScore.current.numPeriods)
							}`,
							keyboardShortcut: "q",
							onClick: () => {
								playSeconds(Infinity);
							},
						},
					] as FastForward[])),
		];

		if (
			!boxScore.current.elam &&
			!boxScore.current.shootout &&
			!isSport("baseball")
		) {
			menuItems.push({
				label: "Last 2 minutes",
				keyboardShortcut: "u",
				onClick: () => {
					playUntilLastTwoMinutes();
				},
			});
		}

		if (
			bySport({
				baseball: false,
				basketball: false,
				football: true,
				hockey: false,
			}) &&
			!boxScore.current.shootout
		) {
			menuItems.push({
				label: "Change of possession",
				keyboardShortcut: "c",
				onClick: () => {
					playUntilChangeOfPossession();
				},
			});
		}

		if (
			bySport({
				baseball: true,
				basketball: false,
				football: true,
				hockey: true,
			}) &&
			!boxScore.current.shootout
		) {
			menuItems.push({
				label: `Next ${bySport({
					hockey: "goal",
					default: "score",
				})}`,
				keyboardShortcut: "g",
				onClick: () => {
					playUntilNextScore();
				},
			});
		}

		if (
			boxScore.current.elam &&
			!boxScore.current.elamOvertime &&
			boxScore.current.elamTarget === undefined
		) {
			menuItems.push({
				label: "Elam Ending",
				keyboardShortcut: "u",
				onClick: () => {
					playUntilElamEnding();
				},
			});
		}

		return menuItems;
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [
		boxScore.current.elam,
		boxScore.current.elamTarget,
		boxScore.current.overtime,
		boxScore.current.shootout,
		quarters.current.length,
		processToNextPause,
	]);

	const scrollTop = useRef<HTMLDivElement>(null);

	const [showWarning, setShowWarning] = useLocalStorageState(
		"showLiveSimWarning",
		{
			defaultValue: true,
		},
	);

	const [liveGameStickyDiv, setLiveGameStickyDiv] =
		useState<HTMLElement | null>(null);
	const isStuck = useIsStuck(liveGameStickyDiv);

	// Needs to return actual div, not fragment, for AutoAffix!!!
	return (
		<div>
			{confetti.display ? <Confetti colors={confetti.colors} /> : null}

			{isFollower || isBroadcaster ? null : showWarning ? ( // Broadcasting/watching needs no banner - the live game itself shows it.
				<p className="text-danger">
					{navigateWarning}
					<>
						{" "}
						<button
							className="btn btn-link p-0 border-0"
							onClick={() => {
								setShowWarning(false);
							}}
						>
							(Dismiss)
						</button>
					</>
				</p>
			) : null}

			<div
				className="row"
				ref={scrollTop}
				style={{
					scrollMarginTop: 174,
				}}
			>
				<div className="col-md-9">
					{boxScore.current.gid >= 0 ? (
						<div className="live-game-sticky mb-3" ref={setLiveGameStickyDiv}>
							<div className="pt-1 pt-md-0 live-game-score-wrapper">
								{boxScore.current.replay ? (
									<div className="text-center small text-body-secondary mb-1">
										<span className="badge text-bg-secondary">▶ Replay</span>{" "}
										{boxScore.current.replayLabel}
									</div>
								) : null}
								<HeadlineScoreLive
									boxScore={boxScore.current}
									isStuck={isStuck}
								/>
								{!isFollower ? (
									<div className="d-flex align-items-center d-md-none pt-2">
										<PlayPauseNext
											className="me-2"
											disabled={boxScore.current.gameOver}
											fastForwardAlignRight
											fastForwards={fastForwardMenuItems}
											onPlay={handlePlay}
											onPause={handlePause}
											onNext={handleNextPlay}
											paused={paused}
											titlePlay="Resume Simulation"
											titlePause="Pause Simulation"
											titleNext="Show Next Play"
											// Since we have two PlayPauseNexts rendered, ignore shortcuts on one
											ignoreKeyboardShortcuts
										/>
										<input
											type="range"
											className="form-range flex-grow-1"
											min="1"
											max="33"
											step="1"
											value={speed}
											onChange={handleSpeedChange}
											title="Speed"
										/>
									</div>
								) : null}
							</div>
							<div className="d-flex d-md-none">
								<div className="ms-auto btn-group">
									<button
										className="btn btn-light-bordered"
										onClick={() => {
											scrollTop.current?.scrollIntoView();
										}}
									>
										Top
									</button>
									{!isSport("football") ? (
										<>
											<button
												className="btn btn-light-bordered"
												onClick={() => {
													document
														.getElementById("scroll-team-1")
														?.scrollIntoView();
												}}
											>
												{boxScore.current.teams[0].abbrev}
											</button>
											<button
												className="btn btn-light-bordered"
												onClick={() => {
													document
														.getElementById("scroll-team-2")
														?.scrollIntoView();
												}}
											>
												{boxScore.current.teams[1].abbrev}
											</button>
										</>
									) : null}
									<button
										className="btn btn-light-bordered"
										onClick={() => {
											playByPlayDiv.current?.scrollIntoView();
										}}
									>
										Plays
									</button>
								</div>
							</div>
						</div>
					) : null}
					{boxScore.current.gid >= 0 ? (
						<>
							{isSport("basketball") ? (
								<LiveCourt
									scene={courtScene.current}
									dots={courtDots.current}
									teams={[
										boxScore.current.teams?.[0],
										boxScore.current.teams?.[1],
									]}
									finals={!!boxScore.current.finals}
									season={boxScore.current.season}
								/>
							) : null}
							<BoxScoreWrapper
								Row={PlayerRow}
								boxScore={boxScore.current}
								live
								playIndex={playIndex}
								sportState={sportState.current}
							/>
						</>
					) : (
						<h2>Loading...</h2>
					)}
				</div>
				<div className="col-md-3">
					<div className="live-game-affix">
						{!isFollower ? (
							<div className="d-none d-md-flex align-items-center mb-3 pt-md-2">
								<PlayPauseNext
									className="me-2"
									disabled={boxScore.current.gameOver}
									fastForwardAlignRight
									fastForwards={fastForwardMenuItems}
									onPlay={handlePlay}
									onPause={handlePause}
									onNext={handleNextPlay}
									paused={paused}
									titlePlay="Resume Simulation"
									titlePause="Pause Simulation"
									titleNext="Show Next Play"
								/>
								<input
									type="range"
									className="form-range flex-grow-1"
									min="1"
									max="33"
									step="1"
									value={speed}
									onChange={handleSpeedChange}
									title="Speed"
								/>
							</div>
						) : null}
						<PlayByPlay
							boxScore={boxScore.current}
							entries={playByPlayEntries.current}
							playByPlayDivRef={playByPlayDiv}
						/>
					</div>
				</div>
			</div>
		</div>
	);
};

const LiveGameWrapper = (props: View<"liveGame">) => {
	useTitleBar({ title: "Live Game Simulation", hideNewWindow: true });

	// When following a broadcast, remount LiveGame for each NEW live sim (so a
	// follower still parked on the previous game's final box score gets a fresh
	// replay). The counter only ever advances on a new broadcast and never reverts
	// when one ends, so ending a broadcast doesn't remount (and restart) the
	// finished game, and the broadcaster / single-player never remount at all.
	const { mpLiveBroadcast } = useLocal(["mpLiveBroadcast"]);
	const remountKey = useRef(0);
	const lastFollowedStartedAt = useRef<number | undefined>(undefined);
	if (
		mpLiveBroadcast?.active &&
		!mpLiveBroadcast.isBroadcaster &&
		mpLiveBroadcast.startedAt !== lastFollowedStartedAt.current
	) {
		lastFollowedStartedAt.current = mpLiveBroadcast.startedAt;
		remountKey.current += 1;
	}

	return <LiveGame key={remountKey.current} {...props} />;
};

export default LiveGameWrapper;
