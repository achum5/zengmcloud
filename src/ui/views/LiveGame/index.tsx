import clsx from "clsx";
import { clearCourtRng, seedCourtRng } from "./courtRng.ts";
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
import { realtimeUpdate } from "../../util/realtimeUpdate.ts";
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
	buildFreeThrowActors,
	buildLineupActors,
	courtActionFromEventType,
	rimXFor,
	scorerTableRow,
	setupBallPath,
	synthHeaveSpot,
	synthPlaySpot,
	synthReboundSpot,
	synthShotSpot,
	glideSeconds,
	type CourtActor,
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

	// A checkpoint per shown play - the events consumed so far (cursor), the
	// period, and the game clock - so the rewind controls can jump back to a point
	// (a minute ago, the start of the quarter) by re-simming to that cursor.
	const playHistory = useRef<{ cursor: number; q: string; clock: number }[]>(
		[],
	);

	// Live court graphic (basketball): the scene currently playing on the
	// court, and the in-flight attempt (a block event only names the blocker, so
	// the shooter/spot are remembered from the attempt so the players don't
	// teleport between the two).
	const courtScene = useRef<CourtScene | undefined>(undefined);
	const courtSceneCount = useRef(0);
	// Seed for the play currently being turned into scenes.
	const currentSceneSeed = useRef("");
	// Where every player last stood on the floor, so the ball can come up WITH
	// the ball-handler from his previous spot instead of beating him to the shot.
	const lastActorPos = useRef<Map<number, { x: number; y: number }>>(new Map());
	const lastFga = useRef<
		| {
				pid: number;
				zone: CourtZone;
				t: 0 | 1;
				spot: { x: number; y: number };
		  }
		| undefined
	>(undefined);
	// A live fast break: set the moment a team gains the ball off a steal or a
	// defensive rebound, so the shot that immediately follows is staged as a
	// transition break (streaking offense, recovering defense) instead of a
	// half-court set. `requireRim` gates it to a rim attempt for defensive boards
	// (a walk-it-up jumper off a rebound isn't a break); a steal fires on any
	// quick score. Cleared the instant the possession does anything but that shot.
	const breakContext = useRef<{ t: 0 | 1; requireRim: boolean } | undefined>(
		undefined,
	);
	// True on the tick that is showing a synthetic SETUP beat (see the injection
	// in processToNextPause), so the very next tick consumes the real event
	// instead of injecting a second setup.
	const injectedSetup = useRef(false);

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

	const pushScene = (
		scene: Omit<CourtScene, "key">,
		opts: {
			// Credit an assisted make: the assister is featured at his formation
			// spot and the ball passes from him to the shooter before the shot.
			assistPid?: number;
			// A DECOY pass with no real assist: a random on-floor teammate is
			// featured as the passer. Shown on some misses so a pass never reveals
			// whether the shot is going in. Ignored if assistPid is set.
			decoyAssist?: boolean;
			// A live shot at this spot: the nearest defender steps up to contest.
			contestSpot?: { x: number; y: number };
			// This possession is a fast break (off a steal / defensive board): the
			// offense streaks the floor and the defense is caught recovering.
			transition?: boolean;
			// A free throw: line the other nine along the paint (3 defenders + 2
			// offense on the lane, the rest behind the arc) instead of a normal set.
			// The value is the shooter's pid, excluded from the lane.
			freeThrowShooterPid?: number;
		} = {},
	) => {
		courtSceneCount.current += 1;
		const sceneKey = courtSceneCount.current;
		// Distinct per scene within a play (one event can push several beats), so
		// two beats of the same play don't share identical jitter.
		const sceneSeed = `${currentSceneSeed.current}|${sceneKey}`;
		// Populate the rest of the 5-on-5 as a background formation, so the court
		// shows a full team instead of just the play's actors. Which end the ball
		// is at anchors both teams' formations: prefer the shot's rim, else the
		// average x of the play's actors. Away attacks left, home right (fixed), so
		// a play on the right half means the home team is on offense. (Skipped for
		// the opening tip - a half-court set around a center-court jump reads
		// wrong.)
		// Never let one player appear twice in a scene: the court keys every body
		// by pid, so a duplicate (e.g. a foul whose stand-in victim is also the
		// fouler) collides as a React key and strands a face on the floor. Keep the
		// first occurrence (the more meaningful role comes first).
		const seenPids = new Set<number>();
		const playActors = scene.actors.filter((a) => {
			if (seenPids.has(a.pid)) {
				return false;
			}
			seenPids.add(a.pid);
			return true;
		});
		let passFrom = scene.passFrom;
		let anchorX = scene.rimX;
		if (anchorX === undefined && playActors.length > 0) {
			anchorX = playActors.reduce((sum, a) => sum + a.x, 0) / playActors.length;
		}
		let actors = playActors;
		if (
			scene.kind !== "jump" &&
			anchorX !== undefined &&
			Array.isArray(boxScore.current.teams)
		) {
			// Midcourt = halfway between the two rims (away's left, home's right).
			const midX = (rimXFor(0) + rimXFor(1)) / 2;
			const offenseT: 0 | 1 = anchorX > midX ? 1 : 0;
			const teams = boxScore.current.teams;
			const playPids = new Set(playActors.map((a) => a.pid));
			const teamLineups: [any[], any[]] = [
				teams[0]?.players ?? [],
				teams[1]?.players ?? [],
			];

			if (opts.freeThrowShooterPid !== undefined) {
				// Free throw: everyone lines the paint (see buildFreeThrowActors), the
				// shooter stays at the line. No contest, no assist/decoy pass.
				const ftActors = buildFreeThrowActors({
					teams: teamLineups,
					offenseT,
					shooterPid: opts.freeThrowShooterPid,
				}).filter((e) => !playPids.has(e.pid));
				if (ftActors.length > 0) {
					actors = [...playActors, ...ftActors];
				}
			} else {
				const lineup = buildLineupActors({
					teams: teamLineups,
					offenseT,
					transition: opts.transition,
				});

				const extras: CourtActor[] = [];
				for (const entry of lineup) {
					if (playPids.has(entry.pid)) {
						continue; // already featured at an action spot
					}
					if (opts.assistPid !== undefined && entry.pid === opts.assistPid) {
						// The assister: featured (name tag, full strength) at his spot; the
						// ball's pass leg starts here.
						extras.push({ ...entry, role: "assist" });
						passFrom = { x: entry.x, y: entry.y };
						continue;
					}
					extras.push(entry);
				}

				// A decoy pass (no real assist): promote a random on-floor OFFENSE
				// teammate to the passer, so a pass reads the same on a miss as it does
				// on an assisted make and never gives the outcome away. Picked from the
				// actual rendered lineup, so the passer's face always shows.
				if (opts.decoyAssist && passFrom === undefined) {
					const offCands = extras.filter(
						(e) => e.role === "onCourt" && e.t === offenseT,
					);
					if (offCands.length > 0) {
						const pick = offCands[Math.floor(Math.random() * offCands.length)]!;
						pick.role = "assist";
						passFrom = { x: pick.x, y: pick.y };
					}
				}

				// The nearest defender slides over to contest a live shot (skipped for
				// free throws, and for blocks - the blocker IS the contest).
				if (opts.contestSpot) {
					const defT: 0 | 1 = offenseT === 0 ? 1 : 0;
					let closest: CourtActor | undefined;
					let closestDist = Infinity;
					for (const e of extras) {
						if (e.role !== "onCourt" || e.t !== defT) {
							continue;
						}
						const d =
							(e.x - opts.contestSpot.x) ** 2 + (e.y - opts.contestSpot.y) ** 2;
						if (d < closestDist) {
							closestDist = d;
							closest = e;
						}
					}
					if (closest) {
						const toward = rimXFor(offenseT) > opts.contestSpot.x ? 1 : -1;
						closest.x = Math.min(
							90,
							Math.max(4, opts.contestSpot.x + toward * 2.3),
						);
						closest.y = Math.min(
							46,
							Math.max(
								4,
								opts.contestSpot.y +
									(closest.y >= opts.contestSpot.y ? 1.5 : -1.5),
							),
						);
					}
				}

				if (extras.length > 0) {
					actors = [...playActors, ...extras];
				}
			}
		}
		courtScene.current = {
			key: sceneKey,
			...scene,
			seed: sceneSeed,
			actors,
			passFrom,
		};
		// Remember where everyone ended up, so the next scene's ball can travel
		// with the handler from his real previous spot.
		for (const a of actors) {
			lastActorPos.current.set(a.pid, { x: a.x, y: a.y });
		}
	};

	// A textless SETUP beat, shown for one interval BEFORE the first shot of a new
	// possession: the offense (display team `offenseT`) brings the ball up and
	// settles into its set - a fast break if `transition`, otherwise a half-court
	// set - so the trip up the floor gets its own beat instead of the shot scene
	// teleporting everyone across. No actors are passed, so pushScene fills the
	// whole 5-on-5 as the background formation; the ball is animated up the floor
	// (kind "advance"). Because everyone lands in this set, the shot scene that
	// follows (built with the SAME transition flag) barely has to move them.
	const pushSetupScene = (offenseT: 0 | 1, transition: boolean) => {
		const { ballFrom, ballTo } = setupBallPath(offenseT, transition);
		pushScene(
			{
				kind: "advance",
				t: offenseT,
				actors: [],
				text: null,
				rimX: rimXFor(offenseT),
				ballFrom,
				ballTo,
			},
			{ transition },
		);
	};

	// The ball-handler's glide turned into ms (same speed-capped curve the players
	// use), so the ball's arrival lands exactly when he does - at whatever
	// playback speed the scene interval is running.
	const glideMs = (
		from: { x: number; y: number } | undefined,
		to: { x: number; y: number },
	): number => {
		const d = from ? Math.hypot(to.x - from.x, to.y - from.y) : 0;
		return glideSeconds(d, speedToMs(speedRef.current)) * 1000;
	};

	// Look ahead (without consuming) to this attempt's result, so we can decide -
	// BEFORE the shot - whether to show a pass and who threw it. Returns the shot
	// outcome and the real assister (if any).
	const peekShotResult = ():
		| { made: boolean; blocked: boolean; pidAst?: number }
		| undefined => {
		const evs = events.current;
		if (!evs) {
			return undefined;
		}
		for (let i = 0; i < evs.length && i < 10; i++) {
			const e = evs[i];
			if (!e || typeof e.type !== "string") {
				continue;
			}
			const a = courtActionFromEventType(e.type);
			if (!a) {
				continue;
			}
			if (a.kind === "attempt") {
				return undefined; // hit the next shot before any result - give up
			}
			return {
				made: !!a.made,
				blocked: !!a.blocked,
				pidAst: typeof e.pidAst === "number" ? e.pidAst : undefined,
			};
		}
		return undefined;
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
		// Seed this play's invented geometry from (game, play index). That index
		// is the SAME number the broadcaster publishes as its cursor and the
		// follower steps to, and every device loads the identical events array -
		// so two people watching one broadcast now see a shot taken from the same
		// spot, and a saved replay looks the same every time it is watched. No
		// extra data crosses the wire to achieve it.
		const playIdx = initialEventCount.current - (events.current?.length ?? 0);
		currentSceneSeed.current = `${props.initialBoxScore?.gid ?? 0}|${playIdx}`;
		seedCourtRng(currentSceneSeed.current);

		const rawT: 0 | 1 = event.t === 0 ? 0 : 1;
		// Box score display order swaps the raw team index.
		const displayT: 0 | 1 = rawT === 0 ? 1 : 0;

		// Prefix every play line with the game clock (quarter + time left) so it's
		// easy to follow WHEN each play happened. Reassigning `text` here means all
		// the pushScene branches below carry it automatically.
		const clockLabel = `${boxScore.current.quarterShort ?? ""} ${
			boxScore.current.time ?? ""
		}`.trim();
		if (clockLabel) {
			text = (
				<>
					<span
						style={{
							opacity: 0.65,
							fontWeight: 600,
							marginRight: 6,
							whiteSpace: "nowrap",
						}}
					>
						{clockLabel}
					</span>
					{text}
				</>
			);
		}

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

		// Is this the shot that finishes a fast break? A break is armed by the
		// preceding steal / defensive board (breakContext). It stays armed through
		// the attempt AND its result (so both stage as transition), then clears when
		// the shot resolves - or the instant the possession does anything else.
		let isTransition = false;
		{
			const bc = breakContext.current;
			if (bc) {
				const isShotByBreakTeam =
					!!action && !action.blocked && displayT === bc.t;
				if (
					action?.kind === "attempt" &&
					isShotByBreakTeam &&
					(!bc.requireRim || action.zone === "atRim")
				) {
					// The break shot goes up: keep the context alive for the result.
					isTransition = true;
				} else if (
					action?.kind === "result" &&
					isShotByBreakTeam &&
					(!bc.requireRim || action.zone === "atRim")
				) {
					// The break shot resolved: last transition scene, then stand down.
					isTransition = true;
					breakContext.current = undefined;
				} else {
					// Anything else (a pull-out jumper, the other team, a whistle) ends
					// the break before it materialized.
					breakContext.current = undefined;
				}
			}
		}

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

				// Decide - BEFORE the shot - whether a pass sets it up. A real assist
				// is credited to its actual passer and always shown (so the assist
				// reads the way it happens: passer first, then the shooter shoots). On
				// a miss/block we show a DECOY pass at about the same rate assists
				// happen on makes, so a pass on the floor is just as likely before a
				// miss as before a make and never gives the outcome away. Free throws
				// and heaves are never set up by a pass.
				const outcome = peekShotResult();
				let realAssist: number | undefined;
				let decoyAssist = false;
				if (outcome?.made && typeof outcome.pidAst === "number") {
					realAssist = outcome.pidAst;
				} else if (outcome && !outcome.made && action.zone !== "ft") {
					decoyAssist = Math.random() < 0.6;
				}

				// The ball comes up WITH the handler from his last spot, arriving as
				// he does (arriveMs = his glide) so it never beats him to the spot.
				const shooterFrom = lastActorPos.current.get(event.pid);
				const arriveMs = glideMs(shooterFrom, spot);

				// Just the shooter on an attempt - a defender only appears if the
				// shot is actually blocked (handled on the result below) - but the
				// nearest background defender steps up to contest.
				pushScene(
					{
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
						shooterFrom,
						arriveMs,
					},
					{
						contestSpot: spot,
						assistPid: realAssist,
						decoyAssist,
						transition: isTransition,
					},
				);
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

			pushScene(
				{
					kind: action.blocked ? "block" : action.made ? "make" : "miss",
					t: shooterT,
					// The shooter's animation keys off the zone (post move / layup / dunk
					// / jumper / set free throw), so carry it on the scene.
					zone,
					actors,
					text,
					score: scoreNode,
					ballFrom: spot,
					rimX: rimXFor(shooterT),
				},
				{
					// The assist is shown as a pass on the ATTEMPT (before the shot),
					// not here - so on the result the shooter simply lets it fly.
					// Nobody contests a free throw, and on a block the blocker IS the
					// contest.
					contestSpot: isFt || action.blocked ? undefined : spot,
					transition: isTransition,
					// A free throw lines everyone up along the paint (see pushScene).
					freeThrowShooterPid: isFt ? shooterPid : undefined,
				},
			);
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
			const victimX = spot.x + (rimXFor(victimT) > spot.x ? -4 : 4);
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
						x: victimX,
						y: spot.y,
						role: "victim",
					},
				],
				text,
				// The ball is stripped from the victim and darts to the stealer.
				ballFrom: { x: victimX, y: spot.y },
				ballTo: { x: spot.x, y: spot.y },
			});
			// Arm a fast break for the stealing team: whatever they score next is
			// off the turnover, so stage it as a coast-to-coast break.
			breakContext.current = { t: displayT, requireRim: false };
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
			// A defensive board can lead to a break - but only if they push it and
			// finish at the rim (requireRim); walking it up into a jumper is not a
			// break. An offensive board is a putback at the same rim, never a break.
			if (type === "drb") {
				breakContext.current = { t: displayT, requireRim: true };
			} else {
				breakContext.current = undefined;
			}
		} else if (type === "sub" && Array.isArray(event.pids)) {
			// Subs check in at the scorer's table in TWO rows near center court: the
			// players LEAVING on the top row, the players COMING ON just below them,
			// the way a real check-in lines up.
			const inPids = (event.pids as number[]).slice(0, 4);
			const outPids = ((event.pidsOff as number[]) ?? []).slice(0, 4);
			const outSpots = scorerTableRow(outPids.length, 8);
			const inSpots = scorerTableRow(inPids.length, 15);
			const actors: CourtActor[] = [
				...outPids.map((pid, i) => ({
					pid,
					name: playerNameByPid(pid),
					x: outSpots[i]!.x,
					y: outSpots[i]!.y,
					role: "out" as const,
				})),
				...inPids.map((pid, i) => ({
					pid,
					name: playerNameByPid(pid),
					x: inSpots[i]!.x,
					y: inSpots[i]!.y,
					role: "in" as const,
				})),
			];
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
			// Skip the victim when the stand-in (last shooter) IS the fouler - a guy
			// can't foul himself, and two actors sharing a pid would collide as
			// duplicate React keys and leave a face stuck on the court.
			if (typeof victimPid === "number" && victimPid !== event.pid) {
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
			// winner (on displayT); event.pid2 the loser. Each jumper stands on the
			// side of the line AWAY from the rim his team attacks, so he's squared up
			// tipping TOWARD his own basket: the home team (right rim) lines up on the
			// LEFT going right, the away team (left rim) on the RIGHT going left. The
			// winner taps the ball back behind him, toward his backcourt.
			const winnerT = displayT;
			const cx = (rimXFor(0) + rimXFor(1)) / 2;
			const cy = 25;
			const attackDir = rimXFor(winnerT) > cx ? 1 : -1;
			const actors: CourtActor[] = [
				{
					pid: event.pid,
					name: playerNameByPid(event.pid),
					x: cx - attackDir * 2.5,
					y: cy,
					role: "main",
				},
			];
			if (typeof event.pid2 === "number") {
				actors.push({
					pid: event.pid2,
					name: playerNameByPid(event.pid2),
					x: cx + attackDir * 2.5,
					y: cy,
					role: "defender",
				});
			}
			// The other eight players ring the center circle with their jersey
			// numbers showing, the way both teams line up around the tip in real
			// life. The two teams alternate around the ring (a real jump-ball
			// formation interleaves opponents around the circle), just outside it.
			const jumperPids = new Set(actors.map((a) => a.pid));
			if (Array.isArray(boxScore.current.teams)) {
				const teams = boxScore.current.teams;
				const ringR = 9;
				const ringTeams = ([0, 1] as const).map((t) =>
					(teams[t]?.players ?? [])
						.filter((p: any) => p.inGame && !jumperPids.has(p.pid))
						.slice(0, 4),
				) as [any[], any[]];
				const nextIdx = [0, 0];
				for (let s = 0; s < 8; s++) {
					const t: 0 | 1 = s % 2 === 0 ? 0 : 1;
					const p = ringTeams[t][nextIdx[t]!];
					nextIdx[t]! += 1;
					if (!p) {
						continue;
					}
					// Eight evenly spaced slots offset by 22.5° so none sits due
					// east/west where a jumper leans.
					const ang = ((22.5 + s * 45) * Math.PI) / 180;
					actors.push({
						pid: p.pid,
						name: playerNameByPid(p.pid),
						x: cx + ringR * Math.cos(ang),
						y: cy + ringR * Math.sin(ang),
						role: "onCourt",
						t,
					});
				}
			}
			pushScene({
				kind: "jump",
				t: winnerT,
				actors,
				text,
				ballFrom: { x: cx, y: cy - 1 },
				ballTo: { x: cx - attackDir * 16, y: cy },
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

			// Between possessions, hold a display-only SETUP beat before the next
			// possession's first SHOT: the ball is brought up and the five settle into
			// their set (a fast break or a half-court set, decided by peeking at the
			// shot that's coming), so the court DEVELOPS the possession over its own
			// beat instead of teleporting everyone end-to-end and firing at once. It
			// consumes no event, prints no play-by-play line, and moves no cursor - it
			// just holds for one interval, then the next tick plays the real shot with
			// everyone already in position. Real-time auto-play ONLY (never
			// fast-forward, rewind, or a multiplayer follower - `!force` gates that),
			// where an extra display beat is harmless.
			if (
				isSport("basketball") &&
				!force &&
				!injectedSetup.current &&
				(possessionChange.current === true ||
					breakContext.current !== undefined)
			) {
				const next = events.current[0];
				const nextAction =
					next && typeof next.type === "string"
						? courtActionFromEventType(next.type)
						: undefined;
				if (
					nextAction &&
					nextAction.kind === "attempt" &&
					typeof next.t === "number"
				) {
					// Box-score display order swaps the raw team index (see displayT).
					const offenseT: 0 | 1 = next.t === 0 ? 1 : 0;
					const bc = breakContext.current;
					const transition =
						!!bc &&
						bc.t === offenseT &&
						(!bc.requireRim || nextAction.zone === "atRim");
					pushSetupScene(offenseT, transition);
					injectedSetup.current = true;
					// Hold this beat, then let the next tick consume the real event.
					// Mirror the normal auto-play scheduler (only it reaches here).
					if (!pausedRef.current && !followerRef.current) {
						setTimeout(() => {
							processToNextPause();
							setPlayIndex((prev) => prev + 1);
						}, speedToMs(speedRef.current));
					}
					return 0;
				}
			}
			injectedSetup.current = false;

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

			// Bank this play as a rewind checkpoint (cursor = events consumed so far).
			if (events.current) {
				playHistory.current.push({
					cursor: initialEventCount.current - events.current.length,
					q: String(boxScore.current.quarterShort ?? ""),
					clock: getSeconds(boxScore.current.time),
				});
			}

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
			// Release the deterministic stream: it is module-global, and anything
			// else that invents court positions (the team court editor's preview)
			// wants ordinary randomness.
			clearCourtRng();
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
			// Copy, don't alias: playback mutates the box score in place, so props
			// must stay pristine - a remount re-initializes from them, and replaying
			// a game on top of its own already-final box score doubles everything.
			boxScore.current = helpers.deepCopy(props.initialBoxScore);
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

	// The game this view delivered vs the game being broadcast. They differ when
	// the navigation carrying a new broadcast's payload was dropped (this device
	// was already parked on this page), which used to leave the previous game on
	// screen - and its events replaying into the wrong box score.
	const propsGid = props.initialBoxScore?.gid;
	const broadcastGid = isFollower ? mpLiveBroadcast?.gid : undefined;

	// Recovery: while following a broadcast for a DIFFERENT game than the one on
	// screen, keep asking the worker to refresh this view. It serves the cached
	// broadcast payload, and the wrapper below remounts once the data lands.
	useEffect(() => {
		if (broadcastGid === undefined || broadcastGid === propsGid) {
			return;
		}
		const interval = setInterval(() => {
			void realtimeUpdate(
				["mpLiveBroadcast"],
				helpers.leagueUrl(["live_game"]),
			);
		}, 1500);
		return () => {
			clearInterval(interval);
		};
	}, [broadcastGid, propsGid]);

	// Follower lockstep: whenever the simmer's cursor advances, step our own
	// playback forward to the same position (fast-forwarding through any gap, e.g.
	// when we first join mid-game). Pure catch-up - it never runs ahead of the
	// simmer, so we always show exactly what they've shown.
	const followerCursor = isFollower ? (mpLiveBroadcast?.cursor ?? 0) : 0;
	useEffect(() => {
		if (!isFollower || !started || !events.current) {
			return;
		}
		// The cursor belongs to the game being broadcast; never let it step
		// playback of a different (previous) game still on screen.
		if (broadcastGid !== undefined && broadcastGid !== propsGid) {
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
	}, [
		broadcastGid,
		followerCursor,
		isFollower,
		propsGid,
		started,
		processToNextPause,
	]);

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

	// Jump playback to a given cursor (number of events consumed). Going forward
	// just steps ahead; going back resets to the opening box score and re-sims up
	// to the target - the same mechanism the multiplayer follower uses to catch up.
	const seekTo = useCallback(
		(targetCursor: number) => {
			if (
				!componentIsMounted.current ||
				!props.events ||
				props.initialBoxScore === undefined
			) {
				return;
			}
			// Seeking pauses playback at the landing point (like a video scrubber),
			// which also stops the self-scheduling auto-play loop from firing a timer
			// for every step we re-sim, and neutralizes any already-pending tick.
			pausedRef.current = true;
			setPaused(true);

			const target = Math.max(
				0,
				Math.min(Math.round(targetCursor), initialEventCount.current),
			);
			const current = initialEventCount.current - (events.current?.length ?? 0);
			if (target < current) {
				// Rewind: rebuild playback state from the pristine opening, then replay
				// forward to the target below.
				boxScore.current = helpers.deepCopy(props.initialBoxScore);
				events.current = props.events.slice();
				overtimes.current = 0;
				quarters.current = [];
				possessionChange.current = undefined;
				sportState.current = DEFAULT_SPORT_STATE
					? { ...DEFAULT_SPORT_STATE }
					: undefined;
				playByPlayEntries.current = [];
				playHistory.current = [];
				courtScene.current = undefined;
				courtSceneCount.current = 0;
				lastActorPos.current = new Map();
				lastFga.current = undefined;
				breakContext.current = undefined;
				injectedSetup.current = false;
			}
			while (
				events.current &&
				events.current.length > 0 &&
				initialEventCount.current - events.current.length < target
			) {
				processToNextPause(true);
			}
			setPlayIndex((prev) => prev + 1);
		},
		[processToNextPause, props.events, props.initialBoxScore],
	);

	// The rewind menu: back roughly a game-minute, to the start of the current
	// quarter, or to the tip-off. Targets are read from playHistory at click time
	// (all refs), so this list is stable. Only offered when rewinding is possible
	// (see the `rewinds` prop below) - never for a multiplayer follower.
	const rewindMenuItems = useMemo<FastForward[]>(() => {
		const startOfQuarterCursor = (): number => {
			const hist = playHistory.current;
			if (hist.length === 0) {
				return 0;
			}
			const lastIdx = hist.length - 1;
			const curQ = hist[lastIdx]!.q;
			let firstIdx = lastIdx;
			while (firstIdx > 0 && hist[firstIdx - 1]!.q === curQ) {
				firstIdx -= 1;
			}
			// Already at the quarter's opening play? Then step back a whole quarter.
			if (firstIdx === lastIdx && firstIdx > 0) {
				const prevQ = hist[firstIdx - 1]!.q;
				let pIdx = firstIdx - 1;
				while (pIdx > 0 && hist[pIdx - 1]!.q === prevQ) {
					pIdx -= 1;
				}
				firstIdx = pIdx;
			}
			return hist[firstIdx]!.cursor;
		};
		const backOneMinuteCursor = (): number => {
			const hist = playHistory.current;
			if (hist.length === 0) {
				return 0;
			}
			const cur = hist.at(-1)!;
			let targetIdx = hist.length - 1;
			for (let i = hist.length - 1; i >= 0; i -= 1) {
				if (hist[i]!.q !== cur.q) {
					break; // don't cross a quarter boundary
				}
				targetIdx = i;
				if (hist[i]!.clock >= cur.clock + 60) {
					break;
				}
			}
			return hist[targetIdx]!.cursor;
		};
		return [
			{ label: "Back 1:00", onClick: () => seekTo(backOneMinuteCursor()) },
			{
				label: "Start of quarter",
				onClick: () => seekTo(startOfQuarterCursor()),
			},
			{ label: "Restart game", onClick: () => seekTo(0) },
		];
	}, [seekTo]);

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
											rewinds={
												isFollower || isBroadcaster
													? undefined
													: rewindMenuItems
											}
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
									teams={[
										boxScore.current.teams?.[0],
										boxScore.current.teams?.[1],
									]}
									finals={!!boxScore.current.finals}
									season={boxScore.current.season}
									sceneMs={speedToMs(speedRef.current)}
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
									rewinds={
										isFollower || isBroadcaster ? undefined : rewindMenuItems
									}
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
	// Crucially, wait until the new game's data has actually ARRIVED (props gid
	// matches the broadcast gid) - remounting with the previous game's props
	// replays the old game, and LiveGame's own recovery effect keeps refreshing
	// the view until the payload lands.
	const { mpLiveBroadcast } = useLocal(["mpLiveBroadcast"]);
	const remountKey = useRef(0);
	const lastFollowedStartedAt = useRef<number | undefined>(undefined);
	if (
		mpLiveBroadcast?.active &&
		!mpLiveBroadcast.isBroadcaster &&
		mpLiveBroadcast.startedAt !== lastFollowedStartedAt.current &&
		props.initialBoxScore?.gid === mpLiveBroadcast.gid
	) {
		lastFollowedStartedAt.current = mpLiveBroadcast.startedAt;
		remountKey.current += 1;
	}

	return <LiveGame key={remountKey.current} {...props} />;
};

export default LiveGameWrapper;
