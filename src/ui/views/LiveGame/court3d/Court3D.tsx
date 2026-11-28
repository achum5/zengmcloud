import type { CourtStyle, ScoreBugStyle } from "../../../../common/types.ts";
import { adsFor } from "./arenaAds.ts";
import {
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
	type ReactNode,
} from "react";
import { useLocal } from "../../../util/local.ts";
import type { ArenaLooks, ReplayLooks } from "../../../../common/types.ts";
import { weekdayName } from "../../../../common/calendarDates.ts";
import LiveCourt from "../LiveCourt.tsx";
import {
	paintBench,
	paintBoards,
	courtsideFor,
	paintStanchionAd,
	paintStanchionPanel,
	paintRafters,
	paintStands,
	paintTable,
} from "./arena.ts";
import { scorersTableFor } from "./scorersTable.ts";
import { GameSound } from "./sound/engine.ts";
import { loadBank } from "./sound/bank.ts";
import { keepSoundAwake, wakeSound } from "./sound/context.ts";
import { useSound } from "./sound/setting.ts";
import {
	CAM_HOLD,
	CAM_PAN,
	courtFit,
	dodgeFor,
	follow,
	makeCamera,
	MAIN_RIG,
	operate,
	project,
	zoomedOn,
	type Cover,
	type Rig,
} from "./camera.ts";
import { courtTexture, edgeColor, type CourtPicture } from "./courtTexture.ts";
import { useWideFloor } from "../useWideFloor.ts";
import {
	adjust,
	fineNow,
	makeFine,
	makeResolution,
	sizeCanvas,
} from "./resolution.ts";
import { FAST, makePace } from "./pace.ts";
import { makeFarm } from "./farm.ts";
import { shortsHemFor } from "./shortsEra.ts";
import {
	glReady,
	makeScratch,
	makeSpriteCache,
	releaseSprites,
} from "./sprite.ts";
import {
	buildClocks,
	finalStretchAt,
	formatGameClock,
	gameClockAt,
	shotClockAt,
} from "./clock.ts";
import { clipDipAt, type CourtTimeline } from "./director.ts";
import { crewAt, crewFor } from "./crew.ts";
import {
	headColors,
	loadFaceTex,
	loadHead,
	profileOf,
	type HeadSprite,
} from "./faces.ts";
import { loadMesh, MESH_KINDS, meshReady } from "./meshBody.ts";

const MESH_HEAD = MESH_KINDS.includes("head");
// How much of posing a man drawn as one no coarser picture makes quicker
// (see resolution.ts adjust): his points, skinned and set on the screen.
const MESH_FIXED = 0.6;
import { arenaSpecFor } from "./arenaSpec.ts";
import { makeJumbotron } from "./jumbotron.ts";
import { crowdFaces, crowdFacesMade, type CrowdFace } from "./crowdFaces.ts";
import { type Bowl, bowlLoaded, loadBowl } from "./bowlData.ts";
import { fanLooks, paintFanPool, seatCrowd } from "./bowlFans.ts";
import { bowlHere } from "./bowlSetting.ts";
import { drawGpu, gpuScene } from "./gpu/scene.ts";
import { makeMeter, meterTick } from "./gpu/meter.ts";
import { gpuMode } from "./gpu/setting.ts";
import { gearFor, kitsFor, shade, streetClothes, type Look } from "./figure.ts";
import { courtRosterOf, courtTeamsOf, introKindOf } from "./courtInput.ts";
import { dressKit } from "./kitArt.ts";
import { logoPicture } from "./logoPicture.ts";
import { COURT_H, COURT_W, rimX, type Side } from "./geometry.ts";
import { eventsMatchRoster } from "./rosterMatch.ts";
import { Caption, Overlays } from "./Overlays.tsx";
import {
	bugTeamOf,
	GameBug,
	PictureBox,
	PictureCanvas,
	standingOf,
} from "./Picture.tsx";
import {
	boxOf,
	type Box,
	captionOf,
	darkAndPace,
	dipOf,
	NARROW_W,
	runFrames,
	holdReady,
	setOpacity,
	showCaption,
	showWipe,
	useStaged,
	useStakes,
	useAdPictures,
	useFaces,
	useKitArts,
	useViewSize,
} from "./broadcast.ts";
import { spawnCourtWorker, stageCourt } from "./stageCourt.ts";
import { goesOn, type Staged } from "./parts.ts";
import {
	stageWarmup,
	WARMUP_MS,
	warmupAim,
	warmupMoment,
	type Warmup,
} from "./pregame.ts";
import {
	addReport,
	makeFollow,
	heardAs,
	saidAs,
	stagedAs,
	type FollowReport,
	type Shown,
} from "./follow.ts";
import {
	askedReplay,
	clipStopAt,
	lastHolder,
	makeWipe,
	onCursor,
	REPLAY_AFTER,
	stepPlay,
	wipeAt,
	type Replay,
} from "./playback.ts";
import {
	keyOutAt,
	planKeyPlays,
	type KeyPlaySettings,
	type KeySegment,
} from "./keyPlays.ts";
import { bodyOf, MESH_BUILD, type Body } from "./poses.ts";
import { physiqueOf } from "./ratings.ts";
import { glReport } from "./sculptGl.ts";
import { getCamera } from "./cameraSetting.ts";
import { registerCaptureExtra } from "../../../util/syncDebugStore.ts";
import { carryFaces, offenseAt, showWhole, warmFaces } from "./evaluate.ts";
import { usePacing } from "./pacing.ts";
import { shotSpots } from "./shotSpots.ts";
import type { ShotSpot } from "../gamecast/shots.ts";
import {
	buildFouls,
	breakText,
	finalText,
	foulsAt,
	isBreak,
	isFinal,
	SHOT_LOW,
	shotClockText,
} from "./scoreBug.ts";
import { seriesLine } from "../gamecast/seriesLine.ts";
import { bugShown, useBugRefs, writeBug } from "./bugWriter.ts";
import { IntroCard, IntroTitle } from "./IntroCard.tsx";
import type { Authored } from "./authored.ts";
import { callAt, TITLE_MS } from "./intro.ts";
import {
	planShow,
	showAim,
	showEndsAt,
	warmAt,
	type ShowPlan,
} from "./pregameShow.ts";
import {
	MatchupBoard,
	type ShowCardsInfo,
	type ShowLeader,
} from "./ShowCards.tsx";
import { matchupFacts } from "../matchupFacts.ts";
import type {
	GameLeader,
	GamecastInfo,
} from "../../../../worker/views/liveGameInfo.ts";
import {
	aimFor,
	arenaAim,
	closeUpAim,
	crowdAt,
	drawFrame,
	type Frame,
	introAim,
	momentAt,
	replayAim,
	replayRig,
	type StandsPaint,
} from "./scene.ts";

// THE 3D COURT: the game as a broadcast - the home team's own floor, the
// players with their faces, a camera that follows the ball - acting out the
// play-by-play, in place of the 2D court when this device has chosen it.
//
// The whole game is staged up front (see director.ts), so playback is a clock
// running along that timeline. The page's playback cursor (events consumed)
// says where the clock may run to: the moment the next line happens. When the
// clock gets there, it asks the page for that line (onReady) - so the
// animation, not a timer, sets the pace, and every line's text appears as its
// play happens on screen.

// In close on a man, the building behind him this far out of focus
// (picture pixels - see drawFrame).
const CLOSE_SOFT = 1.1;
// WHAT IS OVER THE PICTURE: the score bug along its foot (see boxOf, and
// camera.ts's dodgeFor).
const bugBoxOf = (wrap: HTMLElement | null, holder: HTMLElement | null) =>
	boxOf(wrap, holder?.firstElementChild);
// The score bug: what the camera keeps the man with the ball out from
// behind, and the tag under him too (see scene.ts).
const coversOf = (
	wrap: HTMLElement | null,
	holder: HTMLElement | null,
): Cover[] => [bugBoxOf(wrap, holder)].filter((b) => b !== undefined);
// WHAT THE COURT IS DOING ON THIS DEVICE, in a line of a capture of the log
// (see syncDebugStore): how fine the picture is - its rows, the screen
// pixels to each of its own, and how many sizes coarser than the finest it
// has had to go - how long a frame takes to draw, whether poses are in
// finer steps, the pace, the pictures kept, and what became of the graphics
// card (see sculptGl.ts). A picture that looks blocky on somebody's phone
// can then be told from here: gone coarse, and why.
const doing: {
	rows?: number;
	art?: number;
	dpr?: number;
	coarser?: number;
	changes?: number;
	drawMs?: number;
	fine?: boolean;
	fps?: number;
	kept?: number;
} = {};
registerCaptureExtra(() =>
	doing.rows === undefined
		? `court3d: not drawn yet; card ${glReport()}`
		: `court3d: rows=${doing.rows} art=${doing.art} dpr=${doing.dpr} coarser=${doing.coarser} (changes ${doing.changes}) drawMs=${doing.drawMs?.toFixed(1) ?? "-"} fine=${doing.fine} fps=${doing.fps} kept=${doing.kept}; card ${glReport()}; cores=${navigator.hardwareConcurrency ?? "?"}`,
);

// Where the middle of the men on the floor is, across it (feet) - for a
// camera in closer than the whole floor (see zoomedOn).
const midAcross = (players: { y: number }[]): number => {
	let sum = 0;
	let n = 0;
	for (const st of players) {
		if (st.y > -1.5 && st.y < COURT_H + 1.5) {
			sum += st.y;
			n += 1;
		}
	}
	return n > 0 ? sum / n : COURT_H / 2;
};

// Drawing a frame in under this long (ms, smoothed), there is time for his
// poses in finer steps (see fineNow).
const FINE_ON_MS = 5;
// The most of a frame (ms) given to drawing players in poses not drawn
// before; over that, a man waits a frame for his new one (see sprite.ts).
const SPRITE_BUDGET_MS = 6;

// A player whose face is a photo is drawn in this, head to toe.
const SILHOUETTE = "#101012";

// A jump in the moment shown from one frame to the next this far on (game
// ms) - or any back - is a cut (see fromCut), and the frames just after it
// worked out as just cut to. (Taken for one while the game plays on - a
// slow frame at the fastest through dead time - it changes nothing: what
// it would begin afresh is worked out already, carried on from the frame
// before.)
const CUT_JUMP = 6000;
const CUT_FRAMES = 3;

// What goes across the back of his jersey: everything after his first name.
const lastNameOf = (name: string | undefined): string => {
	const n = (name ?? "").trim();
	const i = n.indexOf(" ");
	return i < 0 ? n : n.slice(i + 1);
};

// The building's pictures together. (Put together here, not in a closure
// of Court3D's own: every function made in one of its renders shares what
// any of them uses, and React keeps some of them - an effect's clean-up, a
// memo's getter - for many renders after, so the first night's pictures,
// before the crowd's faces were in, were kept all game beside the new ones:
// about 37 MB more on a phone.)
const usePainted = <P extends object, R, J, B>(
	paint: P,
	rafters: R,
	jumbotron: J,
	bowl: B,
) =>
	useMemo(
		() => ({ ...paint, rafters, jumbotron, bowl }),
		[paint, rafters, jumbotron, bowl],
	);

// How many of the page's events the picture has staged: the rest still to
// come in (see parts.ts), the page held at the last of them meanwhile.
const stagedLength = (
	events: unknown[] | undefined,
	tl: Staged | undefined,
): number => Math.min(events?.length ?? 0, tl?.upTo ?? Infinity);

type Props = {
	// The game's full play-by-play, never consumed.
	events: any[] | undefined;
	// How many of those the page has shown.
	cursor: number;
	boxScore: any;
	// The broadcast picked to watch it on (see LocalBroadcasts): its score
	// bug, in place of the game's own (null, the default bug).
	scoreBug?: ScoreBugStyle | null;
	caption: ReactNode;
	// The box score team (0 home, 1 away) the caption's play belongs to, if any.
	captionT?: 0 | 1;
	paused: boolean;
	// How many times real time it plays at (1 is real time).
	rate: number;
	// A multiplayer follower is stepped by the device in charge of simming, so
	// it never asks for the next line - it only keeps up.
	follower: boolean;
	// Watching a league-mate's live sim that says where its court is: this
	// one plays the game along with it instead, a moment behind (see
	// follow.ts) - the latest word of where it is.
	along?: FollowReport;
	// Where this court is, for anyone watching along (see follow.ts).
	report?: { current: Shown | undefined };
	// How many times "next play" has been asked for: each cuts straight past
	// the play it shows, like any other skip ahead.
	skips?: number;
	// Watching only the key plays (see keyPlays.ts) - and which raw box
	// score team is the viewer's own, if either.
	keyPlays?: { settings: KeyPlaySettings; mine?: 0 | 1 };
	onReady: () => void;
	// Key plays: on past the plays left out, the page catching up to
	// `cursor` events.
	onJump?: (cursor: number) => void;
	// The court cannot play this game: the page goes on without it.
	onFail?: () => void;
	// Playing along, the other court is not showing the same game (see
	// stagedAs): the page has this one stepped line by line instead.
	onLost?: () => void;
	// No game: the two teams warming up on the home floor, round and round -
	// a look at the court (see CourtPreview).
	preview?: boolean;
	// No game: a scene of its own, in place of the warm-ups (see authored.ts).
	authored?: Authored;
	// A play the page asks to see again (a highlight tapped): the line that
	// ended at `cursor`, shown again each time `n` goes up (see askedReplay).
	replayAsk?: { n: number; cursor: number };
	// The game's shots and where each was let go from, once it is staged
	// (see shotSpots) - for the shot chart.
	onShots?: (shots: ShotSpot[]) => void;
	// The game staged and on the court - this game (its gid).
	onStaged?: (gid: number | undefined) => void;
	// A clip (a highlight played on its own - see HighlightClip): it plays
	// no further than a moment past the line before this cursor.
	until?: number;
	// A reel's cut to its next clip (the kth), as the picture gets there.
	onCut?: (k: number) => void;
};

const Court3D = ({
	events,
	cursor,
	boxScore,
	scoreBug,
	caption,
	captionT,
	paused,
	rate,
	follower,
	along,
	report,
	skips = 0,
	keyPlays,
	onReady,
	onJump,
	onFail,
	onLost,
	preview = false,
	authored: authoredProp,
	replayAsk,
	onShots,
	onStaged,
	until,
	onCut,
}: Props) => {
	const onShotsRef = useRef(onShots);
	onShotsRef.current = onShots;
	const onCutRef = useRef(onCut);
	onCutRef.current = onCut;
	// (The last cut the picture got to.)
	const cutSeen = useRef(-1);
	const onStagedRef = useRef(onStaged);
	onStagedRef.current = onStaged;
	const { lid, gender } = useLocal(["lid", "gender"]);
	const gid: number | undefined = boxScore?.gid;
	const season: number | undefined = boxScore?.season;
	// A saved replay remembers how everyone looked that night.
	const looksThen: ReplayLooks | undefined = boxScore?.replayLooks;
	const teamsNow: unknown = boxScore?.teams;
	const raw: any[] = useMemo(
		() => courtTeamsOf(teamsNow, looksThen),
		[teamsNow, looksThen],
	);
	// Display order, same as the 2D court: [away (attacks left), home].
	const away = raw[1];
	const home = raw[0];

	// The roster is fixed for the game - read it once.
	const roster = useMemo(
		() => courtRosterOf(away, home),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);

	// A playoff game's stakes, as they stood going into it (see stakesOf).
	const stakes = useStakes(gid, {
		playoffs: boxScore?.playoffs,
		finals: boxScore?.finals,
		numGamesToWinSeries: boxScore?.numGamesToWinSeries,
		gameOver: boxScore?.gameOver,
		won: boxScore?.won,
		teams: boxScore?.teams,
	});
	// (Whether the starting lineups are called out first - see intro.ts.)
	const introKind = introKindOf(boxScore?.playoffs);
	// The game staged, off the page's thread (see stageCourt.ts and the
	// engine's useStaged) - the court on the warm-ups until it is. A part at
	// a time (see parts.ts): the first played as soon as it is in, each after
	// it as it comes (see `onTl`).
	const shotsOf = (tl: Staged) => {
		try {
			onShotsRef.current?.(shotSpots(tl));
		} catch {
			// The chart just doesn't show.
		}
	};
	const { timeline: latestTl } = useStaged<Staged>(
		events,
		roster,
		`${gid}|${gender}|${introKind}|${JSON.stringify(stakes ?? null)}`,
		(w) =>
			stageCourt(
				{
					events: events!,
					players: roster,
					gid,
					gender,
					intro: introKind,
					stakes,
				},
				w,
			),
		"court",
		onFail,
		(tl) => {
			shotsOf(tl);
			onStagedRef.current?.(gid);
		},
		shotsOf,
	);
	// Key plays, and a clip to a line, are cut from the whole game: not
	// started till all of it is in.
	const wholeWanted = !!keyPlays?.settings.on || until !== undefined;
	const stagedTl =
		latestTl && (!wholeWanted || latestTl.upTo === undefined)
			? latestTl
			: undefined;
	// THE GAME SO FAR, AND THE NEXT PART OF IT. A part comes in while the one
	// before plays - and a new picture of the game is new to everything the
	// court works out off it and keeps (each man's runs and steps, the cuts,
	// the replays): worked out afresh, all at once, the picture stopped dead
	// a moment in the middle of a play. So the game goes on as it was till it
	// is played up to where it is staged to - the end of a break between
	// periods - and on into the next part from there (see the frame loop).
	const [onTl, setOnTl] = useState<Staged>();
	const nextTl = useRef<Staged | undefined>(undefined);
	// (The game last drawn - see the frame loop.)
	const drawnTl = useRef<CourtTimeline | undefined>(undefined);
	useEffect(() => {
		setOnTl((was) => {
			if (stagedTl && was && goesOn(was, stagedTl) && drawnTl.current === was) {
				nextTl.current = stagedTl;
				return was;
			}
			nextTl.current = undefined;
			return stagedTl;
		});
	}, [stagedTl]);

	// THE PREGAME SHOW (see pregameShow.ts), in place of the warm-ups while
	// the game is staged: for a game watched from its start - not a clip, not
	// a look at the court or a scene of its own, not a league-mate's game
	// already under way. Decided once a game.
	const showFor = useRef<{ gid: number | undefined; wanted: boolean }>(
		undefined,
	);
	if (showFor.current?.gid !== gid) {
		showFor.current = {
			gid,
			wanted:
				!preview &&
				!authoredProp &&
				until === undefined &&
				!along &&
				gid !== undefined &&
				cursor === 0,
		};
	}
	const showWanted = showFor.current?.wanted ?? false;
	const [showPlan, setShowPlan] = useState<ShowPlan | undefined>();
	// (Over or skipped - or not to be had: no tip in the play-by-play, or it
	// would not stage.)
	const [showOver, setShowOver] = useState(false);
	const eventsRef = useRef(events);
	eventsRef.current = events;
	const hasEvents = !!events && events.length > 0;
	// Each team's leading scorer going in, road first (see leadersOf).
	const leaders = useMemo(() => {
		const ls: GameLeader[] | undefined = boxScore?.gamecast?.leaders;
		const bt = boxScore?.teams;
		const road = ls?.find((l) => l.tid === bt?.[1]?.tid);
		const homeL = ls?.find((l) => l.tid === bt?.[0]?.tid);
		return road && homeL ? ([road, homeL] as const) : undefined;
	}, [boxScore?.gamecast, boxScore?.teams]);
	useEffect(() => {
		setShowPlan(undefined);
		setShowOver(false);
		if (!showWanted || !hasEvents) {
			return;
		}
		// (Only what would stage: a game whose play-by-play is this roster's.)
		const evs = eventsRef.current;
		let plan: ShowPlan | undefined;
		if (evs && eventsMatchRoster(evs, roster)) {
			try {
				plan = planShow(roster, gid ?? 0);
			} catch (error) {
				console.error("3D pregame show failed to stage", error);
			}
		}
		if (plan) {
			setShowPlan(plan);
		} else {
			setShowOver(true);
		}
	}, [gid, roster, showWanted, hasEvents]);
	const showOn = showWanted && !showOver;
	// The game, once the show is over: till then, as if still staging.
	const timeline = showOn ? undefined : onTl;
	const readyRef = useRef(false);
	readyRef.current = !!stagedTl;
	const showScene = useMemo((): Authored | undefined => {
		if (!showPlan) {
			return undefined;
		}
		const plan = showPlan;
		// (The show's time when the game was there to go on to.)
		let readyAt: number | undefined;
		return {
			show: plan.warm,
			get ms() {
				return showEndsAt(readyAt);
			},
			// (In their warm-up tops.)
			moment: (t, bodyFor) => warmupMoment(plan.warm, warmAt(t), bodyFor),
			aim: (t, narrow) => showAim(plan, t, narrow),
			onTick: (t) => {
				if (readyAt === undefined && readyRef.current) {
					readyAt = t;
				}
			},
			onEnd: () => setShowOver(true),
			run: 1,
		};
	}, [showPlan]);
	const authored = authoredProp ?? (showOn ? showScene : undefined);
	// Watching along with a league-mate's game: once it is staged here, on
	// to wherever theirs has got to.
	useEffect(() => {
		if (showOn && stagedTl && (along || (follower && cursor > 0))) {
			setShowOver(true);
		}
	}, [showOn, stagedTl, along, follower, cursor]);
	// While it is: the two teams out warming up at their baskets - not a
	// black screen. (Staged here: it takes no time at all.) Or a scene of its
	// own (see authored.ts).
	const authoredShow = authored?.show;
	const showComing = showOn && !showPlan;
	const pregame = useMemo((): Warmup | undefined => {
		if (authoredShow) {
			return authoredShow;
		}
		// (The show staging: black for that moment, not a glimpse of the
		// warm-ups.)
		if (showComing) {
			return undefined;
		}
		if (
			!preview &&
			(!events || events.length === 0 || !eventsMatchRoster(events, roster))
		) {
			return undefined;
		}
		try {
			return stageWarmup(roster, gid ?? 0);
		} catch {
			return undefined;
		}
	}, [events, roster, gid, preview, authoredShow, showComing]);
	const clocks = useMemo(
		() => (timeline && events ? buildClocks(timeline, events) : undefined),
		[timeline, events],
	);
	// Key plays only: the stretches of the game shown.
	const numPeriods: number = boxScore?.numPeriods ?? 4;
	const spread: unknown = boxScore?.spread;
	const neutralSite = !!boxScore?.neutralSite;
	const keyPlan = useMemo(
		(): KeySegment[] | undefined =>
			timeline && events && keyPlays?.settings.on
				? planKeyPlays(timeline, events, keyPlays.settings, {
						rate,
						numPeriods,
						mine: keyPlays.mine,
						// (Who was expected to win, by how much - see winProb.ts. No
						// home team on a neutral floor.)
						...(typeof spread === "number" ? { spread } : {}),
						...(neutralSite ? { edge: 0 } : {}),
					})
				: undefined,
		[timeline, events, keyPlays, rate, numPeriods, spread, neutralSite],
	);
	// What this court staged, for whoever plays along with it to check
	// against their own (see stagedAs).
	const fp = useMemo(
		() => (timeline ? stagedAs(timeline) : undefined),
		[timeline],
	);
	// Whole possessions, or on to the action (see pacing.ts): as this device
	// has it - or, playing along with another court, as that one plays it.
	const pacing = usePacing();
	const whole = along ? heardAs(along.fp ?? "").whole : pacing.whole;
	// Team fouls and the bonus through the game, for the score bug.
	const fouls = useMemo(
		() =>
			timeline && events
				? buildFouls(
						timeline,
						events,
						boxScore?.foulsUntilBonus,
						boxScore?.numPeriods,
					)
				: undefined,
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[timeline, events],
	);

	const kits = useMemo(
		() => kitsFor(away, home),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);

	// Each side's uniform drawn from a picture, if its team has one (see
	// useKitArts).
	const arts = useKitArts(gid, boxScore?.teams, away, home);
	const dressed = useMemo(
		() => [dressKit(kits[0], arts[0]), dressKit(kits[1], arts[1])] as const,
		[kits, arts],
	);

	// Each side's logo, drawn small, for the front of its warm-up tops.
	const [crests, setCrests] = useState<
		[HTMLCanvasElement?, HTMLCanvasElement?]
	>([]);
	const awayLogo = away?.imgURL || away?.imgURLSmall;
	const homeLogo = home?.imgURL || home?.imgURLSmall;
	useEffect(() => {
		let alive = true;
		void Promise.all([logoPicture(awayLogo), logoPicture(homeLogo)]).then(
			(made) => {
				if (alive) {
					setCrests([made[0], made[1]]);
				}
			},
		);
		return () => {
			alive = false;
		};
	}, [awayLogo, homeLogo]);

	// The team's own pictures for its arena ads, once loaded (see adsFor).
	const ownAds: CourtStyle["arenaAds"] = home?.court?.arenaAds;
	const adPictures = useAdPictures(ownAds);

	// The building, painted once a game.
	// Faces for the crowd, made in the background (see crowdFaces): the stands
	// painted again once they are ready, the same fans with faces now - or
	// with them from the start, if they were made for a court before.
	const [fans, setFans] = useState<CrowdFace[] | undefined>(crowdFacesMade);
	useEffect(() => {
		let alive = true;
		void crowdFaces().then((f) => {
			if (alive && f.length > 0) {
				setFans(f);
			}
		});
		return () => {
			alive = false;
		};
	}, []);

	const paint = useMemo(() => {
		const a = {
			abbrev: away?.abbrev,
			name: away?.name,
			region: away?.region,
			colors: away?.colors,
		};
		const h = {
			abbrev: home?.abbrev,
			name: home?.name,
			region: home?.region,
			colors: home?.colors,
		};
		const table = paintTable(h, a);
		// The building's sponsors (see adsFor).
		const ads = adsFor(
			home?.abbrev ?? home?.name ?? "",
			home?.colors,
			ownAds,
			(url) => adPictures.get(url),
		);
		// The home building that night: as full as the gate says, its banners
		// in the rafters.
		const building: ArenaLooks | undefined = boxScore?.arena;
		const crowd = { att: boxScore?.att, capacity: building?.capacity };
		// Half of them, for when the seats are emptier (see seatsAt).
		const sparse = {
			att:
				0.5 *
				(crowd.att !== undefined && crowd.capacity
					? Math.min(0.97, crowd.att / crowd.capacity)
					: 0.93),
			capacity: 1,
		};
		const seed = String(gid ?? 0);
		const spec = arenaSpecFor(h);
		// A playoff game: the home crowd's color-out (see paintStands).
		const playoffs = !!boxScore?.playoffs;
		const stands = (
			up: 0 | 1 | 2,
			seats: typeof crowd,
			end: 0 | undefined,
			clap = false,
		) => paintStands(h, a, seed, up, seats, end, fans, spec, playoffs, clap);
		// (The stands painted only once they are drawn: on the graphics card,
		// never - the bowl is drawn in their place. Nine pictures, 19 MB on a
		// phone.)
		let crowdPaint: StandsPaint | undefined;
		return {
			courtside: courtsideFor(h, a, spec, fans),
			scorersTable: scorersTableFor(h, fans),
			stanchion: paintStanchionPanel(h, h?.colors?.[0] ?? "#8c1d40"),
			crowd: () =>
				(crowdPaint ??= {
					stands: stands(0, crowd, undefined),
					standsUp: stands(1, crowd, undefined),
					standsWave: stands(2, crowd, undefined),
					standsClap: stands(0, crowd, undefined, true),
					standsSparse: stands(0, sparse, undefined),
					endStandsSparse: stands(0, sparse, 0),
					endStands: [0, 1, 2].map((up) =>
						stands(up as 0 | 1 | 2, crowd, 0),
					) as [HTMLCanvasElement, HTMLCanvasElement, HTMLCanvasElement],
				}),
			boards: paintBoards(h, a, ads),
			stanchionAds: ads.map((ad) => paintStanchionAd(ad)),
			tableTop: table.top,
			tableFront: table.front,
			bench0: paintBench(a),
			bench1: paintBench(h),
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid, fans, adPictures]);

	// The banners in the rafters - each title's in the team's colors and logo
	// that season - painted again once the logos are in.
	const [bannerLogos, setBannerLogos] = useState<
		Map<string, HTMLCanvasElement>
	>(() => new Map());
	useEffect(() => {
		const building: ArenaLooks | undefined = boxScore?.arena;
		const urls = [
			...new Set(
				(building?.titleLooks ?? [])
					.map((l) => l.imgURL)
					.filter((u): u is string => u !== undefined),
			),
		];
		if (urls.length === 0) {
			return;
		}
		let alive = true;
		void Promise.all(urls.map((u) => logoPicture(u))).then((pics) => {
			if (alive) {
				const got = new Map<string, HTMLCanvasElement>();
				urls.forEach((u, i) => {
					const pic = pics[i];
					if (pic) {
						got.set(u, pic);
					}
				});
				setBannerLogos(got);
			}
		});
		return () => {
			alive = false;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);
	const rafters = useMemo(
		() =>
			paintRafters(
				{
					abbrev: home?.abbrev,
					name: home?.name,
					region: home?.region,
					colors: home?.colors,
				},
				boxScore?.arena,
				bannerLogos,
			),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid, bannerLogos],
	);
	// The board over center court, its screens painted as they come up -
	// again once the logos are in.
	const jumbotron = useMemo(
		() => {
			const h = {
				abbrev: home?.abbrev,
				name: home?.name,
				region: home?.region,
				colors: home?.colors,
			};
			const a = {
				abbrev: away?.abbrev,
				name: away?.name,
				region: away?.region,
				colors: away?.colors,
			};
			return makeJumbotron(arenaSpecFor(h).jumbotron.style, {
				home: h,
				away: a,
				logos: crests,
				ads: adsFor(
					home?.abbrev ?? home?.name ?? "",
					home?.colors,
					ownAds,
					(url) => adPictures.get(url),
				),
			});
		},
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid, crests, adPictures],
	);
	// The bowl built in Blender (Global Settings, "3D Arena": see bowlGl.ts),
	// fetched once a visit - and the crowd sat in it once a game, with
	// faces once there are.
	// (And for the game drawn on the graphics card: see gpu/setting.ts.)
	const [bowlData, setBowlData] = useState<Bowl | undefined>(() =>
		bowlHere() || gpuMode() === "test" ? bowlLoaded() : undefined,
	);
	useEffect(() => {
		if (!bowlHere() && gpuMode() !== "test") {
			return;
		}
		let alive = true;
		void loadBowl().then((b) => {
			if (alive && b) {
				setBowlData(b);
			}
		});
		return () => {
			alive = false;
		};
	}, []);
	const bowl = useMemo(
		() => {
			if (!bowlData) {
				return undefined;
			}
			const h = {
				abbrev: home?.abbrev,
				name: home?.name,
				region: home?.region,
				colors: home?.colors,
			};
			const a = {
				abbrev: away?.abbrev,
				name: away?.name,
				region: away?.region,
				colors: away?.colors,
			};
			const seed = String(gid ?? 0);
			const playoffs = !!boxScore?.playoffs;
			const looks = fanLooks(h, a, seed, fans, playoffs);
			const building: ArenaLooks | undefined = boxScore?.arena;
			return {
				data: bowlData,
				crowd: seatCrowd(bowlData, looks, paintFanPool(looks), seed, playoffs, {
					att: boxScore?.att,
					capacity: building?.capacity,
				}),
				seatColor: arenaSpecFor(h).stands.seat,
			};
		},
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid, fans, bowlData],
	);
	// (The finals trophy is on the floor, at center court, on every court -
	// see LiveCourt.)
	const painted = usePainted(paint, rafters, jumbotron, bowl);
	const paintRef = useRef(painted);
	paintRef.current = painted;

	// The players drawn from the body built in Blender (see meshBody.ts) -
	// nothing of the court drawn till it is here (the sculpted players kept
	// only for when it can't be had: offline, say).
	const [mesh, setMesh] = useState<"loading" | "ok" | "failed">(() =>
		meshReady() ? "ok" : "loading",
	);
	useEffect(() => {
		if (mesh !== "loading") {
			return;
		}
		let alive = true;
		loadMesh().then(
			() => {
				if (alive) {
					setMesh("ok");
				}
			},
			() => {
				if (alive) {
					setMesh("failed");
				}
			},
		);
		return () => {
			alive = false;
		};
	}, [mesh]);
	const real = mesh === "ok";
	const meshWait = useRef(mesh === "loading");
	meshWait.current = mesh === "loading";
	// The players' faces (see useFaces) - and each one's head drawn from his.
	const heads = useRef(
		new Map<number, { sprite?: HeadSprite; skin?: string }>(),
	);
	const { faces, facesVersion, onFace } = useFaces(
		looksThen,
		roster,
		(pid, face, current, bump) => {
			// A photo face is drawn as a silhouette - no head to load.
			if (face?.imgURL) {
				heads.current.delete(pid);
				return;
			}
			const team = roster.find((p) => p.pid === pid)?.team;
			const colors = team === 0 ? away?.colors : home?.colors;
			void (
				authored?.head
					? authored.head(pid, face?.face, face?.colors ?? colors)
					: loadHead(
							face?.face,
							face?.colors ?? colors,
							false,
							// (His face painted on a head of the mesh's own, if it
							// has one: the court's drawn head on him otherwise.)
							MESH_HEAD,
						)
			).then((head) => {
				if (current()) {
					heads.current.set(pid, head);
					bump();
				}
			});
		},
	);

	// (Each man's look kept from one time to the next unless what makes it
	// has changed: his pictures are kept by his look, and a new one for
	// everybody each time somebody's face came in had every one of them
	// made again.)
	const kept = useRef(
		new Map<number, { made: unknown[]; look: Look; body: Body }>(),
	);
	// Drawn as men: each face for his head too, for the heads drawn before
	// (see faces.ts).
	const [texVersion, setTexVersion] = useState(0);
	useEffect(() => {
		if (!real || !MESH_HEAD) {
			return;
		}
		let alive = true;
		void (async () => {
			for (const [pid, head] of heads.current) {
				const sprite = head.sprite;
				const f = faces.current.get(pid);
				if (!sprite || sprite.tex || !f?.face) {
					continue;
				}
				const team = roster.find((p) => p.pid === pid)?.team;
				try {
					const tex = await loadFaceTex(
						f.face,
						f.colors ?? (team === 0 ? away?.colors : home?.colors),
					);
					if (!alive) {
						return;
					}
					if (heads.current.get(pid) === head) {
						heads.current.set(pid, { ...head, sprite: { ...sprite, tex } });
						setTexVersion((v) => v + 1);
					}
				} catch {
					// (A face that will not draw: a plain head on him.)
				}
			}
		})();
		return () => {
			alive = false;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [real, facesVersion]);
	const dress = authored?.dress;
	// His season's shorts (see shortsEra.ts).
	const hem = shortsHemFor(season);
	const appearance = useMemo(() => {
		const looks = new Map<number, Look>();
		const bodies = new Map<number, Body>();
		for (const p of roster) {
			const f = faces.current.get(p.pid) ?? undefined;
			const head = heads.current.get(p.pid);
			const colors = headColors(f?.face);
			const team = p.team === 0 ? away : home;
			const kit = dressed[p.team as Side];
			const art = arts[p.team as Side];
			const crest = crests[p.team as Side];
			// (Drawn as a man: not a photo's silhouette, nor out hurt in his
			// street clothes.)
			// (Out hurt, in his street clothes, he is sculpted: the mesh is
			// a man in his uniform.)
			const man = real && !p.out;
			const made = [
				man,
				f,
				head,
				kit,
				art,
				crest,
				team,
				p.name,
				p.jerseyNumber,
				p.out,
				p.ratings,
				dress,
				hem,
			];
			const was = kept.current.get(p.pid);
			if (was && was.made.every((x, i) => x === made[i])) {
				looks.set(p.pid, was.look);
				bodies.set(p.pid, was.body);
				continue;
			}
			looks.set(p.pid, {
				kit,
				...(art ? { kitArt: art } : {}),
				...(crest ? { crest } : {}),
				gear: gearFor(p.pid, kit),
				...(f?.imgURL
					? // His face is a photo: a silhouette in the uniform.
						{
							skin: SILHOUETTE,
							hair: SILHOUETTE,
							cut: "short" as const,
							silhouette: true,
						}
					: {
							skin: head?.skin ?? colors.skin,
							hair: colors.hair,
							cut: colors.cut,
							style: colors.style,
							// In the colors his face is drawn in, so a headband is
							// the same one from every side.
							profile: profileOf(f?.face, f?.colors ?? team?.colors),
						}),
				jerseyNumber: f?.jerseyNumber ?? p.jerseyNumber ?? "",
				name: p.name ?? "",
				lastName: lastNameOf(p.name),
				// NBA style: the team's name at home, the city on the road.
				wordmark:
					kit.chestText ??
					(p.team === 1 ? team?.name : team?.region) ??
					team?.abbrev ??
					"",
				head: head?.sprite,
				...(man ? { real: true as const } : {}),
				// His shorts as long as they were worn that season.
				hem,
			});
			// Out hurt: in his street clothes - nothing of the uniform on him.
			if (p.out) {
				const look = looks.get(p.pid)!;
				delete look.kitArt;
				delete look.crest;
				looks.set(p.pid, {
					...look,
					...streetClothes(p.pid),
					jerseyNumber: "",
					lastName: "",
					wordmark: "",
					street: true,
				});
			}
			// (A scene's own clothes on him - see authored.ts.)
			if (dress) {
				looks.set(p.pid, dress(p.pid, looks.get(p.pid)!));
			}
			bodies.set(
				p.pid,
				bodyOf(
					f?.hgt,
					f?.weight,
					man ? MESH_BUILD : undefined,
					physiqueOf(p.ratings),
				),
			);
			kept.current.set(p.pid, {
				made,
				look: looks.get(p.pid)!,
				body: bodies.get(p.pid)!,
			});
		}
		return { looks, bodies };
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [
		roster,
		dressed,
		arts,
		crests,
		facesVersion,
		dress,
		real,
		texVersion,
		hem,
	]);
	// The officials, the coaches and the photographers, picked once a game,
	// and their heads drawn from their faces as they come.
	// (And a scene's own people - see authored.ts.)
	const crew = useMemo(
		() => [...crewFor(gid ?? 0, away, home), ...(authored?.cast ?? [])],
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);
	const crewHeads = useRef(
		new Map<number, { sprite?: HeadSprite; skin?: string }>(),
	);
	const [crewVersion, setCrewVersion] = useState(0);
	useEffect(() => {
		let alive = true;
		crewHeads.current = new Map();
		// (One at a time, each a task of its own: all of them at once held
		// up the first frames of the game. A scene of its own: only those in
		// it - see authored.ts.)
		const asked = authored?.crew;
		const cast = new Set((authored?.cast ?? []).map((m) => m.pid));
		const wanted = asked
			? crew.filter((m) => cast.has(m.pid) || asked(m.role))
			: crew;
		void (async () => {
			for (const m of wanted) {
				if (!alive) {
					return;
				}
				const team =
					m.role === "coach" ? (m.team === 0 ? away : home) : undefined;
				crewHeads.current.set(m.pid, await loadHead(m.face, team?.colors));
				await new Promise((resolve) => setTimeout(resolve, 0));
			}
			if (alive) {
				setCrewVersion((v) => v + 1);
				authored?.onCast?.();
			}
		})();
		return () => {
			alive = false;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [crew]);
	const crewLooks = useMemo(() => {
		const out = new Map<number, { look: Look; body: Body }>();
		for (const m of crew) {
			const head = crewHeads.current.get(m.pid);
			const c = headColors(m.face);
			out.set(m.pid, {
				look: {
					...m.dress,
					skin: head?.skin ?? c.skin,
					hair: c.hair,
					cut: c.cut,
					style: c.style,
					profile: profileOf(
						m.face,
						m.role === "coach"
							? (m.team === 0 ? away : home)?.colors
							: undefined,
					),
					jerseyNumber: "",
					name: "",
					lastName: "",
					wordmark: "",
					head: head?.sprite,
				},
				body: bodyOf(m.hgt, m.weight),
			});
		}
		return out;
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [crew, crewVersion]);
	// (Those of them a scene of its own has there - see authored.ts.)
	const crewAsked = authored?.crew;
	const sceneCrew = useMemo(
		() => (crewAsked ? crew.filter((m) => crewAsked(m.role)) : crew),
		[crew, crewAsked],
	);
	const crewRef = useRef({ crew, crewLooks, sceneCrew });
	crewRef.current = { crew, crewLooks, sceneCrew };

	// Read by the animation loop, which outlives any one render.
	const looks = useRef(appearance.looks);
	const bodies = useRef(appearance.bodies);
	looks.current = appearance.looks;
	bodies.current = appearance.bodies;

	// The picture's size: 16:9, or 4:3 on a phone so the players stay big.
	const wrapRef = useRef<HTMLDivElement | null>(null);
	const bugRef = useRef<HTMLDivElement | null>(null);
	// The score bug's clocks, fouls and possession marker (see writeBug).
	const bugRefs = useBugRefs();
	const canvasRef = useRef<HTMLCanvasElement | null>(null);
	// Drawn on the graphics card instead, or its frame rate shown (see
	// gpu/setting.ts): its canvas, and the frame rate over it.
	const [drawing] = useState(gpuMode);
	const gpuRef = useRef<HTMLCanvasElement | null>(null);
	const meterRef = useRef<HTMLDivElement | null>(null);
	const size = useViewSize(wrapRef);
	const narrow = size.w < NARROW_W;

	// The home floor as a picture, made from the 2D court's drawing of it -
	// at 20 pixels a foot, a whole-floor picture's own (see courtTemplate):
	// at 10 its planks ran together, the floor coarser than the frame round
	// it at the finest.
	const floorRef = useRef<HTMLDivElement | null>(null);
	const courtPicture = useRef<CourtPicture | undefined>(undefined);
	// A floor picture's edge color, the floor carried on out in it.
	const floorEdge = useRef<string | undefined>(undefined);
	// (A floor picture is laid out once its shape is known: see useWideFloor.)
	const floorWide = useWideFloor(home?.court?.floorImageURL || undefined);
	useEffect(() => {
		courtPicture.current = undefined;
		floorEdge.current = undefined;
		const svg = floorRef.current?.querySelector("svg");
		if (!svg || floorWide === undefined) {
			return;
		}
		let alive = true;
		void courtTexture(svg, 20).then((pic) => {
			if (alive) {
				courtPicture.current = pic;
				floorEdge.current =
					pic && home?.court?.floorImageURL ? edgeColor(pic.canvas) : undefined;
			}
		});
		return () => {
			alive = false;
		};
	}, [
		gid,
		home?.court,
		home?.colors,
		home?.imgURL,
		boxScore?.finals,
		floorWide,
	]);
	const dipRef = useRef<HTMLDivElement | null>(null);
	const flashRef = useRef<HTMLDivElement | null>(null);
	const replayRef = useRef<HTMLDivElement | null>(null);
	// (One for each side, [visitors, home]: the one whose play it is shows.)
	const stingRefs = useRef<[HTMLDivElement | null, HTMLDivElement | null]>([
		null,
		null,
	]);

	const play = useRef({
		t: 0,
		camX: COURT_W / 2,
		camW: 60,
		// (How fast the camera is panning and zooming, feet a second.)
		camVX: 0,
		camVW: 0,
		// Off the floor's own framing (a replay): how far across the floor
		// it looks and how high, and how fast those are moving.
		camY: 25,
		camZ: 2,
		camVY: 0,
		camVZ: 0,
		snapCam: true,
		// Panned clear of the score bug (see dodgeFor): by how much (feet),
		// which way last, its box (measured now and then), and the man with
		// the ball last and when (game ms).
		dodge: 0,
		dodgeV: 0,
		dodgeSide: 0,
		covers: [] as Cover[],
		coversAt: -Infinity,
		caption: undefined as Box | undefined,
		heldBy: undefined as number | undefined,
		heldAt: -Infinity,
		last: undefined as number | undefined,
		readyFor: -1,
		stepping: false,
		// Key plays: the cursor the page is catching up to (-1 none), and when
		// the picture last came up out of black onto the next one (ms).
		jumpTo: -1,
		keyDip: -Infinity,
		keyHold: -Infinity,
		// When the picture last went as good as black (a dip at a cut, into
		// or out of a replay, onto the next key play), and the moment drawn
		// last frame (timeline ms) - for how slowly poses come round.
		darkFrom: -Infinity,
		drawnAt: undefined as number | undefined,
		// The game moment worked out last frame, and for how many frames more
		// it is worked out as just cut to (see fromCut).
		evalAt: -Infinity,
		cutLeft: 0,
		// The timeline last drawn (the warm-ups' or the game's).
		shown: undefined as CourtTimeline | undefined,
		prevCursor: -1,
		prevPaused: paused,
		prevSkips: skips,
		// Following another device's sim: where its court is estimated to be
		// (see follow.ts).
		lead: 0,
		// When playing along last cut back (see alignTo), and whether it has
		// given up on that for stepping line by line.
		slips: undefined as number[] | undefined,
		lost: false,
		// How much of the show has been played (ms): the game played through
		// at its own pace, and the replays, but none of what is cut past. Two
		// devices playing the same game the same way agree on it, so it is
		// how one plays along with the other (see follow.ts).
		w: 0,
		pace: 1,
		paceAt: -1,
		// What the bug was last written (see writeBug).
		bug: bugShown(),
		// Which of its cameras the replay showing is seen from (see
		// replayRig): a cut to the other is straight there, through a flash
		// (when it went off, and when it was last held).
		replayRig: undefined as Rig | undefined,
		flashAt: -Infinity,
		flashHold: -Infinity,
		// The dunk replay showing now, and the next dunk that would get one.
		replay: undefined as Replay | undefined,
		nextReplay: 0,
		replayDue: undefined as number | undefined,
		// The wipe into a replay and out of it, and whose play it is.
		wipe: makeWipe(),
		wipeSide: 1 as Side,
		// The starting lineups: the man being called (his place in the
		// order, -1 none), and whether they are on.
		introCall: -1,
		introOn: false,
		introTitle: false,
		// In close on a man (see closeUpAim).
		closeOn: false,
		// A scene of its own (see authored.ts): the run of it showing, and
		// whether it has got to its end.
		authoredRun: undefined as number | undefined,
		authoredOver: false,
		// No period on: the final horn gone, or between two (see isFinal,
		// isBreak).
		ended: false,
		// The last two minutes or overtime (see finalStretchAt).
		late: false,
	});
	const [intro, setIntro] = useState({ call: -1, on: false, title: false });
	// No period on, in the picture - the final horn gone, or between two: the
	// bug says so (see the frame loop).
	const [ended, setEnded] = useState(false);
	// From two minutes left in the last period: the bug's timeouts are the
	// game's own (see finalStretchAt).
	const [late, setLate] = useState(false);
	// Onto the game from the warm-ups (see pregame): a cut, up out of black
	// (see tick). (Not on into the next part of it: the same game.)
	const snappedTo = useRef<Staged | undefined>(undefined);
	useEffect(() => {
		if (timeline) {
			if (!snappedTo.current || !goesOn(snappedTo.current, timeline)) {
				play.current.snapCam = true;
			}
			snappedTo.current = timeline;
		}
	}, [timeline]);

	// Follow the page's cursor (see playback.ts).
	useEffect(() => {
		if (timeline) {
			onCursor(play.current, timeline, { cursor, paused, skips, follower });
		}
	}, [cursor, follower, paused, skips, timeline]);

	const alongWith = useRef(makeFollow());
	useEffect(() => {
		if (along) {
			addReport(alongWith.current, along);
		}
	}, [along]);
	// Playing along with a court that staged something else: stepped line
	// by line instead, said once.
	const lostSaid = useRef(false);
	useEffect(() => {
		if (along?.fp && fp && heardAs(along.fp).fp !== fp && !lostSaid.current) {
			lostSaid.current = true;
			onLost?.();
		}
	}, [along?.fp, fp, onLost]);

	const homePad = home?.colors?.[0] ?? "#8c1d40";
	const lineColor: string =
		home?.court?.floorImageURL && home.court.floorLines === "picture"
			? ""
			: home?.court?.lines || "#f8f5f0";
	const apron: string = home?.court?.apron || homePad;
	const warmups = useMemo(
		(): [string, string] => [
			shade(kits[0].jersey, -0.3),
			shade(kits[1].trim, -0.2),
		],
		[kits],
	);
	// Each side's colors [road, home], for the lights at the starting lineups.
	const teamColors = useMemo(
		(): [string, string][] =>
			[away, home].map((t): [string, string] => [
				t?.colors?.[0] ?? "#ffffff",
				t?.colors?.[1] ?? "#ffffff",
			]),
		[away, home],
	);
	const rosterRef = useRef(roster);
	rosterRef.current = roster;
	const stopAt = useMemo(
		() =>
			until !== undefined && timeline ? clipStopAt(timeline, until) : undefined,
		[until, timeline],
	);
	// THE GAME'S SOUND, with sound on on this device (see sound/): the
	// building's night - a playoff game, as full as the gate says - and
	// the sounds fetched once it is on.
	const soundOn = useSound() && !preview && !authoredProp;
	const sound = useRef<GameSound | undefined>(undefined);
	const playoffsNight = !!boxScore?.playoffs;
	const attNight: number | undefined = boxScore?.att;
	const capacityNight: number | undefined = boxScore?.arena?.capacity;
	useEffect(() => {
		if (!soundOn) {
			return;
		}
		const ctx = wakeSound();
		if (!ctx) {
			return;
		}
		const awake = keepSoundAwake();
		const g = new GameSound(ctx, ctx.destination, {
			playoffs: playoffsNight,
			full:
				attNight !== undefined && capacityNight
					? Math.min(1, attNight / capacityNight)
					: 0.93,
		});
		sound.current = g;
		let gone = false;
		void loadBank(ctx).then((bank) => {
			if (!gone) {
				g.setBank(bank);
			}
		});
		return () => {
			gone = true;
			awake();
			g.dispose();
			sound.current = undefined;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [soundOn, gid]);

	const live = useRef({
		cursor,
		paused,
		rate,
		follower,
		along: !!along,
		report,
		onReady,
		onJump,
		onLost,
		keyPlan,
		fp: saidAs(fp, whole),
		whole,
		stopAt,
		timeline,
		pregame,
		clocks,
		fouls,
		size,
		narrow,
		homePad,
		lineColor,
		apron,
		warmups,
		teamColors,
		setIntro,
		setEnded,
		setLate,
		numPeriods,
		// The period, as the page's box score has it ("Q4", "OT").
		period: boxScore?.quarterShort ?? "",
		// The score [road, home], as the score bug has it.
		score: [boxScore?.teams?.[1]?.pts ?? 0, boxScore?.teams?.[0]?.pts ?? 0] as [
			number,
			number,
		],
		eventsLength: stagedLength(events, timeline),
		preview,
		authored,
	});
	live.current = {
		cursor,
		paused,
		rate,
		follower,
		along: !!along,
		report,
		onReady,
		onJump,
		onLost,
		keyPlan,
		fp: saidAs(fp, whole),
		whole,
		stopAt,
		timeline,
		pregame,
		clocks,
		fouls,
		size,
		narrow,
		homePad,
		lineColor,
		apron,
		warmups,
		teamColors,
		setIntro,
		setEnded,
		setLate,
		numPeriods,
		// The period, as the page's box score has it ("Q4", "OT").
		period: boxScore?.quarterShort ?? "",
		// The score [road, home], as the score bug has it.
		score: [boxScore?.teams?.[1]?.pts ?? 0, boxScore?.teams?.[0]?.pts ?? 0] as [
			number,
			number,
		],
		eventsLength: stagedLength(events, timeline),
		preview,
		authored,
	};

	// A highlight tapped: that play again, the game held while it shows -
	// not on a court following another device's, which shows what that one
	// does.
	const askedN = replayAsk?.n;
	useEffect(() => {
		if (askedN === undefined || !replayAsk || !timeline) {
			return;
		}
		if (follower && !along) {
			return;
		}
		const r = askedReplay(timeline, replayAsk.cursor);
		if (!r) {
			return;
		}
		const s = play.current;
		s.replay = r;
		s.snapCam = true;
		s.paceAt = -1;
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [askedN]);

	useEffect(() => {
		// The picture is drawn small - pixel art - and the page blows it up
		// without smoothing (see the canvas's style).
		const scratch = makeScratch();
		// All of it on the graphics card instead, if this device is to (see
		// gpu/setting.ts) and has one - and the frame rate over it.
		const drawn = gpuMode();
		let gpu =
			drawn === "test" && gpuRef.current ? gpuScene(gpuRef.current) : undefined;
		const meter = drawn === "off" ? undefined : makeMeter();
		const gpuLaid = { w: 0, h: 0 };
		// Hands off the page's own thread to make new pictures on, where the
		// device has them (see farm.ts).
		const farm =
			gpu ||
			typeof Worker === "undefined" ||
			typeof OffscreenCanvas === "undefined"
				? undefined
				: makeFarm(
						spawnCourtWorker,
						// (Two on a phone of four cores or more: a man drawn as one
						// takes a few times as long to pose as the court's own.)
						(navigator.hardwareConcurrency || 2) >= 4 ? 2 : 1,
					);
		let sprites = makeSpriteCache();
		sprites.budget = SPRITE_BUDGET_MS;
		sprites.farm = farm;
		// When finer poses were last taken up, and last given up for want of
		// time.
		const fine = makeFine();
		// How fine the picture is: as fine as this device can draw it.
		const res = makeResolution(performance.now() + 1500);
		// How often it is drawn (see pace.ts) - and whether this frame says
		// anything about how fast the device draws (not held back, paused).
		const pacing = makePace(
			performance.now() + 1500,
			typeof matchMedia !== "undefined" &&
				matchMedia("(pointer: coarse)").matches,
		);
		// (Drawn on the card, a test: at 60 whatever, for the readout to say
		// whether it keeps up.)
		pacing.fixed = gpu !== undefined;
		// The size the canvas is laid on the page at (CSS px).
		const laid = { w: 0, h: 0 };
		const lookOf = (pid: number) => looks.current.get(pid)!;
		const bodyOfPid = (pid: number) => bodies.current.get(pid) ?? bodyOf();

		const tick = (now: number, draw: boolean, still: boolean) => {
			const began = performance.now();
			const p = live.current;
			// (Whole possessions, or on to the action - see pacing.ts.)
			showWhole(p.whole);
			// (Nothing till the players' body is here - see mesh.)
			if (meshWait.current) {
				return;
			}
			// (Still being staged: the building in the meantime - see pregame.)
			const staging = !p.timeline;
			const tl = p.timeline ?? p.pregame?.tl;
			if (!tl) {
				return;
			}
			const s = play.current;
			// On into the next part of the game, played up to where the part
			// before ends (see onTl) - or, gone on past it, from the moment
			// it is in. (Cut to where the page is, if that is further on: see
			// onCursor.)
			const more = nextTl.current;
			const seam =
				more && p.timeline
					? more.seams?.find((x) => x.at >= p.timeline!.end - 1)
					: undefined;
			if (
				more &&
				!staging &&
				p.timeline &&
				goesOn(p.timeline, more) &&
				(s.t >= (seam?.since ?? p.timeline.end - 1) ||
					p.cursor > (p.timeline.upTo ?? Infinity))
			) {
				nextTl.current = undefined;
				carryFaces(p.timeline, more, seam?.since ?? p.timeline.end);
				s.prevCursor = Math.min(s.prevCursor, p.timeline.upTo ?? Infinity);
				p.timeline = more;
				p.eventsLength = stagedLength(eventsRef.current, more);
				onCursor(s, more, {
					cursor: p.cursor,
					paused: p.paused,
					skips: s.prevSkips,
					follower: p.follower,
				});
				setOnTl(more);
				return;
			}
			// Up out of black onto the first picture, and onto the game from
			// the warm-ups - from this frame, before anybody in it is drawn.
			// (Not on into the next part of it.)
			if (s.shown !== tl) {
				const same = !!s.shown && goesOn(s.shown, tl);
				s.shown = tl;
				drawnTl.current = tl;
				if (!same) {
					s.keyDip = now;
					s.keyHold = -Infinity;
				}
			}
			// A scene of its own (see authored.ts): from the top each run of
			// it, and held where it is while paused.
			if (p.authored) {
				if (p.authored.run !== s.authoredRun) {
					s.authoredRun = p.authored.run;
					s.authoredOver = false;
					s.t = 0;
					s.snapCam = true;
					s.keyDip = now;
				}
				if (p.paused) {
					s.last = now;
				}
			}
			const { dt, rate, cut } = stepPlay(
				s,
				p,
				alongWith.current,
				tl,
				staging,
				now,
				Date.now(),
				p.authored ? Infinity : undefined,
			);
			sound.current?.tick({
				tl,
				t: s.t,
				rate,
				on: !p.paused && !s.replay,
				camX: s.camX,
			});
			if (!staging && tl.clips && onCutRef.current) {
				const k = tl.clips.findLastIndex((c) => c <= s.t);
				if (k !== cutSeen.current) {
					cutSeen.current = k;
					if (k >= 0) {
						onCutRef.current(k);
					}
				}
			}
			// (And held on its last moment.)
			if (p.authored && staging && s.t >= p.authored.ms) {
				s.t = p.authored.ms;
				if (!s.authoredOver) {
					s.authoredOver = true;
					p.authored.onEnd?.();
				}
			}
			if (p.authored && staging) {
				p.authored.onTick?.(s.t);
			}
			// A look at the court: the warm-ups over, round again from the top.
			if (p.preview && staging && s.t >= WARMUP_MS) {
				s.t = 0;
				s.snapCam = true;
				s.keyDip = now;
			}
			if (s.lost && p.along && !lostSaid.current) {
				lostSaid.current = true;
				p.onLost?.();
			}
			if (!draw) {
				return;
			}

			const canvas = canvasRef.current;
			const ctx = canvas?.getContext("2d");
			if (!canvas || !ctx) {
				return;
			}
			const { w, h } = p.size;
			if (w <= 0 || h <= 0) {
				return;
			}
			// On the graphics card - the game itself only, not a scene of its
			// own (draft night, a press conference: see authored.ts).
			const card = !p.authored ? gpu : undefined;
			const gpuCanvas = gpuRef.current;
			if (gpuCanvas) {
				const shown = card ? "visible" : "hidden";
				if (gpuCanvas.style.visibility !== shown) {
					gpuCanvas.style.visibility = shown;
				}
			}
			// The picture: fine (see resolution.ts), each of its pixels a whole
			// number of the screen's.
			const { art, dpr, fw, fh } =
				card && gpuCanvas
					? sizeCanvas(gpuCanvas, p.size, res, gpuLaid)
					: sizeCanvas(canvas, p.size, res, laid);

			const replay = s.replay;
			// Cut to (on to the next key play, back into a replay, on past
			// what is skipped): worked out afresh round there for a few
			// frames, not carried on from what was shown before (see fromCut).
			const at = replay ? replay.at : s.t;
			if (at < s.evalAt || at > s.evalAt + CUT_JUMP) {
				s.cutLeft = CUT_FRAMES;
			}
			const cold = s.cutLeft > 0;
			const evalFrom = performance.now();
			const moment =
				staging && p.authored
					? p.authored.moment(s.t, bodyOfPid)
					: staging && p.pregame
						? warmupMoment(p.pregame, s.t, bodyOfPid)
						: momentAt(
								tl,
								at,
								rosterRef.current,
								bodyOfPid,
								replay ? undefined : { rate, wall: now },
								cold,
							);
			if (!staging) {
				s.evalAt = at;
				s.cutLeft = Math.max(0, s.cutLeft - 1);
			}
			// (For the frame rate meter: the moment worked out.)
			const evalMs = performance.now() - evalFrom;
			// Over a break, a look round the building; in close on a man at the
			// line; at the starting lineups, each man called.
			const arenaView = replay ? undefined : arenaAim(tl, s.t, p.narrow);
			const closeView =
				replay || arenaView ? undefined : closeUpAim(tl, moment, p.narrow);
			const view = replay
				? undefined
				: staging && p.authored
					? p.authored.aim(s.t, p.narrow, moment)
					: (arenaView ?? closeView ?? introAim(tl, moment, p.narrow));
			// (The timeline's time: a scene of its own keeps its own clock -
			// see pregameShow's.)
			const tNow = staging && p.authored ? moment.t : s.t;
			const on = !!tl.intro && tNow >= tl.intro.t0 && tNow < tl.intro.t1;
			const called = callAt(tl.intro, tNow);
			const k = called ? tl.intro!.calls.indexOf(called) : -1;
			// The title, just before the first name.
			const firstCall = tl.intro?.calls[0]?.t0 ?? 0;
			const title =
				on &&
				tNow < firstCall &&
				tNow > firstCall - TITLE_MS &&
				tNow - tl.intro!.t0 > 250;
			if (on !== s.introOn || k !== s.introCall || title !== s.introTitle) {
				s.introOn = on;
				s.introCall = k;
				s.introTitle = title;
				p.setIntro({ call: k, on, title });
			}
			// Following the play, the whole floor stays in the picture.
			const fit = replay || view ? undefined : courtFit(fw / fh);
			const aim = replay
				? replayAim(moment, replay, fw / fh)
				: (view?.shot ??
					(staging
						? warmupAim(s.t, p.narrow, fit)
						: zoomedOn(
								aimFor(moment, p.narrow, tl, fit),
								getCamera().zoom,
								fit,
								moment.ball,
								midAcross(moment.players),
								rimX(offenseAt(tl, s.t)),
							)));
			const rigNow = replay ? replayRig(replay) : undefined;
			if (rigNow !== s.replayRig) {
				if (replay && s.replayRig) {
					s.snapCam = true;
					s.flashAt = now;
					s.flashHold = -Infinity;
				}
				s.replayRig = rigNow;
			}
			// (In close on a man and back out: straight there, a cut.)
			if (!!closeView !== s.closeOn) {
				s.closeOn = !!closeView;
				s.snapCam = true;
			}
			// (A scene of its own moves its cameras itself, and cuts between
			// them: straight there, every frame.)
			const snap = s.snapCam || (staging && !!p.authored);
			const secs = (dt / 1000) * Math.min(24, Math.max(1, rate));
			// (Following the play, the shot held through its little moves - see
			// operate.)
			operate(s, aim, fit, secs, snap, fit ? s.camW * CAM_HOLD : 0);
			s.snapCam = false;
			if (now - s.coversAt > 500) {
				s.coversAt = now;
				s.covers = coversOf(wrapRef.current, bugRef.current);
				s.caption = boxOf(wrapRef.current, captionOf(wrapRef.current));
			}
			// The man with the ball (or the ball, loose or in the air) never
			// left behind the score bug: the camera eased over off him.
			let dodge = 0;
			const block = s.covers[0];
			if (fit && !staging && block) {
				const { ball } = moment;
				const who =
					(ball.holder === undefined
						? undefined
						: moment.players.find((pl) => pl.pid === ball.holder)) ?? ball;
				const plain = makeCamera(
					{ x: s.camX, width: s.camW, y: fit.y(s.camW) },
					fw,
					fh,
					MAIN_RIG,
				);
				[dodge, s.dodgeSide] = dodgeFor(plain, who, block, s.dodgeSide);
			}
			if (snap) {
				s.dodge = dodge;
				s.dodgeV = 0;
			} else {
				[s.dodge, s.dodgeV] = follow(s.dodge, s.dodgeV, dodge, CAM_PAN, secs);
			}
			const cam = makeCamera(
				{
					x: s.camX + s.dodge,
					width: s.camW,
					y: s.camY,
					z: s.camZ,
				},
				fw,
				fh,
				replay ? replayRig(replay) : (view?.rig ?? MAIN_RIG),
			);

			// While the picture is as good as black - dipped at a cut, into a
			// replay or out of it, onto the next of the key plays - whoever has
			// no picture yet is left out of it (see SpriteCache.dark), and it
			// is held black until everybody in it is ready to be drawn: a
			// moment more of black, not a freeze halfway out of it.
			// (Behind the wipe: the wipe held over all of the picture.)
			// (Or white, at a flash between a replay's angles.)
			const flash = holdReady(s, sprites, s.wipe, !!replay, s.flashAt, now);
			// The wipe: in over the picture to the cut into a replay or out of
			// it, held there while need be, then on off it.
			const sting = wipeAt(s.wipe, s, now, p.rate, rate);
			// (Whose play it is: the man who made the one in the replay, or the
			// one it is about to show - kept through the wipe back out of it.)
			if (sting !== undefined) {
				const pid = s.replay
					? s.replay.pid
					: s.replayDue !== undefined
						? lastHolder(tl, s.replayDue - REPLAY_AFTER)
						: undefined;
				const team =
					pid === undefined
						? undefined
						: rosterRef.current.find((r) => r.pid === pid)?.team;
				if (team !== undefined) {
					s.wipeSide = team;
				}
			}
			// (And between clips of a highlight reel, and the key plays: down
			// to black out of one, up out of it into the next.)
			const o = dipOf({
				replay: !!replay,
				near: cut === undefined ? Infinity : Math.abs(s.t - cut),
				keyDip: Math.max(s.keyDip, s.keyHold),
				now,
				keyOut:
					p.keyPlan && !p.follower && !p.paused
						? keyOutAt(p.keyPlan, s.t, s.pace)
						: 0,
				also: clipDipAt(tl, s.t),
			});
			darkAndPace(
				s,
				sprites,
				o,
				flash,
				s.wipe,
				replay ? replay.at : s.t,
				dt,
				now,
			);
			setOpacity(dipRef.current, o);
			setOpacity(flashRef.current, flash);
			const ballAt = project(cam, moment.ball);
			showCaption(
				captionOf(wrapRef.current),
				s.caption,
				{ x: ballAt.x / fw, y: ballAt.y / fh },
				!!replay,
			);
			showWipe(
				stingRefs.current,
				replayRef.current,
				sting,
				s.wipeSide,
				!!replay,
				w,
				h,
			);
			// The crowd on its feet after a big play, arms going up and out -
			// and the whole way through a tight finish.
			const crowd = crowdAt(tl, moment.t, now);

			let shotText = "";
			// (The bug's: in its last seconds, tenths - see shotClockText.)
			let bugShot = "";
			let clockText = "";
			// (Over: FINAL where the clock was, and no period; between two,
			// END Q1 - or HALF.)
			const final = !staging && isFinal(tl, s.t);
			const between = !staging && !final && isBreak(tl, s.t);
			const ended = final || between;
			if (ended !== s.ended) {
				s.ended = ended;
				p.setEnded(ended);
			}
			const late =
				!staging && !!p.clocks && finalStretchAt(p.clocks, s.t, p.numPeriods);
			if (late !== s.late) {
				s.late = late;
				p.setLate(late);
			}
			if (final) {
				clockText = finalText(p.period);
			} else if (between) {
				clockText = breakText(p.period, p.numPeriods);
			} else if (p.clocks) {
				const game = gameClockAt(p.clocks, s.t);
				const shot = shotClockAt(p.clocks, s.t);
				const elam = p.clocks.elam;
				// (An Elam Ending: no clock - the score to play to.)
				clockText =
					elam && s.t >= elam.t
						? `TARGET ${elam.target}`
						: game === undefined || !Number.isFinite(game)
							? ""
							: formatGameClock(game);
				shotText = shot === undefined ? "" : String(Math.ceil(shot - 1e-6));
				bugShot = shot === undefined ? "" : shotClockText(shot);
			}
			// (The way the ball faces worked out a little ahead, each frame -
			// see warmFaces.)
			const warmFrom = performance.now();
			if (!staging) {
				warmFaces(tl, 1);
			}
			const warmMs = performance.now() - warmFrom;
			const pt = paintRef.current;
			const cr = crewRef.current;
			const working = crewAt(
				tl,
				moment.t,
				p.authored?.crew ? cr.sceneCrew : cr.crew,
			);
			const drawStart = performance.now();
			const frame: Frame = {
				ctx,
				scratch,
				sprites,
				cam,
				// (The ball in an official's hands at the height he has it.)
				moment: working.ballLift
					? {
							...moment,
							ball: { ...moment.ball, z: moment.ball.z + working.ballLift },
						}
					: moment,
				tl,
				roster: rosterRef.current,
				bodyFor: bodyOfPid,
				// (A scene of its own changing what somebody wears as it goes -
				// a draft pick's cap put on: see authored.ts.)
				lookFor: p.authored?.lookAt
					? (pid: number) => p.authored!.lookAt!(pid, moment.t, lookOf(pid))
					: lookOf,
				padColor: p.homePad,
				lineColor: p.lineColor,
				apron: floorEdge.current ?? p.apron,
				warmups: p.warmups,
				shotClock: shotText,
				score: staging ? undefined : p.score,
				clock: clockText,
				period: p.period,
				arena: {
					court: courtPicture.current,
					crowd: pt.crowd,
					courtside: pt.courtside,
					table: pt.scorersTable,
					stanchion: pt.stanchion,
					stanchionAds: pt.stanchionAds,
					boards: pt.boards,
					jumbotron: pt.jumbotron,
					rafters: pt.rafters,
					tableTop: pt.tableTop,
					tableFront: pt.tableFront,
					bench: [pt.bench0, pt.bench1],
					...(pt.bowl ? { bowl: pt.bowl } : {}),
				},
				crowd,
				now,
				// Lettering about 8 CSS pixels tall: a 7-pixel font, each of its
				// pixels this many picture pixels.
				textScale: Math.max(1, Math.round(8 / ((7 * art) / dpr))),
				tagClear: s.covers,
				away: !!view || !!replay,
				...(closeView ||
				(staging && p.authored && (view as { close?: boolean })?.close)
					? { soft: CLOSE_SOFT }
					: {}),
				crew: [
					...working.states,
					...(staging && p.authored?.castAt ? p.authored.castAt(moment.t) : []),
				].flatMap((st) => {
					const c = cr.crewLooks.get(st.pid);
					return c ? [{ st, body: c.body, look: c.look }] : [];
				}),
				...(staging && p.authored?.venue ? { venue: p.authored.venue } : {}),
				flashes: p.authored?.flashes
					? [...working.flashes, ...p.authored.flashes(moment.t)]
					: working.flashes,
				teamColors: p.teamColors,
				black: o >= 0.999 || flash >= 0.999 || s.wipe.hold === now,
			};
			if (card && !drawGpu(card, frame, fw, fh)) {
				// (The card lost: today's drawing from the next frame.)
				gpu = undefined;
				pacing.fixed = false;
			} else if (!card) {
				drawFrame(frame);
			}
			if (meter) {
				const done = performance.now();
				const game = drawStart - began;
				meterTick(
					meter,
					meterRef.current,
					now,
					done - began,
					card
						? {
								// (The game's part: the moment, the way the ball faces
								// worked out ahead, the rest.)
								eval: evalMs,
								warm: warmMs,
								game: game - evalMs - warmMs,
								...card.ms,
							}
						: { game, draw: done - drawStart },
					{
						gpu: drawn !== "test" || (gpu && !card) ? undefined : !!card,
						fps: pacing.fps,
						w: fw,
						h: fh,
					},
				);
			}
			if (
				!still &&
				!card &&
				adjust(
					res,
					h * dpr,
					dt,
					performance.now() - drawStart,
					now,
					sprites.spent,
					1000 / pacing.fps,
					meshReady() ? MESH_FIXED : 0,
				)
			) {
				// Sprites drawn for the old size are no use at the new one - let
				// go of now.
				releaseSprites(sprites);
				sprites = makeSpriteCache();
				sprites.budget = SPRITE_BUDGET_MS;
				sprites.farm = farm;
			}
			// (What the court is doing on this device, for a capture of the
			// log - see court3dReport.)
			doing.rows = fh;
			doing.art = art;
			doing.dpr = dpr;
			doing.coarser = res.coarser;
			doing.changes = res.changes;
			doing.drawMs = res.drawMs;
			doing.fine = sprites.fine === true;
			doing.fps = pacing.fps;
			doing.kept = sprites.kept.size;
			// Time to spare, at the finest picture: poses in finer steps.
			sprites.fine = fineNow(
				sprites.fine === true,
				fine,
				res,
				pacing.fps === FAST,
				now,
				FINE_ON_MS,
			);

			// Each side's fouls this period - or BONUS - in the bug's order,
			// visitors first (the marks are in box score order, home first).
			const f = p.fouls ? foulsAt(p.fouls, s.t) : undefined;
			writeBug(bugRefs, s.bug, {
				clock: clockText,
				shot: bugShot,
				low: bugShot !== "" && Number(bugShot) < SHOT_LOW,
				fouls:
					f &&
					(([1, 0] as const).map((k) =>
						f.bonus[k] ? "BONUS" : `FOULS ${f.fouls[k]}`,
					) as [string, string]),
				counts: f && [f.fouls[1], f.fouls[0]],
				bonus: f && [f.bonus[1], f.bonus[0]],
				ball: offenseAt(tl, s.t),
			});
		};

		const stop = runFrames({
			canvas: canvasRef.current,
			pacing,
			still: () => {
				const s = play.current;
				return live.current.paused && !s.stepping && !s.replay;
			},
			tick,
		});
		return () => {
			stop();
			releaseSprites(sprites);
			farm?.stop();
		};
	}, [bugRefs]);

	// The score off the live box score, never off `away`/`home`: on a replay
	// those are copies (that night's looks laid over each team), made once when
	// the page opened - at 0-0 - and the box score's teams are updated in place
	// as the game plays, so nothing ever tells the copies to refresh.
	// The starting lineups: the card for the man being called, and the way
	// past them.
	const introCard = useMemo(() => {
		const c = intro.call >= 0 ? timeline?.intro?.calls[intro.call] : undefined;
		if (!c) {
			return undefined;
		}
		const p = roster.find((r) => r.pid === c.pid);
		const team = c.team === 0 ? away : home;
		const f = faces.current.get(c.pid) ?? undefined;
		return {
			name: p?.name ?? "",
			jerseyNumber: f?.jerseyNumber ?? p?.jerseyNumber,
			pos: p?.pos,
			hgt: f?.hgt,
			colors: team?.colors,
			imgURL: team?.imgURL,
			imgURLSmall: team?.imgURLSmall,
			abbrev: team?.abbrev,
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [intro.call, timeline, roster, facesVersion]);
	// The matchup board over the show (see ShowCards).
	const showInfo = useMemo((): ShowCardsInfo | undefined => {
		if (!showPlan) {
			return undefined;
		}
		const bt = boxScore?.teams;
		const info = boxScore?.gamecast;
		const real =
			bt?.[0] &&
			bt?.[1] &&
			bt[0].tid >= 0 &&
			bt[1].tid >= 0 &&
			!boxScore?.exhibition;
		// (Over it, a playoff game's series; under it, the line, the total and
		// the season series, as the header has them.)
		const series =
			real && boxScore?.playoffs
				? seriesLine(info, true, [bt[1], bt[0]])
				: undefined;
		const facts = real
			? matchupFacts(boxScore).filter((x) => x !== series && x !== "Play-In")
			: [];
		const tape: GamecastInfo["tape"] = info?.tape;
		const tapeOf = (tid: number | undefined) =>
			tape?.find((x) => x?.tid === tid);
		const road = tapeOf(bt?.[1]?.tid);
		const homeTape = tapeOf(bt?.[0]?.tid);
		return {
			teams: [
				bugTeamOf(away, 0, bt?.[1], standingOf(info, bt?.[1]?.tid)),
				bugTeamOf(home, 0, bt?.[0], standingOf(info, bt?.[0]?.tid)),
			],
			...(road && homeTape ? { tape: [road, homeTape] } : {}),
			...(leaders
				? {
						leaders: leaders.map((l) => {
							const p = roster.find((r) => r.pid === l.pid);
							const f = faces.current.get(l.pid) ?? undefined;
							return {
								name: p?.name ?? l.name,
								jerseyNumber: f?.jerseyNumber ?? p?.jerseyNumber,
								pts: l.pts,
								trb: l.trb,
								ast: l.ast,
							};
						}) as [ShowLeader, ShowLeader],
					}
				: {}),
			title: series ?? (boxScore?.playoffs ? "Playoffs" : "Regular season"),
			facts,
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [showPlan, roster, away, home, leaders, facesVersion]);
	const skipIntro = useCallback(() => {
		const s = play.current;
		const end = timeline?.intro?.t1;
		if (end !== undefined && s.t < end) {
			s.t = end;
			s.snapCam = true;
		}
	}, [timeline]);

	const awayPts: number = boxScore?.teams?.[1]?.pts ?? 0;
	const homePts: number = boxScore?.teams?.[0]?.pts ?? 0;
	// (No period on: FINAL, or END Q1, where the clock was - see the frame
	// loop - and no period.)
	const quarter = ended ? "" : (boxScore?.quarterShort ?? "");
	// Who made the play the caption describes: the same team the Plays list
	// marks with a logo. Taken off `raw` so a replay shows that night's logo.
	const captionTeam = captionT === undefined ? undefined : raw[captionT];

	return (
		<PictureBox
			wrapRef={wrapRef}
			narrow={narrow}
			roster={roster}
			looksThen={looksThen}
			season={season}
			lid={lid}
			onFace={onFace}
		>
			{/* The home floor as the 2D court draws it, out of sight: the 3D court
			    makes its picture of the floor from it (see courtTexture). */}
			<div
				ref={floorRef}
				aria-hidden
				style={{
					position: "fixed",
					left: -10000,
					top: 0,
					width: 1664,
					height: 880,
					visibility: "hidden",
					pointerEvents: "none",
				}}
			>
				<LiveCourt
					scene={undefined}
					teams={[away, home]}
					finals={!!boxScore?.finals}
					season={season}
					sceneMs={undefined}
				/>
			</div>
			<PictureCanvas canvasRef={canvasRef} label="Live court" />
			{drawing === "test" ? (
				<PictureCanvas canvasRef={gpuRef} label="Live court" />
			) : null}
			{drawing !== "off" ? (
				<div
					ref={meterRef}
					aria-hidden
					style={{
						position: "absolute",
						left: 4,
						top: 4,
						zIndex: 5,
						padding: "1px 5px",
						font: "10px/1.4 monospace",
						color: "#fff",
						background: "rgba(0,0,0,0.6)",
						borderRadius: 3,
						maxWidth: "calc(100% - 8px)",
						pointerEvents: "none",
					}}
				/>
			) : null}
			<Overlays
				dipRef={dipRef}
				flashRef={flashRef}
				stingRefs={stingRefs}
				replayRef={replayRef}
				teams={[away, home]}
				logos={[awayLogo, homeLogo]}
				// (Still being staged - see stageCourt.ts: not stuck. The pregame
				// show is what it is staged behind.)
				staging={
					!timeline && !(showOn && showPlan) && !!events && events.length > 0
				}
			/>
			{authoredProp ? (
				authoredProp.card && intro.call >= 0 ? (
					<IntroCard info={authoredProp.card} callKey={intro.call} />
				) : null
			) : introCard ? (
				<IntroCard info={introCard} callKey={intro.call} />
			) : null}
			{authored?.overlay}
			{intro.title && (!authored || showOn) ? (
				<IntroTitle playoffs={!!timeline?.intro?.big} />
			) : null}
			{showOn && showInfo ? <MatchupBoard info={showInfo} /> : null}
			{showOn && showPlan && stagedTl && !follower && !along ? (
				<button
					type="button"
					onClick={() => setShowOver(true)}
					style={{
						position: "absolute",
						right: "2cqw",
						bottom: "2cqw",
						padding: "0.4em 0.9em",
						fontFamily:
							"system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
						fontWeight: 700,
						fontSize: "clamp(11px, 1.6cqw, 14px)",
						color: "#fff",
						background: "rgba(10, 10, 14, 0.7)",
						border: "1px solid rgba(255, 255, 255, 0.35)",
						borderRadius: 999,
						cursor: "pointer",
					}}
				>
					Skip ›
				</button>
			) : null}
			{intro.on && !follower && !along && !authored ? (
				<button
					type="button"
					onClick={skipIntro}
					style={{
						position: "absolute",
						right: "2cqw",
						bottom: "2cqw",
						padding: "0.4em 0.9em",
						fontFamily:
							"system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
						fontWeight: 700,
						fontSize: "clamp(10px, 1.6cqw, 14px)",
						color: "#fff",
						background: "rgba(10, 10, 14, 0.7)",
						border: "1px solid rgba(255, 255, 255, 0.35)",
						borderRadius: 999,
						cursor: "pointer",
					}}
				>
					Skip intro ›
				</button>
			) : null}
			<GameBug
				bugRef={bugRef}
				own={scoreBug === undefined ? boxScore?.scoreBug : scoreBug}
				hidden={intro.on || preview || !!authored}
				away={bugTeamOf(
					away,
					awayPts,
					boxScore?.teams?.[1],
					standingOf(boxScore?.gamecast, boxScore?.teams?.[1]?.tid),
				)}
				home={bugTeamOf(
					home,
					homePts,
					boxScore?.teams?.[0],
					standingOf(boxScore?.gamecast, boxScore?.teams?.[0]?.tid),
				)}
				quarter={quarter}
				late={late}
				series={
					boxScore?.teams?.[0] && boxScore?.teams?.[1]
						? seriesLine(boxScore.gamecast, !!boxScore.playoffs, [
								boxScore.teams[1],
								boxScore.teams[0],
							])
						: undefined
				}
				day={
					typeof boxScore?.date === "string"
						? weekdayName(boxScore.date)
						: undefined
				}
				refs={bugRefs}
			/>
			<Caption caption={caption} team={captionTeam} home={home} />
		</PictureBox>
	);
};

export default Court3D;

// What the first court of a visit sets up for itself, done ahead of it (the
// Nightly Recap's, while its poster is up): the crowd's faces made, a face
// at a time, and - `all`, with nothing on the screen moving for it to hold
// up - the card that draws the men checked against the page. Once a visit.
export const warmCourt = (all: boolean) => {
	void crowdFaces();
	if (all) {
		glReady();
	}
};
