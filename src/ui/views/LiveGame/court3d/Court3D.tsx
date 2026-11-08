import type { CourtStyle } from "../../../../common/types.ts";
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
import {
	CAM_PAN,
	courtFit,
	dodgeFor,
	follow,
	makeCamera,
	MAIN_RIG,
	operate,
	project,
	type Cover,
	type Rig,
} from "./camera.ts";
import { courtTexture, edgeColor } from "./courtTexture.ts";
import {
	adjust,
	fineNow,
	makeFine,
	makeResolution,
	sizeCanvas,
} from "./resolution.ts";
import { FAST, makePace } from "./pace.ts";
import { makeFarm } from "./farm.ts";
import { makeScratch, makeSpriteCache, releaseSprites } from "./sprite.ts";
import {
	buildClocks,
	formatGameClock,
	gameClockAt,
	shotClockAt,
} from "./clock.ts";
import { clipDipAt, type CourtPlayer, type CourtTimeline } from "./director.ts";
import { crewAt, crewFor } from "./crew.ts";
import { headColors, loadHead, profileOf, type HeadSprite } from "./faces.ts";
import { arenaSpecFor } from "./arenaSpec.ts";
import { crowdFaces, type CrowdFace } from "./crowdFaces.ts";
import { gearFor, kitsFor, shade, type Look } from "./figure.ts";
import { dressKit } from "./kitArt.ts";
import { logoPicture } from "./logoPicture.ts";
import { COURT_W, type Side } from "./geometry.ts";
import { eventsMatchRoster } from "./rosterMatch.ts";
import { Caption, Overlays } from "./Overlays.tsx";
import { bugTeamOf, GameBug, PictureBox, PictureCanvas } from "./Picture.tsx";
import {
	boxOf,
	type Box,
	captionOf,
	dipOf,
	FLASH_MS,
	NARROW_W,
	runFrames,
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
import { bodyOf, type Body } from "./poses.ts";
import { offenseAt } from "./evaluate.ts";
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
import { callAt } from "./intro.ts";
import {
	aimFor,
	arenaAim,
	closeUpAim,
	crowdAt,
	drawFrame,
	introAim,
	momentAt,
	replayAim,
	replayRig,
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
// The longest it is held black for the picture to be ready (ms) - and, at
// a replay's flash between angles (see replayRig and broadcast.ts FLASH_MS),
// white, while the men in it are made ready at their new size.
const KEY_HOLD_MS = 900;
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
// Whether a game opens with its starting lineups (see intro.ts).
const LINEUP_INTROS = false;

// Drawing a frame in under this long (ms, smoothed), there is time for his
// poses in finer steps (see fineNow).
const FINE_ON_MS = 5;
// The most of a frame (ms) given to drawing players in poses not drawn
// before; over that, a man waits a frame for his new one (see sprite.ts).
const SPRITE_BUDGET_MS = 6;

// A player whose face is a photo is drawn in this, head to toe.
const SILHOUETTE = "#101012";

// What goes across the back of his jersey: everything after his first name.
const lastNameOf = (name: string | undefined): string => {
	const n = (name ?? "").trim();
	const i = n.indexOf(" ");
	return i < 0 ? n : n.slice(i + 1);
};

type Props = {
	// The game's full play-by-play, never consumed.
	events: any[] | undefined;
	// How many of those the page has shown.
	cursor: number;
	boxScore: any;
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
	// A play the page asks to see again (a highlight tapped): the line that
	// ended at `cursor`, shown again each time `n` goes up (see askedReplay).
	replayAsk?: { n: number; cursor: number };
	// The game's shots and where each was let go from, once it is staged
	// (see shotSpots) - for the shot chart.
	onShots?: (shots: ShotSpot[]) => void;
	// A clip (a highlight played on its own - see HighlightClip): it plays
	// no further than a moment past the line before this cursor.
	until?: number;
};

const Court3D = ({
	events,
	cursor,
	boxScore,
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
	replayAsk,
	onShots,
	until,
}: Props) => {
	const onShotsRef = useRef(onShots);
	onShotsRef.current = onShots;
	const { lid, gender } = useLocal(["lid", "gender"]);
	const gid: number | undefined = boxScore?.gid;
	const season: number | undefined = boxScore?.season;
	// A saved replay remembers how everyone looked that night.
	const looksThen: ReplayLooks | undefined = boxScore?.replayLooks;
	const teamsNow: unknown = boxScore?.teams;
	const raw: any[] = useMemo(() => {
		const teams: any[] = Array.isArray(teamsNow) ? teamsNow : [];
		return teams.map((t) => {
			const then = looksThen?.teams[t?.tid];
			return then ? { ...t, ...then, court: then.court } : t;
		});
	}, [teamsNow, looksThen]);
	// Display order, same as the 2D court: [away (attacks left), home].
	const away = raw[1];
	const home = raw[0];

	// The roster is fixed for the game - read it once.
	const roster = useMemo(() => {
		const out: (CourtPlayer & { name: string; jerseyNumber?: string })[] = [];
		const teams: [any, any] = [away, home];
		for (const t of [0, 1] as const) {
			for (const p of teams[t]?.players ?? []) {
				if (typeof p.pid === "number") {
					out.push({
						pid: p.pid,
						team: t,
						pos: p.pos,
						skills: Array.isArray(p.skills) ? p.skills : undefined,
						ratings:
							p.courtRatings && typeof p.courtRatings === "object"
								? p.courtRatings
								: undefined,
						injury:
							p.injury?.newThisGame && typeof p.injury.type === "string"
								? {
										type: p.injury.type,
										games: p.injury.gamesRemaining ?? 0,
									}
								: undefined,
						name: p.name,
						jerseyNumber: p.jerseyNumber,
					});
				}
			}
		}
		return out;
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);

	// The starting lineups called out before the opening tip (see intro.ts):
	// off for now, kept to come back to.
	// A playoff game's stakes, as they stood going into it (see stakesOf).
	const stakes = useStakes(gid, {
		playoffs: boxScore?.playoffs,
		finals: boxScore?.finals,
		numGamesToWinSeries: boxScore?.numGamesToWinSeries,
		gameOver: boxScore?.gameOver,
		won: boxScore?.won,
		teams: boxScore?.teams,
	});
	const introKind: "playoffs" | "regular" | undefined = LINEUP_INTROS
		? boxScore?.playoffs
			? "playoffs"
			: "regular"
		: undefined;
	// The game staged, off the page's thread (see stageCourt.ts and the
	// engine's useStaged) - the court on the warm-ups until it is.
	const { timeline } = useStaged(
		events,
		roster,
		`${gid}|${gender}|${introKind}|${JSON.stringify(stakes ?? null)}`,
		() =>
			stageCourt({
				events: events!,
				players: roster,
				gid,
				gender,
				intro: introKind,
				stakes,
			}),
		"court",
		onFail,
		(tl) => {
			try {
				onShotsRef.current?.(shotSpots(tl));
			} catch {
				// The chart just doesn't show.
			}
		},
	);
	// While it is: the two teams out warming up at their baskets - not a
	// black screen. (Staged here: it takes no time at all.)
	const pregame = useMemo((): Warmup | undefined => {
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
	}, [events, roster, gid, preview]);
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
	// painted again once they are ready, the same fans with faces now.
	const [fans, setFans] = useState<CrowdFace[]>();
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
		return {
			courtside: courtsideFor(h, a, spec, fans),
			stanchion: paintStanchionPanel(h, h?.colors?.[0] ?? "#8c1d40"),
			stands: stands(0, crowd, undefined),
			standsUp: stands(1, crowd, undefined),
			standsWave: stands(2, crowd, undefined),
			standsClap: stands(0, crowd, undefined, true),
			standsSparse: stands(0, sparse, undefined),
			endStandsSparse: stands(0, sparse, 0),
			endStands: [0, 1, 2].map((up) => stands(up as 0 | 1 | 2, crowd, 0)) as [
				HTMLCanvasElement,
				HTMLCanvasElement,
				HTMLCanvasElement,
			],
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
	// (The finals trophy is on the floor, at center court, on every court -
	// see LiveCourt.)
	const painted = useMemo(() => ({ ...paint, rafters }), [paint, rafters]);
	const paintRef = useRef(painted);
	paintRef.current = painted;

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
			void loadHead(face?.face, face?.colors ?? colors).then((head) => {
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
			const made = [f, head, kit, art, crest, team, p.name, p.jerseyNumber];
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
			});
			bodies.set(p.pid, bodyOf(f?.hgt, f?.weight));
			kept.current.set(p.pid, {
				made,
				look: looks.get(p.pid)!,
				body: bodies.get(p.pid)!,
			});
		}
		return { looks, bodies };
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [roster, dressed, arts, crests, facesVersion]);
	// The officials, the coaches and the photographers, picked once a game,
	// and their heads drawn from their faces as they come.
	const crew = useMemo(
		() => crewFor(gid ?? 0, away, home),
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
		// up the first frames of the game.)
		void (async () => {
			for (const m of crew) {
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
	const crewRef = useRef({ crew, crewLooks });
	crewRef.current = { crew, crewLooks };

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
	const size = useViewSize(wrapRef);
	const narrow = size.w < NARROW_W;

	// The home floor as a picture, made from the 2D court's drawing of it.
	const floorRef = useRef<HTMLDivElement | null>(null);
	const courtPicture = useRef<HTMLCanvasElement | undefined>(undefined);
	// A floor picture's edge color, the floor carried on out in it.
	const floorEdge = useRef<string | undefined>(undefined);
	useEffect(() => {
		courtPicture.current = undefined;
		floorEdge.current = undefined;
		const svg = floorRef.current?.querySelector("svg");
		if (!svg) {
			return;
		}
		let alive = true;
		void courtTexture(svg, 10).then((canvas) => {
			if (alive) {
				courtPicture.current = canvas;
				floorEdge.current =
					canvas && home?.court?.floorImageURL ? edgeColor(canvas) : undefined;
			}
		});
		return () => {
			alive = false;
		};
	}, [gid, home?.court, home?.colors, home?.imgURL, boxScore?.finals]);
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
		// No period on: the final horn gone, or between two (see isFinal,
		// isBreak).
		ended: false,
	});
	const [intro, setIntro] = useState({ call: -1, on: false, title: false });
	// No period on, in the picture - the final horn gone, or between two: the
	// bug says so (see the frame loop).
	const [ended, setEnded] = useState(false);
	// Onto the game from the warm-ups (see pregame): a cut, up out of black
	// (see tick).
	useEffect(() => {
		if (timeline) {
			play.current.snapCam = true;
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
		if (along?.fp && fp && along.fp !== fp && !lostSaid.current) {
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
		fp,
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
		numPeriods,
		// The period, as the page's box score has it ("Q4", "OT").
		period: boxScore?.quarterShort ?? "",
		// The score [road, home], as the score bug has it.
		score: [boxScore?.teams?.[1]?.pts ?? 0, boxScore?.teams?.[0]?.pts ?? 0] as [
			number,
			number,
		],
		eventsLength: events?.length ?? 0,
		preview,
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
		fp,
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
		numPeriods,
		// The period, as the page's box score has it ("Q4", "OT").
		period: boxScore?.quarterShort ?? "",
		// The score [road, home], as the score bug has it.
		score: [boxScore?.teams?.[1]?.pts ?? 0, boxScore?.teams?.[0]?.pts ?? 0] as [
			number,
			number,
		],
		eventsLength: events?.length ?? 0,
		preview,
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
		// Hands off the page's own thread to make new pictures on, where the
		// device has them (see farm.ts).
		const farm =
			typeof Worker === "undefined" || typeof OffscreenCanvas === "undefined"
				? undefined
				: makeFarm(
						spawnCourtWorker,
						(navigator.hardwareConcurrency || 2) >= 8 ? 2 : 1,
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
		const pacing = makePace(performance.now() + 1500);
		// The size the canvas is laid on the page at (CSS px).
		const laid = { w: 0, h: 0 };
		const lookOf = (pid: number) => looks.current.get(pid)!;
		const bodyOfPid = (pid: number) => bodies.current.get(pid) ?? bodyOf();

		const tick = (now: number, draw: boolean, still: boolean) => {
			const p = live.current;
			// (Still being staged: the building in the meantime - see pregame.)
			const staging = !p.timeline;
			const tl = p.timeline ?? p.pregame?.tl;
			if (!tl) {
				return;
			}
			const s = play.current;
			// Up out of black onto the first picture, and onto the game from
			// the warm-ups - from this frame, before anybody in it is drawn.
			if (s.shown !== tl) {
				s.shown = tl;
				s.keyDip = now;
				s.keyHold = -Infinity;
			}
			const { dt, rate, cut } = stepPlay(
				s,
				p,
				alongWith.current,
				tl,
				staging,
				now,
				Date.now(),
			);
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
			// The picture: fine (see resolution.ts), each of its pixels a whole
			// number of the screen's.
			const { art, dpr, fw, fh } = sizeCanvas(canvas, p.size, res, laid);

			const replay = s.replay;
			const moment =
				staging && p.pregame
					? warmupMoment(p.pregame, s.t, bodyOfPid)
					: momentAt(
							tl,
							replay ? replay.at : s.t,
							rosterRef.current,
							bodyOfPid,
							replay ? undefined : { rate, wall: now },
						);
			// Over a break, a look round the building; in close on a man at the
			// line; at the starting lineups, each man called.
			const arenaView = replay ? undefined : arenaAim(tl, s.t, p.narrow);
			const closeView =
				replay || arenaView ? undefined : closeUpAim(tl, moment, p.narrow);
			const view = replay
				? undefined
				: (arenaView ?? closeView ?? introAim(tl, moment, p.narrow));
			const on = !!tl.intro && s.t >= tl.intro.t0 && s.t < tl.intro.t1;
			const called = callAt(tl.intro, s.t);
			const k = called ? tl.intro!.calls.indexOf(called) : -1;
			// The title, as the lights go down.
			const title =
				on && s.t < (tl.intro!.calls[0]?.t0 ?? 0) && s.t - tl.intro!.t0 > 250;
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
						: aimFor(moment, p.narrow, tl, fit)));
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
			const snap = s.snapCam;
			const secs = (dt / 1000) * Math.min(24, Math.max(1, rate));
			operate(s, aim, fit, secs, snap);
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
			const flashing =
				1 - (now - Math.max(s.flashAt, s.flashHold)) / FLASH_MS >= 0.75;
			if (
				sprites.skipped > 0 &&
				sprites.dark &&
				now - s.darkFrom < KEY_HOLD_MS
			) {
				if (s.wipe.covered) {
					s.wipe.hold = now;
				} else if (flashing && replay) {
					s.flashHold = now;
				} else {
					s.keyHold = now;
				}
			}
			const flash = replay
				? Math.max(0, 1 - (now - Math.max(s.flashAt, s.flashHold)) / FLASH_MS)
				: 0;
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
			const dark = o >= 0.75 || flash >= 0.75 || s.wipe.covered;
			if (dark && !sprites.dark) {
				s.darkFrom = now;
			}
			sprites.dark = dark;
			// Poses come round as slowly as the moment shown moves on: in slow
			// motion a man may be drawn as he was for that many more frames.
			const drawnAt = replay ? replay.at : s.t;
			const pace =
				s.drawnAt === undefined || dt <= 0 ? 1 : (drawnAt - s.drawnAt) / dt;
			// (Stood still, or jumped back to a replay: as at real speed.)
			sprites.pace = pace > 0 ? pace : 1;
			s.drawnAt = drawnAt;
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
			const pt = paintRef.current;
			const cr = crewRef.current;
			const working = crewAt(tl, moment.t, cr.crew);
			const drawStart = performance.now();
			drawFrame({
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
				lookFor: lookOf,
				padColor: p.homePad,
				lineColor: p.lineColor,
				apron: floorEdge.current ?? p.apron,
				warmups: p.warmups,
				shotClock: shotText,
				score: staging ? undefined : p.score,
				arena: {
					court: courtPicture.current,
					stands: pt.stands,
					standsUp: pt.standsUp,
					standsWave: pt.standsWave,
					standsClap: pt.standsClap,
					standsSparse: pt.standsSparse,
					endStandsSparse: pt.endStandsSparse,
					endStands: pt.endStands,
					courtside: pt.courtside,
					stanchion: pt.stanchion,
					stanchionAds: pt.stanchionAds,
					boards: pt.boards,
					rafters: pt.rafters,
					tableTop: pt.tableTop,
					tableFront: pt.tableFront,
					bench: [pt.bench0, pt.bench1],
				},
				crowd,
				now,
				// Lettering about 8 CSS pixels tall: a 7-pixel font, each of its
				// pixels this many picture pixels.
				textScale: Math.max(1, Math.round(8 / ((7 * art) / dpr))),
				tagClear: s.covers,
				away: !!view || !!replay,
				...(closeView ? { soft: CLOSE_SOFT } : {}),
				crew: working.states.flatMap((st) => {
					const c = cr.crewLooks.get(st.pid);
					return c ? [{ st, body: c.body, look: c.look }] : [];
				}),
				flashes: working.flashes,
				teamColors: p.teamColors,
				black: o >= 0.999 || flash >= 0.999 || s.wipe.hold === now,
			});
			if (
				!still &&
				adjust(
					res,
					h * dpr,
					dt,
					performance.now() - drawStart,
					now,
					sprites.spent,
				)
			) {
				// Sprites drawn for the old size are no use at the new one - let
				// go of now.
				releaseSprites(sprites);
				sprites = makeSpriteCache();
				sprites.budget = SPRITE_BUDGET_MS;
				sprites.farm = farm;
			}
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
			<Overlays
				dipRef={dipRef}
				flashRef={flashRef}
				stingRefs={stingRefs}
				replayRef={replayRef}
				teams={[away, home]}
				logos={[awayLogo, homeLogo]}
				// (Still being staged - see stageCourt.ts: not stuck.)
				staging={!timeline && !!events && events.length > 0}
			/>
			{introCard ? <IntroCard info={introCard} callKey={intro.call} /> : null}
			{intro.title ? <IntroTitle playoffs={!!timeline?.intro?.big} /> : null}
			{intro.on && !follower && !along ? (
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
				own={boxScore?.scoreBug}
				hidden={intro.on || preview}
				away={bugTeamOf(away, awayPts, boxScore?.teams?.[1])}
				home={bugTeamOf(home, homePts, boxScore?.teams?.[0])}
				quarter={quarter}
				series={
					boxScore?.teams?.[0] && boxScore?.teams?.[1]
						? seriesLine(boxScore.gamecast, !!boxScore.playoffs, [
								boxScore.teams[1],
								boxScore.teams[0],
							])
						: undefined
				}
				refs={bugRefs}
			/>
			<Caption caption={caption} team={captionTeam} home={home} />
		</PictureBox>
	);
};

export default Court3D;
