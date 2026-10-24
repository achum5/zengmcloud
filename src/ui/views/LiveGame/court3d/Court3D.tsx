import {
	useCallback,
	useEffect,
	useLayoutEffect,
	useMemo,
	useRef,
	useState,
	type ReactNode,
} from "react";
import { useLocal } from "../../../util/local.ts";
import { toWorker } from "../../../util/toWorker.ts";
import type { PlayerFace } from "../../../util/playerFaces.ts";
import type {
	ArenaLooks,
	ReplayLooks,
	ScoreBugStyle,
} from "../../../../common/types.ts";
import LiveCourt from "../LiveCourt.tsx";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import {
	paintBench,
	paintBoards,
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
import { FaceLoader } from "./FaceLoader.tsx";
import { adjust, artFor, makeResolution } from "./resolution.ts";
import { makeFarm } from "./farm.ts";
import { makeScratch, makeSpriteCache, releaseSprites } from "./sprite.ts";
import {
	buildClocks,
	formatGameClock,
	gameClockAt,
	shotClockAt,
} from "./clock.ts";
import {
	clipDipAt,
	type CourtPlayer,
	type CourtTimeline,
	type Stakes,
} from "./director.ts";
import { crewAt, crewFor } from "./crew.ts";
import { courtsideFor } from "./courtside.ts";
import { headColors, loadHead, profileOf, type HeadSprite } from "./faces.ts";
import { gearFor, kitsFor, shade, type Look } from "./figure.ts";
import { dressKit, kitArtOf, loadKitPicture, type KitArt } from "./kitArt.ts";
import { logoPicture } from "./logoPicture.ts";
import { COURT_W, type Side } from "./geometry.ts";
import { eventsMatchRoster } from "./rosterMatch.ts";
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
import {
	buildFouls,
	breakText,
	finalText,
	foulsAt,
	isBreak,
	isFinal,
	SHOT_LOW,
	SHOT_LOW_INK,
	shotClockText,
} from "./scoreBug.ts";
import { ScoreBug, type BugRefs } from "./ScoreBug.tsx";
import { CustomScoreBug } from "./CustomScoreBug.tsx";
import { IntroCard, IntroTitle } from "./IntroCard.tsx";
import { callAt } from "./intro.ts";
import { STARTING_NUM_TIMEOUTS } from "../../../../common/constants.ts";
import { helpers } from "../../../util/helpers.ts";
import {
	aimFor,
	arenaAim,
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

// How long the picture dips to black either side of a cut (timeline ms).
const DIP_MS = 130;
// Up out of black onto the next of the key plays (ms).
const KEY_DIP_MS = 320;
// The longest it is held black for the picture to be ready (ms).
const KEY_HOLD_MS = 900;
// A replay cut from one of its cameras to the next (see replayRig): a
// flash of white, and the new angle up out of it over this long (ms) - held
// white, as black is, while the men in it are made ready at their new size.
const FLASH_MS = 260;
// THE WIPE into a replay and out of it (see playback's wipeAt): a band in
// the colors of the team whose play it is, its logo in the middle, slanting,
// sweeping across
// the picture - this much wider than it (%: wide enough to cover all of it,
// the slant and all, while it is near halfway across - see STING_COVERS),
// slanting this far (deg).
const STING_W = 140;
const STING_SLANT = 18;
// WHAT IS OVER THE PICTURE: the score bug along its foot and the line of
// play-by-play across its top, each's box as shares of the picture (see
// camera.ts's dodgeFor).
const boxOf = (
	wrap: HTMLElement | null,
	el: Element | null | undefined,
): (Cover & { y1: number }) | undefined => {
	if (!wrap || !el) {
		return undefined;
	}
	const w = wrap.getBoundingClientRect();
	const b = el.getBoundingClientRect();
	if (w.width <= 0 || w.height <= 0 || b.width <= 0 || b.height <= 0) {
		return undefined;
	}
	return {
		x0: (b.left - w.left) / w.width,
		x1: (b.right - w.left) / w.width,
		y0: (b.top - w.top) / w.height,
		y1: (b.bottom - w.top) / w.height,
	};
};
// The line across the top goes faint while the ball is behind it - a shot
// arcing up from the far side of the floor - and comes back as it drops;
// and is off the picture through a replay, the way a broadcast shows one
// clean but for its REPLAY tag (the rim, seen from under it, is up there).
const CAPTION_FAINT = 0.18;
const captionOver = (
	cap: Cover & { y1: number },
	at: { x: number; y: number },
): boolean =>
	at.x > cap.x0 - 0.02 &&
	at.x < cap.x1 + 0.02 &&
	at.y > cap.y0 - 0.02 &&
	at.y < cap.y1 + 0.03;
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

// How long the play-by-play may disagree with the roster before the court
// gives up on the game (ms).
const ROSTER_WAIT = 5000;
// Drawing a frame in under this long (ms, smoothed), there is time for his
// poses in finer steps (see sprite.ts); over that, back to the usual ones -
// and not tried again for a while.
const FINE_ON_MS = 5;
const FINE_OFF_MS = 11;
const FINE_RETRY_MS = 20_000;
const FINE_SETTLE_MS = 3000;
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
}: Props) => {
	const { lid, gender } = useLocal(["lid", "gender"]);
	const gid: number | undefined = boxScore?.gid;
	const season: number | undefined = boxScore?.season;
	// A saved replay remembers how everyone looked that night.
	const looksThen: ReplayLooks | undefined = boxScore?.replayLooks;
	const raw: any[] = useMemo(() => {
		const teams: any[] = Array.isArray(boxScore?.teams) ? boxScore.teams : [];
		return teams.map((t) => {
			const then = looksThen?.teams[t?.tid];
			return then ? { ...t, ...then, court: then.court } : t;
		});
	}, [boxScore?.teams, looksThen]);
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
	// A playoff game's stakes, as they stood going into it - the box score's
	// series records take this game in once it is over, so they are read
	// once, and one won by now is taken back out.
	const stakes = useMemo((): Stakes | undefined => {
		const bs = boxScore as
			| {
					playoffs?: boolean;
					finals?: boolean;
					numGamesToWinSeries?: number;
					gameOver?: boolean;
					won?: { tid?: number };
					teams?: { tid?: number; playoffs?: { won?: number } }[];
			  }
			| undefined;
		if (!bs?.playoffs || typeof bs.numGamesToWinSeries !== "number") {
			return undefined;
		}
		const won = (raw: 0 | 1) => {
			const t = bs.teams?.[raw];
			const w = Number(t?.playoffs?.won ?? 0);
			return bs.gameOver && bs.won?.tid === t?.tid ? w - 1 : w;
		};
		// (Raw team 0 is home, side 1.)
		return {
			won: [won(1), won(0)],
			toWin: bs.numGamesToWinSeries,
			finals: !!bs.finals,
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);
	const introKind: "playoffs" | "regular" | undefined = LINEUP_INTROS
		? boxScore?.playoffs
			? "playoffs"
			: "regular"
		: undefined;
	// The game staged, off the page's thread (see stageCourt.ts) - the court
	// stays dark until it is.
	const [timeline, setTimeline] = useState<CourtTimeline | undefined>();
	const onFailRef = useRef(onFail);
	onFailRef.current = onFail;
	useEffect(() => {
		setTimeline(undefined);
		if (!events || events.length === 0) {
			return;
		}
		// THE EVENTS HAVE TO BE THIS GAME'S. When the game on this page changes
		// under it - a league-mate starts another game while this device is
		// following one - the new game's events can arrive a render ahead of its
		// box score, and staging them against the previous game's roster puts
		// players on the floor that the court has never heard of (the field
		// report: a crash in the free throw lineup). Wait for the two to agree -
		// and if they never do, give the game back to the page.
		if (!eventsMatchRoster(events, roster)) {
			const id = setTimeout(() => onFailRef.current?.(), ROSTER_WAIT);
			return () => clearTimeout(id);
		}
		let alive = true;
		stageCourt({
			events,
			players: roster,
			gid,
			gender,
			intro: introKind,
			stakes,
		}).then(
			(tl) => {
				if (alive) {
					setTimeline(tl);
				}
			},
			(error: unknown) => {
				// A broken staging must not take the whole page down with it, or
				// leave it waiting on a court that will never play: the
				// play-by-play and box score go on without it.
				console.error("3D court failed to compile", error);
				if (alive) {
					onFailRef.current?.();
				}
			},
		);
		return () => {
			alive = false;
		};
	}, [events, roster, gid, gender, introKind, stakes]);
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
	const keyPlan = useMemo(
		(): KeySegment[] | undefined =>
			timeline && events && keyPlays?.settings.on
				? planKeyPlays(timeline, events, keyPlays.settings, {
						rate,
						numPeriods,
						mine: keyPlays.mine,
						// (Who was expected to win, by how much - see winProb.ts. No
						// home team on a neutral floor.)
						...(typeof boxScore?.spread === "number"
							? { spread: boxScore.spread }
							: {}),
						...(boxScore?.neutralSite ? { edge: 0 } : {}),
					})
				: undefined,
		[
			timeline,
			events,
			keyPlays,
			rate,
			numPeriods,
			boxScore?.spread,
			boxScore?.neutralSite,
		],
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

	// Each side's uniform drawn from a picture, if its team has one: the
	// visitors' away one, the home team's home one. A replay wears the ones
	// worn that night - or, where one of those has since been replaced, the
	// team's own now.
	const [arts, setArts] = useState<[KitArt?, KitArt?]>([]);
	useEffect(() => {
		const now: any[] = Array.isArray(boxScore?.teams) ? boxScore.teams : [];
		const choices = [
			[away?.jerseySkins?.away, now[1]?.jerseySkins?.away],
			[home?.jerseySkins?.home, now[0]?.jerseySkins?.home],
		].map((ids) => ids.filter((id): id is string => typeof id === "string"));
		const ids = [...new Set(choices.flat())];
		if (ids.length === 0) {
			setArts([]);
			return;
		}
		let alive = true;
		void (async () => {
			const urls = await toWorker("main", "getJerseySkins", ids);
			const made = await Promise.all(
				choices.map(async (side) => {
					const id = side.find((choice) => urls?.[choice] !== undefined);
					if (id === undefined) {
						return undefined;
					}
					try {
						return kitArtOf(await loadKitPicture(urls[id]!), id);
					} catch {
						return undefined;
					}
				}),
			);
			if (alive) {
				setArts([made[0], made[1]]);
			}
		})();
		return () => {
			alive = false;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);
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

	// The building, painted once a game.
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
		return {
			stands: paintStands(h, a, seed, 0, crowd),
			standsUp: paintStands(h, a, seed, 1, crowd),
			standsWave: paintStands(h, a, seed, 2, crowd),
			standsSparse: paintStands(h, a, seed, 0, sparse),
			endStandsSparse: paintStands(h, a, seed, 0, sparse, 0),
			endStands: [0, 1, 2].map((up) =>
				paintStands(h, a, seed, up as 0 | 1 | 2, crowd, 0),
			) as [HTMLCanvasElement, HTMLCanvasElement, HTMLCanvasElement],
			boards: paintBoards(h, a),
			tableTop: table.top,
			tableFront: table.front,
			bench0: paintBench(a),
			bench1: paintBench(h),
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);

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
	const painted = useMemo(() => ({ ...paint, rafters }), [paint, rafters]);
	const paintRef = useRef(painted);
	paintRef.current = painted;

	// Faces arrive asynchronously; until then a player has a plain head and an
	// average build.
	const faces = useRef(new Map<number, PlayerFace | null>());
	const heads = useRef(
		new Map<number, { sprite?: HeadSprite; skin?: string }>(),
	);
	const [facesVersion, setFacesVersion] = useState(0);
	const onFace = useCallback(
		(pid: number, face: PlayerFace | null) => {
			if (faces.current.get(pid) === face) {
				return;
			}
			faces.current.set(pid, face);
			setFacesVersion((v) => v + 1);
			// A photo face is drawn as a silhouette - no head to load.
			if (face?.imgURL) {
				heads.current.delete(pid);
				return;
			}
			const team = roster.find((p) => p.pid === pid)?.team;
			const colors = team === 0 ? away?.colors : home?.colors;
			void loadHead(face?.face, face?.colors ?? colors).then((head) => {
				if (faces.current.get(pid) === face) {
					heads.current.set(pid, head);
					setFacesVersion((v) => v + 1);
				}
			});
		},
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[roster],
	);

	// A replay's faces are already here: no need to ask for today's.
	useEffect(() => {
		if (!looksThen) {
			return;
		}
		for (const p of roster) {
			const then = looksThen.players[p.pid];
			if (then) {
				onFace(p.pid, then);
			}
		}
	}, [looksThen, roster, onFace]);

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
	// The people in the courtside seats.
	const courtside = useMemo(
		() => courtsideFor(gid ?? 0, away, home),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);
	const crewRef = useRef({ crew, crewLooks, courtside });
	crewRef.current = { crew, crewLooks, courtside };

	// Read by the animation loop, which outlives any one render.
	const looks = useRef(appearance.looks);
	const bodies = useRef(appearance.bodies);
	looks.current = appearance.looks;
	bodies.current = appearance.bodies;

	// The picture's size: 16:9, or 4:3 on a phone so the players stay big.
	const wrapRef = useRef<HTMLDivElement | null>(null);
	const bugRef = useRef<HTMLDivElement | null>(null);
	const canvasRef = useRef<HTMLCanvasElement | null>(null);
	const [size, setSize] = useState({ w: 640, h: 360 });
	useLayoutEffect(() => {
		const el = wrapRef.current;
		if (!el) {
			return;
		}
		const measure = () => {
			setSize({ w: el.clientWidth, h: el.clientHeight });
		};
		measure();
		const observer = new ResizeObserver(measure);
		observer.observe(el);
		return () => {
			observer.disconnect();
		};
	}, []);
	const narrow = size.w < 560;

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
	const clockRef = useRef<HTMLSpanElement | null>(null);
	const dipRef = useRef<HTMLDivElement | null>(null);
	const flashRef = useRef<HTMLDivElement | null>(null);
	const replayRef = useRef<HTMLDivElement | null>(null);
	// (One for each side, [visitors, home]: the one whose play it is shows.)
	const stingRefs = useRef<[HTMLDivElement | null, HTMLDivElement | null]>([
		null,
		null,
	]);
	const shotRef = useRef<HTMLSpanElement | null>(null);
	// The score bug's fouls and possession marker, per side [visitors, home].
	const foulsAwayRef = useRef<HTMLSpanElement | null>(null);
	const foulsHomeRef = useRef<HTMLSpanElement | null>(null);
	const ballAwayRef = useRef<HTMLSpanElement | null>(null);
	const ballHomeRef = useRef<HTMLSpanElement | null>(null);

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
		caption: undefined as (Cover & { y1: number }) | undefined,
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
		clockText: "",
		shotText: "",
		// The shot clock's own color on the bug, kept while it shows red.
		shotInk: undefined as string | undefined,
		// Which of its cameras the replay showing is seen from (see
		// replayRig): a cut to the other is straight there, through a flash
		// (when it went off, and when it was last held).
		replayRig: undefined as Rig | undefined,
		flashAt: -Infinity,
		flashHold: -Infinity,
		foulsText: ["", ""] as [string, string],
		ballSide: -1,
		// The dunk replay showing now, and the next dunk that would get one.
		replay: undefined as Replay | undefined,
		nextDunk: 0,
		replayDue: undefined as number | undefined,
		// The wipe into a replay and out of it, and whose play it is.
		wipe: makeWipe(),
		wipeSide: 1 as Side,
		// The starting lineups: the man being called (his place in the
		// order, -1 none), and whether they are on.
		introCall: -1,
		introOn: false,
		introTitle: false,
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
		let fineOnAt = -Infinity;
		let fineOffAt = -Infinity;
		// How fine the picture is: as fine as this device can draw it.
		const res = makeResolution(performance.now() + 1500);
		// The size the canvas is laid on the page at (CSS px).
		let laidW = 0;
		let laidH = 0;
		const lookOf = (pid: number) => looks.current.get(pid)!;
		const bodyOfPid = (pid: number) => bodies.current.get(pid) ?? bodyOf();

		const tick = (now: number, draw: boolean) => {
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
			const dpr = Math.min(3, window.devicePixelRatio || 1);
			if (w <= 0 || h <= 0) {
				return;
			}
			// The picture: fine (see resolution.ts), each of its pixels a whole
			// number of the screen's.
			const art = artFor(h * dpr, res);
			const fw = Math.ceil((w * dpr) / art);
			const fh = Math.ceil((h * dpr) / art);
			if (canvas.width !== fw || canvas.height !== fh) {
				canvas.width = fw;
				canvas.height = fh;
			}
			// Laid on the page that many screen pixels to each of its pixels
			// exactly - not stretched to the box it is in, which is a fraction
			// of a pixel off that (831.5 x 467.7, say): stretched, a column and
			// a row of the picture came out a screen pixel narrower than the
			// rest, a seam everything crawled across as the camera moved. (The
			// sliver past the box's edge is cut off by it.)
			const cssW = (fw * art) / dpr;
			const cssH = (fh * art) / dpr;
			if (cssW !== laidW || cssH !== laidH) {
				laidW = cssW;
				laidH = cssH;
				canvas.style.width = `${cssW}px`;
				canvas.style.height = `${cssH}px`;
			}

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
			// Over a break, a look round the building; at the starting lineups,
			// each man called.
			const view = replay
				? undefined
				: (arenaAim(tl, s.t, p.narrow) ?? introAim(tl, moment, p.narrow));
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
			const snap = s.snapCam;
			const secs = (dt / 1000) * Math.min(24, Math.max(1, rate));
			operate(s, aim, fit, secs, snap);
			s.snapCam = false;
			if (now - s.coversAt > 500) {
				s.coversAt = now;
				s.covers = coversOf(wrapRef.current, bugRef.current);
				s.caption = boxOf(
					wrapRef.current,
					wrapRef.current?.querySelector(".court3d-caption"),
				);
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
			const keyDark = 1 - (now - Math.max(s.keyDip, s.keyHold)) / KEY_DIP_MS;
			let o: number;
			if (replay) {
				o = Math.max(0, keyDark);
			} else {
				const near = cut === undefined ? Infinity : Math.abs(s.t - cut);
				// (And between clips of a highlight reel, and the key plays:
				// down to black out of one, up out of it into the next.)
				const keyOut =
					p.keyPlan && !p.follower && !p.paused
						? keyOutAt(p.keyPlan, s.t, s.pace)
						: 0;
				o = Math.max(0, 1 - near / DIP_MS, clipDipAt(tl, s.t), keyDark, keyOut);
			}
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
			const dip = dipRef.current;
			if (dip && dip.style.opacity !== String(o)) {
				dip.style.opacity = String(o);
			}
			const white = flashRef.current;
			if (white && white.style.opacity !== String(flash)) {
				white.style.opacity = String(flash);
			}
			const capEl =
				wrapRef.current?.querySelector<HTMLElement>(".court3d-caption");
			if (capEl) {
				const b = project(cam, moment.ball);
				const faint =
					s.caption !== undefined &&
					captionOver(s.caption, { x: b.x / fw, y: b.y / fh });
				const o = replay ? "0" : faint ? String(CAPTION_FAINT) : "";
				if (capEl.style.opacity !== o) {
					capEl.style.opacity = o;
				}
			}
			for (const side of [0, 1] as const) {
				const band = stingRefs.current[side];
				if (!band) {
					continue;
				}
				const on = sting !== undefined && side === s.wipeSide;
				const show = on ? "flex" : "none";
				if (band.style.display !== show) {
					band.style.display = show;
				}
				if (on && sting !== undefined) {
					// From all the way off the picture to its left to all the
					// way off it to its right, the slant and all.
					const off = (50 * h * Math.tan((STING_SLANT * Math.PI) / 180)) / w;
					const from = -STING_W - off;
					const to = 100 + off;
					const x = ((from + (to - from) * sting) * w) / 100;
					const tr = `translate3d(${x.toFixed(1)}px,0,0) skewX(${-STING_SLANT}deg)`;
					if (band.style.transform !== tr) {
						band.style.transform = tr;
					}
				}
			}
			const tag = replayRef.current;
			if (tag) {
				const show =
					replay && (sting === undefined || sting > 0.75) ? "block" : "none";
				if (tag.style.display !== show) {
					tag.style.display = show;
				}
			}
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
				clockText = game === undefined ? "" : formatGameClock(game);
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
				moment,
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
					standsSparse: pt.standsSparse,
					endStandsSparse: pt.endStandsSparse,
					endStands: pt.endStands,
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
				crew: working.states.flatMap((st) => {
					const c = cr.crewLooks.get(st.pid);
					return c ? [{ st, body: c.body, look: c.look }] : [];
				}),
				flashes: working.flashes,
				courtside: cr.courtside,
				teamColors: p.teamColors,
				black: o >= 0.999 || flash >= 0.999 || s.wipe.hold === now,
			});
			if (
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
			// Time to spare, at the finest picture: poses in finer steps - and
			// back to the usual ones well before that costs a size.
			const spare = res.drawMs;
			if (res.coarser > 0 || spare === undefined) {
				sprites.fine = false;
			} else if (
				!sprites.fine &&
				spare < FINE_ON_MS &&
				now - fineOffAt > FINE_RETRY_MS
			) {
				sprites.fine = true;
				fineOnAt = now;
			} else if (
				sprites.fine &&
				spare > FINE_OFF_MS &&
				// (Not over the first few seconds of them: every pose is new then,
				// and drawn for the first time.)
				now - fineOnAt > FINE_SETTLE_MS
			) {
				sprites.fine = false;
				fineOffAt = now;
			}

			if (clockText !== s.clockText && clockRef.current) {
				s.clockText = clockText;
				clockRef.current.textContent = clockText;
			}
			if (bugShot !== s.shotText && shotRef.current) {
				const el = shotRef.current;
				s.shotText = bugShot;
				el.textContent = bugShot;
				// (Off once the game clock is shorter: gone from the bug.)
				el.style.display = bugShot ? "flex" : "none";
				// Red in its last seconds - and back to its own color after.
				s.shotInk ??= el.style.color;
				const low = bugShot !== "" && Number(bugShot) < SHOT_LOW;
				el.style.color = low ? SHOT_LOW_INK : s.shotInk;
			}
			// Each side's fouls this period - or BONUS - in the bug's order,
			// visitors first (the marks are in box score order, home first).
			if (p.fouls) {
				const f = foulsAt(p.fouls, s.t);
				const text = ([1, 0] as const).map((k) =>
					f.bonus[k] ? "BONUS" : `FOULS ${f.fouls[k]}`,
				);
				[foulsAwayRef, foulsHomeRef].forEach((ref, i) => {
					const el = ref.current;
					if (el && text[i] !== s.foulsText[i]) {
						s.foulsText[i] = text[i]!;
						el.textContent = text[i]!;
						el.style.color = text[i] === "BONUS" ? "#f2c14e" : "";
					}
				});
			}
			const ball = offenseAt(tl, s.t);
			if (ball !== s.ballSide) {
				s.ballSide = ball;
				[ballAwayRef, ballHomeRef].forEach((ref, i) => {
					if (ref.current) {
						ref.current.style.visibility = i === ball ? "visible" : "hidden";
					}
				});
			}
		};

		let raf = requestAnimationFrame(function frame(now) {
			tick(now, true);
			raf = requestAnimationFrame(frame);
		});
		// A background tab gets no animation frames. Keep the game moving anyway
		// (nothing to draw), or a hidden tab would hold up everyone watching.
		const interval = setInterval(() => {
			if (document.hidden) {
				tick(performance.now(), false);
			}
		}, 250);
		return () => {
			cancelAnimationFrame(raf);
			clearInterval(interval);
			releaseSprites(sprites);
			farm?.stop();
		};
	}, []);

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
		<div
			ref={wrapRef}
			className="mb-3"
			style={{
				position: "relative",
				width: "100%",
				aspectRatio: narrow ? "4 / 3" : "16 / 9",
				background: "#040406",
				borderRadius: 6,
				overflow: "hidden",
				containerType: "inline-size",
				isolation: "isolate",
			}}
		>
			{roster
				.filter((p) => !looksThen?.players[p.pid])
				.map((p) => (
					<FaceLoader
						key={p.pid}
						pid={p.pid}
						season={season}
						lid={lid}
						onFace={onFace}
					/>
				))}
			<style>
				{".court3d-caption .text-body-secondary { color: #c9c3d3 !important; }"}
			</style>
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
			<canvas
				ref={canvasRef}
				role="img"
				aria-label="Live court"
				style={{
					position: "absolute",
					inset: 0,
					width: "100%",
					height: "100%",
					imageRendering: "pixelated",
				}}
			/>
			<div
				aria-hidden
				style={{
					position: "absolute",
					inset: 0,
					pointerEvents: "none",
					background:
						"radial-gradient(ellipse at 50% 55%, transparent 58%, rgba(0,0,0,0.32) 100%)",
				}}
			/>
			<div
				ref={dipRef}
				aria-hidden
				style={{
					position: "absolute",
					inset: 0,
					background: "#000",
					opacity: 0,
					pointerEvents: "none",
				}}
			/>
			<div
				ref={flashRef}
				aria-hidden
				style={{
					position: "absolute",
					inset: 0,
					background: "#fff",
					opacity: 0,
					pointerEvents: "none",
				}}
			/>
			{([away, home] as const).map((team, side) => {
				const logo = side === 0 ? awayLogo : homeLogo;
				const main = team?.colors?.[0] ?? "#8c1d40";
				const accent = team?.colors?.[1] ?? "#f2c14e";
				return (
					<div
						key={side}
						ref={(el) => {
							stingRefs.current[side] = el;
						}}
						aria-hidden
						style={{
							display: "none",
							position: "absolute",
							top: 0,
							left: 0,
							width: `${STING_W}%`,
							height: "100%",
							alignItems: "center",
							justifyContent: "center",
							pointerEvents: "none",
							willChange: "transform",
							// (Lit from above, a little.)
							background: `linear-gradient(180deg, rgba(255,255,255,0.16), rgba(0,0,0,0.22)), linear-gradient(90deg, #fff 0 2%, ${accent} 2% 7%, ${main} 7% 93%, ${accent} 93% 98%, #fff 98% 100%)`,
						}}
					>
						{logo ? (
							<img
								alt=""
								src={logo}
								style={{
									height: "34%",
									maxWidth: "24%",
									objectFit: "contain",
									transform: `skewX(${STING_SLANT}deg)`,
									filter: "drop-shadow(0 0.4cqw 0.6cqw rgba(0,0,0,0.35))",
								}}
							/>
						) : null}
					</div>
				);
			})}
			<div
				ref={replayRef}
				style={{
					display: "none",
					position: "absolute",
					right: "2cqw",
					top: "2cqw",
					padding: "0.35em 0.7em",
					fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
					fontWeight: 800,
					fontSize: "clamp(10px, 1.7cqw, 14px)",
					letterSpacing: "0.08em",
					color: "#fff",
					background: "rgba(10, 10, 14, 0.85)",
					borderLeft: `3px solid ${home?.colors?.[0] ?? "#888"}`,
					borderRadius: 3,
				}}
			>
				REPLAY
			</div>
			{/* Still being staged (see stageCourt.ts): not stuck. */}
			{!timeline && events && events.length > 0 ? (
				<div
					className="spinner-border text-light"
					role="status"
					aria-label="Loading"
					style={{
						position: "absolute",
						left: "50%",
						top: "50%",
						marginLeft: "-1rem",
						marginTop: "-1rem",
						opacity: 0.7,
						pointerEvents: "none",
					}}
				/>
			) : null}
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
			{(() => {
				// (Its record, where the game keeps one.)
				const recordOf = (t: any): string | undefined =>
					typeof t?.won === "number" && typeof t?.lost === "number"
						? helpers.formatRecord(t)
						: undefined;
				const bug = {
					hidden: intro.on || preview,
					away: away && {
						abbrev: away.abbrev,
						region: away.region,
						name: away.name,
						colors: away.colors,
						imgURL: away.imgURL,
						imgURLSmall: away.imgURLSmall,
						pts: awayPts,
						timeouts: boxScore?.teams?.[1]?.timeouts,
						record: recordOf(boxScore?.teams?.[1]),
					},
					home: home && {
						abbrev: home.abbrev,
						region: home.region,
						name: home.name,
						colors: home.colors,
						imgURL: home.imgURL,
						imgURLSmall: home.imgURLSmall,
						pts: homePts,
						timeouts: boxScore?.teams?.[0]?.timeouts,
						record: recordOf(boxScore?.teams?.[0]),
					},
					quarter,
					totalTimeouts: STARTING_NUM_TIMEOUTS,
					refs: {
						clock: clockRef,
						shot: shotRef,
						fouls: [foulsAwayRef, foulsHomeRef] as BugRefs["fouls"],
						ball: [ballAwayRef, ballHomeRef] as BugRefs["ball"],
					},
				};
				// The league's own bug, if it has one (see ScoreBugStyle).
				const own: ScoreBugStyle | null | undefined = boxScore?.scoreBug;
				return (
					<div ref={bugRef} style={{ display: "contents" }}>
						{own ? (
							<CustomScoreBug style={own} {...bug} />
						) : (
							<ScoreBug {...bug} />
						)}
					</div>
				);
			})()}
			{caption ? (
				<div
					className="court3d-caption"
					style={{
						position: "absolute",
						left: "50%",
						// Up top, over the crowd - never over the floor, where it hid
						// the play along the near sideline (the score bug alone is
						// down there, and the camera keeps the ball clear of it).
						top: "1.6cqw",
						transform: "translateX(-50%)",
						width: "max-content",
						// (Clear of the REPLAY tag in the corner.)
						maxWidth: "70%",
						transition: "opacity 0.15s",
						textAlign: "center",
						fontSize: "clamp(10px, 1.85cqw, 16px)",
						lineHeight: 1.35,
						padding: "0.45em 1em",
						background: "rgba(8, 8, 12, 0.84)",
						color: "#f1ede6",
						borderLeft: `4px solid ${
							(captionTeam ?? home)?.colors?.[0] ?? "#888"
						}`,
						borderRadius: 3,
						display: "flex",
						alignItems: "center",
						gap: "0.6em",
					}}
				>
					{captionTeam ? (
						<TeamLogoInline
							alt={captionTeam.abbrev}
							className="flex-shrink-0"
							imgURL={captionTeam.imgURL}
							imgURLSmall={captionTeam.imgURLSmall}
							includePlaceholderIfNoLogo
						/>
					) : null}
					<div>{caption}</div>
				</div>
			) : null}
		</div>
	);
};

export default Court3D;
