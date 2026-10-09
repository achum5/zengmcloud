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
import { usePlayerFace, type PlayerFace } from "../../../util/playerFaces.ts";
import type { ArenaLooks, ReplayLooks } from "../../../../common/types.ts";
import LiveCourt from "../LiveCourt.tsx";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import {
	paintBench,
	paintBoards,
	paintRafters,
	paintStands,
	paintTable,
} from "./arena.ts";
import { courtFit, makeCamera, MAIN_RIG, REPLAY_RIG } from "./camera.ts";
import { courtTexture } from "./courtTexture.ts";
import { adjust, artFor, makeResolution } from "./resolution.ts";
import { makeScratch, makeSpriteCache } from "./sprite.ts";
import {
	buildClocks,
	formatGameClock,
	gameClockAt,
	shotClockAt,
} from "./clock.ts";
import {
	clipDipAt,
	compileCourt,
	linesBetween,
	snapForCursor,
	targetForCursor,
	type CourtPlayer,
} from "./director.ts";
import { crewAt, crewFor } from "./crew.ts";
import { courtsideFor } from "./courtside.ts";
import { headColors, loadHead, profileOf, type HeadSprite } from "./faces.ts";
import { gearFor, kitsFor, shade, type Look } from "./figure.ts";
import { dressKit, kitArtOf, type KitArt } from "./kitArt.ts";
import { COURT_W, type Side } from "./geometry.ts";
import { eventsMatchRoster } from "./rosterMatch.ts";
import { advanceLead, followRate } from "./follow.ts";
import { bodyOf, type Body } from "./poses.ts";
import { cameraCuts, fastAt, offenseAt } from "./evaluate.ts";
import { buildFouls, foulsAt } from "./scoreBug.ts";
import { ScoreBug } from "./ScoreBug.tsx";
import { IntroCard, IntroTitle } from "./IntroCard.tsx";
import { callAt } from "./intro.ts";
import { STARTING_NUM_TIMEOUTS } from "../../../../common/constants.ts";
import {
	aimFor,
	arenaAim,
	crowdAt,
	drawFrame,
	introAim,
	momentAt,
	replayAim,
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

// The timeline is in real time: played at 1x, a second of it is a second
// (bar the dead time it runs through fast - see fastAt). Watched quicker
// than this, a dunk is not shown again.
const REPLAY_RATE_MAX = 2;
// How long the picture dips to black either side of a cut (timeline ms).
const DIP_MS = 130;
// A dunk's replay: from just before he takes off to just after, in slow
// motion, once its own play is over.
const REPLAY_FROM = 1700;
const REPLAY_TO = 450;
const REPLAY_AFTER = 1150;
const REPLAY_SPEED = 0.42;
// Replay-time ms the picture takes to come up out of black and go back down.
const REPLAY_DIP = 70;

// A player whose face is a photo is drawn in this, head to toe.
const SILHOUETTE = "#101012";

// What goes across the back of his jersey: everything after his first name.
const lastNameOf = (name: string | undefined): string => {
	const n = (name ?? "").trim();
	const i = n.indexOf(" ");
	return i < 0 ? n : n.slice(i + 1);
};

// The cut nearest to t, by binary search.
const nearestCut = (cuts: number[], t: number): number | undefined => {
	let lo = 0;
	let hi = cuts.length - 1;
	while (lo < hi) {
		const mid = (lo + hi) >> 1;
		if (cuts[mid]! < t) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	const a = cuts[lo];
	const b = cuts[lo - 1];
	if (a === undefined) {
		return b;
	}
	return b !== undefined && t - b < a - t ? b : a;
};
const FaceLoader = ({
	pid,
	season,
	lid,
	onFace,
}: {
	pid: number;
	season: number | undefined;
	lid: number | undefined;
	onFace: (pid: number, face: PlayerFace | null) => void;
}) => {
	const face = usePlayerFace(pid, season, lid);
	useEffect(() => {
		if (face !== undefined) {
			onFace(pid, face);
		}
	}, [face, onFace, pid]);
	return null;
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
	// How many times "next play" has been asked for: each cuts straight past
	// the play it shows, like any other skip ahead.
	skips?: number;
	onReady: () => void;
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
	skips = 0,
	onReady,
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

	// Every game opens with its starting lineups called out - more of a
	// show in the playoffs.
	const introKind = boxScore?.playoffs ? "playoffs" : "regular";
	const timeline = useMemo(() => {
		if (!events || events.length === 0) {
			return undefined;
		}
		// THE EVENTS HAVE TO BE THIS GAME'S. When the game on this page changes
		// under it - a league-mate starts another game while this device is
		// following one - the new game's events can arrive a render ahead of its
		// box score, and staging them against the previous game's roster puts
		// players on the floor that the court has never heard of (the field
		// report: a crash in the free throw lineup). Wait for the two to agree.
		if (!eventsMatchRoster(events, roster)) {
			return undefined;
		}
		try {
			return compileCourt({
				events,
				players: roster,
				gid,
				gender,
				intro: introKind,
			});
		} catch (error) {
			// A broken staging must not take the whole page down with it - the
			// play-by-play and box score still work without the court.
			console.error("3D court failed to compile", error);
			return undefined;
		}
	}, [events, roster, gid, gender, introKind]);
	const clocks = useMemo(
		() => (timeline && events ? buildClocks(timeline, events) : undefined),
		[timeline, events],
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
					const img = new Image();
					img.src = urls[id]!;
					try {
						await img.decode();
					} catch {
						return undefined;
					}
					return kitArtOf(img, id);
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
			rafters: paintRafters(h, building),
			tableTop: table.top,
			tableFront: table.front,
			bench0: paintBench(a),
			bench1: paintBench(h),
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);

	const paintRef = useRef(paint);
	paintRef.current = paint;

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
			looks.set(p.pid, {
				kit,
				...(art ? { kitArt: art } : {}),
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
		}
		return { looks, bodies };
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [roster, dressed, arts, facesVersion]);
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
		void Promise.all(
			crew.map(async (m) => {
				const team =
					m.role === "coach" ? (m.team === 0 ? away : home) : undefined;
				crewHeads.current.set(m.pid, await loadHead(m.face, team?.colors));
			}),
		).then(() => {
			if (alive) {
				setCrewVersion((v) => v + 1);
			}
		});
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
	useEffect(() => {
		courtPicture.current = undefined;
		const svg = floorRef.current?.querySelector("svg");
		if (!svg) {
			return;
		}
		let alive = true;
		void courtTexture(svg, 10).then((canvas) => {
			if (alive) {
				courtPicture.current = canvas;
			}
		});
		return () => {
			alive = false;
		};
	}, [gid, home?.court, home?.colors, home?.imgURL, boxScore?.finals]);
	const clockRef = useRef<HTMLSpanElement | null>(null);
	const dipRef = useRef<HTMLDivElement | null>(null);
	const replayRef = useRef<HTMLDivElement | null>(null);
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
		snapCam: true,
		last: undefined as number | undefined,
		readyFor: -1,
		stepping: false,
		prevCursor: -1,
		prevPaused: paused,
		prevSkips: skips,
		// Following another device's sim: where its court is estimated to be
		// (see follow.ts).
		lead: 0,
		clockText: "",
		shotText: "",
		foulsText: ["", ""] as [string, string],
		ballSide: -1,
		// The dunk replay showing now, and the next dunk that would get one.
		replay: undefined as { at: number; from: number; to: number } | undefined,
		nextDunk: 0,
		// The starting lineups: the man being called (his place in the
		// order, -1 none), and whether they are on.
		introCall: -1,
		introOn: false,
		introTitle: false,
	});
	const [intro, setIntro] = useState({ call: -1, on: false, title: false });

	// Follow the page's cursor: run on to the next line normally; cut straight
	// there on a rewind or a big jump ahead (fast-forward, joining late).
	useEffect(() => {
		if (!timeline) {
			return;
		}
		const s = play.current;
		const target = targetForCursor(timeline, cursor);
		// "Next play": past what is left of the play now shown, straight on to
		// the next one - never back.
		const skipped =
			s.prevCursor >= 0 && skips !== s.prevSkips && cursor > s.prevCursor;
		const snap =
			s.prevCursor < 0 ||
			cursor < s.prevCursor ||
			target < s.t - 1 ||
			skipped ||
			(linesBetween(timeline, s.prevCursor, cursor) > 2 && target - s.t > 9000);
		if (snap) {
			const at = snapForCursor(timeline, cursor);
			s.t = skipped ? Math.max(s.t, at) : at;
			s.snapCam = true;
		}
		if (follower) {
			// Where the other court is: just past the play it last reported
			// (it reports one the moment it gets there) - or, after a cut,
			// where this one now is.
			s.lead = snap
				? s.t
				: Math.max(s.lead, s.t, targetForCursor(timeline, s.prevCursor));
		}
		if (paused && s.prevCursor >= 0 && cursor > s.prevCursor && !skipped) {
			// "Next play" while paused: show that one play, then hold.
			s.stepping = true;
		}
		if (s.prevPaused && !paused) {
			s.readyFor = -1;
		}
		if (s.snapCam) {
			s.replay = undefined;
			s.nextDunk = timeline.fx.findIndex(
				(f) => f.kind === "dunk" && !!f.big && f.t + REPLAY_AFTER > s.t,
			);
		}
		s.prevCursor = cursor;
		s.prevPaused = paused;
		s.prevSkips = skips;
	}, [cursor, follower, paused, skips, timeline]);

	const homePad = home?.colors?.[0] ?? "#8c1d40";
	const lineColor: string = home?.court?.lines || "#f8f5f0";
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
		onReady,
		timeline,
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
		eventsLength: events?.length ?? 0,
	});
	live.current = {
		cursor,
		paused,
		rate,
		follower,
		onReady,
		timeline,
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
		eventsLength: events?.length ?? 0,
	};

	useEffect(() => {
		// The picture is drawn small - pixel art - and the page blows it up
		// without smoothing (see the canvas's style).
		const scratch = makeScratch();
		let sprites = makeSpriteCache();
		// How fine the picture is: as fine as this device can draw it.
		const res = makeResolution(performance.now() + 1500);
		const lookOf = (pid: number) => looks.current.get(pid)!;
		const bodyOfPid = (pid: number) => bodies.current.get(pid) ?? bodyOf();

		const tick = (now: number, draw: boolean) => {
			const p = live.current;
			const tl = p.timeline;
			if (!tl) {
				return;
			}
			const s = play.current;
			const dt = s.last === undefined ? 0 : Math.min(100, now - s.last);
			s.last = now;
			const target = targetForCursor(tl, p.cursor);
			const base = p.rate;
			// Through the dead stretches, fast.
			let rate = base * fastAt(tl, s.t);
			if (p.follower) {
				// Behind the device in charge of simming: hold a steady gap to
				// where its court is, rather than running to the last play it
				// reported and stopping there.
				if (!p.paused) {
					s.lead = advanceLead(
						Math.max(s.lead, s.t),
						target,
						dt,
						base * fastAt(tl, s.lead),
					);
				}
				rate *= followRate(s.lead - s.t);
			}
			const before = s.t;
			// A replay holds the live clock while it plays.
			const r = s.replay;
			if (r) {
				if (!p.paused) {
					r.at += dt * base * REPLAY_SPEED;
				}
				if (r.at >= r.to) {
					s.replay = undefined;
					s.snapCam = true;
				}
			} else if ((!p.paused || s.stepping) && s.t < target) {
				s.t = Math.min(target, s.t + dt * rate);
			}
			// A dunk just finished: show it again - unless this device is only
			// following, stepping play by play, or watching on fast.
			const fx = tl.fx;
			while (
				s.nextDunk >= 0 &&
				s.nextDunk < fx.length &&
				(fx[s.nextDunk]!.kind !== "dunk" ||
					!fx[s.nextDunk]!.big ||
					fx[s.nextDunk]!.t + REPLAY_AFTER <= before)
			) {
				s.nextDunk += 1;
			}
			const dunk = fx[s.nextDunk];
			if (
				!s.replay &&
				dunk &&
				dunk.t + REPLAY_AFTER > before &&
				dunk.t + REPLAY_AFTER <= s.t
			) {
				s.nextDunk += 1;
				if (
					!p.follower &&
					!p.paused &&
					!s.stepping &&
					p.rate <= REPLAY_RATE_MAX
				) {
					s.t = dunk.t + REPLAY_AFTER;
					s.replay = {
						at: dunk.t - REPLAY_FROM,
						from: dunk.t - REPLAY_FROM,
						to: dunk.t + REPLAY_TO,
					};
					s.snapCam = true;
				}
			}
			// Through a cut: the camera starts fresh on the other side.
			const cut = nearestCut(cameraCuts(tl), s.t);
			if (cut !== undefined && cut > before && cut <= s.t) {
				s.snapCam = true;
			}
			if (s.t >= target && !s.replay) {
				s.stepping = false;
				if (
					!p.paused &&
					!p.follower &&
					p.cursor < p.eventsLength &&
					s.readyFor !== p.cursor
				) {
					s.readyFor = p.cursor;
					p.onReady();
				}
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

			const replay = s.replay;
			const moment = momentAt(
				tl,
				replay ? replay.at : s.t,
				rosterRef.current,
				bodyOfPid,
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
				? replayAim(moment, p.narrow)
				: (view?.shot ?? aimFor(moment, p.narrow, tl, fit));
			if (s.snapCam) {
				s.camX = aim.x;
				s.camW = aim.width;
				s.snapCam = false;
			} else {
				const secs = (dt / 1000) * Math.min(24, Math.max(1, rate));
				s.camX += (aim.x - s.camX) * (1 - Math.exp(-secs * 2.6));
				s.camW += (aim.width - s.camW) * (1 - Math.exp(-secs * 1.5));
			}
			const cam = makeCamera(
				{
					x: s.camX,
					width: s.camW,
					y: fit ? fit.y(s.camW) : aim.y,
					z: aim.z,
				},
				fw,
				fh,
				replay ? REPLAY_RIG : (view?.rig ?? MAIN_RIG),
			);

			const dip = dipRef.current;
			if (dip) {
				let o: number;
				if (replay) {
					const edge = Math.min(replay.at - replay.from, replay.to - replay.at);
					o = Math.max(0, 1 - edge / REPLAY_DIP);
				} else {
					const near = cut === undefined ? Infinity : Math.abs(s.t - cut);
					// (And between clips of a highlight reel.)
					o = Math.max(0, 1 - near / DIP_MS, clipDipAt(tl, s.t));
				}
				if (dip.style.opacity !== String(o)) {
					dip.style.opacity = String(o);
				}
			}
			const tag = replayRef.current;
			if (tag) {
				const show = replay ? "block" : "none";
				if (tag.style.display !== show) {
					tag.style.display = show;
				}
			}
			// The crowd on its feet after a big play, arms going up and out -
			// and the whole way through a tight finish.
			const { up, wave } = crowdAt(tl, moment.t, now);

			let shotText = "";
			let clockText = "";
			if (p.clocks) {
				const game = gameClockAt(p.clocks, s.t);
				const shot = shotClockAt(p.clocks, s.t);
				clockText = game === undefined ? "" : formatGameClock(game);
				shotText = shot === undefined ? "" : String(Math.ceil(shot - 1e-6));
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
				apron: p.apron,
				warmups: p.warmups,
				shotClock: shotText,
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
				crowd: { up, wave },
				now,
				// Lettering at least 10 CSS pixels tall: a 7-pixel font, each of
				// its pixels this many picture pixels.
				textScale: Math.max(1, Math.ceil(10 / ((7 * art) / dpr))),
				crew: working.states.flatMap((st) => {
					const c = cr.crewLooks.get(st.pid);
					return c ? [{ st, body: c.body, look: c.look }] : [];
				}),
				flashes: working.flashes,
				courtside: cr.courtside,
				teamColors: p.teamColors,
			});
			if (adjust(res, h * dpr, dt, performance.now() - drawStart, now)) {
				// Sprites drawn for the old size are no use at the new one.
				sprites = makeSpriteCache();
			}

			if (clockText !== s.clockText && clockRef.current) {
				s.clockText = clockText;
				clockRef.current.textContent = clockText;
			}
			if (shotText !== s.shotText && shotRef.current) {
				s.shotText = shotText;
				shotRef.current.textContent = shotText;
				// (Off once the game clock is shorter: gone from the bug.)
				shotRef.current.style.display = shotText ? "flex" : "none";
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
	const quarter = boxScore?.quarterShort ?? "";
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
			{introCard ? <IntroCard info={introCard} callKey={intro.call} /> : null}
			{intro.title ? <IntroTitle playoffs={!!timeline?.intro?.big} /> : null}
			{intro.on && !follower ? (
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
			<ScoreBug
				hidden={intro.on}
				away={
					away && {
						abbrev: away.abbrev,
						colors: away.colors,
						imgURL: away.imgURL,
						imgURLSmall: away.imgURLSmall,
						pts: awayPts,
						timeouts: boxScore?.teams?.[1]?.timeouts,
					}
				}
				home={
					home && {
						abbrev: home.abbrev,
						colors: home.colors,
						imgURL: home.imgURL,
						imgURLSmall: home.imgURLSmall,
						pts: homePts,
						timeouts: boxScore?.teams?.[0]?.timeouts,
					}
				}
				quarter={quarter}
				totalTimeouts={STARTING_NUM_TIMEOUTS}
				refs={{
					clock: clockRef,
					shot: shotRef,
					fouls: [foulsAwayRef, foulsHomeRef],
					ball: [ballAwayRef, ballHomeRef],
				}}
			/>
			{caption ? (
				<div
					className="court3d-caption"
					style={{
						position: "absolute",
						left: "50%",
						// Just over the score bug.
						bottom: "calc(2.4cqw + clamp(10px, 1.75cqw, 16px) * 3.3)",
						transform: "translateX(-50%)",
						width: "max-content",
						maxWidth: "92%",
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
