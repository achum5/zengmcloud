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
import type { PlayerFace } from "../../../util/playerFaces.ts";
import { TeamLogoInline } from "../../../components/TeamLogoInline.tsx";
import { STARTING_NUM_TIMEOUTS } from "../../../../common/constants.ts";
import { helpers } from "../../../util/helpers.ts";
import { depthOf } from "../court3d/camera.ts";
import { FaceLoader } from "../court3d/FaceLoader.tsx";
import { headColors } from "../court3d/faces.ts";
import { logoPicture } from "../court3d/logoPicture.ts";
import { drawn, due, FAST, makePace } from "../court3d/pace.ts";
import { adjust, artFor, makeResolution } from "../court3d/resolution.ts";
import { eventsMatchRoster } from "../court3d/rosterMatch.ts";
import { ScoreBug, type BugRefs } from "../court3d/ScoreBug.tsx";
import { CustomScoreBug } from "../court3d/CustomScoreBug.tsx";
import type { ScoreBugStyle } from "../../../../common/types.ts";
import {
	makeScratch,
	makeSpriteCache,
	releaseSprites,
	startFrame,
} from "../court3d/sprite.ts";
import type { Body } from "../court3d/poses.ts";
import { aimFor, glideAim, type Glide } from "./aim.ts";
import { drawFlyingBall, drawLooseBall } from "./ball.ts";
import { drawFlag } from "./flag.ts";
import { drawnBody, FIGURE_SCALE, footballBody, officialBody } from "./body.ts";
import { makeFieldCamera, MAIN_RIG, REPLAY_RIG } from "./camera.ts";
import type { FieldTimeline } from "./director.ts";
import { stageField, stageOpening } from "./stageField.ts";
import { momentAt, playAt, type HandsOf } from "./evaluate.ts";
import { bodyPoint } from "./sculpt.ts";
import {
	fieldStyleFor,
	paintEndZone,
	paintMidLogo,
	paintNumbers,
	paintTurf,
	type FieldTeam,
} from "./fieldPaint.ts";
import { FIELD_L, type Side } from "./geometry.ts";
import { sidelineGear } from "./bench.ts";
import { chainCrewFor } from "./chains.ts";
import { crewFor } from "./crew.ts";
import { crowdAt } from "./crowd.ts";
import { marksAt } from "./marks.ts";
import {
	chainCrewLook,
	coachLook,
	trainerLook,
	fieldGearFor,
	fieldKitsFor,
	officialLook,
	photographerLook,
	tvCrewLook,
	type FieldLook,
} from "./kit.ts";
import type { RosterMan } from "./personnel.ts";
import {
	fieldStagedAs,
	makeFieldClock,
	makeFieldWipe,
	nextReplay,
	onFieldCursor,
	REPLAY_SPEED,
	stepField,
	wipeAt,
} from "./playback.ts";
import {
	addReport,
	makeFollow,
	type FollowReport,
	type Shown,
} from "../court3d/follow.ts";
import {
	keyOutAt,
	type KeyPlaySettings,
	type KeySegment,
} from "../court3d/keyPlays.ts";
import { planFieldKeyPlays } from "./keyPlays.ts";
import { replayAim } from "./replays.ts";
import { poolsOf } from "./pools.ts";
import { drawFieldFrame, type FieldPaint } from "./scene.ts";
import {
	coachId,
	COACHES,
	netBallsAt,
	sidelineAt,
	sidelineFor,
	STAFF,
	staffId,
} from "./sideline.ts";
import { HURT_MOST, MEDICS, medicId } from "./hurt.ts";
import {
	cameraGear,
	photographersAt,
	photographersFor,
	tvCameraGear,
} from "./media.ts";
import { tvCrewFor } from "./tv.ts";
import { runOutGear } from "./tunnel.ts";
import { makeFieldFarm, spawnFieldWorker } from "./farm.ts";
import {
	drawFieldPlayer,
	drawnBall,
	drawnLift,
	prepareFieldSprites,
	steadied,
} from "./sprite.ts";
import { drawTags, type TagCover } from "./tags.ts";
import { showerGear } from "./shower.ts";
import { chainGear } from "./sticks.ts";
import { waterGear } from "./water.ts";
import { paintStadium } from "./stadium.ts";
import { inShadow, venueFor } from "./venue.ts";

// THE 3D FIELD: the football game as a broadcast - the home team's stadium,
// its field, the twenty-two in their uniforms, a camera up in the press box
// - acting out the play-by-play, in place of the 2D field when this device
// has chosen it. The 3D court's sister (see court3d/Court3D.tsx), and run
// the same way: the whole game staged up front (see director.ts), playback a
// clock running along it toward the page's cursor, asking the page for each
// line when its moment comes (onReady) - so every line appears as its play
// happens on screen.

// How long the picture dips to black either side of a cut (timeline ms).
const DIP_MS = 150;
// THE WIPE into a replay and out of it (see playback's wipeAt): the 3D
// court's band in the colors of the team whose play it is, its logo in the
// middle, slanting, sweeping across the picture - this much wider than it
// (%), slanting this far (deg).
const STING_W = 140;
const STING_SLANT = 18;
// Screen ms the picture takes to come up out of black into the next of the
// key plays (see keyPlays.ts).
const KEY_DIP_MS = 320;
// How quickly the camera pans and zooms after what it follows (per second).
const CAM_PAN = 3.2;
const CAM_ZOOM = 2.4;
// The most of a frame (ms) given to drawing players in poses not drawn
// before; over that, a man waits a frame for his new one (see sprite.ts).
const SPRITE_BUDGET_MS = 6;
// Drawing a frame in under this long (ms, smoothed), there is time for the
// men's poses in finer steps (see sprite.ts); over that, back to the usual
// ones - and not tried again for a while. (A little sooner than
// basketball's: most of the new poses are made off the page - see farm.ts -
// and finer ones cost the page little more.)
const FINE_ON_MS = 7;
const FINE_OFF_MS = 11;
const FINE_RETRY_MS = 20_000;
const FINE_SETTLE_MS = 3000;
// How long the play-by-play may disagree with the roster before the field
// gives up on the game (ms).
const ROSTER_WAIT = 5000;
// Narrower than this (CSS pixels), the picture is squarer.
const NARROW_W = 560;

// Critically damped: from where it is and how fast it is going (`v`), on
// toward `to` - exact, however long the step.
const follow = (
	x: number,
	v: number,
	to: number,
	w: number,
	secs: number,
): [number, number] => {
	const y = x - to;
	const e = Math.exp(-w * secs);
	const k = (v + w * y) * secs;
	return [to + (y + k) * e, (v - w * k) * e];
};

// What goes across the back of his jersey: everything after his first name.
const lastNameOf = (name: string | undefined): string => {
	const n = (name ?? "").trim();
	const i = n.indexOf(" ");
	return i < 0 ? n : n.slice(i + 1);
};

// Everyone drawn bigger than life (see body.ts), and so how high anything
// is off the turf as it is drawn.
const drawnFootballBody = (hgt?: number, weight?: number) =>
	drawnBody(footballBody(hgt, weight));
const drawnOfficialBody = (hgt?: number, weight?: number) =>
	drawnBody(officialBody(hgt, weight));
const DRAWN_DEFAULT = drawnFootballBody();

// Where the ball a man has is drawn: in his hands, the way his picture has
// him (see evaluate.ts) - for the ball to leave them, and get to them.
const handsOfFor =
	(bodyOf: (pid: number) => Body, fine: () => boolean): HandsOf =>
	(st) => {
		const drawn = drawnBall(st, bodyOf(st.id), fine());
		if (!drawn) {
			return undefined;
		}
		const at = bodyPoint(
			{
				x: st.x + drawn.shift.x,
				y: st.y + drawn.shift.y,
				z: drawnLift(st, fine()),
				yaw: drawn.yaw,
			},
			drawn.ball.at,
		);
		return { x: at.x, y: at.y, z: at.z / FIGURE_SCALE };
	};

// Where the score bug and the line over it sit in the picture: what the
// tags keep out from behind (see tags.ts).
const coverOf = (
	wrap: HTMLElement | null,
	el: Element | null | undefined,
): TagCover | undefined => {
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
	};
};
const coversOf = (wrap: HTMLElement | null, bug: HTMLElement | null) =>
	[
		coverOf(wrap, bug?.firstElementChild),
		coverOf(wrap, wrap?.querySelector(".court3d-caption")),
	].filter((b) => b !== undefined);
const drawnUp = <T extends { z: number }>(p: T): T =>
	p.z === 0 ? p : { ...p, z: p.z * FIGURE_SCALE };

// A man as he looks with his helmet off, the same each time.
const bareLooks = new WeakMap<FieldLook, FieldLook>();
const bareLook = (look: FieldLook | undefined): FieldLook | undefined => {
	if (!look) {
		return undefined;
	}
	let bare = bareLooks.get(look);
	if (!bare) {
		bare = { ...look, bare: true };
		bareLooks.set(look, bare);
	}
	return bare;
};

type Props = {
	// The game's full play-by-play, never consumed.
	events: any[] | undefined;
	// How many of those the page has shown.
	cursor: number;
	boxScore: any;
	caption: ReactNode;
	// The box score team (0 home, 1 away) the caption's play belongs to.
	captionT?: 0 | 1;
	paused: boolean;
	// How many times real time it plays at.
	rate: number;
	// Stepped by the device in charge of simming: never asks for a line.
	follower: boolean;
	// Watching a league-mate's live sim that says where its field is: this
	// one plays the game along with it instead, a moment behind (see
	// court3d/follow.ts) - the latest word of where it is.
	along?: FollowReport;
	// Where this field is, for anyone watching along.
	report?: { current: Shown | undefined };
	// How many times "next play" has been asked for.
	skips?: number;
	// Only the key plays (see keyPlays.ts), and the viewer's own team (box
	// score team) among them, if playing.
	keyPlays?: { settings: KeyPlaySettings; mine?: 0 | 1 };
	onReady: () => void;
	// Key plays: on past the plays left out, the page caught up to `cursor`.
	onJump?: (cursor: number) => void;
	// The field cannot play this game: the page goes on without it.
	onFail?: () => void;
	// Playing along, the other field is not showing the same game: the page
	// has this one stepped line by line instead.
	onLost?: () => void;
};

const Field3D = ({
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
}: Props) => {
	const { lid } = useLocal(["lid"]);
	const gid: number | undefined = boxScore?.gid;
	const season: number | undefined = boxScore?.season;
	const raw: any[] = useMemo(
		() => (Array.isArray(boxScore?.teams) ? boxScore.teams : []),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);
	// Display order, as on the 2D field: [away, home].
	const away = raw[1];
	const home = raw[0];

	// The roster is fixed for the game - read it once.
	const roster = useMemo(() => {
		const out: (RosterMan & { name: string; jerseyNumber?: string })[] = [];
		const teams: [any, any] = [away, home];
		for (const t of [0, 1] as const) {
			for (const p of teams[t]?.players ?? []) {
				if (typeof p.pid === "number") {
					out.push({
						pid: p.pid,
						team: t,
						pos: typeof p.pos === "string" ? p.pos : "",
						name: p.name,
						jerseyNumber: p.jerseyNumber,
						...(p.courtRatings ? { ratings: p.courtRatings } : {}),
					});
				}
			}
		}
		return out;
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);

	const onFailRef = useRef(onFail);
	onFailRef.current = onFail;
	const [matched, setMatched] = useState(false);
	useEffect(() => {
		setMatched(false);
		if (!events || events.length === 0) {
			return;
		}
		// THE EVENTS HAVE TO BE THIS GAME'S (see Court3D): wait for the two to
		// agree, and if they never do, give the game back to the page.
		if (!eventsMatchRoster(events, roster)) {
			const id = setTimeout(() => onFailRef.current?.(), ROSTER_WAIT);
			return () => clearTimeout(id);
		}
		setMatched(true);
	}, [events, roster]);
	// Staged off the page (see stageField.ts) - its first play shown in the
	// meantime (see stageOpening), from the start.
	const [timeline, setTimeline] = useState<FieldTimeline | undefined>();
	const opening = useMemo(() => {
		if (!matched || !events) {
			return undefined;
		}
		try {
			return stageOpening({
				events,
				players: roster,
				gid,
				numPeriods: boxScore?.numPeriods,
			});
		} catch {
			return undefined;
		}
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [matched, events, roster, gid]);
	const shownTl =
		timeline ?? (opening && cursor <= opening.upTo ? opening.tl : undefined);
	useEffect(() => {
		setTimeline(undefined);
		if (!matched || !events) {
			return;
		}
		let alive = true;
		stageField({
			events,
			players: roster,
			gid,
			numPeriods: boxScore?.numPeriods,
		}).then(
			(tl) => {
				if (alive) {
					setTimeline(tl);
				}
			},
			(error: unknown) => {
				// A broken staging must not take the whole page down with it, or
				// leave it waiting on a field that will never play.
				console.error("3D field failed to compile", error);
				if (alive) {
					onFailRef.current?.();
				}
			},
		);
		return () => {
			alive = false;
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [matched, events, roster, gid]);
	// Key plays only: the stretches of the game shown.
	const numPeriods: number = boxScore?.numPeriods ?? 4;
	const keyPlan = useMemo(
		(): KeySegment[] | undefined =>
			timeline && events && keyPlays?.settings.on
				? planFieldKeyPlays(timeline, events, keyPlays.settings, {
						rate,
						numPeriods,
						...(keyPlays.mine === undefined ? {} : { mine: keyPlays.mine }),
						...(typeof boxScore?.spread === "number"
							? { spread: boxScore.spread }
							: {}),
					})
				: undefined,
		[timeline, events, keyPlays, rate, numPeriods, boxScore?.spread],
	);

	// The stadium and the field, painted once a game.
	const team = (t: any): FieldTeam => ({
		abbrev: t?.abbrev,
		name: t?.name,
		region: t?.region,
		colors: t?.colors,
		...(t?.stadium ? { stadium: t.stadium } : {}),
	});
	const [logos, setLogos] = useState<[HTMLCanvasElement?, HTMLCanvasElement?]>(
		[],
	);
	const awayLogo = away?.imgURL || away?.imgURLSmall;
	const homeLogo = home?.imgURL || home?.imgURLSmall;
	useEffect(() => {
		let alive = true;
		void Promise.all([logoPicture(awayLogo), logoPicture(homeLogo)]).then(
			(made) => {
				if (alive) {
					setLogos([made[0], made[1]]);
				}
			},
		);
		return () => {
			alive = false;
		};
	}, [awayLogo, homeLogo]);
	const paint = useMemo((): FieldPaint => {
		const h = team(home);
		const a = team(away);
		const venue = venueFor(h, {
			gid: gid ?? 0,
			primeTime: boxScore?.primeTime === true,
		});
		const style = fieldStyleFor(h, a, false, venue);
		const seed = String(gid ?? 0);
		const mid = logos[1];
		return {
			style,
			turf: paintTurf(style, seed),
			endZones: [paintEndZone(style, 0), paintEndZone(style, 1)],
			numbers: paintNumbers(style),
			...(mid ? { logo: paintMidLogo(mid) } : {}),
			stadium: paintStadium(
				h,
				a,
				seed,
				{ att: boxScore?.att, capacity: boxScore?.arena?.capacity },
				mid,
				venue,
			),
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid, logos]);

	// Faces arrive asynchronously; until then a player has a plain skin tone
	// and an average build.
	const faces = useRef(new Map<number, PlayerFace | null>());
	const [facesVersion, setFacesVersion] = useState(0);
	const onFace = useCallback((pid: number, face: PlayerFace | null) => {
		if (faces.current.get(pid) === face) {
			return;
		}
		faces.current.set(pid, face);
		setFacesVersion((v) => v + 1);
	}, []);

	const kits = useMemo(
		() =>
			fieldKitsFor(away?.colors, home?.colors, [away?.helmet, home?.helmet]),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);
	const appearance = useMemo(() => {
		const looks = new Map<number, FieldLook>();
		const bodies = new Map<number, Body>();
		for (const p of roster) {
			const f = faces.current.get(p.pid) ?? undefined;
			const kit = kits[p.team];
			const head = headColors(f?.face);
			looks.set(p.pid, {
				kit,
				gear: fieldGearFor(p.pid, p.pos, kit),
				skin: head.skin,
				hair: head.hair,
				jerseyNumber: f?.jerseyNumber ?? p.jerseyNumber ?? "",
				lastName: lastNameOf(p.name),
				...(logos[p.team] && [away, home][p.team]?.helmet?.logo !== false
					? { logo: logos[p.team] }
					: {}),
			});
			bodies.set(p.pid, drawnFootballBody(f?.hgt, f?.weight));
		}
		for (const o of crewFor(gid ?? 0)) {
			looks.set(o.id, officialLook(o.o === "R", o.number, `${gid}|${o.o}`));
			bodies.set(o.id, drawnOfficialBody(o.hgt, o.weight));
		}
		for (const o of chainCrewFor(gid ?? 0)) {
			looks.set(o.id, chainCrewLook(`${gid}|${o.c}`));
			bodies.set(o.id, drawnOfficialBody(o.hgt, o.weight));
		}
		for (const team of [0, 1] as const) {
			for (let i = 0; i < COACHES; i++) {
				const id = coachId(team, i);
				looks.set(id, coachLook(kits[team], `${gid}|${id}`, i === 0));
				bodies.set(
					id,
					drawnOfficialBody(68 + ((i * 5) % 7), 180 + ((i * 17) % 70)),
				);
			}
			for (let i = 0; i < STAFF; i++) {
				const id = staffId(team, i);
				looks.set(id, trainerLook(kits[team], `${gid}|${id}`));
				bodies.set(
					id,
					drawnOfficialBody(67 + ((i * 3) % 8), 170 + ((i * 23) % 60)),
				);
			}
			// The trainers, out to a man hurt.
			for (let n = 0; n < HURT_MOST; n++) {
				for (let k = 0; k < MEDICS; k++) {
					const id = medicId(team, n, k);
					looks.set(id, trainerLook(kits[team], `${gid}|${id}`));
					bodies.set(id, drawnOfficialBody(68 + ((k * 3) % 5), 180 + k * 14));
				}
			}
		}
		for (const o of tvCrewFor(gid ?? 0)) {
			looks.set(o.id, tvCrewLook(`${gid}|${o.id}`));
			bodies.set(o.id, drawnOfficialBody(o.hgt, o.weight));
		}
		for (const [k, s] of photographersFor(gid ?? 0).entries()) {
			looks.set(s.id, photographerLook(`${gid}|${s.id}`));
			bodies.set(
				s.id,
				drawnOfficialBody(67 + ((k * 7) % 8), 170 + ((k * 13) % 60)),
			);
		}
		return { looks, bodies };
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [roster, kits, logos, facesVersion]);
	// The men on each sideline, where each stands all game.
	const sideline = useMemo(
		() => sidelineFor(roster, gid ?? 0),
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[roster],
	);
	const sidelineRef = useRef(sideline);
	sidelineRef.current = sideline;
	const photographers = useMemo(() => photographersFor(gid ?? 0), [gid]);
	const photographersRef = useRef(photographers);
	photographersRef.current = photographers;
	const sideColors = useRef<[string, string]>([kits[0].helmet, kits[1].helmet]);
	sideColors.current = [kits[0].helmet, kits[1].helmet];
	const looks = useRef(appearance.looks);
	const bodies = useRef(appearance.bodies);
	looks.current = appearance.looks;
	bodies.current = appearance.bodies;

	const wrapRef = useRef<HTMLDivElement | null>(null);
	const canvasRef = useRef<HTMLCanvasElement | null>(null);
	const [size, setSize] = useState({ w: 640, h: 360 });
	useLayoutEffect(() => {
		const el = wrapRef.current;
		if (!el) {
			return;
		}
		const measure = () => {
			const r = el.getBoundingClientRect();
			setSize({ w: r.width, h: r.height });
		};
		measure();
		const observer = new ResizeObserver(measure);
		observer.observe(el);
		return () => observer.disconnect();
	}, []);
	const narrow = size.w < NARROW_W;

	const bugRef = useRef<HTMLDivElement | null>(null);
	const covers = useRef({ at: -Infinity, boxes: [] as TagCover[] });
	const dipRef = useRef<HTMLDivElement | null>(null);
	const stingRefs = useRef<[HTMLDivElement | null, HTMLDivElement | null]>([
		null,
		null,
	]);
	// Each man's side, for the wipe in the colors of whoever made the play.
	const sideOf = useRef(new Map<number, Side>());
	sideOf.current = new Map(roster.map((r) => [r.pid, r.team]));
	const replayRef = useRef<HTMLDivElement | null>(null);
	const clockRef = useRef<HTMLSpanElement | null>(null);
	const shotRef = useRef<HTMLSpanElement | null>(null);
	const downRef = useRef<HTMLSpanElement | null>(null);
	const foulsAwayRef = useRef<HTMLSpanElement | null>(null);
	const foulsHomeRef = useRef<HTMLSpanElement | null>(null);
	const ballAwayRef = useRef<HTMLSpanElement | null>(null);
	const ballHomeRef = useRef<HTMLSpanElement | null>(null);

	const clock = useRef(makeFieldClock(paused, skips));
	const cam = useRef({
		x: FIELD_L / 2,
		vx: 0,
		w: 200,
		vw: 0,
		y: 80,
		vy: 0,
		z: 2,
		vz: 0,
	});
	const glide = useRef<Glide>({ was: [], off: [], ms: 0 });
	// The turn each man was drawn in last frame (see steadied).
	const turns = useRef(
		new Map<number, { n: number; turn: number; at: number }>(),
	);
	const shown = useRef({ down: "", ball: -1 });
	useEffect(() => {
		if (shownTl) {
			onFieldCursor(clock.current, shownTl, {
				cursor,
				paused,
				skips,
				follower,
			});
		}
	}, [cursor, follower, paused, skips, shownTl]);
	// What this field staged, said along with where it is (see
	// fieldStagedAs) - and, playing along, the other field's word.
	const fp = useMemo(
		() => (timeline ? fieldStagedAs(timeline) : undefined),
		[timeline],
	);
	const alongWith = useRef(makeFollow());
	useEffect(() => {
		if (along) {
			addReport(alongWith.current, along);
		}
	}, [along]);
	// Playing along with a field that staged something else: stepped line
	// by line instead, said once.
	const lostSaid = useRef(false);
	useEffect(() => {
		if (along?.fp && fp && along.fp !== fp && !lostSaid.current) {
			lostSaid.current = true;
			onLost?.();
		}
	}, [along?.fp, fp, onLost]);
	// (No line past the opening until the rest is staged.)
	const eventsLength = timeline
		? (events?.length ?? 0)
		: Math.min(events?.length ?? 0, opening?.upTo ?? 0);

	const live = useRef({
		cursor,
		paused,
		rate,
		follower,
		// (The game's own staging there, not the opening: see stageOpening.)
		along: !!along && !!timeline,
		report: timeline ? report : undefined,
		fp,
		onLost,
		keyPlan,
		onReady,
		onJump,
		timeline: shownTl,
		paint,
		size,
		eventsLength,
	});
	live.current = {
		cursor,
		paused,
		rate,
		follower,
		// (The game's own staging there, not the opening: see stageOpening.)
		along: !!along && !!timeline,
		report: timeline ? report : undefined,
		fp,
		onLost,
		keyPlan,
		onReady,
		onJump,
		timeline: shownTl,
		paint,
		size,
		eventsLength,
	};

	useEffect(() => {
		// The picture is drawn small - pixel art - and the page blows it up
		// without smoothing (see the canvas's style).
		const scratch = makeScratch();
		// Hands off the page's own thread to make new pictures on, where the
		// device has them (see farm.ts) - two, if it has the cores: twice the
		// men basketball has, most of them on the move at once.
		const farm =
			typeof Worker === "undefined" || typeof OffscreenCanvas === "undefined"
				? undefined
				: makeFieldFarm(
						spawnFieldWorker,
						(navigator.hardwareConcurrency || 2) >= 4 ? 2 : 1,
					);
		let sprites = makeSpriteCache();
		sprites.budget = SPRITE_BUDGET_MS;
		// When finer poses were last taken up, and last given up for want of
		// time.
		let fineOnAt = -Infinity;
		let fineOffAt = -Infinity;
		const handsOf = handsOfFor(
			(pid) => bodies.current.get(pid) ?? DRAWN_DEFAULT,
			() => sprites.fine === true,
		);
		const res = makeResolution(performance.now() + 1500);
		// How often it is drawn (see court3d/pace.ts) - and whether this frame
		// says anything about how fast the device draws (not paused).
		const pacing = makePace(performance.now() + 1500);
		let judging = true;
		const wipe = makeFieldWipe();
		let wipeSide: Side = 1;

		const tick = (now: number, draw: boolean) => {
			const p = live.current;
			const tl = p.timeline;
			if (!tl) {
				return;
			}
			const s = clock.current;
			const { dt, cut } = stepField(
				s,
				p,
				tl,
				now,
				alongWith.current,
				Date.now(),
			);
			if (s.lost && p.along && !lostSaid.current) {
				lostSaid.current = true;
				p.onLost?.();
			}
			if (!draw) {
				return;
			}
			const canvas = canvasRef.current;
			const ctx = canvas?.getContext("2d");
			const { w, h } = p.size;
			if (!canvas || !ctx || w <= 0 || h <= 0) {
				return;
			}
			const dpr = Math.min(3, window.devicePixelRatio || 1);
			const art = artFor(h * dpr, res);
			const fw = Math.ceil((w * dpr) / art);
			const fh = Math.ceil((h * dpr) / art);
			if (canvas.width !== fw || canvas.height !== fh) {
				canvas.width = fw;
				canvas.height = fh;
			}
			const replay = s.replay;
			const moment = momentAt(tl, replay ? replay.at : s.t, handsOf);
			const aim = replay ? replayAim(moment, replay) : aimFor(moment);
			// (Squarer, the picture takes in a little more of the field.)
			const width = aim.width * (fw / fh < 1.5 ? 0.8 : 1);
			const c = cam.current;
			const lift = aim.z ?? 2;
			if (s.snapCam) {
				c.x = aim.x;
				c.w = width;
				c.y = aim.y;
				c.z = lift;
				c.vx = 0;
				c.vw = 0;
				c.vy = 0;
				c.vz = 0;
				s.snapCam = false;
				glide.current = { was: [aim.x, aim.y, width, lift], off: [], ms: 0 };
			} else {
				const secs =
					(dt / 1000) * Math.max(1, p.rate) * (replay ? REPLAY_SPEED : 1);
				const [gx, gy, gw, gz] = glideAim(
					glide.current,
					[aim.x, aim.y, width, lift],
					secs * 1000,
				);
				[c.x, c.vx] = follow(c.x, c.vx, gx!, CAM_PAN, secs);
				[c.w, c.vw] = follow(c.w, c.vw, gw!, CAM_ZOOM, secs);
				[c.y, c.vy] = follow(c.y, c.vy, gy!, CAM_ZOOM, secs);
				[c.z, c.vz] = follow(c.z, c.vz, gz!, CAM_ZOOM, secs);
			}
			const view = makeFieldCamera(
				{ x: c.x, width: c.w, y: c.y, z: c.z },
				fw,
				fh,
				replay ? REPLAY_RIG : MAIN_RIG,
			);
			const look = (pid: number) => looks.current.get(pid);
			const body = (pid: number) => bodies.current.get(pid) ?? DRAWN_DEFAULT;
			const items: { depth: number; draw: () => void }[] = [];
			const shadow = p.paint.stadium.style.shadow;
			const snappers = photographersAt(photographersRef.current, moment);
			const standing = [
				...moment.players,
				...sidelineAt(sidelineRef.current, tl, moment),
				...snappers,
			];
			// (Their new pictures made before anything is drawn: see
			// prepareFieldSprites.)
			const men: Parameters<typeof prepareFieldSprites>[2] = [];
			standing.forEach((st, i) => {
				const l = st.bare ? bareLook(look(st.id)) : look(st.id);
				if (!l) {
					return;
				}
				const shaded = inShadow(shadow, st);
				const up = drawnUp(steadied(st, turns.current, sprites.fine === true));
				men.push({
					st: up,
					body: body(st.id),
					look: l,
					shaded,
					off: i >= moment.players.length,
				});
				items.push({
					depth: depthOf(view, { x: st.x, y: st.y, z: 0 }),
					draw: () => {
						drawFieldPlayer(
							ctx,
							scratch,
							view,
							up,
							body(st.id),
							l,
							1,
							sprites,
							shaded,
						);
					},
				});
			});
			const ball = moment.ball;
			if (ball) {
				const axis = ball.axis;
				items.push({
					depth: depthOf(view, { ...ball.at, z: 0 }),
					draw: () => {
						if (axis) {
							drawFlyingBall(ctx, view, drawnUp(ball.at), axis);
						} else {
							drawLooseBall(ctx, view, drawnUp(ball.at), ball.stood);
						}
					},
				});
			}
			for (const b of netBallsAt(sidelineRef.current, moment)) {
				const axis = b.axis;
				items.push({
					depth: depthOf(view, { ...b.at, z: 0 }),
					draw: () => {
						if (axis) {
							drawFlyingBall(ctx, view, drawnUp(b.at), axis);
						} else {
							drawLooseBall(ctx, view, drawnUp(b.at), b.stood);
						}
					},
				});
			}
			items.push(
				...chainGear(ctx, view, moment, body),
				...sidelineGear(ctx, view, sideColors.current),
				...cameraGear(ctx, view, snappers, body),
				...tvCameraGear(ctx, view, moment.players, body),
				...waterGear(ctx, view, moment.players, body),
				...showerGear(ctx, view, moment, body, sprites.fine === true),
				...runOutGear(ctx, view, moment.play?.runOut, moment.t),
			);
			for (const f of moment.flags ?? []) {
				items.push({
					depth: depthOf(view, { ...f.at, z: 0 }),
					draw: () => {
						drawFlag(ctx, view, drawnUp(f.at), f.down, f.turn);
					},
				});
			}
			startFrame(sprites);
			const drawStart = performance.now();
			prepareFieldSprites(scratch, view, men, 1, sprites, farm);
			drawFieldFrame({
				ctx,
				cam: view,
				paint: p.paint,
				items,
				pools: poolsOf(standing, moment.ball?.at),
				crowd: crowdAt(tl, moment.t, now),
				...(replay ? {} : { marks: marksAt(moment.play, moment.t) }),
			});
			if (now - covers.current.at > 500) {
				covers.current = {
					at: now,
					boxes: coversOf(wrapRef.current, bugRef.current),
				};
			}
			drawTags(
				ctx,
				view,
				moment.play,
				moment.t,
				moment.players,
				look,
				Math.max(1, Math.round(8 / ((7 * art) / dpr))),
				covers.current.boxes,
			);
			if (
				judging &&
				adjust(
					res,
					h * dpr,
					dt,
					performance.now() - drawStart,
					now,
					sprites.spent,
				)
			) {
				releaseSprites(sprites);
				sprites = makeSpriteCache();
				sprites.budget = SPRITE_BUDGET_MS;
			}
			// Time to spare, at the finest picture: poses in finer steps - and
			// back to the usual ones well before that costs a size.
			const spare = res.drawMs;
			if (res.coarser > 0 || spare === undefined || pacing.fps !== FAST) {
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
			// Into and out of black across a cut.
			const dip = dipRef.current;
			if (dip) {
				const near = tl.cuts.reduce(
					(m, x) => Math.min(m, Math.abs(s.t - x)),
					Infinity,
				);
				// (And between the key plays: down to black out of one, up out
				// of it into the next.)
				const keyOut =
					p.keyPlan && !p.follower && !p.paused
						? keyOutAt(p.keyPlan, s.t, Math.max(1e-6, p.rate))
						: 0;
				const keyUp = 1 - (now - s.keyDip) / KEY_DIP_MS;
				const o = replay
					? 0
					: cut
						? 1
						: Math.max(0, 1 - near / DIP_MS, keyOut, keyUp);
				if (dip.style.opacity !== String(o)) {
					dip.style.opacity = String(o);
				}
			}
			// The wipe: in over the picture to the cut into a replay or out of
			// it, then on off it - in the colors of whoever made the play.
			const sting = wipeAt(wipe, s, p, tl, now);
			if (sting !== undefined) {
				const pid = (replay ?? nextReplay(tl, s.t))?.pid;
				const team = pid === undefined ? undefined : sideOf.current.get(pid);
				if (team !== undefined) {
					wipeSide = team;
				}
			}
			for (const side of [0, 1] as const) {
				const band = stingRefs.current[side];
				if (!band) {
					continue;
				}
				const on = sting !== undefined && side === wipeSide;
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
			// The down and distance, and which side has the ball.
			const play = playAt(tl, s.t);
			const down = play?.situation ?? "";
			if (down !== shown.current.down && downRef.current) {
				shown.current.down = down;
				downRef.current.textContent = down;
			}
			const side = play ? play.offense : -1;
			if (side !== shown.current.ball) {
				shown.current.ball = side;
				[ballAwayRef, ballHomeRef].forEach((ref, i) => {
					if (ref.current) {
						ref.current.style.visibility = i === side ? "visible" : "hidden";
					}
				});
			}
		};

		// Off the screen - scrolled away to the box score - nothing to draw.
		let onScreen = true;
		const seen =
			typeof IntersectionObserver === "undefined"
				? undefined
				: new IntersectionObserver((entries) => {
						onScreen = entries.some((entry) => entry.isIntersecting);
					});
		if (canvasRef.current) {
			seen?.observe(canvasRef.current);
		}
		let raf = requestAnimationFrame(function frame(now) {
			raf = requestAnimationFrame(frame);
			if (!onScreen) {
				tick(now, false);
				return;
			}
			const s = clock.current;
			const paused = live.current.paused && !s.stepping && !s.replay;
			if (!due(pacing, now, paused)) {
				return;
			}
			judging = !paused;
			const began = performance.now();
			tick(now, true);
			drawn(pacing, now, performance.now() - began, paused);
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
			seen?.disconnect();
			releaseSprites(sprites);
			farm?.stop();
		};
	}, []);

	// The game clock, as the page keeps it (it moves on with each line).
	const time: string = boxScore?.time ?? "";
	useEffect(() => {
		if (clockRef.current) {
			clockRef.current.textContent = time;
		}
	}, [time]);

	const awayPts: number = boxScore?.teams?.[1]?.pts ?? 0;
	const homePts: number = boxScore?.teams?.[0]?.pts ?? 0;
	const quarter = boxScore?.quarterShort ?? "";
	const captionTeam = captionT === undefined ? undefined : raw[captionT];
	const recordOf = (t: any): string | undefined =>
		typeof t?.won === "number" && typeof t?.lost === "number"
			? helpers.formatRecord(t)
			: undefined;
	const bugTeam = (t: any, pts: number, i: 0 | 1) =>
		t && {
			abbrev: t.abbrev,
			region: t.region,
			name: t.name,
			colors: t.colors,
			imgURL: t.imgURL,
			imgURLSmall: t.imgURLSmall,
			pts,
			timeouts: boxScore?.teams?.[i]?.timeouts,
			record: recordOf(boxScore?.teams?.[i]),
		};
	const refs: BugRefs = {
		clock: clockRef,
		shot: shotRef,
		fouls: [foulsAwayRef, foulsHomeRef],
		ball: [ballAwayRef, ballHomeRef],
		down: downRef,
	};
	const bug = {
		away: bugTeam(away, awayPts, 1),
		home: bugTeam(home, homePts, 0),
		quarter,
		totalTimeouts: STARTING_NUM_TIMEOUTS,
		refs,
	};
	// The league's own bug, if it has one (see ScoreBugStyle).
	const own: ScoreBugStyle | null | undefined = boxScore?.scoreBug;

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
			{roster.map((p) => (
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
			<canvas
				ref={canvasRef}
				role="img"
				aria-label="Live field"
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
					pointerEvents: "none",
				}}
			>
				REPLAY
			</div>
			{!shownTl && events && events.length > 0 ? (
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
			<div ref={bugRef} style={{ display: "contents" }}>
				{own ? <CustomScoreBug style={own} {...bug} /> : <ScoreBug {...bug} />}
			</div>
			{caption ? (
				<div
					className="court3d-caption"
					style={{
						position: "absolute",
						left: "50%",
						// Just over the score bug.
						bottom: own
							? `calc(2.4cqw + ${((own.span ?? 0.6) * 100 * own.height) / own.width}cqw)`
							: "calc(2.4cqw + clamp(10px, 1.75cqw, 16px) * 3.3)",
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

export default Field3D;
