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
import { adjust, artFor, makeResolution } from "../court3d/resolution.ts";
import { eventsMatchRoster } from "../court3d/rosterMatch.ts";
import { ScoreBug, type BugRefs } from "../court3d/ScoreBug.tsx";
import {
	makeScratch,
	makeSpriteCache,
	releaseSprites,
	startFrame,
} from "../court3d/sprite.ts";
import type { Body } from "../court3d/poses.ts";
import { aimFor } from "./aim.ts";
import { drawFlyingBall, drawLooseBall } from "./ball.ts";
import { drawFlag } from "./flag.ts";
import { drawnBody, FIGURE_SCALE, footballBody, officialBody } from "./body.ts";
import { makeFieldCamera, MAIN_RIG, REPLAY_RIG } from "./camera.ts";
import { compileField, type FieldTimeline } from "./director.ts";
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
import { FIELD_L } from "./geometry.ts";
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
	makeFieldClock,
	nextReplay,
	onFieldCursor,
	REPLAY_RATE_MAX,
	REPLAY_SPEED,
	stepField,
} from "./playback.ts";
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
import { drawFieldPlayer, drawnBall } from "./sprite.ts";
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
// Replay-time ms the picture takes to come up out of black and go back down,
// and (ms) to come back up out of it to the game.
const REPLAY_DIP = 90;
const BACK_DIP_MS = 180;
// How quickly the camera pans and zooms after what it follows (per second).
const CAM_PAN = 3.2;
const CAM_ZOOM = 2.4;
// The most of a frame (ms) given to drawing players in poses not drawn
// before; over that, a man waits a frame for his new one (see sprite.ts).
const SPRITE_BUDGET_MS = 6;
// Drawing a frame in under this long (ms, smoothed), there is time for the
// men's poses in finer steps (see sprite.ts); over that, back to the usual
// ones - and not tried again for a while.
const FINE_ON_MS = 5;
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
			{ x: st.x, y: st.y, z: st.z * FIGURE_SCALE, yaw: drawn.yaw },
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
	// How many times "next play" has been asked for.
	skips?: number;
	onReady: () => void;
	// The field cannot play this game: the page goes on without it.
	onFail?: () => void;
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
	skips = 0,
	onReady,
	onFail,
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
					});
				}
			}
		}
		return out;
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [gid]);

	// The game staged - in no time at all, so right here.
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
	const timeline = useMemo((): FieldTimeline | undefined => {
		if (!matched || !events) {
			return undefined;
		}
		try {
			return compileField({
				events,
				players: roster,
				gid,
				numPeriods: boxScore?.numPeriods,
			});
		} catch (error) {
			console.error("3D field failed to compile", error);
			return undefined;
		}
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [matched, events, roster, gid]);
	useEffect(() => {
		if (matched && !timeline) {
			onFailRef.current?.();
		}
	}, [matched, timeline]);

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
	const shown = useRef({ down: "", ball: -1 });
	useEffect(() => {
		if (timeline) {
			onFieldCursor(clock.current, timeline, { cursor, paused, skips });
		}
	}, [cursor, paused, skips, timeline]);

	const live = useRef({
		cursor,
		paused,
		rate,
		follower,
		onReady,
		timeline,
		paint,
		size,
		eventsLength: events?.length ?? 0,
	});
	live.current = {
		cursor,
		paused,
		rate,
		follower,
		onReady,
		timeline,
		paint,
		size,
		eventsLength: events?.length ?? 0,
	};

	useEffect(() => {
		// The picture is drawn small - pixel art - and the page blows it up
		// without smoothing (see the canvas's style).
		const scratch = makeScratch();
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

		const tick = (now: number, draw: boolean) => {
			const p = live.current;
			const tl = p.timeline;
			if (!tl) {
				return;
			}
			const s = clock.current;
			const { dt, cut } = stepField(s, p, tl, now);
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
			} else {
				const secs =
					(dt / 1000) * Math.max(1, p.rate) * (replay ? REPLAY_SPEED : 1);
				[c.x, c.vx] = follow(c.x, c.vx, aim.x, CAM_PAN, secs);
				[c.w, c.vw] = follow(c.w, c.vw, width, CAM_ZOOM, secs);
				[c.y, c.vy] = follow(c.y, c.vy, aim.y, CAM_ZOOM, secs);
				[c.z, c.vz] = follow(c.z, c.vz, lift, CAM_ZOOM, secs);
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
			for (const st of standing) {
				const l = st.bare ? bareLook(look(st.id)) : look(st.id);
				if (!l) {
					continue;
				}
				const shaded = inShadow(shadow, st);
				items.push({
					depth: depthOf(view, { x: st.x, y: st.y, z: 0 }),
					draw: () => {
						drawFieldPlayer(
							ctx,
							scratch,
							view,
							drawnUp(st),
							body(st.id),
							l,
							1,
							sprites,
							shaded,
						);
					},
				});
			}
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
			if (adjust(res, h * dpr, dt, performance.now() - drawStart, now)) {
				releaseSprites(sprites);
				sprites = makeSpriteCache();
				sprites.budget = SPRITE_BUDGET_MS;
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
			// Into and out of black across a cut.
			const dip = dipRef.current;
			if (dip) {
				const near = tl.cuts.reduce(
					(m, x) => Math.min(m, Math.abs(s.t - x)),
					Infinity,
				);
				// (And down into a replay coming up, and up out of one.)
				const coming = nextReplay(tl, s.t);
				const toReplay =
					coming && !s.stepping && p.rate <= REPLAY_RATE_MAX
						? coming.cue - s.t
						: Infinity;
				const o = replay
					? Math.max(
							0,
							1 -
								Math.min(replay.at - replay.from, replay.to - replay.at) /
									REPLAY_DIP,
						)
					: cut
						? 1
						: Math.max(
								0,
								1 - Math.min(near, toReplay) / DIP_MS,
								1 - (now - s.backAt) / BACK_DIP_MS,
							);
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
			<div ref={bugRef} style={{ display: "contents" }}>
				<ScoreBug
					away={bugTeam(away, awayPts, 1)}
					home={bugTeam(home, homePts, 0)}
					quarter={quarter}
					totalTimeouts={STARTING_NUM_TIMEOUTS}
					refs={refs}
				/>
			</div>
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

export default Field3D;
