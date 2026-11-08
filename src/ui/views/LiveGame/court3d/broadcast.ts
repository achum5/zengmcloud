import {
	useCallback,
	useEffect,
	useLayoutEffect,
	useMemo,
	useRef,
	useState,
	type RefObject,
} from "react";
import { toWorker } from "../../../util/toWorker.ts";
import type { PlayerFace } from "../../../util/playerFaces.ts";
import type { ReplayLooks } from "../../../../common/types.ts";
import { kitArtOf, loadKitPicture, type KitArt } from "./kitArt.ts";
import { logoPicture } from "./logoPicture.ts";
import type { Stakes } from "./director.ts";
import { drawn, due, type Pace } from "./pace.ts";
import { eventsMatchRoster } from "./rosterMatch.ts";

// THE PAGE'S SIDE OF A 3D GAME, for any sport (see Court3D, Field3D): what
// every sport's picture does the same way round the picture itself - the
// game's stakes, each side's uniform pictures and the home team's ads, the
// picture's size, and its frames.

// A playoff game's stakes, as they stood going into it - the box score's
// series records take this game in once it is over, so they are read once
// (a new `gid`, again), and one won by now is taken back out.
// (Only what it reads of the box score: see the page's own.)
export type SeriesBox = {
	playoffs?: boolean;
	finals?: boolean;
	numGamesToWinSeries?: number;
	gameOver?: boolean;
	won?: { tid?: number };
	teams?: { tid?: number; playoffs?: { won?: number } }[];
};
export const useStakes = (
	gid: number | undefined,
	bs: SeriesBox,
): Stakes | undefined =>
	useMemo(
		() => {
			if (!bs.playoffs || typeof bs.numGamesToWinSeries !== "number") {
				return undefined;
			}
			const won = (raw: 0 | 1) => {
				const t = bs.teams?.[raw];
				const w = Number(t?.playoffs?.won ?? 0);
				return bs.gameOver && bs.won?.tid === t?.tid ? w - 1 : w;
			};
			// (Raw team 0 is home, side 1.)
			return {
				won: [won(1), won(0)] as [number, number],
				toWin: bs.numGamesToWinSeries,
				finals: !!bs.finals,
			};
		},
		// eslint-disable-next-line react-hooks/exhaustive-deps
		[gid],
	);

// THE GAME STAGED, off the page's thread (see stager.ts) - the picture dark
// (or on whatever is shown meanwhile) till it is. THE EVENTS HAVE TO BE
// THIS GAME'S. When the game on the page changes under it - a league-mate
// starts another game while this device is following one - the new game's
// events can arrive a render ahead of its box score, and staging them
// against the previous game's roster puts men on the floor the picture has
// never heard of (the court's field report: a crash in the free throw
// lineup). So it waits for the two to agree (`matched`) - and if they never
// do, gives the game back to the page (`onFail`). Staged again whenever the
// events, the roster or `key` (whatever else it is staged from) change; a
// staging that breaks gives the game back to the page too, rather than
// leaving it waiting on a picture that will never play. `onStaged`: the
// moment it is.
const ROSTER_WAIT = 5000;
export const useStaged = <T>(
	events: any[] | undefined,
	roster: { pid: number }[],
	key: string,
	stage: () => Promise<T>,
	what: string,
	onFail: (() => void) | undefined,
	onStaged?: (tl: T) => void,
): { timeline: T | undefined; matched: boolean } => {
	const [timeline, setTimeline] = useState<T>();
	const [matched, setMatched] = useState(false);
	const onFailRef = useRef(onFail);
	onFailRef.current = onFail;
	const stageRef = useRef(stage);
	stageRef.current = stage;
	const onStagedRef = useRef(onStaged);
	onStagedRef.current = onStaged;
	useEffect(() => {
		setTimeline(undefined);
		setMatched(false);
		if (!events || events.length === 0) {
			return;
		}
		if (!eventsMatchRoster(events, roster)) {
			const id = setTimeout(() => onFailRef.current?.(), ROSTER_WAIT);
			return () => clearTimeout(id);
		}
		setMatched(true);
		let alive = true;
		stageRef.current().then(
			(tl) => {
				if (alive) {
					setTimeline(tl);
					onStagedRef.current?.(tl);
				}
			},
			(error: unknown) => {
				console.error(`3D ${what} failed to compile`, error);
				if (alive) {
					onFailRef.current?.();
				}
			},
		);
		return () => {
			alive = false;
		};
	}, [events, roster, key, what]);
	return { timeline, matched };
};

// Each side's uniform drawn from a picture, if its team has one: the
// visitors' away one, the home team's home one. A replay wears the ones worn
// that night (`away`, `home`: the teams with that night's looks laid over
// them) - or, where one of those has since been replaced, the team's own now
// (`teams`, the box score's). Loaded once a game.
export const useKitArts = (
	gid: number | undefined,
	teams: any,
	away: any,
	home: any,
): [KitArt?, KitArt?] => {
	const [arts, setArts] = useState<[KitArt?, KitArt?]>([]);
	useEffect(() => {
		const now: any[] = Array.isArray(teams) ? teams : [];
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
	return arts;
};

// The home team's own pictures for its ads, once loaded (see adsFor) - each
// no bigger than `px` across.
export const useAdPictures = (
	ads: { imageURL?: string }[] | undefined,
	px?: number,
): Map<string, HTMLCanvasElement> => {
	const adURLs = (ads ?? [])
		.map((a) => a.imageURL)
		.filter((u): u is string => !!u)
		.join("\n");
	const [pictures, setPictures] = useState<Map<string, HTMLCanvasElement>>(
		() => new Map(),
	);
	useEffect(() => {
		const urls = adURLs ? adURLs.split("\n") : [];
		if (urls.length === 0) {
			return;
		}
		let alive = true;
		void Promise.all(urls.map((u) => logoPicture(u, px))).then((made) => {
			if (alive) {
				const got = new Map<string, HTMLCanvasElement>();
				made.forEach((c, i) => {
					if (c) {
						got.set(urls[i]!, c);
					}
				});
				setPictures(got);
			}
		});
		return () => {
			alive = false;
		};
	}, [adURLs, px]);
	return pictures;
};

// THE PLAYERS' FACES, as they come in (see FaceLoader) - or, a replay, the
// faces of that night, here already. Till his is in, a player has a plain
// head and an average build. `also`: whatever more a sport makes of a face
// once it is in (the court draws his head from it) - `current` saying it
// is still the one he has, `bump` that there is something new to draw.
export const useFaces = (
	looksThen: ReplayLooks | undefined,
	roster: { pid: number }[],
	also?: (
		pid: number,
		face: PlayerFace | null,
		current: () => boolean,
		bump: () => void,
	) => void,
): {
	faces: RefObject<Map<number, PlayerFace | null>>;
	facesVersion: number;
	onFace: (pid: number, face: PlayerFace | null) => void;
} => {
	const faces = useRef(new Map<number, PlayerFace | null>());
	const [facesVersion, setFacesVersion] = useState(0);
	const alsoRef = useRef(also);
	alsoRef.current = also;
	const onFace = useCallback((pid: number, face: PlayerFace | null) => {
		if (faces.current.get(pid) === face) {
			return;
		}
		faces.current.set(pid, face);
		const bump = () => {
			setFacesVersion((v) => v + 1);
		};
		bump();
		alsoRef.current?.(pid, face, () => faces.current.get(pid) === face, bump);
	}, []);
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
	return { faces, facesVersion, onFace };
};

// The picture's size, as laid out on the page (CSS px) - and kept up with.
export const useViewSize = (
	wrapRef: RefObject<HTMLDivElement | null>,
): { w: number; h: number } => {
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
	}, [wrapRef]);
	return size;
};
// Narrower than this (CSS px), the picture is squarer (see each sport's).
export const NARROW_W = 560;

// THE FRAMES: `tick` once an animation frame - drawing it (`draw`) when it is
// due at the pace the device can keep up (see pace.ts; `still`: nothing on
// the move, so slower will do - and said to `tick`, nothing to judge the
// device by), just keeping the game going when the picture is off the screen
// (scrolled away to the box score) - and, in a background tab, which gets no
// animation frames, kept going a few times a second anyway, or a hidden tab
// would hold up everyone watching. The way to stop them.
export const runFrames = ({
	canvas,
	pacing,
	still,
	tick,
}: {
	canvas: HTMLCanvasElement | null;
	pacing: Pace;
	still: () => boolean;
	tick: (now: number, draw: boolean, still: boolean) => void;
}): (() => void) => {
	let onScreen = true;
	const seen =
		typeof IntersectionObserver === "undefined"
			? undefined
			: new IntersectionObserver((entries) => {
					onScreen = entries.some((entry) => entry.isIntersecting);
				});
	if (canvas) {
		seen?.observe(canvas);
	}
	let raf = requestAnimationFrame(function frame(now) {
		raf = requestAnimationFrame(frame);
		if (!onScreen) {
			tick(now, false, true);
			return;
		}
		const paused = still();
		if (!due(pacing, now, paused)) {
			return;
		}
		const began = performance.now();
		tick(now, true, paused);
		drawn(pacing, now, performance.now() - began, paused);
	});
	const interval = setInterval(() => {
		if (document.hidden) {
			tick(performance.now(), false, true);
		}
	}, 250);
	return () => {
		cancelAnimationFrame(raf);
		seen?.disconnect();
		clearInterval(interval);
	};
};

// THE PICTURE DIPPED, FLASHED, WIPED (see Overlays).
// How long the picture dips to black either side of a cut (timeline ms).
export const DIP_MS = 130;
// Up out of black onto the next of the key plays (ms).
export const KEY_DIP_MS = 320;
// A replay cut from one of its cameras to the next: a flash of white, this
// long (ms).
export const FLASH_MS = 260;
// The wipe into a replay and out of it (see show.ts wipeAt): a band in the
// colors of the team whose play it is, this wide (a share of the picture's
// width, %) and slanted this much (degrees).
export const STING_W = 140;
export const STING_SLANT = 18;

// How dark the picture is (0 to 1): dipped to black either side of a cut
// (`near`, timeline ms off the nearest), coming up out of it onto the next
// of the key plays (`keyDip`, when it went on to it - or held black there
// till then: see show.ts keyHold), going down into it at the end of one
// (`keyOut` - see keyPlays.ts keyOutAt) - and anything else a sport dips to
// black for (`also`). A replay only comes up out of black.
export const dipOf = ({
	replay,
	near,
	keyDip,
	now,
	keyOut = 0,
	also = 0,
}: {
	replay: boolean;
	near: number;
	keyDip: number;
	now: number;
	keyOut?: number;
	also?: number;
}): number => {
	const keyDark = 1 - (now - keyDip) / KEY_DIP_MS;
	return replay
		? Math.max(0, keyDark)
		: Math.max(0, 1 - near / DIP_MS, also, keyDark, keyOut);
};

// The wipe this frame: the band in the colors of the side whose play it is
// (`side`) across the picture by `sting` (0 to 1; undefined, off it) - from
// all the way off its left to all the way off its right, the slant and all
// - and the REPLAY tag up while a replay shows, once the wipe is most of the
// way across. (`w`, `h`: the picture's size, CSS px.)
export const showWipe = (
	bands: [HTMLDivElement | null, HTMLDivElement | null],
	tag: HTMLDivElement | null,
	sting: number | undefined,
	side: 0 | 1,
	replay: boolean,
	w: number,
	h: number,
) => {
	for (const s of [0, 1] as const) {
		const band = bands[s];
		if (!band) {
			continue;
		}
		const on = sting !== undefined && s === side;
		const show = on ? "flex" : "none";
		if (band.style.display !== show) {
			band.style.display = show;
		}
		if (on && sting !== undefined) {
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
	if (tag) {
		const show =
			replay && (sting === undefined || sting > 0.75) ? "block" : "none";
		if (tag.style.display !== show) {
			tag.style.display = show;
		}
	}
};

// An element's opacity, set only when it is not that already.
export const setOpacity = (el: HTMLElement | null, o: number | string) => {
	if (el && el.style.opacity !== String(o)) {
		el.style.opacity = String(o);
	}
};

// WHERE SOMETHING OVER THE PICTURE IS (the score bug, the caption): its box
// as shares of the picture - or nothing, not laid out.
export type Box = { x0: number; x1: number; y0: number; y1: number };
export const boxOf = (
	wrap: HTMLElement | null,
	el: Element | null | undefined,
): Box | undefined => {
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
export const captionOf = (wrap: HTMLElement | null) =>
	wrap?.querySelector<HTMLElement>(".court3d-caption") ?? null;

// The line across the top (see Overlays' Caption) goes faint while the ball
// is behind it (`ball`, on the picture as shares of it - a shot or a kick
// arcing up from the far side) and comes back as it drops; and is off the
// picture through a replay, the way a broadcast shows one clean but for
// its REPLAY tag. (`box`: where it is - see boxOf.)
const CAPTION_FAINT = 0.18;
export const showCaption = (
	el: HTMLElement | null,
	box: Box | undefined,
	ball: { x: number; y: number } | undefined,
	replay: boolean,
) => {
	if (!el) {
		return;
	}
	const faint =
		box !== undefined &&
		ball !== undefined &&
		ball.x > box.x0 - 0.02 &&
		ball.x < box.x1 + 0.02 &&
		ball.y > box.y0 - 0.02 &&
		ball.y < box.y1 + 0.03;
	const o = replay ? "0" : faint ? String(CAPTION_FAINT) : "";
	if (el.style.opacity !== o) {
		el.style.opacity = o;
	}
};
