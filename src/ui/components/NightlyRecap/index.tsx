import { Dropdown } from "react-bootstrap";
import {
	type ReactNode,
	type RefObject,
	Suspense,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import {
	DEFAULT_RECAP_LENGTH,
	lengthRow,
	pickRecapPlays,
	playsPerGame,
	RECAP_LENGTHS,
	starPlays,
	type RecapLength,
} from "../../../common/recapLength.ts";
import {
	subReel,
	type RecapPlay,
	type RecapReel,
} from "../../../common/recapReel.ts";
import type {
	RecapNight,
	RecapNightGame,
	RecapStandings,
	RecapStar,
	RecapTopPlay,
} from "../../../worker/util/nightlyRecap.ts";
import { isLine } from "../../util/filterPlayerHighlights.ts";
import { safeLocalStorage } from "../../util/safeLocalStorage.ts";
import { ClipCourt } from "../../views/LiveGame/gamecast/HighlightClip.tsx";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
import {
	COUNT_STINGER_MS,
	CountStinger,
	FinalCard,
	GameRecapCard,
	OPEN_MS,
	posterTitleTop,
	RecapPoster,
	StandingsCard,
	STANDINGS_MS,
	STAR_STINGER_MS,
	StarStinger,
	StarStripBox,
	Stinger,
	STINGER_MS,
	WIPE_MS,
} from "./RecapCards.tsx";

// THE NIGHTLY RECAP: a night's games, the biggest first, each its GAME RECAP
// card, its plays on the 3D court under its own score bug, and its FINAL
// card; then the Player of the Night's plays, the night's top plays counted
// down and the standings - at the length picked on this device. Nothing at
// all for a night with no recap plays (see core/game/saveRecapReels.ts).

const CARD_MS = 4500;
// How long the controls stay up over the picture after a touch.
const CONTROLS_MS = 2500;
const FINAL_MS = 5500;
// About how long a play runs, for the running times.
const PLAY_S = 7;
// Below this wide, the 3D court's picture is 4:3 (see court3d/broadcast.ts).
const NARROW_W = 560;

const LENGTH_KEY = "nightlyRecapLength";
const readLength = (): RecapLength => {
	const raw = safeLocalStorage.getItem(LENGTH_KEY);
	return RECAP_LENGTHS.some((row) => row.key === raw)
		? (raw as RecapLength)
		: DEFAULT_RECAP_LENGTH;
};

// How fast the plays run on the 3D court, picked on each device (the cards
// keep their own time).
const RECAP_SPEEDS = [1, 2] as const;
type RecapSpeed = (typeof RECAP_SPEEDS)[number];
const SPEED_KEY = "nightlyRecapSpeed";
const readSpeed = (): RecapSpeed =>
	safeLocalStorage.getItem(SPEED_KEY) === "2" ? 2 : 1;

type Feed = {
	events: any[];
	initialBoxScore: any;
	plays: RecapPlay[];
};

// The night's games' plays, each fetched once (a game's plays come back in
// the Player of the Night's and the top plays), and fetched ahead.
type Feeds = Map<number, Promise<Feed | null>>;
const loadFeed = (feeds: Feeds, gid: number) => {
	let had = feeds.get(gid);
	if (!had) {
		had = (async () => {
			try {
				const got: any = await toWorker("main", "getRecapFeed", gid);
				return got && got.plays?.length > 0 ? (got as Feed) : null;
			} catch (error) {
				console.error(error);
				return null;
			}
		})();
		feeds.set(gid, had);
	}
	return had;
};
// undefined: on its way. null: none to play.
const useFeed = (feeds: Feeds, gid: number): Feed | null | undefined => {
	const [got, setGot] = useState<{ gid: number; feed: Feed | null }>();
	useEffect(() => {
		let live = true;
		void loadFeed(feeds, gid).then((feed) => {
			if (live) {
				setGot({ gid, feed });
			}
		});
		return () => {
			live = false;
		};
	}, [feeds, gid]);
	return got?.gid === gid ? got.feed : undefined;
};

// From just before a reel's first play - the game up to there applied at
// once, not staged a period at a time - to its last play's last line, no
// period's end or the game's staged after it.
const spanOf = (clip: RecapReel | null | undefined): [number, number] => {
	if (!clip || clip.plays.length === 0) {
		return [0, 0];
	}
	let from = clip.plays[0]!.from - 1;
	while (from >= 0 && !isLine(clip.events[from])) {
		from -= 1;
	}
	return [from + 1, Math.max(...clip.plays.map((p) => p.to))];
};

// A timer that holds while paused, picking up where it was.
const useHold = (
	on: boolean,
	ms: number,
	paused: boolean,
	onDone: () => void,
) => {
	const left = useRef(ms);
	const onDoneRef = useRef(onDone);
	onDoneRef.current = onDone;
	useEffect(() => {
		if (!on) {
			left.current = ms;
		}
	}, [on, ms]);
	useEffect(() => {
		if (!on || paused) {
			return;
		}
		const start = performance.now();
		const id = setTimeout(() => {
			left.current = ms;
			onDoneRef.current();
		}, left.current);
		return () => {
			clearTimeout(id);
			left.current = Math.max(0, left.current - (performance.now() - start));
		};
	}, [on, paused, ms]);
};

// The 16:9 picture (4:3 on a phone) the graphics fill, over the court or
// standing in for it.
const Frame = ({
	narrow,
	over,
	children,
}: {
	narrow: boolean;
	over?: boolean;
	children?: ReactNode;
}) => (
	<div
		className={over ? undefined : "mb-3"}
		style={{
			position: over ? "absolute" : "relative",
			left: 0,
			top: 0,
			width: "100%",
			aspectRatio: narrow ? "4 / 3" : "16 / 9",
			background: over ? undefined : "#040406",
			borderRadius: 6,
			overflow: "hidden",
			containerType: "inline-size",
			zIndex: over ? 5 : undefined,
			pointerEvents: over ? "none" : undefined,
		}}
	>
		{children}
	</div>
);

// The stinger a line of the game calls for, as the recap cuts to it: into
// the second half, and into each overtime.
const stingerOf = (e: any, numPeriods: number): string | undefined => {
	if (e?.type === "overtime") {
		const k = (e.period ?? numPeriods + 1) - numPeriods;
		return k <= 1 ? "OVERTIME" : `${helpers.ordinal(k)} OVERTIME`;
	}
	if (
		e?.type === "period" &&
		numPeriods % 2 === 0 &&
		e.period === numPeriods / 2 + 1
	) {
		return "2nd HALF";
	}
};

type Playing = {
	feeds: Feeds;
	paused: boolean;
	rate: number;
	narrow: boolean;
	onDone: () => void;
};

const RecapGame = ({
	game,
	length,
	feeds,
	paused,
	rate,
	narrow,
	onDone,
}: Playing & {
	game: RecapNightGame;
	length: RecapLength;
}) => {
	const feed = useFeed(feeds, game.gid);

	const clip = useMemo((): RecapReel | undefined => {
		if (!feed) {
			return;
		}
		const picked = new Set(
			pickRecapPlays(feed.plays, playsPerGame(length), game.mvp).map(
				(p) => p.i,
			),
		);
		return subReel({ events: feed.events, plays: feed.plays }, (p) =>
			picked.has(p.i),
		);
	}, [feed, length, game.mvp]);

	const [start, end] = useMemo(() => spanOf(clip), [clip]);

	const [phase, setPhase] = useState<"card" | "plays" | "final">("card");
	useHold(phase === "card" && feed !== undefined, CARD_MS, paused, () =>
		setPhase(clip ? "plays" : "final"),
	);
	useHold(phase === "final", FINAL_MS, paused, onDone);

	// A stinger called for by the period just begun, up as the picture gets
	// to it (the last play of the half over), the court held under it.
	const [sting, setSting] = useState<string | undefined>();
	useHold(sting !== undefined, STINGER_MS, paused, () => setSting(undefined));
	const numPeriods: number = feed?.initialBoxScore?.numPeriods ?? 4;

	const [home, away] = game.teams;
	return (
		<div style={{ position: "relative" }}>
			{clip && feed && phase !== "final" ? (
				<Suspense fallback={<Frame narrow={narrow} />}>
					<ClipCourt
						events={clip.events}
						start={start}
						end={end}
						initialBoxScore={feed.initialBoxScore}
						paused={paused || phase === "card" || sting !== undefined}
						rate={rate}
						onDone={() => setPhase("final")}
						onFail={() => setPhase("final")}
						onLine={(e) => {
							const text = stingerOf(e, numPeriods);
							if (text !== undefined) {
								setSting(text);
							}
						}}
					/>
				</Suspense>
			) : (
				<Frame narrow={narrow} />
			)}
			{phase === "card" ? (
				<Frame narrow={narrow} over>
					<GameRecapCard away={away} home={home} playoffs={game.playoffs} />
				</Frame>
			) : phase === "final" ? (
				<Frame narrow={narrow} over>
					<FinalCard
						away={away}
						home={home}
						playoffs={game.playoffs}
						overtimes={game.overtimes}
						narrow={narrow}
					/>
				</Frame>
			) : null}
			{sting !== undefined && phase === "plays" ? (
				<Frame narrow={narrow} over>
					<Stinger key={sting} text={sting} />
				</Frame>
			) : null}
		</div>
	);
};

// THE PLAYER OF THE NIGHT'S STRIP over his plays: beside the score bug
// along the foot of the picture where there is room for it (the bug is in
// the middle, or on the right), else just above it - the bug's place is
// its broadcast's, so it is looked for.
const StarStrip = ({
	star,
	boxRef,
}: {
	star: RecapStar;
	boxRef: RefObject<HTMLDivElement | null>;
}) => {
	const stripRef = useRef<HTMLDivElement | null>(null);
	const [bottom, setBottom] = useState("1.6cqw");
	useEffect(() => {
		const fit = () => {
			const strip = stripRef.current;
			const frame = strip?.parentElement?.getBoundingClientRect();
			const bug = boxRef.current
				?.querySelector("[data-score-bug]")
				?.firstElementChild?.getBoundingClientRect();
			if (!strip || !frame || frame.width === 0) {
				return;
			}
			let next = "1.6cqw";
			if (bug && bug.width > 0) {
				const gap = frame.width * 0.012;
				const room = bug.left - frame.left - frame.width * 0.016 - gap;
				next =
					strip.offsetWidth <= room
						? `${Math.max(0, frame.bottom - bug.bottom)}px`
						: `${Math.max(0, frame.bottom - bug.top + gap)}px`;
			}
			setBottom((had) => (had === next ? had : next));
		};
		fit();
		const id = setInterval(fit, 400);
		return () => {
			clearInterval(id);
		};
	}, [boxRef]);
	return (
		<StarStripBox
			name={star.name}
			line={star.line}
			team={star}
			bottom={bottom}
			stripRef={stripRef}
		/>
	);
};

// PLAYER OF THE NIGHT: its stinger, then his plays from his game, his strip
// under them.
const StarSegment = ({
	star,
	n,
	feeds,
	paused,
	rate,
	narrow,
	onDone,
}: Playing & { star: RecapStar; n: number }) => {
	const feed = useFeed(feeds, star.gid);
	const clip = useMemo((): RecapReel | null | undefined => {
		if (feed === undefined) {
			return undefined;
		}
		const picked = new Set(
			feed ? starPlays(feed.plays, star.pid, n).map((p) => p.i) : [],
		);
		return feed && picked.size > 0
			? subReel({ events: feed.events, plays: feed.plays }, (p) =>
					picked.has(p.i),
				)
			: null;
	}, [feed, star.pid, n]);
	const [start, end] = useMemo(() => spanOf(clip), [clip]);
	const onDoneRef = useRef(onDone);
	onDoneRef.current = onDone;
	useEffect(() => {
		if (clip === null) {
			onDoneRef.current();
		}
	}, [clip]);
	const [phase, setPhase] = useState<"sting" | "plays">("sting");
	useHold(phase === "sting" && !!clip, STAR_STINGER_MS, paused, () =>
		setPhase("plays"),
	);
	const boxRef = useRef<HTMLDivElement | null>(null);
	return (
		<div ref={boxRef} style={{ position: "relative" }}>
			{clip && feed ? (
				<Suspense fallback={<Frame narrow={narrow} />}>
					<ClipCourt
						events={clip.events}
						start={start}
						end={end}
						initialBoxScore={feed.initialBoxScore}
						paused={paused || phase === "sting"}
						rate={rate}
						onDone={onDone}
						onFail={onDone}
					/>
				</Suspense>
			) : (
				<Frame narrow={narrow} />
			)}
			<Frame narrow={narrow} over>
				{phase === "sting" ? (
					<StarStinger />
				) : (
					<StarStrip star={star} boxRef={boxRef} />
				)}
			</Frame>
		</div>
	);
};

// One of the top plays: its number's stinger, the court staging the play
// under it, then the play under its own game's score bug.
const TopPlay = ({
	play,
	n,
	feeds,
	paused,
	rate,
	narrow,
	onDone,
}: Playing & { play: RecapTopPlay; n: number }) => {
	const feed = useFeed(feeds, play.gid);
	const clip = useMemo((): RecapReel | null | undefined => {
		if (feed === undefined) {
			return undefined;
		}
		const one = feed
			? subReel(
					{ events: feed.events, plays: feed.plays },
					(p) => p.i === play.i,
				)
			: undefined;
		return one && one.plays.length > 0 ? one : null;
	}, [feed, play.i]);
	const [start, end] = useMemo(() => spanOf(clip), [clip]);
	const onDoneRef = useRef(onDone);
	onDoneRef.current = onDone;
	useEffect(() => {
		if (clip === null) {
			onDoneRef.current();
		}
	}, [clip]);
	const [phase, setPhase] = useState<"sting" | "play">("sting");
	useHold(phase === "sting" && !!clip, COUNT_STINGER_MS, paused, () =>
		setPhase("play"),
	);
	return (
		<div style={{ position: "relative" }}>
			{clip && feed ? (
				<Suspense fallback={<Frame narrow={narrow} />}>
					<ClipCourt
						events={clip.events}
						start={start}
						end={end}
						initialBoxScore={feed.initialBoxScore}
						paused={paused || phase === "sting"}
						rate={rate}
						onDone={onDone}
						onFail={onDone}
					/>
				</Suspense>
			) : (
				<Frame narrow={narrow} />
			)}
			{phase === "sting" ? (
				<Frame narrow={narrow} over>
					<CountStinger n={n} />
				</Frame>
			) : null}
		</div>
	);
};

// THE NIGHT'S TOP PLAYS, counted down to the best.
const TopSegment = ({
	plays,
	feeds,
	...playing
}: Playing & { plays: RecapTopPlay[] }) => {
	// (How many are done.)
	const [k, setK] = useState(0);
	useEffect(() => {
		for (const play of plays) {
			void loadFeed(feeds, play.gid);
		}
	}, [feeds, plays]);
	const n = plays.length - k;
	const play = plays[n - 1];
	if (!play) {
		return <Frame narrow={playing.narrow} />;
	}
	return (
		<TopPlay
			key={k}
			{...playing}
			feeds={feeds}
			play={play}
			n={n}
			onDone={() => {
				if (n <= 1) {
					playing.onDone();
				} else {
					setK(k + 1);
				}
			}}
		/>
	);
};

const ConfCard = ({
	standings,
	k,
	paused,
	narrow,
	onDone,
}: {
	standings: RecapStandings;
	k: number;
	paused: boolean;
	narrow: boolean;
	onDone: () => void;
}) => {
	useHold(true, STANDINGS_MS, paused, onDone);
	return (
		<StandingsCard
			conf={standings.confs[k]!}
			through={standings.through}
			k={k}
			narrow={narrow}
		/>
	);
};

// THE STANDINGS: the wipe, then each conference's card.
const StandingsSegment = ({
	standings,
	paused,
	narrow,
	onDone,
}: {
	standings: RecapStandings;
	paused: boolean;
	narrow: boolean;
	onDone: () => void;
}) => {
	// (-1: the wipe.)
	const [k, setK] = useState(-1);
	useHold(k === -1, WIPE_MS, paused, () => setK(0));
	return (
		<Frame narrow={narrow}>
			{k === -1 ? (
				<Stinger text="STANDINGS" title={false} ms={WIPE_MS} />
			) : (
				<ConfCard
					key={k}
					standings={standings}
					k={k}
					paused={paused}
					narrow={narrow}
					onDone={() => {
						if (k + 1 < standings.confs.length) {
							setK(k + 1);
						} else {
							onDone();
						}
					}}
				/>
			)}
		</Frame>
	);
};

type Segment =
	| { kind: "open" }
	| { kind: "game"; game: RecapNightGame }
	| { kind: "star"; star: RecapStar; n: number }
	| { kind: "top"; plays: RecapTopPlay[] }
	| { kind: "standings"; standings: RecapStandings };

// The night at a length: the open, each game, then what the length has of
// the Player of the Night, the top plays (on a night of more than one game)
// and the standings.
const segmentsOf = (night: RecapNight, length: RecapLength): Segment[] => {
	const row = lengthRow(length);
	const out: Segment[] = [{ kind: "open" }];
	for (const game of night.games) {
		out.push({ kind: "game", game });
	}
	if (row.star > 0 && night.star) {
		out.push({ kind: "star", star: night.star, n: row.star });
	}
	if (row.top > 0 && night.games.length > 1 && night.top.length > 0) {
		out.push({ kind: "top", plays: night.top.slice(0, row.top) });
	}
	if (row.standings && night.standings) {
		out.push({ kind: "standings", standings: night.standings });
	}
	return out;
};

const gidsOf = (segment: Segment | undefined): number[] =>
	segment?.kind === "game"
		? [segment.game.gid]
		: segment?.kind === "star"
			? [segment.star.gid]
			: segment?.kind === "top"
				? segment.plays.map((play) => play.gid)
				: [];

const minutes = (night: RecapNight, length: RecapLength, speed: number) => {
	const n = playsPerGame(length);
	let s = 0;
	for (const segment of segmentsOf(night, length)) {
		if (segment.kind === "open") {
			s += OPEN_MS / 1000;
		} else if (segment.kind === "game") {
			s +=
				(CARD_MS + FINAL_MS) / 1000 +
				(Math.min(n, segment.game.numPlays) * PLAY_S) / speed;
		} else if (segment.kind === "star") {
			s += STAR_STINGER_MS / 1000 + (segment.n * PLAY_S) / speed;
		} else if (segment.kind === "top") {
			s += segment.plays.length * (COUNT_STINGER_MS / 1000 + PLAY_S / speed);
		} else {
			s += (WIPE_MS + segment.standings.confs.length * STANDINGS_MS) / 1000;
		}
	}
	return Math.max(1, Math.round(s / 60));
};

// Full size: the picture as big as the screen takes it.
const BigIcon = ({ big }: { big: boolean }) => (
	<svg
		aria-hidden
		viewBox="0 0 16 16"
		width="12"
		height="12"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
		style={{ display: "block" }}
	>
		{big ? (
			<path d="M6 1v5H1M10 1v5h5M6 15v-5H1M10 15v-5h5" />
		) : (
			<path d="M1 6V1h5M15 6V1h-5M1 10v5h5M15 10v5h-5" />
		)}
	</svg>
);

const NightlyRecap = ({
	season,
	day,
	autoplay = false,
	hold = false,
	headline = false,
	onEnd,
	onNone,
}: {
	season: number;
	day: number;
	// Start on its own (once), when not held.
	autoplay?: boolean;
	// Held where it is: off the screen, games being simmed.
	hold?: boolean;
	// In a headline: a tap on the picture opens it full size.
	headline?: boolean;
	// Played to its end.
	onEnd?: () => void;
	// The night turned out to have nothing to show.
	onNone?: () => void;
}) => {
	const [night, setNight] = useState<RecapNight | undefined>();
	useEffect(() => {
		let live = true;
		setNight(undefined);
		(async () => {
			try {
				const got = (await toWorker("main", "getNightlyRecap", {
					season,
					day,
				})) as RecapNight | undefined;
				if (live) {
					setNight(got);
				}
			} catch (error) {
				console.error(error);
			}
		})();
		return () => {
			live = false;
		};
	}, [season, day]);
	// (A night's own.)
	// eslint-disable-next-line react-hooks/exhaustive-deps
	const feeds = useMemo((): Feeds => new Map(), [season, day]);

	const wrapRef = useRef<HTMLDivElement | null>(null);
	const [narrow, setNarrow] = useState(false);
	useEffect(() => {
		const el = wrapRef.current;
		if (!el) {
			return;
		}
		const ro = new ResizeObserver(() => {
			setNarrow(el.clientWidth < NARROW_W);
		});
		ro.observe(el);
		return () => {
			ro.disconnect();
		};
	}, [night]);

	const [length, setLengthState] = useState(readLength);
	const [speed, setSpeedState] = useState(readSpeed);
	const setSpeed = (next: RecapSpeed) => {
		setSpeedState(next);
		safeLocalStorage.setItem(SPEED_KEY, String(next));
	};
	// Which segment (0: the open), or none yet (the poster).
	const [at, setAt] = useState<number | undefined>();
	const [paused, setPaused] = useState(false);
	// (Bumped to start a segment over: a new length, a step back to it.)
	const [take, setTake] = useState(0);

	const segments = useMemo(
		() => (night ? segmentsOf(night, length) : []),
		[night, length],
	);
	const segment = at === undefined ? undefined : segments[at];

	// (The next one's plays fetched while this one plays.)
	useEffect(() => {
		if (at !== undefined) {
			for (const gid of gidsOf(segments[at + 1])) {
				void loadFeed(feeds, gid);
			}
		}
	}, [at, segments, feeds]);

	const onEndRef = useRef(onEnd);
	onEndRef.current = onEnd;
	const go = (k: number) => {
		if (k < 0 || k >= segments.length) {
			setAt(undefined);
			if (k >= segments.length) {
				onEndRef.current?.();
			}
			return;
		}
		setAt(k);
		setPaused(false);
		setTake((n) => n + 1);
	};
	// (Held, as well as paused - but not full size, where nothing else is
	// on the screen.)
	const [big, setBig] = useState(false);
	const stopped = paused || (hold && !big);
	useHold(segment?.kind === "open", OPEN_MS, stopped, () => go(1));

	// On its own, once, the first time it can.
	const autoRef = useRef(false);
	useEffect(() => {
		if (
			autoplay &&
			!hold &&
			!autoRef.current &&
			night &&
			night.games.length > 0
		) {
			autoRef.current = true;
			setAt(0);
			setTake((n) => n + 1);
		}
	}, [autoplay, hold, night]);
	const onNoneRef = useRef(onNone);
	onNoneRef.current = onNone;
	useEffect(() => {
		if (night && night.games.length === 0) {
			onNoneRef.current?.();
		}
	}, [night]);

	// Full size: the page's own full screen where there is one (not on an
	// iPhone), and over the whole page either way.
	const rootRef = useRef<HTMLDivElement | null>(null);
	const openBig = () => {
		setBig(true);
		const el = rootRef.current;
		if (el && document.fullscreenEnabled && !document.fullscreenElement) {
			el.requestFullscreen?.().catch(() => {});
		}
	};
	const closeBig = () => {
		setBig(false);
		if (document.fullscreenElement) {
			document.exitFullscreen?.().catch(() => {});
		}
	};
	useEffect(() => {
		if (!big) {
			return;
		}
		const onChange = () => {
			if (!document.fullscreenElement) {
				setBig(false);
			}
		};
		const onKey = (event: KeyboardEvent) => {
			if (event.key === "Escape") {
				setBig(false);
			}
		};
		const had = document.body.style.overflow;
		document.body.style.overflow = "hidden";
		document.addEventListener("fullscreenchange", onChange);
		window.addEventListener("keydown", onKey);
		return () => {
			document.body.style.overflow = had;
			document.removeEventListener("fullscreenchange", onChange);
			window.removeEventListener("keydown", onKey);
		};
	}, [big]);

	// The controls over the picture: up while it is paused or not yet
	// playing, and for a moment after a touch or the mouse moves over it.
	const [awake, setAwake] = useState(false);
	const sleepRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
	const wake = () => {
		setAwake(true);
		clearTimeout(sleepRef.current);
		sleepRef.current = setTimeout(() => {
			setAwake(false);
		}, CONTROLS_MS);
	};
	useEffect(() => () => clearTimeout(sleepRef.current), []);

	if (!night || night.games.length === 0) {
		return null;
	}
	const controlsUp = awake || paused || at === undefined;

	// A new length: the same game (or the same part after the games) over at
	// it, or on to what is next if it has none.
	const setLength = (next: RecapLength) => {
		setLengthState(next);
		safeLocalStorage.setItem(LENGTH_KEY, next);
		if (at === undefined || !segment) {
			return;
		}
		const nextSegments = segmentsOf(night, next);
		if (segment.kind === "open" || segment.kind === "game") {
			setTake((n) => n + 1);
			return;
		}
		const same = nextSegments.findIndex((row) => row.kind === segment.kind);
		if (same >= 0) {
			setAt(same);
			setTake((n) => n + 1);
		} else {
			const order = ["star", "top", "standings"];
			const later = nextSegments.findIndex(
				(row) => order.indexOf(row.kind) > order.indexOf(segment.kind),
			);
			setAt(later >= 0 ? later : undefined);
			setTake((n) => n + 1);
		}
	};

	const button = (label: string, icon: string, onClick: () => void) => (
		<button
			type="button"
			className="btn btn-sm btn-light-bordered"
			title={label}
			aria-label={label}
			onClick={onClick}
		>
			<span
				className={`glyphicon glyphicon-${icon.replace("back:", "")}`}
				style={
					icon.startsWith("back:") ? { transform: "scaleX(-1)" } : undefined
				}
			/>
		</button>
	);

	const playing = {
		feeds,
		paused: stopped,
		rate: speed,
		narrow,
		onDone: () => go(at! + 1),
	};
	const key = `${at}|${length}|${take}`;

	return (
		<div
			ref={rootRef}
			style={
				big
					? {
							position: "fixed",
							inset: 0,
							zIndex: 1070,
							background: "#000",
							display: "flex",
							alignItems: "center",
							justifyContent: "center",
						}
					: undefined
			}
		>
			<div
				ref={wrapRef}
				className={
					big ? "position-relative nightly-recap-big" : "position-relative"
				}
				style={
					big
						? {
								width: `min(100vw, calc(100dvh * ${narrow ? "4 / 3" : "16 / 9"}))`,
							}
						: undefined
				}
				onPointerMove={wake}
				onPointerDown={wake}
			>
				{segment?.kind === "open" ? (
					<Frame narrow={narrow}>
						<RecapPoster key={take} when={night.when} narrow={narrow} open />
					</Frame>
				) : segment?.kind === "game" ? (
					<RecapGame
						key={key}
						game={segment.game}
						length={length}
						{...playing}
					/>
				) : segment?.kind === "star" ? (
					<StarSegment
						key={key}
						star={segment.star}
						n={segment.n}
						{...playing}
					/>
				) : segment?.kind === "top" ? (
					<TopSegment key={key} plays={segment.plays} {...playing} />
				) : segment?.kind === "standings" ? (
					<StandingsSegment
						key={key}
						standings={segment.standings}
						paused={stopped}
						narrow={narrow}
						onDone={playing.onDone}
					/>
				) : (
					<Frame narrow={narrow}>
						<RecapPoster when={night.when} narrow={narrow}>
							<button
								type="button"
								aria-label="Play the recap"
								title="Play"
								onClick={() => go(0)}
								style={{
									position: "absolute",
									inset: 0,
									background: "transparent",
									border: "none",
									cursor: "pointer",
								}}
							>
								<svg
									aria-hidden
									viewBox="0 0 48 48"
									style={{
										position: "absolute",
										left: "50%",
										top: `${(posterTitleTop(narrow) + (narrow ? 0.3 : 0.32)) * 100}%`,
										transform: "translate(-50%, -50%)",
										width: "clamp(44px, 8cqw, 76px)",
										height: "clamp(44px, 8cqw, 76px)",
										filter: "drop-shadow(0 2px 6px rgba(0, 0, 0, 0.35))",
									}}
								>
									<circle cx="24" cy="24" r="24" fill="#fff" />
									<path d="M 19 15 L 34 24 L 19 33 Z" fill="#13297a" />
								</svg>
							</button>
						</RecapPoster>
					</Frame>
				)}
				{headline && !big && at !== undefined ? (
					<button
						type="button"
						aria-label="Full size"
						title="Full size"
						onClick={openBig}
						style={{
							position: "absolute",
							inset: 0,
							zIndex: 6,
							background: "transparent",
							border: "none",
							cursor: "zoom-in",
						}}
					/>
				) : null}
				<div
					className="gamecast-controls d-flex align-items-center gap-1"
					style={{
						zIndex: 7,
						...(controlsUp
							? undefined
							: { opacity: 0, pointerEvents: "none" as const }),
					}}
				>
					{at !== undefined ? (
						<>
							{button("Previous", "back:step-forward", () => go(at - 1))}
							{button(
								paused ? "Play" : "Pause",
								paused ? "play" : "pause",
								() => setPaused((p) => !p),
							)}
							{button("Next", "step-forward", () => go(at + 1))}
							{at >= 1 && !narrow ? (
								<span
									className="small px-1"
									style={{
										color: "#fff",
										background: "rgba(10, 10, 14, 0.72)",
										borderRadius: "var(--bs-border-radius-sm)",
										alignSelf: "stretch",
										display: "flex",
										alignItems: "center",
									}}
								>
									{at}/{segments.length - 1}
								</span>
							) : null}
						</>
					) : null}
					<Dropdown align="end">
						<Dropdown.Toggle
							id="nightly-recap-speed"
							className="btn-light-bordered"
							variant={"no-class" as any}
							title="Speed of the plays"
						>
							{speed}x
						</Dropdown.Toggle>
						<Dropdown.Menu>
							{RECAP_SPEEDS.map((s) => (
								<Dropdown.Item
									key={s}
									active={s === speed}
									onClick={() => {
										setSpeed(s);
									}}
								>
									{s}x
								</Dropdown.Item>
							))}
						</Dropdown.Menu>
					</Dropdown>
					<select
						className="form-select form-select-sm"
						style={{ width: "auto" }}
						aria-label="Recap length"
						value={length}
						onChange={(event) => {
							setLength(event.target.value as RecapLength);
						}}
					>
						{RECAP_LENGTHS.map((row) => (
							<option key={row.key} value={row.key}>
								{narrow ? row.short : row.label} ·{" "}
								{minutes(night, row.key, speed)} min
							</option>
						))}
					</select>
					<button
						type="button"
						className="btn btn-sm btn-light-bordered"
						title={big ? "Exit full size" : "Full size"}
						aria-label={big ? "Exit full size" : "Full size"}
						onClick={big ? closeBig : openBig}
						style={{
							alignSelf: "stretch",
							display: "flex",
							alignItems: "center",
						}}
					>
						<BigIcon big={big} />
					</button>
				</div>
			</div>
		</div>
	);
};

export default NightlyRecap;
