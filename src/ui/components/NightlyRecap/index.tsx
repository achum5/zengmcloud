import { Dropdown } from "react-bootstrap";
import {
	type ReactNode,
	Suspense,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import {
	DEFAULT_RECAP_LENGTH,
	pickRecapPlays,
	playsPerGame,
	RECAP_LENGTHS,
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
} from "../../../worker/util/nightlyRecap.ts";
import { isLine } from "../../util/filterPlayerHighlights.ts";
import { safeLocalStorage } from "../../util/safeLocalStorage.ts";
import {
	ClipCourt,
	warmClipCourt,
} from "../../views/LiveGame/gamecast/HighlightClip.tsx";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
import {
	FULL_SIZE,
	FullSizeButton,
	NARROW_W,
	useControlsUp,
	useFullSize,
	useFullSizeWidth,
} from "../pictureControls.tsx";
import {
	FinalCard,
	GameRecapCard,
	OPEN_MS,
	posterTitleTop,
	RecapPoster,
	Stinger,
	STINGER_MS,
} from "./RecapCards.tsx";

// THE NIGHTLY RECAP: a night's games, the biggest first, each its GAME RECAP
// card, its plays on the 3D court under its own score bug, and its FINAL
// card - at the length picked on this device. Nothing at all for a night with
// no recap plays (see core/game/saveRecapReels.ts).

const CARD_MS = 4500;
const FINAL_MS = 5500;
// About how long a play runs, for the running times.
const PLAY_S = 7;

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

// The night's games' plays, each fetched once, and fetched ahead.
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

// The 3D court is set up under the GAME RECAP card once that has come on
// and is still: its building painted, its men drawn. Setting it up takes the
// page a moment, and the card coming on would stutter through it.
const SETTLE_MS = 900;
// (And the first court of the visit readied this long after the poster is
// up at the latest - sooner, if the page is quiet before then.)
const WARM_MS = 2000;
const useSettled = (paused: boolean) => {
	const [settled, setSettled] = useState(false);
	useHold(!settled, SETTLE_MS, paused, () => setSettled(true));
	return settled;
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

	const settled = useSettled(paused);

	const [home, away] = game.teams;
	return (
		<div style={{ position: "relative" }}>
			{clip && feed && settled && phase !== "final" ? (
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

type Segment = { kind: "open" } | { kind: "game"; game: RecapNightGame };

// The night: the open, then each game.
const segmentsOf = (night: RecapNight): Segment[] => [
	{ kind: "open" },
	...night.games.map((game): Segment => ({ kind: "game", game })),
];

const minutes = (night: RecapNight, length: RecapLength, speed: number) => {
	const n = playsPerGame(length);
	let s = OPEN_MS / 1000;
	for (const game of night.games) {
		s +=
			(CARD_MS + FINAL_MS) / 1000 +
			(Math.min(n, game.numPlays) * PLAY_S) / speed;
	}
	return Math.max(1, Math.round(s / 60));
};

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

	const segments = useMemo(() => (night ? segmentsOf(night) : []), [night]);
	const segment = at === undefined ? undefined : segments[at];

	// (The court readied once the page is quiet, the poster up - all of it
	// that is a moment's work only while the poster is still up.)
	const hasGames = segments.length > 1;
	const posterUp = useRef(true);
	posterUp.current = at === undefined;
	useEffect(() => {
		if (!hasGames) {
			return;
		}
		const go = () => {
			warmClipCourt(() => posterUp.current).catch(() => {});
		};
		if (typeof requestIdleCallback === "function") {
			const id = requestIdleCallback(go, { timeout: WARM_MS });
			return () => {
				cancelIdleCallback(id);
			};
		}
		const id = setTimeout(go, WARM_MS);
		return () => {
			clearTimeout(id);
		};
	}, [hasGames]);

	// (The next game's plays fetched while this one plays.)
	useEffect(() => {
		const next = at === undefined ? undefined : segments[at + 1];
		if (next?.kind === "game") {
			void loadFeed(feeds, next.game.gid);
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
	const rootRef = useRef<HTMLDivElement | null>(null);
	const { big, open: openBig, close: closeBig } = useFullSize(rootRef);
	const bigWidth = useFullSizeWidth(big);
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

	// The controls over the picture: up while it is paused or not yet
	// playing, and for a moment after a touch or the mouse moves over it.
	const shown = useControlsUp(paused || at === undefined);

	if (!night || night.games.length === 0) {
		return null;
	}

	// A new length: the same game over at it.
	const setLength = (next: RecapLength) => {
		setLengthState(next);
		safeLocalStorage.setItem(LENGTH_KEY, next);
		if (at !== undefined) {
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
		<div ref={rootRef} style={big ? FULL_SIZE : undefined}>
			<div
				ref={wrapRef}
				className={
					big ? "position-relative nightly-recap-big" : "position-relative"
				}
				style={big ? { width: bigWidth } : undefined}
				onPointerMove={shown.wake}
				onPointerDown={shown.wake}
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
					{...shown.controls}
					className="gamecast-controls d-flex align-items-center gap-1"
					style={{ zIndex: 7, ...shown.hidden }}
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
					<FullSizeButton big={big} open={openBig} close={closeBig} />
				</div>
			</div>
		</div>
	);
};

export default NightlyRecap;
