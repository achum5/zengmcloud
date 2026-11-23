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
import { ClipCourt } from "../../views/LiveGame/gamecast/HighlightClip.tsx";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
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
// card - at the length picked on this device. Nothing at all for a night
// with no recap plays (see core/game/saveRecapReels.ts).

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

const RecapGame = ({
	game,
	length,
	paused,
	rate,
	narrow,
	onDone,
}: {
	game: RecapNightGame;
	length: RecapLength;
	paused: boolean;
	rate: number;
	narrow: boolean;
	onDone: () => void;
}) => {
	// undefined: on its way. null: none to play.
	const [feed, setFeed] = useState<Feed | null | undefined>();
	useEffect(() => {
		let live = true;
		(async () => {
			try {
				const got: any = await toWorker("main", "getRecapFeed", game.gid);
				if (live) {
					setFeed(got && got.plays?.length > 0 ? got : null);
				}
			} catch (error) {
				console.error(error);
				if (live) {
					setFeed(null);
				}
			}
		})();
		return () => {
			live = false;
		};
	}, [game.gid]);

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

	// From just before its first play - the game up to there applied at
	// once, not staged a period at a time - to its last play's last line, no
	// period's end or the game's staged after it.
	const [start, end] = useMemo(() => {
		if (!clip || clip.plays.length === 0) {
			return [0, 0];
		}
		let from = clip.plays[0]!.from - 1;
		while (from >= 0 && !isLine(clip.events[from])) {
			from -= 1;
		}
		return [from + 1, Math.max(...clip.plays.map((p) => p.to))];
	}, [clip]);

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

const minutes = (
	games: RecapNightGame[],
	length: RecapLength,
	speed: number,
) => {
	const n = playsPerGame(length);
	let s = OPEN_MS / 1000;
	for (const game of games) {
		s +=
			(CARD_MS + FINAL_MS) / 1000 +
			(Math.min(n, game.numPlays) * PLAY_S) / speed;
	}
	return Math.max(1, Math.round(s / 60));
};

const NightlyRecap = ({ season, day }: { season: number; day: number }) => {
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
	const setLength = (next: RecapLength) => {
		setLengthState(next);
		safeLocalStorage.setItem(LENGTH_KEY, next);
	};
	const [speed, setSpeedState] = useState(readSpeed);
	const setSpeed = (next: RecapSpeed) => {
		setSpeedState(next);
		safeLocalStorage.setItem(SPEED_KEY, String(next));
	};
	// Which game, -1 for the open, or none yet (the poster).
	const [at, setAt] = useState<number | undefined>();
	const [paused, setPaused] = useState(false);
	// (Bumped to start a game over: a new length, a step back to it.)
	const [take, setTake] = useState(0);
	useHold(at === -1, OPEN_MS, paused, () => {
		setAt(0);
		setTake((n) => n + 1);
	});

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

	const games = night?.games ?? [];
	if (games.length === 0) {
		return null;
	}
	const controlsUp = awake || paused || at === undefined;

	const go = (k: number) => {
		if (k < -1 || k >= games.length) {
			setAt(undefined);
			return;
		}
		setAt(k);
		setPaused(false);
		setTake((n) => n + 1);
	};
	const game = at === undefined ? undefined : games[at];

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

	return (
		<div
			ref={wrapRef}
			className="position-relative"
			onPointerMove={wake}
			onPointerDown={wake}
		>
			{at === -1 ? (
				<Frame narrow={narrow}>
					<RecapPoster key={take} when={night!.when} narrow={narrow} open />
				</Frame>
			) : game ? (
				<RecapGame
					key={`${game.gid}|${length}|${take}`}
					game={game}
					length={length}
					paused={paused}
					rate={speed}
					narrow={narrow}
					onDone={() => go(at! + 1)}
				/>
			) : (
				<Frame narrow={narrow}>
					<RecapPoster when={night!.when} narrow={narrow}>
						<button
							type="button"
							aria-label="Play the recap"
							title="Play"
							onClick={() => go(-1)}
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
						{button("Previous game", "back:step-forward", () => go(at - 1))}
						{button(paused ? "Play" : "Pause", paused ? "play" : "pause", () =>
							setPaused((p) => !p),
						)}
						{button("Next game", "step-forward", () => go(at + 1))}
						{at >= 0 && !narrow ? (
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
								{at + 1}/{games.length}
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
						if (at !== undefined) {
							setTake((n) => n + 1);
						}
					}}
				>
					{RECAP_LENGTHS.map((row) => (
						<option key={row.key} value={row.key}>
							{narrow ? row.short : row.label} ·{" "}
							{minutes(games, row.key, speed)} min
						</option>
					))}
				</select>
			</div>
		</div>
	);
};

export default NightlyRecap;
