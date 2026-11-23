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
import { FinalCard, GameRecapCard, RecapPoster } from "./RecapCards.tsx";

// THE NIGHTLY RECAP: a night's games, the biggest first, each its GAME RECAP
// card, its plays on the 3D court under its own score bug, and its FINAL
// card - at the length picked on this device. Nothing at all for a night
// with no recap plays (see core/game/saveRecapReels.ts).

const CARD_MS = 4500;
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

const RecapGame = ({
	game,
	length,
	paused,
	narrow,
	onDone,
}: {
	game: RecapNightGame;
	length: RecapLength;
	paused: boolean;
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
						paused={paused || phase === "card"}
						onDone={() => setPhase("final")}
						onFail={() => setPhase("final")}
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
		</div>
	);
};

const minutes = (games: RecapNightGame[], length: RecapLength) => {
	const n = playsPerGame(length);
	let s = 0;
	for (const game of games) {
		s += (CARD_MS + FINAL_MS) / 1000 + Math.min(n, game.numPlays) * PLAY_S;
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
	// Which game, or none yet (the poster).
	const [at, setAt] = useState<number | undefined>();
	const [paused, setPaused] = useState(false);
	// (Bumped to start a game over: a new length, a step back to it.)
	const [take, setTake] = useState(0);

	const games = night?.games ?? [];
	if (games.length === 0) {
		return null;
	}

	const go = (k: number) => {
		if (k < 0 || k >= games.length) {
			setAt(undefined);
			return;
		}
		setAt(k);
		setPaused(false);
		setTake((n) => n + 1);
	};
	const game = at === undefined ? undefined : games[at];

	const button = (
		label: string,
		icon: string,
		onClick: () => void,
		disabled = false,
	) => (
		<button
			type="button"
			className="btn btn-sm btn-light-bordered"
			title={label}
			aria-label={label}
			disabled={disabled}
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
		<div ref={wrapRef}>
			{game ? (
				<RecapGame
					key={`${game.gid}|${length}|${take}`}
					game={game}
					length={length}
					paused={paused}
					narrow={narrow}
					onDone={() => go(at! + 1)}
				/>
			) : (
				<Frame narrow={narrow}>
					<RecapPoster when={night!.when}>
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
							<span
								style={{
									position: "absolute",
									left: "50%",
									top: "83%",
									transform: "translate(-50%, -50%)",
									width: "clamp(44px, 9cqw, 84px)",
									height: "clamp(44px, 9cqw, 84px)",
									borderRadius: "50%",
									background: "rgba(0, 0, 0, 0.55)",
									border: "2px solid rgba(255, 255, 255, 0.9)",
									display: "flex",
									alignItems: "center",
									justifyContent: "center",
									color: "#fff",
								}}
							>
								<span
									className="glyphicon glyphicon-play"
									style={{
										fontSize: "clamp(17px, 3.6cqw, 33px)",
										marginLeft: "0.12em",
									}}
								/>
							</span>
						</button>
					</RecapPoster>
				</Frame>
			)}
			<div className="d-flex align-items-center gap-2 flex-wrap mt-n2 mb-3">
				{game ? (
					<>
						{button("Previous game", "back:step-forward", () => go(at! - 1))}
						{button(paused ? "Play" : "Pause", paused ? "play" : "pause", () =>
							setPaused((p) => !p),
						)}
						{button("Next game", "step-forward", () => go(at! + 1))}
						<span className="text-body-secondary small">
							{at! + 1}/{games.length}
						</span>
					</>
				) : (
					button("Play", "play", () => go(0))
				)}
				<select
					className="form-select form-select-sm ms-auto"
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
							{row.label} · {minutes(games, row.key)} min
						</option>
					))}
				</select>
			</div>
		</div>
	);
};

export default NightlyRecap;
