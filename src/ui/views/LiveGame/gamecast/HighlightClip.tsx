import { lazy, Suspense, useCallback, useMemo, useRef, useState } from "react";
import { clipOfPlay, isLine } from "../../../util/filterPlayerHighlights.ts";
import { helpers } from "../../../util/helpers.ts";
import processLiveGameEventsBasketball from "../../../util/processLiveGameEvents.basketball.tsx";
import type { ShotSpot } from "./shots.ts";

const Court3D = lazy(() => import("../court3d/Court3D.tsx"));

// The game a highlight is cut from: its play-by-play and the box score it
// starts from - and, a game still being played, how far the page has got
// (the clip goes no further).
export type ClipGame = {
	events: any[];
	initialBoxScore: any;
	upTo?: number;
	// Where the game's shots were taken in 3D (see shotSpots), as its shot
	// chart has them: a shot in the clip is taken from there too.
	shots?: readonly ShotSpot[];
};

// The game's lines with each shot that has a spot on the chart marked with
// it (`at`, see the director's stageShot): staged on its own, a clip has
// the shot taken where the chart shows it - not wherever the clip's own
// staging of the trip would have had him.
export const withShotSpots = (
	events: any[],
	shots: readonly ShotSpot[] | undefined,
): any[] => {
	if (!shots || shots.length === 0) {
		return events;
	}
	const out = events.slice();
	for (const s of shots) {
		// (Its result is the line before the cursor; the shot, the last
		// attempt before that.)
		for (let i = s.cursor - 2; i >= 0 && i >= s.cursor - 8; i--) {
			const e = out[i];
			if (typeof e?.type === "string" && e.type.startsWith("fga")) {
				if (e.pid === s.pid) {
					out[i] = { ...e, at: { x: s.x, y: s.y } };
				}
				break;
			}
		}
	}
	return out;
};

// Where the court is while it stages.
const Dark = () => (
	<div
		className="mb-3"
		style={{ aspectRatio: "16 / 9", background: "#040406", borderRadius: 6 }}
	/>
);

// The page's side of the clip, as the live game's is: the events applied
// to the box score a line at a time (see processLiveGameEvents) - its score
// and clock along with the court's.
type Run = {
	box: any;
	queue: any[];
	overtimes: number;
	quarters: string[];
};
const step = (run: Run) => {
	const out = processLiveGameEventsBasketball({
		events: run.queue,
		boxScore: run.box,
		overtimes: run.overtimes,
		quarters: run.quarters,
	});
	run.overtimes = out.overtimes;
	run.quarters = out.quarters;
};

// (Also the Nightly Recap's court: a game's recap plays, played through
// - with no button to see them again, `onAgain` left out - and `paused`.)
export const ClipCourt = ({
	events,
	start,
	end,
	initialBoxScore,
	paused = false,
	onAgain,
	onDone,
	onFail,
}: {
	events: any[];
	start: number;
	end: number;
	initialBoxScore: any;
	paused?: boolean;
	onAgain?: () => void;
	onDone?: () => void;
	onFail: () => void;
}) => {
	const total = events.length;
	// (A reel's stretches cut out marked: see common/recapReel.ts's cutReel.)
	const marked = useMemo(() => events.some((e) => e?.gap === true), [events]);
	const run = useMemo((): Run => {
		const r: Run = {
			box: helpers.deepCopy(initialBoxScore),
			queue: events.slice(),
			overtimes: 0,
			quarters: [],
		};
		while (r.queue.length > 0 && total - r.queue.length < start) {
			step(r);
		}
		// (And what is kept between there and its first play - the points,
		// the subs since the period began: the score as it was when it
		// starts, not as of the period's start. A reel's, as far as the
		// stretch cut out goes: its first line's own points as it happens.)
		const head: any[] = [];
		while (
			r.queue.length > 0 &&
			!isLine(r.queue[0]) &&
			!(marked && r.queue[0].type === "stat" && r.queue[0].gap !== true)
		) {
			head.push(r.queue.shift());
		}
		if (head.length > 0) {
			processLiveGameEventsBasketball({
				events: head,
				boxScore: r.box,
				overtimes: r.overtimes,
				quarters: r.quarters,
			});
		}
		return r;
	}, [events, start, total, initialBoxScore, marked]);
	const [cursor, setCursor] = useState(() => total - run.queue.length);
	const [done, setDone] = useState(false);
	const onDoneRef = useRef(onDone);
	onDoneRef.current = onDone;
	const onReady = useCallback(() => {
		if (total - run.queue.length >= end || run.queue.length === 0) {
			setDone(true);
			onDoneRef.current?.();
			return;
		}
		step(run);
		setCursor(total - run.queue.length);
	}, [run, total, end]);
	// Cut to the next clip: the score as it stands there, the stretch cut out
	// before it on the board.
	const [, setCuts] = useState(0);
	const onCut = useCallback(() => {
		const gap: any[] = [];
		while (run.queue.length > 0 && run.queue[0]?.gap === true) {
			gap.push(run.queue.shift());
		}
		if (gap.length > 0) {
			processLiveGameEventsBasketball({
				events: gap,
				boxScore: run.box,
				overtimes: run.overtimes,
				quarters: run.quarters,
			});
			setCuts((n) => n + 1);
		}
	}, [run]);
	return (
		<div className="position-relative">
			<Court3D
				events={events}
				cursor={cursor}
				boxScore={run.box}
				caption={undefined}
				paused={paused}
				rate={1}
				follower={false}
				until={end}
				onReady={onReady}
				onCut={marked ? onCut : undefined}
				onFail={onFail}
			/>
			{done && onAgain ? (
				<button
					type="button"
					className="gamecast-clip-again"
					title="Watch again"
					aria-label="Watch again"
					onClick={onAgain}
				>
					<span className="glyphicon glyphicon-repeat" />
				</button>
			) : null}
		</div>
	);
};

// A HIGHLIGHT PLAYED RIGHT THERE, under its row: the 3D court on just that
// play (see clipOfPlay) - from `first` to `last`, the game's events
// consumed - staged in a moment, not the whole game.
// (`onDone`: told each time it has played through - a headline's moves on.
// `onFail`: the court could not play it.)
export const HighlightClip = ({
	game,
	first,
	last,
	onDone,
	onFail,
}: {
	game: ClipGame;
	first: number;
	last: number;
	onDone?: () => void;
	onFail?: () => void;
}) => {
	// (As the game was when it was opened: a live game going on under it
	// does not stage it again.)
	const [opened] = useState(game);
	const clip = useMemo(
		() =>
			clipOfPlay(
				withShotSpots(opened.events, opened.shots),
				first,
				last,
				opened.upTo,
			),
		[opened, first, last],
	);
	const [take, setTake] = useState(0);
	const [failed, setFailed] = useState(false);
	if (failed) {
		return null;
	}
	return (
		<Suspense fallback={<Dark />}>
			<ClipCourt
				key={take}
				events={clip.events}
				start={clip.start}
				end={clip.end}
				initialBoxScore={opened.initialBoxScore}
				onAgain={() => setTake((n) => n + 1)}
				onDone={onDone}
				onFail={() => {
					setFailed(true);
					onFail?.();
				}}
			/>
		</Suspense>
	);
};
