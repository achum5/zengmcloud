import { useEffect, useRef, useState } from "react";
import { HeadlineScene } from "../LiveGame/court3d/HeadlineScene.tsx";
import { safeLocalStorage } from "../../util/safeLocalStorage.ts";

// THE DRAFT ROOM, LIVE (see court3d/ClockStage.tsx, HeadlineStage.tsx): the
// room in 3D over the draft as it goes. Between picks the team on the clock
// and the cameras on the best left; a pick made, his name read and him up
// onto the stage - once over, the next team on the clock. Several made at
// once (a sim to your next pick), the one that matters most of them: yours,
// a league mate's, or the last. Yours plays out whatever comes in after it.
//
// Graphics only: nothing here touches the draft.

const HIDE_KEY = "bbgmDraftRoom";
// Over, a pick holds on its last moment this long before the next team is
// on the clock.
const HOLD_MS = 2500;

type Row = {
	pid: number;
	imgURL?: string;
	draft: { round: number; pick: number; tid: number };
};

export const DraftRoom = ({
	drafted,
	myTids,
	userTids,
}: {
	drafted: Row[];
	myTids: number[];
	userTids: number[];
}) => {
	const [hidden, setHidden] = useState(
		() => safeLocalStorage.getItem(HIDE_KEY) === "hidden",
	);
	const made = drafted.filter((p) => p.pid >= 0 && p.draft.round >= 1);
	const up = drafted.find((p) => p.pid < 0);
	const [showing, setShowing] = useState<{ pid: number; mine: boolean }>();

	// The picks made since last time: the one to show of them.
	const madeKey = made.map((p) => p.pid).join(",");
	const seen = useRef<Set<number> | undefined>(undefined);
	useEffect(() => {
		const before = seen.current;
		seen.current = new Set(made.map((p) => p.pid));
		if (!before) {
			return;
		}
		const fresh = made.filter((p) => !before.has(p.pid) && !p.imgURL);
		if (fresh.length === 0) {
			return;
		}
		const weight = (p: Row) =>
			myTids.includes(p.draft.tid) ? 2 : userTids.includes(p.draft.tid) ? 1 : 0;
		const order = (p: Row) => p.draft.round * 1000 + p.draft.pick;
		const best = [...fresh].sort(
			(a, b) => weight(b) - weight(a) || order(b) - order(a),
		)[0]!;
		const mine = weight(best) === 2;
		setShowing((now) => (now?.mine && !mine ? now : { pid: best.pid, mine }));
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [madeKey]);

	// Over: the next team on the clock, after a moment - or, the draft over,
	// left where it ends.
	const [over, setOver] = useState<number>();
	const draftGoing = up !== undefined;
	useEffect(() => {
		if (over === undefined || !draftGoing) {
			return;
		}
		const timer = setTimeout(() => {
			setShowing((now) => (now?.pid === over ? undefined : now));
		}, HOLD_MS);
		return () => {
			clearTimeout(timer);
		};
	}, [over, draftGoing]);

	// (No room to show the team on the clock in - see the worker's
	// draftClock.)
	const upKey = up ? up.draft.round * 1000 + up.draft.pick : undefined;
	const [noClock, setNoClock] = useState<number>();
	// (And, given up on, asked for again the next time the draft changes on
	// the page - a league mate's pick in, say.)
	const [again, setAgain] = useState(0);
	useEffect(() => {
		if (noClock !== undefined) {
			setNoClock(undefined);
			setAgain((n) => n + 1);
		}
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [drafted]);
	if (
		__SPORT !== "basketball" ||
		(!showing && (upKey === undefined || noClock === upKey))
	) {
		return null;
	}
	if (hidden) {
		return (
			<button
				type="button"
				className="btn btn-xs btn-light-bordered mb-3"
				onClick={() => {
					setHidden(false);
					safeLocalStorage.removeItem(HIDE_KEY);
				}}
			>
				Show draft room
			</button>
		);
	}
	return (
		<div className="mb-3 position-relative" style={{ maxWidth: 760 }}>
			{showing ? (
				<HeadlineScene
					scene={{ type: "draftPick", pid: showing.pid }}
					playing
					onEnd={() => {
						setOver(showing.pid);
					}}
					onNone={() => {
						setShowing(undefined);
					}}
				/>
			) : upKey !== undefined ? (
				<HeadlineScene
					scene={{ type: "draftClock", pick: upKey, again }}
					playing
					onNone={() => {
						setNoClock(upKey);
					}}
				/>
			) : null}
			<button
				type="button"
				className="btn-close btn-close-white position-absolute"
				style={{ top: 8, right: 8, zIndex: 5, opacity: 0.7 }}
				title="Hide draft room"
				aria-label="Hide draft room"
				onClick={() => {
					setHidden(true);
					safeLocalStorage.setItem(HIDE_KEY, "hidden");
				}}
			/>
		</div>
	);
};
