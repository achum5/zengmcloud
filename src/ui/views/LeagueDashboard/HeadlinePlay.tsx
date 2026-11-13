import { useEffect, useRef, useState, type ReactNode } from "react";
import {
	HighlightClip,
	type ClipGame,
} from "../LiveGame/gamecast/HighlightClip.tsx";
import { feedRows } from "../LiveGame/gamecast/playFeed.ts";
import { feedFromEvents } from "../LiveGame/gamecast/replayFeed.ts";
import { unpackShots } from "../LiveGame/gamecast/shots.ts";
import { useLocal } from "../../util/local.ts";
import { toWorker } from "../../util/toWorker.ts";

// A headline's play, shown on the 3D court right in the headline: from the
// game's saved replay, the story's player's last basket (the game winner,
// the last of a big night) - or the game's last, if he has none. Plays as
// soon as it is there; nothing shown if the game has no replay.
type Clip = { game: ClipGame; first: number; last: number };

const HeadlinePlay = ({
	gid,
	pid,
	fallback,
	onDone,
	onNone,
}: {
	gid: number;
	pid: number | undefined;
	// What stands in its place until the play is ready to show.
	fallback: ReactNode;
	onDone: () => void;
	onNone: () => void;
}) => {
	const { gender } = useLocal(["gender"]);
	const [clip, setClip] = useState<Clip | undefined>();
	const onNoneRef = useRef(onNone);
	onNoneRef.current = onNone;

	useEffect(() => {
		let live = true;
		(async () => {
			try {
				const got: any = await toWorker("main", "getReplayFeed", gid);
				if (!live) {
					return;
				}
				if (!got) {
					onNoneRef.current();
					return;
				}
				const { entries } = feedFromEvents(
					got.events,
					got.initialBoxScore,
					gender,
				);
				const scores = feedRows([...entries].reverse()).filter(
					(row) => row.scored && row.cursor !== undefined && !row.pending,
				);
				const latest = (rows: typeof scores) =>
					rows.reduce<(typeof scores)[number] | undefined>(
						(best, row) => (!best || row.cursor! > best.cursor! ? row : best),
						undefined,
					);
				const row =
					latest(scores.filter((r) => pid !== undefined && r.pid === pid)) ??
					latest(scores);
				if (!row) {
					onNoneRef.current();
					return;
				}
				setClip({
					game: {
						events: got.events,
						initialBoxScore: got.initialBoxScore,
						shots: unpackShots(got.shots),
					},
					first: entries[row.key - 1]?.cursor ?? 0,
					last: row.cursor!,
				});
			} catch {
				if (live) {
					onNoneRef.current();
				}
			}
		})();
		return () => {
			live = false;
		};
	}, [gid, pid, gender]);

	if (!clip) {
		return fallback;
	}
	return (
		<HighlightClip
			game={clip.game}
			first={clip.first}
			last={clip.last}
			onDone={onDone}
			onFail={() => onNoneRef.current()}
		/>
	);
};

export default HeadlinePlay;
