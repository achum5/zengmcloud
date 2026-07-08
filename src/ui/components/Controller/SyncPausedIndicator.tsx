import { useLocal } from "../../util/local.ts";

// A small header pill shown while conflict-prone edits (trades, signings,
// roster/lineup moves) are blocked on this device because a league-mate is
// simming or this device is still catching up. Without it, a blocked action just
// silently does nothing and reads as a glitch; this makes the pause visible and
// expected. Hidden in single-player and whenever edits are free.
const SyncPausedIndicator = () => {
	const { mpEditsPaused } = useLocal(["mpEditsPaused"]);

	if (!mpEditsPaused) {
		return null;
	}

	return (
		<span
			className="badge rounded-pill text-bg-warning ms-2 align-middle"
			title="A league-mate is simming, so trades and roster moves are paused for a moment. Try again once it finishes."
			style={{ fontWeight: 500 }}
		>
			Simming…
		</span>
	);
};

export default SyncPausedIndicator;
