import { useLocal } from "../../util/local.ts";
import { toWorker } from "../../util/toWorker.ts";

// A live sim is running in the room and this device is not in it - it left, or
// the auto-join hasn't landed yet. Clicking drops back in at the simmer's
// current spot. Hidden while this device is inside a live playback of its own
// (watching or simming), where the pill would point at what is already on
// screen.
const LiveWatchPill = () => {
	const { mpLiveWatchable, mpLiveBroadcast, liveGameInProgress } = useLocal([
		"mpLiveWatchable",
		"mpLiveBroadcast",
		"liveGameInProgress",
	]);

	if (!mpLiveWatchable || mpLiveBroadcast?.active || liveGameInProgress) {
		return null;
	}

	const label = mpLiveWatchable.label || "Live game";

	return (
		<button
			type="button"
			className="btn btn-sm btn-danger ms-2 text-nowrap"
			title={`${mpLiveWatchable.byName} is simming ${label} live - watch`}
			onClick={() => {
				void toWorker("main", "watchLiveBroadcast", undefined);
			}}
		>
			● {label}
		</button>
	);
};

export default LiveWatchPill;
