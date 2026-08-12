import { useLocal } from "../../util/local.ts";
import { toWorker } from "../../util/toWorker.ts";

// A league-mate is live-simming their own game and anyone may watch. Purely an
// invitation: nobody is navigated anywhere until they click. Hidden while this
// device is inside a live playback of its own (watching or simming), where the
// pill would be an invitation to the thing already on screen.
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
			title={`${mpLiveWatchable.byName} is simming ${label} live - watch along`}
			onClick={() => {
				void toWorker("main", "watchLiveBroadcast", undefined);
			}}
		>
			● {label}
		</button>
	);
};

export default LiveWatchPill;
