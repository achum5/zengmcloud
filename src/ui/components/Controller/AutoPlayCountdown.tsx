import { useEffect, useMemo, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";
import { helpers } from "../../util/helpers.ts";
import { autoPlayScheduler } from "../../util/autoPlayScheduler.ts";

const format = (ms: number): string => {
	const total = Math.max(0, Math.round(ms / 1000));
	const h = Math.floor(total / 3600);
	const m = Math.floor((total % 3600) / 60);
	const s = total % 60;
	const mm = String(m).padStart(2, "0");
	const ss = String(s).padStart(2, "0");
	return h > 0 ? `${h}:${mm}:${ss}` : `${m}:${ss}`;
};

// Live countdown to the next scheduled auto-sim, shown in the header on EVERY
// device in the room. The simmer reads it straight from its own local scheduler
// (instant, always current); everyone else reads the small snapshot the simmer
// broadcasts (mpAutoPlay). Ticks off the device's local clock.
//
// It stays up during a live game - knowing how long you have before the next
// auto-sim is exactly what you want while watching one - but on the SNAPSHOT it
// held when the playback began, the same arrangement the ticker uses and for the
// same reason. The scheduler keeps running while you watch, and it stops itself
// the moment the regular season is over: left live, the countdown would wink out
// mid-game and tell you that was the last one. A clock cannot spoil a game, but
// a clock DISAPPEARING can. It picks the real state back up when the game ends.
const AutoPlayCountdown = () => {
	const { lid, mpSyncActive, mpSyncIsHost, mpAutoPlay, liveGameInProgress } =
		useLocal([
			"lid",
			"mpSyncActive",
			"mpSyncIsHost",
			"mpAutoPlay",
			"liveGameInProgress",
		]);
	const [now, setNow] = useState(() => Date.now());

	// Keep this device's own scheduler loaded so the simmer runs (and broadcasts)
	// without opening the Auto Play page first, and so the simmer's countdown reads
	// live local state. No-op on non-simmer devices beyond loading their (idle) one.
	const [localNextRunAt, setLocalNextRunAt] = useState<number | undefined>(
		undefined,
	);
	const [localEnabled, setLocalEnabled] = useState(false);
	useEffect(() => {
		if (typeof lid === "number") {
			autoPlayScheduler.loadForLeague(lid);
		}
		setLocalEnabled(autoPlayScheduler.settings.enabled);
		setLocalNextRunAt(autoPlayScheduler.state.nextRunAt);
		return autoPlayScheduler.subscribe((s, st) => {
			setLocalEnabled(s.enabled);
			setLocalNextRunAt(st.nextRunAt);
		});
	}, [lid]);

	// Simmer: local scheduler is the source of truth. Everyone else: the broadcast.
	const isSimmer = mpSyncActive && mpSyncIsHost;
	const liveEnabled = isSimmer ? localEnabled : !!mpAutoPlay?.enabled;
	const liveNextRunAt = isSimmer ? localNextRunAt : mpAutoPlay?.nextRunAt;

	// Held across a live game rather than recomputed - see the note above. Note
	// nextRunAt is an absolute timestamp, so a held one keeps counting DOWN
	// correctly; it is only wrong if the schedule itself moved mid-playback.
	const frozen = useRef({ enabled: liveEnabled, nextRunAt: liveNextRunAt });
	const { enabled, nextRunAt } = useMemo(() => {
		if (liveGameInProgress) {
			return frozen.current;
		}
		frozen.current = { enabled: liveEnabled, nextRunAt: liveNextRunAt };
		return frozen.current;
	}, [liveEnabled, liveNextRunAt, liveGameInProgress]);

	const active = enabled && nextRunAt !== undefined;
	useEffect(() => {
		if (!active) {
			return;
		}
		const id = setInterval(() => setNow(Date.now()), 1000);
		return () => clearInterval(id);
	}, [active]);

	if (!enabled || nextRunAt === undefined) {
		return null;
	}
	return (
		<>
			{" · "}
			<a
				className="text-warning text-decoration-none"
				href={helpers.leagueUrl(["auto_play_schedule"])}
				title="Auto Play Scheduler"
			>
				{"⏱ "}
				{format(nextRunAt - now)}
			</a>
		</>
	);
};

export default AutoPlayCountdown;
