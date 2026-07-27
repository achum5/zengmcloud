import { useEffect, useState } from "react";
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
const AutoPlayCountdown = () => {
	const { lid, mpSyncActive, mpSyncIsHost, mpAutoPlay } = useLocal([
		"lid",
		"mpSyncActive",
		"mpSyncIsHost",
		"mpAutoPlay",
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
	const enabled = isSimmer ? localEnabled : !!mpAutoPlay?.enabled;
	const nextRunAt = isSimmer ? localNextRunAt : mpAutoPlay?.nextRunAt;

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
