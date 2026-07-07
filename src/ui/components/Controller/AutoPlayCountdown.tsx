import { useEffect, useState } from "react";
import { useLocal } from "../../util/local.ts";
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
// device in the room (the simmer broadcasts its schedule; all devices read it
// from mpAutoPlay). Ticks off the device's local clock.
const AutoPlayCountdown = () => {
	const { lid, mpAutoPlay } = useLocal(["lid", "mpAutoPlay"]);
	const [, setNow] = useState(0);

	// Keep this device's own scheduler loaded so the simmer runs (and publishes)
	// without opening the Auto Play page first. No-op on non-simmer devices.
	useEffect(() => {
		if (typeof lid === "number") {
			autoPlayScheduler.loadForLeague(lid);
		}
	}, [lid]);

	const active = !!mpAutoPlay?.enabled && mpAutoPlay.nextRunAt !== undefined;
	useEffect(() => {
		if (!active) {
			return;
		}
		const id = setInterval(() => setNow((n) => n + 1), 1000);
		return () => clearInterval(id);
	}, [active]);

	if (!mpAutoPlay?.enabled || mpAutoPlay.nextRunAt === undefined) {
		return null;
	}
	return (
		<span className="text-warning">
			{" · ⏱ "}
			{format(mpAutoPlay.nextRunAt - Date.now())}
		</span>
	);
};

export default AutoPlayCountdown;
