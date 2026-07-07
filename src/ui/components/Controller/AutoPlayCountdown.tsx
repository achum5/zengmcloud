import { useEffect, useState } from "react";
import { useLocal } from "../../util/local.ts";
import {
	autoPlayScheduler,
	type AutoPlaySettings,
	type AutoPlayState,
} from "../../util/autoPlayScheduler.ts";

const format = (ms: number): string => {
	const total = Math.max(0, Math.round(ms / 1000));
	const h = Math.floor(total / 3600);
	const m = Math.floor((total % 3600) / 60);
	const s = total % 60;
	const mm = String(m).padStart(2, "0");
	const ss = String(s).padStart(2, "0");
	return h > 0 ? `${h}:${mm}:${ss}` : `${m}:${ss}`;
};

// Live countdown to the next scheduled auto-sim, shown in the header on the
// device running the auto-play schedule. Ticks off the device's local clock.
// Renders nothing (a bare separator + null) when auto play isn't scheduled.
const AutoPlayCountdown = () => {
	const { lid } = useLocal(["lid"]);

	const [settings, setSettings] = useState<AutoPlaySettings>(
		autoPlayScheduler.settings,
	);
	const [state, setState] = useState<AutoPlayState>(autoPlayScheduler.state);
	const [, setNow] = useState(0);

	// Load this league's schedule so the header reflects it (and auto play
	// resumes) without having to open the Auto Play page first.
	useEffect(() => {
		if (typeof lid === "number") {
			autoPlayScheduler.loadForLeague(lid);
		}
		setSettings({ ...autoPlayScheduler.settings });
		setState({ ...autoPlayScheduler.state });
		return autoPlayScheduler.subscribe((s, st) => {
			setSettings({ ...s });
			setState({ ...st });
		});
	}, [lid]);

	// Tick once a second while there's something to count down to.
	const active = settings.enabled && state.nextRunAt !== undefined;
	useEffect(() => {
		if (!active) {
			return;
		}
		const id = setInterval(() => setNow((n) => n + 1), 1000);
		return () => clearInterval(id);
	}, [active]);

	if (!settings.enabled || state.nextRunAt === undefined) {
		return null;
	}
	// A plain count-down timer to the next scheduled sim ("⏱ 18:01").
	return (
		<span className="text-warning">
			{" · ⏱ "}
			{format(state.nextRunAt - Date.now())}
		</span>
	);
};

export default AutoPlayCountdown;
