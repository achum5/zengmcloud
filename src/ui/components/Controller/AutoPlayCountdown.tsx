import { useEffect, useMemo, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";
import { helpers } from "../../util/helpers.ts";
import { autoPlayScheduler } from "../../util/autoPlayScheduler.ts";
import { autoPlayCountdownVisible } from "../../util/autoPlayCountdownVisible.ts";

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
	const {
		lid,
		mpSyncActive,
		mpSyncIsHost,
		mpAutoPlay,
		mpPhaseReady,
		liveGameInProgress,
	} = useLocal([
		"lid",
		"mpSyncActive",
		"mpSyncIsHost",
		"mpAutoPlay",
		"mpPhaseReady",
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

	// WHILE THE ROOM IS BEING ASKED TO READY UP, THE CLOCK IS A LIE.
	//
	// A configured sim stop (a day stop, the trade deadline) becomes a ready-up
	// gate in a shared league, and the ordinary sim path REFUSES to cross it -
	// see getPendingSimStop in worker/core/sync/tradeDeadlineGate.ts. So the
	// timer can run down to zero, fire, and advance nothing: what actually
	// crosses the stop is the ready-up evaluator, the moment the last team says
	// it is done.
	//
	// Counting down to a sim that cannot happen is worse than showing nothing,
	// because it reads as "the league moves on in 18 minutes whether you are
	// ready or not". The scheduler is deliberately LEFT RUNNING underneath -
	// this only hides the display - so nothing has to be restarted by hand once
	// the gate opens.
	//
	// Exactly while PhaseReadyControl is up, which is the same condition, so the
	// two never disagree: ready button visible, clock hidden.
	const gated = mpPhaseReady !== undefined;

	// Held across a live game rather than recomputed - see the note above. Note
	// nextRunAt is an absolute timestamp, so a held one keeps counting DOWN
	// correctly; it is only wrong if the schedule itself moved mid-playback.
	//
	// The gate is frozen WITH it, deliberately. A ready-up becomes visible the
	// instant the phase advances, which during a live game is before the
	// playback ends - so reading it live would wink the clock out mid-game,
	// which is the one thing the freeze exists to prevent.
	const frozen = useRef({
		enabled: liveEnabled,
		nextRunAt: liveNextRunAt,
		gated,
	});
	const {
		enabled,
		nextRunAt,
		gated: gatedNow,
	} = useMemo(() => {
		if (liveGameInProgress) {
			return frozen.current;
		}
		frozen.current = { enabled: liveEnabled, nextRunAt: liveNextRunAt, gated };
		return frozen.current;
	}, [liveEnabled, liveNextRunAt, gated, liveGameInProgress]);

	const active = autoPlayCountdownVisible({
		enabled,
		nextRunAt,
		gated: gatedNow,
	});
	useEffect(() => {
		if (!active) {
			return;
		}
		const id = setInterval(() => setNow(Date.now()), 1000);
		return () => clearInterval(id);
	}, [active]);

	// The undefined check is redundant with `active` at runtime; it is here
	// because the narrowing happens inside the helper, where TypeScript cannot
	// follow it to the format() call below.
	if (!active || nextRunAt === undefined) {
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
