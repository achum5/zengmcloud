import { createNanoEvents } from "nanoevents";
import { PHASE } from "../../common/constants.ts";
import { local } from "./local.ts";
import { safeLocalStorage } from "./safeLocalStorage.ts";
import { toWorker } from "./toWorker.ts";

// Scheduled auto-simmer. This is the in-browser ("Tier 1") host: a persistent,
// route-independent timer that fires a normal Play Menu sim on a schedule. It
// lives at module scope (not inside a React component) so it keeps running as
// the commish navigates around the app. The settings page in AutoPlaySchedule
// just configures and observes it.

export type AutoPlayAmount = "day" | "week" | "month";

export type AutoPlaySettings = {
	enabled: boolean;
	intervalMinutes: number;
	amount: AutoPlayAmount;
	// Pause (instead of silently doing nothing) when the league reaches a phase
	// that needs a human decision - draft, re-signing, etc. Default on so we
	// never skip past something a GM should do by hand.
	pauseAtPhaseBoundaries: boolean;
	// Best-effort screen wake lock so a dedicated always-on tab is less likely
	// to get throttled/suspended by the browser.
	keepAwake: boolean;
};

export type AutoPlayState = {
	running: boolean;
	nextRunAt: number | undefined;
	lastRunAt: number | undefined;
	runCount: number;
	// Human-readable reason the scheduler last stopped/paused itself, shown on
	// the settings page.
	pausedReason: string | undefined;
};

const DEFAULT_SETTINGS: AutoPlaySettings = {
	enabled: false,
	intervalMinutes: 30,
	amount: "day",
	pauseAtPhaseBoundaries: true,
	keepAwake: true,
};

// Phases where a plain "sim day/week/month" makes progress on its own. Anything
// else (draft lottery/draft/after draft/re-sign/preseason/fantasy/expansion)
// needs a human, so we pause there.
const PLAYABLE_PHASES = new Set<number>([
	PHASE.REGULAR_SEASON,
	PHASE.AFTER_TRADE_DEADLINE,
	PHASE.PLAYOFFS,
	PHASE.FREE_AGENCY,
]);

const phaseName = (phase: number) => {
	switch (phase) {
		case PHASE.PRESEASON:
			return "preseason";
		case PHASE.DRAFT_LOTTERY:
			return "the draft lottery";
		case PHASE.DRAFT:
			return "the draft";
		case PHASE.AFTER_DRAFT:
			return "after the draft";
		case PHASE.RESIGN_PLAYERS:
			return "re-signing players";
		case PHASE.FANTASY_DRAFT:
			return "a fantasy draft";
		case PHASE.EXPANSION_DRAFT:
			return "an expansion draft";
		default:
			return "this phase";
	}
};

class AutoPlayScheduler {
	settings: AutoPlaySettings = { ...DEFAULT_SETTINGS };

	state: AutoPlayState = {
		running: false,
		nextRunAt: undefined,
		lastRunAt: undefined,
		runCount: 0,
		pausedReason: undefined,
	};

	private emitter = createNanoEvents<{
		change: (settings: AutoPlaySettings, state: AutoPlayState) => void;
	}>();

	private timeoutID: ReturnType<typeof setTimeout> | undefined;

	private lid: number | undefined;

	private wakeLock: any;

	private ticking = false;

	private storageKey(lid: number) {
		return `autoPlayScheduler-${lid}`;
	}

	// Load persisted settings for a league and (re)start if it was left enabled.
	loadForLeague(lid: number) {
		if (this.lid === lid) {
			return;
		}
		// Halt the timer for the previous league WITHOUT persisting - we must not
		// flip the old league's saved "enabled" flag just because we navigated.
		this.haltTimer();
		this.lid = lid;

		let loaded: Partial<AutoPlaySettings> = {};
		try {
			const raw = safeLocalStorage.getItem(this.storageKey(lid));
			if (raw) {
				loaded = JSON.parse(raw);
			}
		} catch {}

		this.settings = { ...DEFAULT_SETTINGS, ...loaded };
		this.emit();

		if (this.settings.enabled) {
			this.start();
		}
	}

	subscribe(cb: (settings: AutoPlaySettings, state: AutoPlayState) => void) {
		return this.emitter.on("change", cb);
	}

	private emit() {
		this.emitter.emit("change", this.settings, this.state);
	}

	private persist() {
		if (this.lid === undefined) {
			return;
		}
		safeLocalStorage.setItem(
			this.storageKey(this.lid),
			JSON.stringify(this.settings),
		);
	}

	updateSettings(partial: Partial<AutoPlaySettings>) {
		const wasEnabled = this.settings.enabled;
		this.settings = { ...this.settings, ...partial };
		this.persist();

		if (this.settings.enabled && !wasEnabled) {
			this.start();
		} else if (!this.settings.enabled && wasEnabled) {
			this.stop("Turned off");
		} else if (this.settings.enabled) {
			// Interval may have changed - reschedule from now.
			this.scheduleNext();
		}

		this.emit();
	}

	start() {
		this.settings.enabled = true;
		this.state.running = true;
		this.state.pausedReason = undefined;
		this.persist();
		this.requestWakeLock();
		this.scheduleNext();
		this.emit();
	}

	stop(reason?: string) {
		this.settings.enabled = false;
		this.state.pausedReason = reason;
		this.haltTimer();
		this.persist();
		this.emit();
	}

	// Clear the running timer and reset runtime state, but do NOT touch or
	// persist settings. Used both by stop() and when switching leagues.
	private haltTimer() {
		this.state.running = false;
		this.state.nextRunAt = undefined;
		if (this.timeoutID !== undefined) {
			clearTimeout(this.timeoutID);
			this.timeoutID = undefined;
		}
		this.releaseWakeLock();
	}

	// Only (re)arm the timer while auto play is actually enabled - otherwise a
	// one-off "Sim now" or a skipped tick could silently start a schedule.
	private rescheduleIfRunning() {
		if (!this.settings.enabled) {
			return;
		}
		if (this.timeoutID !== undefined) {
			clearTimeout(this.timeoutID);
		}
		const ms = Math.max(1, this.settings.intervalMinutes) * 60 * 1000;
		this.state.nextRunAt = Date.now() + ms;
		this.timeoutID = setTimeout(() => {
			void this.tick();
		}, ms);
		this.emit();
	}

	private scheduleNext() {
		this.rescheduleIfRunning();
	}

	// Run one sim immediately (used by the "Sim now" button), independent of
	// whether the recurring schedule is enabled.
	async runNow() {
		await this.tick();
	}

	private async tick() {
		if (this.ticking) {
			// Previous sim still resolving - just reschedule.
			this.rescheduleIfRunning();
			return;
		}

		const state = local.getState();

		// Don't stack sims on top of an in-progress one (e.g. a live game the
		// commish opened, or a still-running previous tick).
		if (state.gameSimInProgress) {
			this.rescheduleIfRunning();
			return;
		}

		const phase = state.phase;

		if (!PLAYABLE_PHASES.has(phase)) {
			if (this.settings.enabled && this.settings.pauseAtPhaseBoundaries) {
				this.stop(
					`Reached ${phaseName(phase)} - advance manually, then re-enable auto play.`,
				);
				return;
			}
			// Not pausing: nothing sensible to auto-do here, so skip this tick.
			this.rescheduleIfRunning();
			return;
		}

		this.ticking = true;
		try {
			await toWorker("playMenu", this.settings.amount, undefined);
			this.state.lastRunAt = Date.now();
			this.state.runCount += 1;
			this.state.pausedReason = undefined;
		} catch (error) {
			console.error("Auto play sim failed", error);
			this.stop("A sim failed - see console. Auto play stopped.");
			return;
		} finally {
			this.ticking = false;
		}

		this.rescheduleIfRunning();
	}

	private async requestWakeLock() {
		if (!this.settings.keepAwake) {
			return;
		}
		try {
			const wl = (navigator as any).wakeLock;
			if (wl?.request) {
				this.wakeLock = await wl.request("screen");
			}
		} catch {
			// Wake lock is best-effort; ignore failures (unsupported, not visible, etc).
		}
	}

	private releaseWakeLock() {
		try {
			this.wakeLock?.release?.();
		} catch {}
		this.wakeLock = undefined;
	}
}

export const autoPlayScheduler = new AutoPlayScheduler();

// Re-acquire the wake lock when the tab becomes visible again (the browser
// drops it on tab switch / minimize).
if (typeof document !== "undefined") {
	document.addEventListener("visibilitychange", () => {
		if (
			document.visibilityState === "visible" &&
			autoPlayScheduler.state.running &&
			autoPlayScheduler.settings.keepAwake
		) {
			// @ts-expect-error - private best-effort re-acquire
			autoPlayScheduler.requestWakeLock();
		}
	});
}
