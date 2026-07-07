import { createNanoEvents } from "nanoevents";
import { PHASE } from "../../common/constants.ts";
import { local } from "./local.ts";
import { safeLocalStorage } from "./safeLocalStorage.ts";
import { toWorker } from "./toWorker.ts";
import {
	newRule,
	nextFireForRule,
	type AutoPlayAmount,
	type ScheduleRule,
} from "./scheduleTime.ts";

// Re-exported so existing imports (UI, etc.) keep working from one place.
export { newRule, nextFireForRule } from "./scheduleTime.ts";
export type { AutoPlayAmount, ScheduleRule } from "./scheduleTime.ts";

// Scheduled auto-simmer. This is the in-browser ("Tier 1") host: a persistent,
// route-independent timer that fires normal Play Menu sims on a real clock
// schedule. It lives at module scope (not inside a React component) so it keeps
// running as you navigate around the app. AutoPlaySchedule just configures it.
//
// The schedule is a list of RULES. Each rule fires either at specific clock
// times, or every N minutes within a daily window, on chosen days of the week,
// simming its own amount. The scheduler always arms toward the earliest next
// fire across all rules, re-checking at least once a minute so it self-corrects
// after the tab sleeps or the clock jumps (DST, etc.).

export type AutoPlaySettings = {
	enabled: boolean;
	rules: ScheduleRule[];
	// Pause (instead of silently doing nothing) when the league reaches a phase
	// that needs a human decision - draft, re-signing, etc.
	pauseAtPhaseBoundaries: boolean;
	// Best-effort screen wake lock so a dedicated always-on tab is less likely to
	// get throttled/suspended by the browser.
	keepAwake: boolean;
};

export type AutoPlayState = {
	running: boolean;
	nextRunAt: number | undefined;
	lastRunAt: number | undefined;
	runCount: number;
	pausedReason: string | undefined;
};

const DEFAULT_SETTINGS = (): AutoPlaySettings => ({
	enabled: false,
	rules: [newRule()],
	pauseAtPhaseBoundaries: true,
	keepAwake: true,
});

// Phases where a plain "sim day/week/month" makes progress on its own. Anything
// else needs a human, so we pause there.
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

// Re-check cadence while idly waiting, so long waits survive tab sleep / clock
// jumps and pick up config changes promptly.
const RECHECK_MS = 60_000;

class AutoPlayScheduler {
	settings: AutoPlaySettings = DEFAULT_SETTINGS();

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

	// The amount to sim when the currently-armed fire lands.
	private nextAmount: AutoPlayAmount = "day";

	private storageKey(lid: number) {
		return `autoPlayScheduler-${lid}`;
	}

	// Accept both the new (rules) shape and the legacy (intervalMinutes) one.
	private migrate(loaded: any): AutoPlaySettings {
		const base = DEFAULT_SETTINGS();
		if (loaded && Array.isArray(loaded.rules)) {
			return {
				enabled: !!loaded.enabled,
				rules: loaded.rules.map((r: any) => ({ ...newRule(), ...r })),
				pauseAtPhaseBoundaries: loaded.pauseAtPhaseBoundaries ?? true,
				keepAwake: loaded.keepAwake ?? true,
			};
		}
		if (loaded && typeof loaded.intervalMinutes === "number") {
			return {
				enabled: !!loaded.enabled,
				rules: [
					{
						...newRule(),
						mode: "every",
						everyMinutes: loaded.intervalMinutes,
						amount: loaded.amount ?? "day",
					},
				],
				pauseAtPhaseBoundaries: loaded.pauseAtPhaseBoundaries ?? true,
				keepAwake: loaded.keepAwake ?? true,
			};
		}
		return base;
	}

	loadForLeague(lid: number) {
		if (this.lid === lid) {
			return;
		}
		this.haltTimer();
		this.lid = lid;

		let loaded: any = undefined;
		try {
			const raw = safeLocalStorage.getItem(this.storageKey(lid));
			if (raw) {
				loaded = JSON.parse(raw);
			}
		} catch {}

		this.settings = this.migrate(loaded);
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
			this.armTimer();
		}
		this.emit();
	}

	// Replace the whole rules list (the editor's single source of truth).
	setRules(rules: ScheduleRule[]) {
		this.updateSettings({ rules });
	}

	start() {
		this.settings.enabled = true;
		this.state.running = true;
		this.state.pausedReason = undefined;
		this.persist();
		this.requestWakeLock();
		this.armTimer();
		this.emit();
	}

	stop(reason?: string) {
		this.settings.enabled = false;
		this.state.pausedReason = reason;
		this.haltTimer();
		this.persist();
		this.emit();
	}

	private haltTimer() {
		this.state.running = false;
		this.state.nextRunAt = undefined;
		if (this.timeoutID !== undefined) {
			clearTimeout(this.timeoutID);
			this.timeoutID = undefined;
		}
		this.releaseWakeLock();
	}

	// Compute the earliest next fire across all rules, and remember its amount.
	private computeNext(): { at: number; amount: AutoPlayAmount } | undefined {
		const now = new Date();
		let best: { at: number; amount: AutoPlayAmount } | undefined;
		for (const rule of this.settings.rules) {
			const at = nextFireForRule(rule, now);
			if (at !== undefined && (best === undefined || at < best.at)) {
				best = { at, amount: rule.amount };
			}
		}
		return best;
	}

	// Arm a timer toward the next fire, capped so we re-evaluate at least once a
	// minute (self-correcting after sleep / clock changes / config edits).
	private armTimer() {
		if (this.timeoutID !== undefined) {
			clearTimeout(this.timeoutID);
		}
		if (!this.settings.enabled) {
			return;
		}
		const next = this.computeNext();
		if (!next) {
			this.state.nextRunAt = undefined;
			this.state.pausedReason = "No active schedule rules.";
			this.emit();
			this.timeoutID = setTimeout(() => this.onTimer(), RECHECK_MS);
			return;
		}
		this.nextAmount = next.amount;
		this.state.nextRunAt = next.at;
		const delay = Math.min(Math.max(0, next.at - Date.now()), RECHECK_MS);
		this.timeoutID = setTimeout(() => this.onTimer(), delay);
		this.emit();
	}

	private async onTimer() {
		if (!this.settings.enabled) {
			return;
		}
		if (
			this.state.nextRunAt !== undefined &&
			Date.now() >= this.state.nextRunAt - 250
		) {
			await this.tick(this.nextAmount);
		}
		// Re-arm toward the next fire (unless a pause/stop turned us off).
		if (this.settings.enabled) {
			this.armTimer();
		}
	}

	// Run one sim immediately, independent of the schedule.
	async runNow(amount: AutoPlayAmount = "day") {
		await this.tick(amount);
	}

	private async tick(amount: AutoPlayAmount) {
		if (this.ticking) {
			return;
		}
		const state = local.getState();
		if (state.gameSimInProgress) {
			return;
		}

		const phase = state.phase;
		if (!PLAYABLE_PHASES.has(phase)) {
			if (this.settings.enabled && this.settings.pauseAtPhaseBoundaries) {
				this.stop(
					`Reached ${phaseName(phase)} - advance manually, then re-enable auto play.`,
				);
			}
			return;
		}

		this.ticking = true;
		try {
			await toWorker("playMenu", amount, undefined);
			this.state.lastRunAt = Date.now();
			this.state.runCount += 1;
			this.state.pausedReason = undefined;
		} catch (error) {
			console.error("Auto play sim failed", error);
			this.stop("A sim failed - see console. Auto play stopped.");
		} finally {
			this.ticking = false;
		}
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
			// Wake lock is best-effort; ignore failures.
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

// Re-acquire the wake lock when the tab becomes visible again (the browser drops
// it on tab switch / minimize).
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
