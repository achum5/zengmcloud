import { createNanoEvents } from "nanoevents";
import { PHASE } from "../../common/constants.ts";
import { local } from "./local.ts";
import { safeLocalStorage } from "./safeLocalStorage.ts";
import { toWorker } from "./toWorker.ts";
import {
	newRule,
	nextFireForRule,
	summarizeRule,
	type AutoPlayAmount,
	type ScheduleRule,
} from "./scheduleTime.ts";
import { hasPassedStop, type AutoPlayPreviewData } from "./autoPlayPreview.ts";

// Re-exported so existing imports (UI, etc.) keep working from one place.
export {
	describeAmount,
	newRule,
	nextFireForRule,
	summarizeRule,
} from "./scheduleTime.ts";
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

// Every way auto play can be told to stop on its own. All are optional and all
// are checked - whichever comes first wins.
export type AutoPlayStopConditions = {
	// Stop once the league has been simmed THROUGH this day (scoped to a season,
	// since day numbers restart each year). Set from the Upcoming-sims preview.
	stopAfter?: { season: number; day: number };
	// Stop after this many more sims. Counts down as they run.
	stopAfterSims?: number;
	// Stop at a wall-clock time.
	stopAt?: number;
	// Stop when the current phase's schedule runs out, rather than rolling into
	// the next one.
	stopAtPhaseEnd?: boolean;
};

export type AutoPlaySettings = AutoPlayStopConditions & {
	enabled: boolean;
	rules: ScheduleRule[];
	// Pause (instead of silently doing nothing) when the league reaches a phase
	// that needs a human decision - draft, re-signing, etc.
	pauseAtPhaseBoundaries: boolean;
	// Best-effort screen wake lock so a dedicated always-on tab is less likely to
	// get throttled/suspended by the browser.
	keepAwake: boolean;
	// How many past sims to keep in the log.
	logLimit: number;
};

// One line of the run log: what a sim actually did, as opposed to what the
// schedule said it would do. Kept because "Sims run: 34" answers none of the
// questions you actually have when you come back to a tab that has been simming
// unattended overnight.
export type AutoPlayLogEntry = {
	at: number;
	amount: AutoPlayAmount;
	numDays: number;
	auto: boolean;
	// Where the league actually moved, read from the calendar either side of the
	// sim. Absent if the preview was unavailable.
	fromDay?: number;
	toDay?: number;
	numGames?: number;
	season?: number;
	// Set instead of the day range when the sim threw.
	error?: string;
};

export type AutoPlayState = {
	running: boolean;
	nextRunAt: number | undefined;
	// What the armed fire will do when it lands.
	nextAmount: AutoPlayAmount | undefined;
	nextNumDays: number;
	lastRunAt: number | undefined;
	runCount: number;
	pausedReason: string | undefined;
	// True when this tab holds the driver lock - i.e. it is the one that will
	// actually fire. Another tab holding it is the single most confusing
	// invisible state this thing has, so it is surfaced.
	isDriver: boolean;
	// Whether the screen wake lock is currently held.
	wakeLockHeld: boolean;
	log: AutoPlayLogEntry[];
};

const DEFAULT_LOG_LIMIT = 50;

const DEFAULT_SETTINGS = (): AutoPlaySettings => ({
	enabled: false,
	rules: [newRule()],
	pauseAtPhaseBoundaries: true,
	keepAwake: true,
	logLimit: DEFAULT_LOG_LIMIT,
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
		nextAmount: undefined,
		nextNumDays: 1,
		lastRunAt: undefined,
		runCount: 0,
		pausedReason: undefined,
		isDriver: false,
		wakeLockHeld: false,
		log: [],
	};

	private emitter = createNanoEvents<{
		change: (settings: AutoPlaySettings, state: AutoPlayState) => void;
	}>();

	private timeoutID: ReturnType<typeof setTimeout> | undefined;

	private lid: number | undefined;

	private wakeLock: any;

	private ticking = false;

	// What the currently-armed fire will sim when it lands.
	private nextFire: { amount: AutoPlayAmount; numDays: number } = {
		amount: "day",
		numDays: 1,
	};

	// Single-driver election across tabs. Two tabs of the same browser share one
	// sim-authority identity (the Firebase anon uid persists across tabs), so
	// without this BOTH tabs' schedulers would fire a sim on the same tick and the
	// league would advance two days per tick instead of one. A per-league Web Lock
	// makes exactly one tab the driver; others hold a pending request and never
	// fire the AUTOMATIC sim (manual "run now" is exempt) until they take over.
	private isLeader = false;
	private driverLockRequested = false;
	private driverLockRelease: (() => void) | undefined;
	private driverLockAbort: AbortController | undefined;

	private storageKey(lid: number) {
		return `autoPlayScheduler-${lid}`;
	}

	// The log lives under its own key rather than in settings, so a reload of a
	// tab that simmed all night still shows what it did - which is the whole
	// reason the log exists.
	private logKey(lid: number) {
		return `autoPlayLog-${lid}`;
	}

	private persistLog() {
		if (this.lid === undefined) {
			return;
		}
		safeLocalStorage.setItem(
			this.logKey(this.lid),
			JSON.stringify(this.state.log),
		);
	}

	private loadLog(lid: number): AutoPlayLogEntry[] {
		try {
			const raw = safeLocalStorage.getItem(this.logKey(lid));
			const parsed = raw ? JSON.parse(raw) : undefined;
			return Array.isArray(parsed) ? parsed : [];
		} catch {
			return [];
		}
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
				logLimit: loaded.logLimit ?? DEFAULT_LOG_LIMIT,
				stopAfter: loaded.stopAfter,
				stopAfterSims: loaded.stopAfterSims,
				stopAt: loaded.stopAt,
				stopAtPhaseEnd: loaded.stopAtPhaseEnd,
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
				logLimit: DEFAULT_LOG_LIMIT,
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
		this.state.log = this.loadLog(lid);
		this.emit();

		if (this.settings.enabled) {
			// Keep the persisted intent, but only actually run once eligible.
			this.armTimer();
		}
	}

	// Auto play advances the shared league, so it's only allowed when connected to
	// the cloud AND being in charge of simming. (The worker enforces sim authority too.)
	private eligible(): boolean {
		const s = local.getState();
		return !!s.mpSyncActive && !!s.mpSyncIsHost;
	}

	subscribe(cb: (settings: AutoPlaySettings, state: AutoPlayState) => void) {
		return this.emitter.on("change", cb);
	}

	private emit() {
		this.emitter.emit("change", this.settings, this.state);
		this.publishToRoom();
	}

	// The last snapshot we broadcast, so we only send when it actually changes.
	private lastPublished = "";
	// True once we've broadcast as the simmer, so when we stop / lose sim control
	// we send exactly ONE "off" snapshot (and don't otherwise write the room while
	// idle, since only the simmer owns the shared schedule).
	private publishedAsSimmer = false;

	// Broadcast a small snapshot of the schedule to the room whenever it changes
	// (start / stop / edit / next-sim recomputed). The schedule itself lives
	// locally on the simmer; this is just a read-only mirror for the other devices,
	// which keep the last snapshot until the next one arrives. Only the simmer
	// writes; followers never do.
	private publishToRoom() {
		const simming = this.eligible() && this.settings.enabled;

		// While not the simmer, stay silent - except for the single "off" snapshot
		// right after we stop or hand off sim authority, so followers clear their view.
		if (!simming && !this.publishedAsSimmer) {
			return;
		}

		const snapshot = simming
			? {
					enabled: true,
					nextRunAt: this.state.nextRunAt,
					rules: this.settings.rules
						.filter((r) => r.enabled)
						.map((r) => summarizeRule(r)),
				}
			: { enabled: false, nextRunAt: undefined, rules: [] };

		const serialized = JSON.stringify(snapshot);
		if (serialized === this.lastPublished) {
			return;
		}
		this.lastPublished = serialized;
		this.publishedAsSimmer = simming;
		void toWorker("main", "publishAutoPlayState", snapshot);
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
		if (!this.eligible()) {
			// Can't enable unless connected + this device is the simmer (the button
			// is also disabled in this state).
			this.state.pausedReason = "Connect and sim here to auto play.";
			this.emit();
			return;
		}
		this.settings.enabled = true;
		this.state.pausedReason = undefined;
		this.persist();
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
		// Stopped driving (turned off, or switching leagues) - give up leadership so
		// another eligible tab can take over.
		this.releaseDriverLock();
	}

	// Acquire the driver lock while this tab is a driver candidate (enabled +
	// eligible for the current league), release it otherwise. Called from the arm
	// and halt paths so leadership always tracks whether this tab wants to drive.
	private updateDriverLock() {
		const wantDrive =
			this.lid !== undefined && this.settings.enabled && this.eligible();
		if (wantDrive) {
			this.requestDriverLock(this.lid);
		} else {
			this.releaseDriverLock();
		}
	}

	private requestDriverLock(lid: number) {
		if (this.driverLockRequested) {
			return;
		}
		this.driverLockRequested = true;

		const lockManager: any = (globalThis as any).navigator?.locks;
		if (!lockManager?.request) {
			// No Web Locks API: assume this is the only driver (pre-existing behavior).
			this.isLeader = true;
			this.state.isDriver = true;
			return;
		}

		this.isLeader = false;
		this.state.isDriver = false;
		const abort = new AbortController();
		this.driverLockAbort = abort;
		// Blocking request: the FIRST tab to ask holds the lock and becomes the sole
		// driver; a second tab's request stays pending (so it never fires an auto
		// sim) until the holder closes or releases, then it takes over. Web Locks
		// auto-release when the holding tab is closed.
		lockManager
			.request(
				`autoPlayDriver-${lid}`,
				{ signal: abort.signal },
				() =>
					new Promise<void>((resolve) => {
						this.isLeader = true;
						this.state.isDriver = true;
						this.driverLockRelease = resolve;
						this.emit();
					}),
			)
			.catch(() => {
				// Aborted (we released / switched leagues) or an unexpected error. On a
				// real error - not our own abort - fall back to driving so auto play is
				// never silently wedged by the lock.
				if (!abort.signal.aborted) {
					this.isLeader = true;
					this.state.isDriver = true;
					this.emit();
				}
			});
	}

	private releaseDriverLock() {
		if (!this.driverLockRequested) {
			return;
		}
		this.driverLockRequested = false;
		this.isLeader = false;
		this.state.isDriver = false;
		if (this.driverLockRelease) {
			this.driverLockRelease();
			this.driverLockRelease = undefined;
		}
		if (this.driverLockAbort) {
			this.driverLockAbort.abort();
			this.driverLockAbort = undefined;
		}
	}

	// Compute the earliest next fire across all rules, and remember its amount.
	private computeNext():
		| { at: number; amount: AutoPlayAmount; numDays: number }
		| undefined {
		const now = new Date();
		let best:
			| { at: number; amount: AutoPlayAmount; numDays: number }
			| undefined;
		for (const rule of this.settings.rules) {
			const at = nextFireForRule(rule, now);
			if (at !== undefined && (best === undefined || at < best.at)) {
				best = { at, amount: rule.amount, numDays: rule.numDays };
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
		// Keep leadership in sync with whether this tab currently wants to drive.
		this.updateDriverLock();
		if (!this.settings.enabled) {
			return;
		}
		if (this.stopTimeReached()) {
			this.settings.stopAt = undefined;
			this.persist();
			this.stop("Reached the scheduled stop time.");
			return;
		}
		// Paused (still enabled) until we're connected + be in charge of simming. Re-check
		// so it resumes automatically once eligible (or pauses if sim authority moves).
		if (!this.eligible()) {
			this.state.running = false;
			this.state.nextRunAt = undefined;
			this.state.pausedReason = "Waiting for cloud connection + sim control.";
			this.releaseWakeLock();
			this.emit();
			this.timeoutID = setTimeout(() => this.onTimer(), RECHECK_MS);
			return;
		}
		if (!this.state.running) {
			this.state.running = true;
			this.state.pausedReason = undefined;
			this.requestWakeLock();
		}
		const next = this.computeNext();
		if (!next) {
			this.state.nextRunAt = undefined;
			this.state.pausedReason = "No active schedule rules.";
			this.emit();
			this.timeoutID = setTimeout(() => this.onTimer(), RECHECK_MS);
			return;
		}
		this.nextFire = { amount: next.amount, numDays: next.numDays };
		this.state.nextAmount = next.amount;
		this.state.nextNumDays = next.numDays;
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
			await this.tick(this.nextFire, { auto: true });
		}
		// Re-arm toward the next fire (unless a pause/stop turned us off).
		if (this.settings.enabled) {
			this.armTimer();
		}
	}

	// Run one sim immediately, independent of the schedule.
	async runNow(amount: AutoPlayAmount = "day", numDays = 1) {
		await this.tick({ amount, numDays });
	}

	private async tick(
		fire: { amount: AutoPlayAmount; numDays: number },
		{ auto = false }: { auto?: boolean } = {},
	) {
		if (this.ticking) {
			return;
		}
		// Lost the connection or sim authority since we armed - skip; armTimer pauses.
		if (!this.eligible()) {
			return;
		}
		// Only the elected driver tab runs the AUTOMATIC schedule, so two open tabs
		// (which share one sim-authority identity) can't each fire and advance the
		// league two days per tick. A manual "run now" on this tab is exempt.
		if (auto && !this.isLeader) {
			return;
		}
		const state = local.getState();
		if (state.gameSimInProgress) {
			return;
		}

		// The eligibility check above says this device is connected and in charge;
		// it says nothing about whether its local league state can be trusted. A
		// device parked at a phantom phase by a bad replay passes every connection
		// check - and an unattended timer firing from that state sims the
		// corruption straight into the shared log for the whole room. The worker
		// refuses such a sim too; asking first means the scheduler PAUSES and
		// retries instead of burning its fire on a refusal (and instead of
		// stopping outright, since the device repairs itself and the schedule
		// should carry on the moment it has).
		if (state.mpSyncActive) {
			const safety = (await toWorker("main", "getAutoSimSafety", undefined)) as
				| { safe: true }
				| { safe: false; reason: string }
				| undefined;
			if (safety && !safety.safe) {
				if (this.settings.enabled) {
					this.armTimer();
				}
				this.state.pausedReason = safety.reason;
				this.emit();
				return;
			}
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
		// Read the calendar either side of the sim so the log can say where the
		// league actually went, not just which button was pressed.
		const before = await this.fetchPreview();

		// The trade deadline is a decision, and an unattended timer is the worst
		// thing to have make it. The sim stops there on its own (see
		// tradeDeadlineGate.ts), so firing anyway would either accomplish nothing
		// or - alone, where the next press is what crosses it - cross the deadline
		// with nobody watching. Stop here instead, before the sim is asked to run.
		if (before?.upcomingDays[0]?.tradeDeadline) {
			this.ticking = false;
			if (state.mpSyncActive) {
				// Shared league: the room crosses it by readying up, and auto play
				// should be waiting to carry on the moment it does - so this PAUSES
				// (stays enabled, keeps re-arming) rather than stopping.
				//
				// armTimer FIRST: it clears pausedReason when it transitions the
				// scheduler back to running, which would wipe the reason set here.
				if (this.settings.enabled) {
					this.armTimer();
				}
				this.state.pausedReason =
					"Trade deadline - waiting for every team to ready up.";
				this.emit();
			} else {
				this.stop(
					"Trade deadline reached - make your moves, then re-enable auto play.",
				);
			}
			return;
		}

		try {
			if (fire.amount === "days") {
				await toWorker(
					"playMenu",
					"days",
					Math.max(1, Math.round(fire.numDays)),
				);
			} else {
				await toWorker("playMenu", fire.amount, undefined);
			}
			this.state.lastRunAt = Date.now();
			this.state.runCount += 1;
			this.state.pausedReason = undefined;
		} catch (error) {
			console.error("Auto play sim failed", error);
			this.appendLog({
				at: Date.now(),
				amount: fire.amount,
				numDays: fire.numDays,
				auto,
				error: error instanceof Error ? error.message : String(error),
			});
			this.stop("A sim failed - see console. Auto play stopped.");
			return;
		} finally {
			this.ticking = false;
		}

		const after = await this.fetchPreview();
		this.appendLog(this.describeRun(fire, auto, before, after));

		await this.maybeStop(after, auto);
	}

	private async fetchPreview(): Promise<AutoPlayPreviewData | undefined> {
		try {
			return (await toWorker(
				"main",
				"getAutoPlayPreview",
				undefined,
			)) as AutoPlayPreviewData;
		} catch (error) {
			console.error("Auto play preview failed", error);
			return undefined;
		}
	}

	// What the sim covered, from the calendar either side of it. The days it
	// actually played are the ones present before and gone after.
	private describeRun(
		fire: { amount: AutoPlayAmount; numDays: number },
		auto: boolean,
		before: AutoPlayPreviewData | undefined,
		after: AutoPlayPreviewData | undefined,
	): AutoPlayLogEntry {
		const entry: AutoPlayLogEntry = {
			at: Date.now(),
			amount: fire.amount,
			numDays: fire.numDays,
			auto,
			season: before?.season,
		};
		if (!before || before.upcomingDays.length === 0) {
			return entry;
		}
		const nextDay =
			after && after.season === before.season
				? after.upcomingDays[0]?.day
				: undefined;
		const played =
			nextDay === undefined
				? before.upcomingDays
				: before.upcomingDays.filter((d) => d.day < nextDay);
		if (played.length === 0) {
			return entry;
		}
		entry.fromDay = played[0]!.day;
		entry.toDay = played.at(-1)!.day;
		entry.numGames = played.reduce((sum, d) => sum + d.numGames, 0);
		return entry;
	}

	private appendLog(entry: AutoPlayLogEntry) {
		const limit = Math.max(1, this.settings.logLimit ?? DEFAULT_LOG_LIMIT);
		this.state.log = [entry, ...this.state.log].slice(0, limit);
		this.persistLog();
		this.emit();
	}

	clearLog() {
		this.state.log = [];
		this.persistLog();
		this.emit();
	}

	// Every stop condition, checked after each sim. Whichever trips first wins.
	private async maybeStop(
		preview: AutoPlayPreviewData | undefined,
		auto: boolean,
	) {
		if (!this.settings.enabled) {
			return;
		}

		const target = this.settings.stopAfter;
		if (
			target &&
			preview &&
			hasPassedStop(target, preview.season, preview.upcomingDays[0]?.day)
		) {
			this.settings.stopAfter = undefined;
			this.persist();
			this.stop(`Stopped after Day ${target.day}, as scheduled.`);
			return;
		}

		// Counts SCHEDULED sims - a manual "sim day now" shouldn't burn one.
		if (auto && typeof this.settings.stopAfterSims === "number") {
			const left = this.settings.stopAfterSims - 1;
			this.settings.stopAfterSims = left > 0 ? left : undefined;
			this.persist();
			if (left <= 0) {
				this.stop("Ran the requested number of sims.");
				return;
			}
			this.emit();
		}

		if (
			this.settings.stopAtPhaseEnd &&
			preview &&
			preview.upcomingDays.length === 0
		) {
			this.stop("Reached the end of the phase, as scheduled.");
		}
	}

	// Time-based stop, checked while waiting rather than after a sim so it lands
	// on time even during a long gap between fires.
	private stopTimeReached(): boolean {
		return (
			typeof this.settings.stopAt === "number" &&
			Date.now() >= this.settings.stopAt
		);
	}

	private async requestWakeLock() {
		if (!this.settings.keepAwake) {
			return;
		}
		try {
			const wl = (navigator as any).wakeLock;
			if (wl?.request) {
				this.wakeLock = await wl.request("screen");
				this.state.wakeLockHeld = true;
				this.emit();
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
		this.state.wakeLockHeld = false;
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
