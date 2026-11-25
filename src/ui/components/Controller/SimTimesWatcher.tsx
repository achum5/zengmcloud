import { useEffect, useMemo, useState } from "react";
import { PHASE } from "../../../common/constants.ts";
import { autoPlayScheduler } from "../../util/autoPlayScheduler.ts";
import type { AutoPlayPreviewData } from "../../util/autoPlayPreview.ts";
import { useLeagueRefresh } from "../../util/leagueRefresh.ts";
import { useLocal } from "../../util/local.ts";
import type { ScheduleRule } from "../../util/scheduleTime.ts";
import { installSimTimeTips } from "../../util/simTimeTip.ts";
import { useSimTimesStore } from "../../util/simTimes.ts";
import {
	formatSimWindow,
	simWindows,
	simWindowsByDate,
	type SimWindowStops,
} from "../../util/simWindows.ts";
import { toWorker } from "../../util/toWorker.ts";

// Keeps every calendar's day sim times (simTimes.ts) up to date: the days
// still to be played, projected onto the room's auto play schedule, the same
// way the Auto Play Scheduler page lists them. On the device in charge of
// simming that's its own schedule; everywhere else it's the one it shares.
// Renders nothing.

const EMPTY = {
	season: undefined,
	byDay: new Map<number, string>(),
	byDate: new Map<string, string>(),
};

// The regular season: auto play stops at the playoffs.
const PHASES = new Set<number>([
	PHASE.REGULAR_SEASON,
	PHASE.AFTER_TRADE_DEADLINE,
]);

const SimTimesWatcher = () => {
	const {
		calendarDate,
		lid,
		mpAutoPlay,
		mpPhaseReady,
		mpSyncActive,
		mpSyncIsHost,
		phase,
	} = useLocal([
		"calendarDate",
		"lid",
		"mpAutoPlay",
		"mpPhaseReady",
		"mpSyncActive",
		"mpSyncIsHost",
		"phase",
	]);

	useEffect(() => {
		installSimTimeTips();
	}, []);

	// This device's own scheduler, for when it's the one simming.
	const [own, setOwn] = useState(() => ({
		settings: autoPlayScheduler.settings,
		nextRunAt: autoPlayScheduler.state.nextRunAt,
	}));
	useEffect(() => {
		if (typeof lid === "number") {
			autoPlayScheduler.loadForLeague(lid);
		}
		setOwn({
			settings: autoPlayScheduler.settings,
			nextRunAt: autoPlayScheduler.state.nextRunAt,
		});
		return autoPlayScheduler.subscribe((settings, state) => {
			setOwn({ settings, nextRunAt: state.nextRunAt });
		});
	}, [lid]);

	// The schedule that's running, while it's counting down to a sim.
	const simmer = !!mpSyncActive && !!mpSyncIsHost;
	let rules: ScheduleRule[] | undefined;
	let stops: SimWindowStops = {};
	if (mpSyncActive && PHASES.has(phase)) {
		if (simmer) {
			if (own.settings.enabled && own.nextRunAt !== undefined) {
				rules = own.settings.rules.filter((rule) => rule.enabled);
				stops = own.settings;
			}
		} else if (mpAutoPlay?.enabled && mpAutoPlay.nextRunAt !== undefined) {
			rules = mpAutoPlay.scheduleRules;
			stops = mpAutoPlay;
		}
	}
	const on = rules !== undefined && rules.length > 0;

	// The days left, asked again whenever the league moves on (a day simmed
	// anywhere in the room) or the room readies up for a stop.
	const refresh = useLeagueRefresh();
	const [preview, setPreview] = useState<AutoPlayPreviewData | undefined>();
	useEffect(() => {
		if (!on) {
			setPreview(undefined);
			return;
		}
		let cancelled = false;
		(async () => {
			try {
				const data = (await toWorker(
					"main",
					"getAutoPlayPreview",
					undefined,
				)) as AutoPlayPreviewData;
				if (!cancelled) {
					setPreview(data);
				}
			} catch (error) {
				console.error("Failed to load auto play days", error);
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [on, lid, refresh, calendarDate, mpPhaseReady]);

	// Once a minute, so a sim that came and went moves every day along, and
	// "Tomorrow" turns into today at midnight.
	const [minute, setMinute] = useState(() => Math.floor(Date.now() / 60_000));
	useEffect(() => {
		if (!on) {
			return;
		}
		const id = window.setInterval(() => {
			setMinute(Math.floor(Date.now() / 60_000));
		}, 15_000);
		return () => {
			window.clearInterval(id);
		};
	}, [on]);

	const rulesKey = JSON.stringify(rules);
	const stopAfterSeason = stops.stopAfter?.season;
	const stopAfterDay = stops.stopAfter?.day;
	const { stopAfterSims, stopAt } = stops;
	const times = useMemo(() => {
		if (!on || !preview || !PHASES.has(preview.phase)) {
			return EMPTY;
		}
		const now = new Date();
		const windows = simWindows({
			now,
			preview,
			rules: rules!,
			stops: {
				...(stopAfterSeason !== undefined && stopAfterDay !== undefined
					? { stopAfter: { season: stopAfterSeason, day: stopAfterDay } }
					: {}),
				...(stopAfterSims !== undefined ? { stopAfterSims } : {}),
				...(stopAt !== undefined ? { stopAt } : {}),
			},
		});
		const nowMs = now.getTime();
		const byDay = new Map<number, string>();
		for (const [day, window] of windows) {
			byDay.set(day, formatSimWindow(window, nowMs));
		}
		const byDate = new Map<string, string>();
		for (const [date, window] of simWindowsByDate(windows, preview)) {
			byDate.set(date, formatSimWindow(window, nowMs));
		}
		return { season: preview.season, byDay, byDate };
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [
		on,
		preview,
		rulesKey,
		stopAfterSeason,
		stopAfterDay,
		stopAfterSims,
		stopAt,
		minute,
	]);

	useEffect(() => {
		const have = useSimTimesStore.getState();
		if (
			have.season !== times.season ||
			!sameMap(have.byDay, times.byDay) ||
			!sameMap(have.byDate, times.byDate)
		) {
			useSimTimesStore.setState(times);
		}
	}, [times]);

	// (Leaving the league: no times left behind for the next one.)
	useEffect(
		() => () => {
			useSimTimesStore.setState(EMPTY);
		},
		[],
	);

	return null;
};

const sameMap = <K, V>(a: Map<K, V>, b: Map<K, V>) => {
	if (a.size !== b.size) {
		return false;
	}
	for (const [key, value] of a) {
		if (b.get(key) !== value) {
			return false;
		}
	}
	return true;
};

export default SimTimesWatcher;
