import { useEffect, useMemo, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { useLocal } from "../util/local.ts";
import { toWorker } from "../util/toWorker.ts";
import {
	autoPlayScheduler,
	newRule,
	type AutoPlayAmount,
	type AutoPlaySettings,
	type AutoPlayState,
	type ScheduleRule,
} from "../util/autoPlayScheduler.ts";
import {
	nextFires,
	projectFires,
	type AutoPlayPreviewData,
} from "../util/autoPlayPreview.ts";

// How many upcoming sims to compute, and how many to show before summarizing.
const MAX_FIRES = 250;
const DISPLAY_FIRES = 12;

const DOW = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];

const formatCountdown = (ms: number) => {
	if (ms <= 0) {
		return "now";
	}
	const s = Math.round(ms / 1000);
	const days = Math.floor(s / 86400);
	const h = Math.floor((s % 86400) / 3600);
	const m = Math.floor((s % 3600) / 60);
	const sec = s % 60;
	if (days > 0) {
		return `${days}d ${h}h`;
	}
	if (h > 0) {
		return `${h}h ${m}m`;
	}
	return `${m}:${sec.toString().padStart(2, "0")}`;
};

const formatTime = (ts: number | undefined) =>
	ts === undefined ? "-" : new Date(ts).toLocaleString();

const AutoPlaySchedule = () => {
	useTitleBar({ title: "Auto Play Scheduler" });

	const { lid, phaseText, mpSyncActive, mpSyncIsHost, mpAutoPlay } = useLocal([
		"lid",
		"phaseText",
		"mpSyncActive",
		"mpSyncIsHost",
		"mpAutoPlay",
	]);

	// Auto play advances the shared league, so it's only allowed when connected
	// to the cloud AND holding the wheel.
	const eligible = mpSyncActive && mpSyncIsHost;

	const [settings, setSettings] = useState<AutoPlaySettings>(
		autoPlayScheduler.settings,
	);
	const [state, setState] = useState<AutoPlayState>(autoPlayScheduler.state);

	useEffect(() => {
		if (typeof lid === "number") {
			autoPlayScheduler.loadForLeague(lid);
		}
		setSettings({ ...autoPlayScheduler.settings });
		setState({ ...autoPlayScheduler.state });
		const unsub = autoPlayScheduler.subscribe((s, st) => {
			setSettings({ ...s });
			setState({ ...st });
		});
		return unsub;
	}, [lid]);

	// Live countdown clock.
	const [now, setNow] = useState(() => Date.now());
	useEffect(() => {
		const id = setInterval(() => setNow(Date.now()), 1000);
		return () => clearInterval(id);
	}, []);

	// The upcoming-season calendar, refetched after each sim (the schedule
	// advances) and when eligibility changes.
	const [preview, setPreview] = useState<AutoPlayPreviewData | undefined>();
	useEffect(() => {
		if (!eligible) {
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
				console.error("Failed to load auto play preview", error);
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [eligible, lid, state.runCount]);

	// Overlay the real-clock fire schedule on the season calendar. Recomputed as
	// rules/data change and roughly every 15s as fires pass (the per-second
	// countdown is derived separately, below).
	const nowBucket = Math.floor(now / 15000);
	const projected = useMemo(() => {
		if (!preview || preview.upcomingDays.length === 0) {
			return [];
		}
		const fires = nextFires(settings.rules, new Date(), MAX_FIRES);
		return projectFires(
			fires,
			preview.upcomingDays,
			preview.amountDays,
			preview.phaseEndNote,
		);
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [preview, settings.rules, nowBucket]);

	const update = (partial: Partial<AutoPlaySettings>) =>
		autoPlayScheduler.updateSettings(partial);

	const setRule = (id: string, patch: Partial<ScheduleRule>) =>
		autoPlayScheduler.setRules(
			settings.rules.map((r) => (r.id === id ? { ...r, ...patch } : r)),
		);
	const addRule = () =>
		autoPlayScheduler.setRules([...settings.rules, newRule()]);
	const removeRule = (id: string) =>
		autoPlayScheduler.setRules(settings.rules.filter((r) => r.id !== id));

	const countdownMs =
		state.nextRunAt !== undefined ? state.nextRunAt - now : undefined;

	const hasEnabledRules = settings.rules.some((r) => r.enabled);

	return (
		<>
			{!eligible && mpAutoPlay?.enabled ? (
				<div className="card mb-3" style={{ maxWidth: 520 }}>
					<div className="card-body">
						<h3 className="card-title h5">Shared schedule</h3>
						{mpAutoPlay.rules.length > 0 ? (
							<ul className="mb-2">
								{mpAutoPlay.rules.map((line, i) => (
									<li key={i}>{line}</li>
								))}
							</ul>
						) : null}
						<div>
							Next sim:{" "}
							<b>
								{mpAutoPlay.nextRunAt !== undefined
									? `${new Date(mpAutoPlay.nextRunAt).toLocaleString()} (${formatCountdown(mpAutoPlay.nextRunAt - Date.now())})`
									: "paused"}
							</b>
						</div>
						<div className="form-text mb-0">
							Set by whoever's simming. Take over simming to change it.
						</div>
					</div>
				</div>
			) : null}

			<div className="d-flex flex-wrap align-items-center gap-2 mb-2">
				{settings.enabled ? (
					<button
						className="btn btn-danger"
						onClick={() => autoPlayScheduler.stop("Turned off")}
					>
						Stop
					</button>
				) : (
					<button
						className="btn btn-primary"
						disabled={!eligible}
						onClick={() => autoPlayScheduler.start()}
					>
						Start
					</button>
				)}
				<button
					className="btn btn-light-bordered"
					disabled={!eligible}
					onClick={() => void autoPlayScheduler.runNow("day")}
				>
					Sim day now
				</button>
			</div>
			{!eligible ? (
				<div className="text-body-secondary small mb-3">
					Requires cloud connection + sim control.
				</div>
			) : null}

			{eligible && hasEnabledRules ? (
				<div className="card mb-3" style={{ maxWidth: 760 }}>
					<div className="card-body py-2">
						<h3 className="card-title h6 mb-2">Upcoming sims</h3>
						{projected.length === 0 ? (
							<div className="text-body-secondary small mb-0">
								{preview && preview.upcomingDays.length === 0
									? "No scheduled games to auto-sim in this phase."
									: "No upcoming sims — check the schedule rules below."}
							</div>
						) : (
							<>
								<div className="table-responsive">
									<table className="table table-sm mb-0 align-middle">
										<thead>
											<tr>
												<th>When</th>
												<th>Sims</th>
												<th>League day</th>
												<th className="text-end">Games</th>
												<th>Notes</th>
											</tr>
										</thead>
										<tbody>
											{projected.slice(0, DISPLAY_FIRES).map((f, i) => (
												<tr key={i}>
													<td>
														<div>{new Date(f.at).toLocaleString()}</div>
														<div className="text-body-secondary small">
															in {formatCountdown(f.at - now)}
														</div>
													</td>
													<td className="text-nowrap">
														{f.numDays === 1 ? "1 day" : `${f.numDays} days`}
													</td>
													<td className="text-nowrap">
														{f.fromDay === f.toDay
															? `Day ${f.fromDay}`
															: `Days ${f.fromDay}–${f.toDay}`}
													</td>
													<td className="text-end">{f.numGames}</td>
													<td className="small">
														{f.events.map((e, j) => (
															<span
																key={j}
																className="badge text-bg-secondary me-1"
															>
																{e}
															</span>
														))}
													</td>
												</tr>
											))}
										</tbody>
									</table>
								</div>
								{projected.length > DISPLAY_FIRES ? (
									<div className="text-body-secondary small mt-2">
										…and {projected.length - DISPLAY_FIRES} more, through Day{" "}
										{projected.at(-1)!.toDay}
										{projected.at(-1)!.endsPhase && preview?.phaseEndNote
											? ` (${preview.phaseEndNote.toLowerCase()})`
											: ""}
										.
									</div>
								) : null}
							</>
						)}
					</div>
				</div>
			) : null}

			<div className="form-check">
				<input
					id="ap-pause"
					type="checkbox"
					className="form-check-input"
					checked={settings.pauseAtPhaseBoundaries}
					onChange={(e) => update({ pauseAtPhaseBoundaries: e.target.checked })}
				/>
				<label className="form-check-label" htmlFor="ap-pause">
					Pause at human-decision phases
				</label>
			</div>
			<div className="form-check mb-3">
				<input
					id="ap-awake"
					type="checkbox"
					className="form-check-input"
					checked={settings.keepAwake}
					onChange={(e) => update({ keepAwake: e.target.checked })}
				/>
				<label className="form-check-label" htmlFor="ap-awake">
					Keep screen awake
				</label>
			</div>

			{settings.rules.map((rule) => (
				<div
					key={rule.id}
					className="card mb-2"
					style={{ maxWidth: 760 }}
				>
					<div className="card-body py-2">
						<div className="d-flex flex-wrap align-items-center gap-2 mb-2">
							<div className="form-check mb-0">
								<input
									id={`en-${rule.id}`}
									type="checkbox"
									className="form-check-input"
									checked={rule.enabled}
									onChange={(e) => setRule(rule.id, { enabled: e.target.checked })}
								/>
								<label className="form-check-label" htmlFor={`en-${rule.id}`} />
							</div>

							<div className="btn-group btn-group-sm" role="group">
								{DOW.map((label, d) => {
									const on = rule.days.includes(d);
									return (
										<button
											key={d}
											type="button"
											className={`btn ${on ? "btn-primary" : "btn-light-bordered"}`}
											onClick={() =>
												setRule(rule.id, {
													days: on
														? rule.days.filter((x) => x !== d)
														: [...rule.days, d].sort((a, b) => a - b),
												})
											}
										>
											{label}
										</button>
									);
								})}
							</div>

							<select
								className="form-select form-select-sm"
								style={{ width: "auto" }}
								value={rule.mode}
								onChange={(e) =>
									setRule(rule.id, { mode: e.target.value as ScheduleRule["mode"] })
								}
							>
								<option value="every">Every</option>
								<option value="at">At times</option>
							</select>

							<select
								className="form-select form-select-sm"
								style={{ width: "auto" }}
								value={rule.amount}
								onChange={(e) =>
									setRule(rule.id, { amount: e.target.value as AutoPlayAmount })
								}
							>
								<option value="day">day</option>
								<option value="week">week</option>
								<option value="month">month</option>
							</select>

							<button
								type="button"
								className="btn btn-sm btn-light-bordered ms-auto"
								onClick={() => removeRule(rule.id)}
								title="Remove rule"
							>
								×
							</button>
						</div>

						{rule.mode === "every" ? (
							<div className="d-flex flex-wrap align-items-center gap-2">
								<input
									type="number"
									min={1}
									className="form-control form-control-sm"
									style={{ width: 80 }}
									value={rule.everyMinutes}
									onChange={(e) => {
										const v = Number.parseInt(e.target.value);
										setRule(rule.id, {
											everyMinutes: Number.isNaN(v) || v < 1 ? 1 : v,
										});
									}}
								/>
								<span className="text-body-secondary">min, between</span>
								<input
									type="time"
									className="form-control form-control-sm"
									style={{ width: "auto" }}
									value={rule.start}
									onChange={(e) => setRule(rule.id, { start: e.target.value })}
								/>
								<span className="text-body-secondary">and</span>
								<input
									type="time"
									className="form-control form-control-sm"
									style={{ width: "auto" }}
									value={rule.end}
									onChange={(e) => setRule(rule.id, { end: e.target.value })}
								/>
							</div>
						) : (
							<div className="d-flex flex-wrap align-items-center gap-2">
								{rule.times.map((t, i) => (
									<div
										key={i}
										className="d-flex align-items-center gap-1"
									>
										<input
											type="time"
											className="form-control form-control-sm"
											style={{ width: "auto" }}
											value={t}
											onChange={(e) => {
												const times = [...rule.times];
												times[i] = e.target.value;
												setRule(rule.id, { times });
											}}
										/>
										<button
											type="button"
											className="btn btn-sm btn-light-bordered"
											onClick={() =>
												setRule(rule.id, {
													times: rule.times.filter((_, x) => x !== i),
												})
											}
											title="Remove time"
										>
											×
										</button>
									</div>
								))}
								<button
									type="button"
									className="btn btn-sm btn-light-bordered"
									onClick={() =>
										setRule(rule.id, { times: [...rule.times, "12:00"] })
									}
								>
									+ time
								</button>
							</div>
						)}
					</div>
				</div>
			))}

			<button className="btn btn-light-bordered btn-sm mb-3" onClick={addRule}>
				+ Add rule
			</button>

			<div className="card" style={{ maxWidth: 760 }}>
				<div className="card-body py-2">
					<table className="table table-nonfluid table-sm mb-0">
						<tbody>
							<tr>
								<th>State</th>
								<td>
									{settings.enabled ? (
										state.running ? (
											<span className="text-success">Running</span>
										) : (
											<span className="text-warning">Paused</span>
										)
									) : (
										<span className="text-danger">Stopped</span>
									)}
								</td>
							</tr>
							<tr>
								<th>Next sim</th>
								<td>
									{state.running && state.nextRunAt !== undefined
										? `${formatTime(state.nextRunAt)}${
												countdownMs !== undefined
													? ` (${formatCountdown(countdownMs)})`
													: ""
											}`
										: "-"}
								</td>
							</tr>
							<tr>
								<th>Last sim</th>
								<td>{formatTime(state.lastRunAt)}</td>
							</tr>
							<tr>
								<th>Sims run</th>
								<td>{state.runCount}</td>
							</tr>
							<tr>
								<th>Phase</th>
								<td>{phaseText || "-"}</td>
							</tr>
						</tbody>
					</table>
					{state.pausedReason ? (
						<div className="alert alert-warning mt-2 mb-0 py-2 small">
							{state.pausedReason}
						</div>
					) : null}
				</div>
			</div>
		</>
	);
};

export default AutoPlaySchedule;
