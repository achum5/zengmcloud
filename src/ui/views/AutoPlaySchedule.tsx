import { useEffect, useMemo, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { useLocal } from "../util/local.ts";
import { toWorker } from "../util/toWorker.ts";
import {
	autoPlayScheduler,
	describeAmount,
	newRule,
	nextFireForRule,
	summarizeRule,
	type AutoPlayAmount,
	type AutoPlayLogEntry,
	type AutoPlaySettings,
	type AutoPlayState,
	type ScheduleRule,
} from "../util/autoPlayScheduler.ts";
import {
	nextFires,
	projectFires,
	type AutoPlayPreviewData,
	type ProjectedFire,
} from "../util/autoPlayPreview.ts";

// How many upcoming sims to compute, and the minimum to show before
// summarizing. Everything firing before local midnight is always shown, so the
// list covers the rest of today and its last row is today's final sim.
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

const formatClock = (ts: number) =>
	new Date(ts).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });

// "Day 41" / "Days 41–43"
const dayRange = (from: number, to: number) =>
	from === to ? `Day ${from}` : `Days ${from}–${to}`;

// A "YYYY-MM-DDTHH:MM" value for <input type="datetime-local">, in local time.
const toLocalInput = (ts: number) => {
	const d = new Date(ts - new Date(ts).getTimezoneOffset() * 60_000);
	return d.toISOString().slice(0, 16);
};

// One line of the readiness checklist. Green when satisfied, muted when not -
// this is the "why isn't it simming" answer, so every requirement is listed
// whether or not it's met.
const Check = ({
	ok,
	label,
	detail,
}: {
	ok: boolean;
	label: string;
	detail?: string;
}) => (
	<div className="d-flex align-items-baseline gap-2">
		<span className={ok ? "text-success" : "text-body-secondary"}>
			{ok ? "✓" : "○"}
		</span>
		<span className={ok ? undefined : "text-body-secondary"}>{label}</span>
		{detail ? (
			<span className="text-body-secondary small">{detail}</span>
		) : null}
	</div>
);

const LogTable = ({
	log,
	onClear,
}: {
	log: AutoPlayLogEntry[];
	onClear: () => void;
}) => (
	<>
		<div className="table-responsive">
			<table className="table table-sm mb-0 align-middle">
				<thead>
					<tr>
						<th>When</th>
						<th>Simmed</th>
						<th>League days</th>
						<th className="text-end">Games</th>
						<th />
					</tr>
				</thead>
				<tbody>
					{log.map((entry, i) => (
						<tr key={i} className={entry.error ? "table-danger" : undefined}>
							<td className="text-nowrap">
								<div>{formatClock(entry.at)}</div>
								<div className="text-body-secondary small">
									{new Date(entry.at).toLocaleDateString()}
								</div>
							</td>
							<td className="text-nowrap">{describeAmount(entry)}</td>
							<td className="text-nowrap">
								{entry.error ? (
									<span className="text-danger">Failed</span>
								) : entry.fromDay !== undefined && entry.toDay !== undefined ? (
									dayRange(entry.fromDay, entry.toDay)
								) : (
									<span className="text-body-secondary">-</span>
								)}
							</td>
							<td className="text-end">
								{entry.numGames ?? (
									<span className="text-body-secondary">-</span>
								)}
							</td>
							<td className="small text-body-secondary">
								{entry.error ?? (entry.auto ? "" : "manual")}
							</td>
						</tr>
					))}
				</tbody>
			</table>
		</div>
		<button
			type="button"
			className="btn btn-sm btn-light-bordered mt-2"
			onClick={onClear}
		>
			Clear log
		</button>
	</>
);

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
	// to the cloud AND being in charge of simming.
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

	// Show at least DISPLAY_FIRES rows, and never cut off mid-day: every fire
	// before local midnight stays visible.
	const shownFires = useMemo(() => {
		const endOfToday = new Date(now);
		endOfToday.setHours(24, 0, 0, 0);
		const todayCount = projected.filter(
			(f) => f.at < endOfToday.getTime(),
		).length;
		return projected.slice(0, Math.max(DISPLAY_FIRES, todayCount));
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [projected, nowBucket]);

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
	const duplicateRule = (rule: ScheduleRule) =>
		autoPlayScheduler.setRules([
			...settings.rules,
			{ ...rule, id: newRule().id },
		]);

	const countdownMs =
		state.nextRunAt !== undefined ? state.nextRunAt - now : undefined;

	const hasEnabledRules = settings.rules.some((r) => r.enabled);

	// Show the schedule preview when running, or on demand via "Show schedule".
	const [showSchedule, setShowSchedule] = useState(false);
	const showPreview =
		eligible && hasEnabledRules && (settings.enabled || showSchedule);

	// The active stop-after day, if it belongs to the season currently previewed.
	const stopDay =
		settings.stopAfter &&
		preview &&
		settings.stopAfter.season === preview.season
			? settings.stopAfter.day
			: undefined;
	const setStopAfter = (day: number) => {
		if (preview) {
			update({ stopAfter: { season: preview.season, day } });
		}
	};
	const clearStopAfter = () => update({ stopAfter: undefined });

	// What the very next sim will actually do, for the header.
	const nextUp: ProjectedFire | undefined = projected[0];

	// Totals for the rest of today, so the header answers "how far do I get
	// today" without reading the table.
	const todayTotals = useMemo(() => {
		const endOfToday = new Date(now);
		endOfToday.setHours(24, 0, 0, 0);
		const today = projected.filter((f) => f.at < endOfToday.getTime());
		if (today.length === 0) {
			return undefined;
		}
		return {
			sims: today.length,
			throughDay: today.at(-1)!.toDay,
			games: today.reduce((sum, f) => sum + f.numGames, 0),
		};
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [projected, nowBucket]);

	const statePill = settings.enabled ? (
		state.running ? (
			<span className="badge text-bg-success">Running</span>
		) : (
			<span className="badge text-bg-warning">Paused</span>
		)
	) : (
		<span className="badge text-bg-secondary">Stopped</span>
	);

	// Anything that will halt auto play on its own, as one readable list.
	const stopLines: string[] = [];
	if (settings.stopAfter) {
		stopLines.push(`after Day ${settings.stopAfter.day}`);
	}
	if (settings.stopAfterSims !== undefined) {
		stopLines.push(
			`after ${settings.stopAfterSims} more sim${settings.stopAfterSims === 1 ? "" : "s"}`,
		);
	}
	if (settings.stopAt !== undefined) {
		stopLines.push(`at ${formatTime(settings.stopAt)}`);
	}
	if (settings.stopAtPhaseEnd) {
		stopLines.push("when the phase ends");
	}

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
									? `${new Date(mpAutoPlay.nextRunAt).toLocaleString()} (${formatCountdown(mpAutoPlay.nextRunAt - now)})`
									: "paused"}
							</b>
						</div>
						<div className="form-text mb-0">
							Set by whoever's simming. Take over simming to change it.
						</div>
					</div>
				</div>
			) : null}

			{/* Status header: state, the countdown, and what the next sim does. */}
			<div className="card mb-3" style={{ maxWidth: 820 }}>
				<div className="card-body">
					<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
						<div>
							<div className="d-flex align-items-center gap-2">
								{statePill}
								<span className="text-body-secondary small">
									{phaseText || "-"}
								</span>
							</div>
							<div className="fs-1 lh-1 mt-1 font-monospace">
								{state.running && countdownMs !== undefined
									? formatCountdown(countdownMs)
									: "—"}
							</div>
							<div className="text-body-secondary small">
								{state.running && state.nextRunAt !== undefined
									? `next sim ${formatTime(state.nextRunAt)}`
									: settings.enabled
										? (state.pausedReason ?? "waiting")
										: "not running"}
							</div>
						</div>

						<div className="ms-auto text-end">
							{nextUp ? (
								<>
									<div className="h5 mb-0">
										{dayRange(nextUp.fromDay, nextUp.toDay)}
									</div>
									<div className="text-body-secondary small">
										{describeAmount(nextUp)} · {nextUp.numGames} games
									</div>
								</>
							) : null}
							{todayTotals ? (
								<div className="text-body-secondary small mt-1">
									{todayTotals.sims} sims left today → Day{" "}
									{todayTotals.throughDay}
								</div>
							) : null}
						</div>
					</div>

					<div className="d-flex flex-wrap align-items-center gap-2">
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
								onClick={() => {
									setShowSchedule(true);
									autoPlayScheduler.start();
								}}
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
						<button
							className="btn btn-light-bordered"
							disabled={!eligible}
							onClick={() => void autoPlayScheduler.runNow("week")}
						>
							Sim week now
						</button>
						{eligible && hasEnabledRules && !settings.enabled ? (
							<button
								className="btn btn-light-bordered ms-auto"
								onClick={() => setShowSchedule((v) => !v)}
							>
								{showSchedule ? "Hide schedule" : "Show schedule"}
							</button>
						) : null}
					</div>

					<hr className="my-3" />

					<div className="row row-cols-1 row-cols-sm-2 g-1 small">
						<div>
							<Check ok={!!mpSyncActive} label="Cloud connected" />
						</div>
						<div>
							<Check ok={!!mpSyncIsHost} label="In charge of simming" />
						</div>
						<div>
							<Check
								ok={state.isDriver}
								label="This tab is driving"
								detail={
									settings.enabled && !state.isDriver
										? "another tab holds it"
										: undefined
								}
							/>
						</div>
						<div>
							<Check
								ok={state.wakeLockHeld}
								label="Screen kept awake"
								detail={settings.keepAwake ? undefined : "off"}
							/>
						</div>
					</div>

					{stopLines.length > 0 ? (
						<div className="alert alert-info py-2 mt-3 mb-0 small d-flex align-items-center gap-2">
							<div className="flex-grow-1">
								Will stop {stopLines.join(", or ")}.
							</div>
							<button
								type="button"
								className="btn btn-sm btn-light-bordered"
								onClick={() =>
									update({
										stopAfter: undefined,
										stopAfterSims: undefined,
										stopAt: undefined,
										stopAtPhaseEnd: false,
									})
								}
							>
								Clear
							</button>
						</div>
					) : null}

					{state.pausedReason && !state.running ? (
						<div className="alert alert-warning py-2 mt-3 mb-0 small">
							{state.pausedReason}
						</div>
					) : null}
				</div>
			</div>

			{showPreview ? (
				<div className="card mb-3" style={{ maxWidth: 820 }}>
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
												<th />
											</tr>
										</thead>
										<tbody>
											{shownFires.map((f, i) => {
												const isStop =
													stopDay !== undefined && f.toDay === stopDay;
												const afterStop =
													stopDay !== undefined && f.fromDay > stopDay;
												return (
													<tr
														key={i}
														className={afterStop ? "opacity-50" : undefined}
													>
														<td>
															<div>{new Date(f.at).toLocaleString()}</div>
															<div className="text-body-secondary small">
																in {formatCountdown(f.at - now)}
															</div>
														</td>
														<td className="text-nowrap">
															{f.numLeagueDays === 1
																? "1 day"
																: `${f.numLeagueDays} days`}
															{f.label ? (
																<div className="text-body-secondary small">
																	{f.label}
																</div>
															) : null}
														</td>
														<td className="text-nowrap">
															{dayRange(f.fromDay, f.toDay)}
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
															{afterStop ? (
																<span className="text-body-secondary">
																	won't run
																</span>
															) : null}
														</td>
														<td className="text-end">
															{isStop ? (
																<button
																	type="button"
																	className="btn btn-sm btn-info text-nowrap"
																	onClick={clearStopAfter}
																	title="Don't stop here"
																>
																	Stops here
																</button>
															) : (
																<button
																	type="button"
																	className="btn btn-sm btn-light-bordered text-nowrap"
																	onClick={() => setStopAfter(f.toDay)}
																	title="Stop auto play after this sim"
																>
																	Stop after
																</button>
															)}
														</td>
													</tr>
												);
											})}
										</tbody>
									</table>
								</div>
								<div className="text-body-secondary small mt-2">
									{projected.length} sims scheduled ·{" "}
									{projected.reduce((sum, f) => sum + f.numGames, 0)} games ·
									through Day {projected.at(-1)!.toDay}
									{projected.at(-1)!.endsPhase && preview?.phaseEndNote
										? ` (${preview.phaseEndNote.toLowerCase()})`
										: ""}
									{projected.length > shownFires.length
										? ` · showing the next ${shownFires.length}`
										: ""}
									.
								</div>
							</>
						)}
					</div>
				</div>
			) : null}

			{/* Rules */}
			<h3 className="h6">Schedule rules</h3>
			{settings.rules.map((rule) => {
				const fireAt = nextFireForRule(rule, new Date(now));
				return (
					<div key={rule.id} className="card mb-2" style={{ maxWidth: 820 }}>
						<div className="card-body py-2">
							<div className="d-flex flex-wrap align-items-center gap-2 mb-2">
								<div className="form-check mb-0">
									<input
										id={`en-${rule.id}`}
										type="checkbox"
										className="form-check-input"
										checked={rule.enabled}
										onChange={(e) =>
											setRule(rule.id, { enabled: e.target.checked })
										}
									/>
									<label
										className="form-check-label"
										htmlFor={`en-${rule.id}`}
									/>
								</div>

								<input
									type="text"
									className="form-control form-control-sm"
									style={{ width: 150 }}
									placeholder="Name (optional)"
									value={rule.label ?? ""}
									onChange={(e) => setRule(rule.id, { label: e.target.value })}
								/>

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

								<div className="ms-auto d-flex gap-1">
									<button
										type="button"
										className="btn btn-sm btn-light-bordered"
										onClick={() => duplicateRule(rule)}
										title="Duplicate rule"
									>
										⧉
									</button>
									<button
										type="button"
										className="btn btn-sm btn-light-bordered"
										onClick={() => removeRule(rule.id)}
										title="Remove rule"
									>
										×
									</button>
								</div>
							</div>

							<div className="d-flex flex-wrap align-items-center gap-2">
								<select
									className="form-select form-select-sm"
									style={{ width: "auto" }}
									value={rule.mode}
									onChange={(e) =>
										setRule(rule.id, {
											mode: e.target.value as ScheduleRule["mode"],
										})
									}
								>
									<option value="every">Every</option>
									<option value="at">At times</option>
								</select>

								{rule.mode === "every" ? (
									<>
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
											onChange={(e) =>
												setRule(rule.id, { start: e.target.value })
											}
										/>
										<span className="text-body-secondary">and</span>
										<input
											type="time"
											className="form-control form-control-sm"
											style={{ width: "auto" }}
											value={rule.end}
											onChange={(e) =>
												setRule(rule.id, { end: e.target.value })
											}
										/>
									</>
								) : (
									<>
										{rule.times.map((t, i) => (
											<div key={i} className="d-flex align-items-center gap-1">
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
									</>
								)}
							</div>

							<div className="d-flex flex-wrap align-items-center gap-2 mt-2">
								<span className="text-body-secondary">Sim</span>
								<select
									className="form-select form-select-sm"
									style={{ width: "auto" }}
									value={rule.amount}
									onChange={(e) =>
										setRule(rule.id, {
											amount: e.target.value as AutoPlayAmount,
										})
									}
								>
									<option value="day">1 day</option>
									<option value="week">week</option>
									<option value="month">month</option>
									<option value="days">custom…</option>
								</select>
								{rule.amount === "days" ? (
									<>
										<input
											type="number"
											min={1}
											max={365}
											className="form-control form-control-sm"
											style={{ width: 80 }}
											value={rule.numDays}
											onChange={(e) => {
												const v = Number.parseInt(e.target.value);
												setRule(rule.id, {
													numDays: Number.isNaN(v) || v < 1 ? 1 : v,
												});
											}}
										/>
										<span className="text-body-secondary">days</span>
									</>
								) : null}
							</div>

							<div className="text-body-secondary small mt-2">
								{summarizeRule(rule)}
								{rule.enabled && fireAt !== undefined
									? ` · next ${formatTime(fireAt)} (${formatCountdown(fireAt - now)})`
									: rule.enabled
										? " · never fires"
										: " · disabled"}
							</div>
						</div>
					</div>
				);
			})}

			<button className="btn btn-light-bordered btn-sm mb-3" onClick={addRule}>
				+ Add rule
			</button>

			{/* Stop conditions */}
			<div className="card mb-3" style={{ maxWidth: 820 }}>
				<div className="card-body py-2">
					<h3 className="card-title h6 mb-2">Stop conditions</h3>

					<div className="d-flex flex-wrap align-items-center gap-2 mb-2">
						<div className="form-check mb-0">
							<input
								id="ap-stop-sims"
								type="checkbox"
								className="form-check-input"
								checked={settings.stopAfterSims !== undefined}
								onChange={(e) =>
									update({ stopAfterSims: e.target.checked ? 10 : undefined })
								}
							/>
							<label className="form-check-label" htmlFor="ap-stop-sims">
								Stop after
							</label>
						</div>
						<input
							type="number"
							min={1}
							className="form-control form-control-sm"
							style={{ width: 90 }}
							disabled={settings.stopAfterSims === undefined}
							value={settings.stopAfterSims ?? 10}
							onChange={(e) => {
								const v = Number.parseInt(e.target.value);
								update({ stopAfterSims: Number.isNaN(v) || v < 1 ? 1 : v });
							}}
						/>
						<span className="text-body-secondary">more sims</span>
					</div>

					<div className="d-flex flex-wrap align-items-center gap-2 mb-2">
						<div className="form-check mb-0">
							<input
								id="ap-stop-at"
								type="checkbox"
								className="form-check-input"
								checked={settings.stopAt !== undefined}
								onChange={(e) =>
									update({
										stopAt: e.target.checked
											? Date.now() + 8 * 3600_000
											: undefined,
									})
								}
							/>
							<label className="form-check-label" htmlFor="ap-stop-at">
								Stop at
							</label>
						</div>
						<input
							type="datetime-local"
							className="form-control form-control-sm"
							style={{ width: "auto" }}
							disabled={settings.stopAt === undefined}
							value={
								settings.stopAt === undefined
									? ""
									: toLocalInput(settings.stopAt)
							}
							onChange={(e) => {
								const ts = new Date(e.target.value).getTime();
								if (!Number.isNaN(ts)) {
									update({ stopAt: ts });
								}
							}}
						/>
					</div>

					<div className="form-check mb-2">
						<input
							id="ap-stop-phase"
							type="checkbox"
							className="form-check-input"
							checked={!!settings.stopAtPhaseEnd}
							onChange={(e) => update({ stopAtPhaseEnd: e.target.checked })}
						/>
						<label className="form-check-label" htmlFor="ap-stop-phase">
							Stop when this phase's schedule runs out
						</label>
					</div>

					{settings.stopAfter ? (
						<div className="d-flex align-items-center gap-2">
							<span>
								Stop after <b>Day {settings.stopAfter.day}</b> (
								{settings.stopAfter.season})
							</span>
							<button
								type="button"
								className="btn btn-sm btn-light-bordered"
								onClick={clearStopAfter}
							>
								Clear
							</button>
						</div>
					) : (
						<div className="text-body-secondary small">
							Pick a day from the Upcoming sims table to stop after it.
						</div>
					)}
				</div>
			</div>

			{/* Options */}
			<div className="card mb-3" style={{ maxWidth: 820 }}>
				<div className="card-body py-2">
					<h3 className="card-title h6 mb-2">Options</h3>
					<div className="form-check">
						<input
							id="ap-pause"
							type="checkbox"
							className="form-check-input"
							checked={settings.pauseAtPhaseBoundaries}
							onChange={(e) =>
								update({ pauseAtPhaseBoundaries: e.target.checked })
							}
						/>
						<label className="form-check-label" htmlFor="ap-pause">
							Pause at human-decision phases
						</label>
					</div>
					<div className="form-check">
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
					<div className="d-flex align-items-center gap-2 mt-2">
						<label className="form-label mb-0" htmlFor="ap-log-limit">
							Keep
						</label>
						<input
							id="ap-log-limit"
							type="number"
							min={1}
							max={500}
							className="form-control form-control-sm"
							style={{ width: 90 }}
							value={settings.logLimit}
							onChange={(e) => {
								const v = Number.parseInt(e.target.value);
								update({ logLimit: Number.isNaN(v) || v < 1 ? 1 : v });
							}}
						/>
						<span className="text-body-secondary">sims in the log</span>
					</div>
				</div>
			</div>

			{/* Run log */}
			<div className="card mb-3" style={{ maxWidth: 820 }}>
				<div className="card-body py-2">
					<h3 className="card-title h6 mb-2">
						Recent sims{" "}
						<span className="text-body-secondary fw-normal">
							({state.runCount} this session
							{state.lastRunAt !== undefined
								? `, last ${formatTime(state.lastRunAt)}`
								: ""}
							)
						</span>
					</h3>
					{state.log.length === 0 ? (
						<div className="text-body-secondary small mb-0">
							Nothing simmed yet.
						</div>
					) : (
						<LogTable
							log={state.log}
							onClear={() => autoPlayScheduler.clearLog()}
						/>
					)}
				</div>
			</div>
		</>
	);
};

export default AutoPlaySchedule;
