import { useEffect, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { useLocal } from "../util/local.ts";
import {
	autoPlayScheduler,
	type AutoPlayAmount,
	type AutoPlaySettings,
	type AutoPlayState,
} from "../util/autoPlayScheduler.ts";

const formatCountdown = (ms: number) => {
	if (ms <= 0) {
		return "now";
	}
	const totalSeconds = Math.round(ms / 1000);
	const m = Math.floor(totalSeconds / 60);
	const s = totalSeconds % 60;
	return `${m}:${s.toString().padStart(2, "0")}`;
};

const formatTime = (ts: number | undefined) => {
	if (ts === undefined) {
		return "Never";
	}
	return new Date(ts).toLocaleTimeString();
};

const AutoPlaySchedule = () => {
	useTitleBar({ title: "Auto Play Scheduler" });

	const { lid, phaseText } = useLocal(["lid", "phaseText"]);

	const [settings, setSettings] = useState<AutoPlaySettings>(
		autoPlayScheduler.settings,
	);
	const [state, setState] = useState<AutoPlayState>(autoPlayScheduler.state);

	// Load this league's saved settings and subscribe to scheduler updates.
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

	// Tick a local clock once per second so the countdown stays live.
	const [, setNow] = useState(0);
	useEffect(() => {
		const id = setInterval(() => {
			setNow((n) => n + 1);
		}, 1000);
		return () => {
			clearInterval(id);
		};
	}, []);

	const update = (partial: Partial<AutoPlaySettings>) => {
		autoPlayScheduler.updateSettings(partial);
	};

	const countdownMs =
		state.nextRunAt !== undefined ? state.nextRunAt - Date.now() : undefined;

	return (
		<>
			<p>
				Automatically sim on a schedule - one step every so many minutes - so
				your league keeps moving without anyone clicking Play. This runs in this
				browser tab, so keep it open on a device that stays awake.
			</p>

			<div className="row" style={{ maxWidth: 700 }}>
				<div className="col-sm-6 mb-3">
					<label className="form-label" htmlFor="autoplay-interval">
						Sim every (minutes)
					</label>
					<input
						id="autoplay-interval"
						type="number"
						min={1}
						className="form-control"
						value={settings.intervalMinutes}
						onChange={(event) => {
							const value = Number.parseInt(event.target.value);
							update({
								intervalMinutes: Number.isNaN(value) || value < 1 ? 1 : value,
							});
						}}
					/>
				</div>

				<div className="col-sm-6 mb-3">
					<label className="form-label" htmlFor="autoplay-amount">
						How much to sim each time
					</label>
					<select
						id="autoplay-amount"
						className="form-select"
						value={settings.amount}
						onChange={(event) => {
							update({ amount: event.target.value as AutoPlayAmount });
						}}
					>
						<option value="day">One day</option>
						<option value="week">One week</option>
						<option value="month">One month</option>
					</select>
				</div>
			</div>

			<div className="form-check mb-2">
				<input
					id="autoplay-pause-phase"
					type="checkbox"
					className="form-check-input"
					checked={settings.pauseAtPhaseBoundaries}
					onChange={(event) => {
						update({ pauseAtPhaseBoundaries: event.target.checked });
					}}
				/>
				<label className="form-check-label" htmlFor="autoplay-pause-phase">
					Pause when a human decision is needed (draft, re-signing, etc.)
				</label>
			</div>

			<div className="form-check mb-3">
				<input
					id="autoplay-keep-awake"
					type="checkbox"
					className="form-check-input"
					checked={settings.keepAwake}
					onChange={(event) => {
						update({ keepAwake: event.target.checked });
					}}
				/>
				<label className="form-check-label" htmlFor="autoplay-keep-awake">
					Try to keep the screen awake while running
				</label>
			</div>

			<div className="d-flex gap-2 mb-3">
				{state.running ? (
					<button
						className="btn btn-danger"
						onClick={() => {
							autoPlayScheduler.stop("Turned off");
						}}
					>
						Stop auto play
					</button>
				) : (
					<button
						className="btn btn-primary"
						onClick={() => {
							autoPlayScheduler.start();
						}}
					>
						Start auto play
					</button>
				)}
				<button
					className="btn btn-light-bordered"
					onClick={() => {
						void autoPlayScheduler.runNow();
					}}
				>
					Sim now
				</button>
			</div>

			<div className="card" style={{ maxWidth: 700 }}>
				<div className="card-body">
					<h3 className="card-title h5">Status</h3>
					<table className="table table-nonfluid mb-0">
						<tbody>
							<tr>
								<th>State</th>
								<td>
									{state.running ? (
										<span className="text-success">Running</span>
									) : (
										<span className="text-danger">Stopped</span>
									)}
								</td>
							</tr>
							<tr>
								<th>Next sim in</th>
								<td>
									{state.running && countdownMs !== undefined
										? formatCountdown(countdownMs)
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
								<th>Current phase</th>
								<td>{phaseText || "-"}</td>
							</tr>
						</tbody>
					</table>
					{state.pausedReason ? (
						<div className="alert alert-warning mt-3 mb-0">
							{state.pausedReason}
						</div>
					) : null}
				</div>
			</div>
		</>
	);
};

export default AutoPlaySchedule;
