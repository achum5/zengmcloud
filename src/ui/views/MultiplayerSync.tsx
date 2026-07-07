import { useEffect, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { useLocal } from "../util/local.ts";
import { toWorker } from "../util/toWorker.ts";
import {
	clearStoredSync,
	getStoredSync,
	setStoredSync,
} from "../util/autoReconnectSync.ts";
import {
	enablePushNotifications,
	getPushPermission,
	getStoredPushName,
	pushConfigured,
	pushSupported,
	restorePushNotifications,
} from "../util/pushNotifications.ts";

type Status = "disconnected" | "connecting" | "connected";

const MultiplayerSync = () => {
	useTitleBar({ title: "Multiplayer Sync (Beta)" });

	const { lid, mpSyncIsHost, mpSyncHostName } = useLocal([
		"lid",
		"mpSyncIsHost",
		"mpSyncHostName",
	]);

	const [code, setCode] = useState("");
	const [isHost, setIsHost] = useState(false);
	const [status, setStatus] = useState<Status>("disconnected");
	const [error, setError] = useState<string | undefined>();
	const [takingWheel, setTakingWheel] = useState(false);

	const [teams, setTeams] = useState<
		{ tid: number; region: string; name: string }[]
	>([]);
	const [userTid, setUserTid] = useState<number | undefined>();
	const [multiTeamMode, setMultiTeamMode] = useState(true);

	// Phone push notifications.
	const [pushName, setPushName] = useState(getStoredPushName());
	const [pushSupport, setPushSupport] = useState(true);
	const [pushPermission, setPushPermission] =
		useState<NotificationPermission>(getPushPermission());
	const [pushBusy, setPushBusy] = useState(false);
	const [pushError, setPushError] = useState<string | undefined>();

	// On mount, reflect whatever the worker's sync engine is currently doing
	// (it may already be connected from an auto-reconnect after refresh), and
	// load the multi-team-mode teams for the team picker.
	useEffect(() => {
		let cancelled = false;
		(async () => {
			const [workerStatus, syncTeams] = await Promise.all([
				toWorker("main", "getSyncStatus", undefined),
				toWorker("main", "getSyncTeams", undefined),
			]);
			if (cancelled) {
				return;
			}
			setTeams(syncTeams.teams);
			setUserTid(syncTeams.userTid);
			setMultiTeamMode(syncTeams.multiTeamMode);
			if (workerStatus.connected) {
				setCode(workerStatus.code ?? "");
				setIsHost(!!workerStatus.isHost);
				setStatus("connected");
			} else if (typeof lid === "number") {
				const stored = getStoredSync(lid);
				if (stored) {
					setCode(stored.code);
					setIsHost(stored.isHost);
				}
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [lid]);

	// Detect push support once, and silently re-assert the token after a refresh
	// if the user had already enabled it.
	useEffect(() => {
		let cancelled = false;
		(async () => {
			const supported = await pushSupported();
			if (cancelled) {
				return;
			}
			setPushSupport(supported);
			if (supported) {
				await restorePushNotifications();
				if (!cancelled) {
					setPushPermission(getPushPermission());
				}
			}
		})();
		return () => {
			cancelled = true;
		};
	}, []);

	const switchTeam = async (tid: number) => {
		await toWorker("main", "updateGameAttributes", { userTid: tid });
		setUserTid(tid);
	};

	const enablePush = async () => {
		setPushError(undefined);
		setPushBusy(true);
		try {
			await enablePushNotifications(pushName.trim() || "A league-mate");
			setPushPermission(getPushPermission());
		} catch (err) {
			setPushError((err as Error).message ?? String(err));
		} finally {
			setPushBusy(false);
		}
	};

	const connect = async () => {
		if (typeof lid !== "number") {
			return;
		}
		setError(undefined);
		setStatus("connecting");
		try {
			await toWorker("main", "connectSharedLeague", { code, isHost });
			setStoredSync(lid, { code, isHost });
			setStatus("connected");
		} catch (err) {
			setError((err as Error).message ?? String(err));
			setStatus("disconnected");
		}
	};

	const disconnect = async () => {
		await toWorker("main", "disconnectSharedLeague", undefined);
		if (typeof lid === "number") {
			clearStoredSync(lid);
		}
		setStatus("disconnected");
	};

	const takeWheel = async () => {
		setTakingWheel(true);
		try {
			await toWorker("main", "claimSyncAuthority", undefined);
		} finally {
			setTakingWheel(false);
		}
	};

	const connected = status === "connected";

	return (
		<>
			<p className="text-body-secondary">
				Everyone loads the same league file and connects with the same code.
				Only the device holding <b>the wheel</b> can sim — tap{" "}
				<b>Take the wheel</b> to move it here.
			</p>

			<div className="row" style={{ maxWidth: 500 }}>
				<div className="col-12 mb-3">
					<label className="form-label" htmlFor="sync-team">
						Your team
					</label>
					{multiTeamMode ? (
						<>
							<select
								id="sync-team"
								className="form-select"
								value={userTid ?? ""}
								onChange={(event) => {
									void switchTeam(Number.parseInt(event.target.value));
								}}
							>
								{teams.map((t) => (
									<option key={t.tid} value={t.tid}>
										{t.region} {t.name}
									</option>
								))}
							</select>
							<div className="form-text">Only affects this device.</div>
						</>
					) : (
						<div className="alert alert-warning mb-0">
							Enable <b>Multi Team Mode</b> (Tools → Multi Team Mode) with each
							team first, then pick yours here.
						</div>
					)}
				</div>
			</div>

			<div className="row" style={{ maxWidth: 500 }}>
				<div className="col-12 mb-3">
					<label className="form-label" htmlFor="sync-code">
						League code
					</label>
					<input
						id="sync-code"
						type="text"
						className="form-control"
						placeholder="e.g. smith-dynasty"
						value={code}
						disabled={connected || status === "connecting"}
						onChange={(event) => setCode(event.target.value)}
					/>
				</div>
			</div>

			<div className="form-check mb-3">
				<input
					id="sync-host"
					type="checkbox"
					className="form-check-input"
					checked={isHost}
					disabled={connected || status === "connecting"}
					onChange={(event) => setIsHost(event.target.checked)}
				/>
				<label className="form-check-label" htmlFor="sync-host">
					Take the wheel on connect
				</label>
			</div>

			<div className="d-flex gap-2 mb-3">
				{connected ? (
					<button className="btn btn-danger" onClick={disconnect}>
						Disconnect
					</button>
				) : (
					<button
						className="btn btn-primary"
						disabled={status === "connecting" || code.trim() === ""}
						onClick={connect}
					>
						{status === "connecting" ? "Connecting…" : "Connect"}
					</button>
				)}
			</div>

			<div className="card" style={{ maxWidth: 500 }}>
				<div className="card-body">
					<h3 className="card-title h5">Status</h3>
					{connected ? (
						<>
							<p className="text-success mb-2">
								Connected to <b>{code.trim()}</b>.
							</p>
							<div className="d-flex align-items-center gap-2 flex-wrap">
								<span>
									{mpSyncIsHost ? (
										<span className="text-success">
											🎮 <b>You have the wheel</b>
										</span>
									) : mpSyncHostName ? (
										<span className="text-body-secondary">
											🔒 <b>{mpSyncHostName}</b> has the wheel
										</span>
									) : (
										<span className="text-body-secondary">
											Nobody has the wheel yet
										</span>
									)}
								</span>
								{!mpSyncIsHost ? (
									<button
										className="btn btn-primary btn-sm"
										disabled={takingWheel}
										onClick={takeWheel}
									>
										{takingWheel ? "Taking…" : "Take the wheel"}
									</button>
								) : null}
							</div>
						</>
					) : status === "connecting" ? (
						<p className="mb-0">Connecting…</p>
					) : (
						<p className="text-body-secondary mb-0">Not connected.</p>
					)}
					{error ? (
						<div className="alert alert-danger mt-3 mb-0">{error}</div>
					) : null}
				</div>
			</div>

			<div className="card mt-3" style={{ maxWidth: 500 }}>
				<div className="card-body">
					<h3 className="card-title h5">Phone notifications</h3>
					<p className="text-body-secondary">
						Push alerts to your phone when the app is closed.
					</p>

					{!pushConfigured() ? (
						<div className="alert alert-warning mb-0">
							Push notifications aren't set up on the server yet. See{" "}
							<code>docs/PUSH_NOTIFICATIONS_SETUP.md</code>.
						</div>
					) : !pushSupport ? (
						<div className="alert alert-warning mb-0">
							This browser can't do push notifications. On iPhone, tap{" "}
							<b>Share → Add to Home Screen</b>, then open ZenGM from the new
							icon and come back here.
						</div>
					) : pushPermission === "granted" ? (
						<p className="text-success mb-0">
							Notifications are on for this device
							{getStoredPushName() ? (
								<>
									{" "}
									as <b>{getStoredPushName()}</b>
								</>
							) : null}
							.
						</p>
					) : (
						<>
							<div className="mb-3">
								<label className="form-label" htmlFor="push-name">
									Your name (shown in notifications)
								</label>
								<input
									id="push-name"
									type="text"
									className="form-control"
									placeholder="e.g. Alex"
									value={pushName}
									onChange={(event) => setPushName(event.target.value)}
								/>
							</div>
							<button
								className="btn btn-primary"
								disabled={pushBusy}
								onClick={enablePush}
							>
								{pushBusy ? "Enabling…" : "Enable phone notifications"}
							</button>
							{pushPermission === "denied" ? (
								<div className="form-text">
									Notifications are blocked for this site. Enable them in your
									browser settings, then try again.
								</div>
							) : null}
						</>
					)}

					{pushError ? (
						<div className="alert alert-danger mt-3 mb-0">{pushError}</div>
					) : null}
				</div>
			</div>
		</>
	);
};

export default MultiplayerSync;
