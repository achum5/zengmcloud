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
	pushConfigured,
	pushSupported,
	restorePushNotifications,
} from "../util/pushNotifications.ts";
import {
	deleteAllRooms,
	deleteRoom,
	listRooms,
	type SyncRoom,
} from "../util/syncAdmin.ts";

// Cosmetic gate for the room-admin panel (real security is the Firestore rules).
const ADMIN_PASSWORD = "abc123";

type Status = "disconnected" | "connecting" | "connected";

type SyncActivityItem = {
	key: string;
	action: string;
	ts: number;
	records: number;
	mine: boolean;
	caughtUp: boolean;
};

// "main.signFreeAgent" → "Signing", "playMenu.day" → "Simmed a day", etc. Falls
// back to the raw action name so nothing is ever hidden.
const prettyAction = (action: string): string => {
	const map: Record<string, string> = {
		"playMenu.day": "Simmed a day",
		"playMenu.week": "Simmed a week",
		"playMenu.month": "Simmed a month",
		"playMenu.untilPlayoffs": "Simmed to playoffs",
		"playMenu.throughPlayoffs": "Simmed through playoffs",
		"playMenu.untilDraft": "Simmed to draft",
		"playMenu.untilFreeAgency": "Simmed to free agency",
		"playMenu.untilRegularSeason": "Simmed to regular season",
		"main.signFreeAgent": "Signed a free agent",
		"main.proposeTrade": "Trade",
		"main.reSign": "Re-signed a player",
		"main.draftUser": "Draft pick",
		"main.setNote": "Edited a note",
	};
	if (map[action]) {
		return map[action];
	}
	const short = action.includes(".")
		? action.slice(action.indexOf(".") + 1)
		: action;
	return short.replace(/([A-Z])/g, " $1").replace(/^./, (c) => c.toUpperCase());
};

const relativeTime = (ts: number): string => {
	if (!ts) {
		return "just now";
	}
	const secs = Math.max(0, Math.round((Date.now() - ts) / 1000));
	if (secs < 60) {
		return `${secs}s ago`;
	}
	const mins = Math.round(secs / 60);
	if (mins < 60) {
		return `${mins}m ago`;
	}
	const hours = Math.round(mins / 60);
	if (hours < 24) {
		return `${hours}h ago`;
	}
	return `${Math.round(hours / 24)}d ago`;
};

const MultiplayerSync = () => {
	useTitleBar({ title: "Multiplayer Sync (Beta)" });

	const { lid, mpSyncIsHost, mpSyncHostName, mpSyncReconnecting } = useLocal([
		"lid",
		"mpSyncIsHost",
		"mpSyncHostName",
		"mpSyncReconnecting",
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
	const [pushSupport, setPushSupport] = useState(true);
	const [pushPermission, setPushPermission] =
		useState<NotificationPermission>(getPushPermission());
	const [pushBusy, setPushBusy] = useState(false);
	const [pushError, setPushError] = useState<string | undefined>();

	// Sync activity log + manual recovery.
	const [activity, setActivity] = useState<SyncActivityItem[]>([]);
	const [activityLoading, setActivityLoading] = useState(false);
	const [resyncing, setResyncing] = useState(false);
	const [resyncResult, setResyncResult] = useState<string | undefined>();

	// Room admin (clear Firestore codes), gated by a cosmetic password.
	const [adminInput, setAdminInput] = useState("");
	const [adminUnlocked, setAdminUnlocked] = useState(false);
	const [rooms, setRooms] = useState<SyncRoom[]>([]);
	const [adminBusy, setAdminBusy] = useState(false);
	const [adminMsg, setAdminMsg] = useState<string | undefined>();
	const [deleteCode, setDeleteCode] = useState("");

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

	const refreshActivity = async () => {
		setActivityLoading(true);
		try {
			const result = await toWorker("main", "getSyncActivity", undefined);
			setActivity(result.items);
		} catch {
			// Best-effort; leave whatever we had.
		} finally {
			setActivityLoading(false);
		}
	};

	const forceResync = async () => {
		setResyncResult(undefined);
		setResyncing(true);
		try {
			const { total, applied } = await toWorker(
				"main",
				"resyncSharedLeague",
				undefined,
			);
			setResyncResult(
				`Re-applied ${applied} of ${total} change${total === 1 ? "" : "s"} from the league. Your file is now up to date.`,
			);
			await refreshActivity();
		} catch (err) {
			setResyncResult((err as Error).message ?? String(err));
		} finally {
			setResyncing(false);
		}
	};

	// Load the activity log once we're connected (and whenever simming changes
	// hands, a cheap signal that something happened).
	useEffect(() => {
		if (status === "connected") {
			void refreshActivity();
		}
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [status, mpSyncIsHost, mpSyncHostName]);

	const refreshRooms = async () => {
		setAdminBusy(true);
		setAdminMsg(undefined);
		try {
			setRooms(await listRooms());
		} catch (err) {
			setAdminMsg((err as Error).message ?? String(err));
		} finally {
			setAdminBusy(false);
		}
	};

	const unlockAdmin = async () => {
		if (adminInput !== ADMIN_PASSWORD) {
			setAdminMsg("Wrong password.");
			return;
		}
		setAdminUnlocked(true);
		setAdminInput("");
		await refreshRooms();
	};

	const removeRoom = async (code: string) => {
		setAdminBusy(true);
		setAdminMsg(undefined);
		try {
			await deleteRoom(code);
			setAdminMsg(`Deleted "${code}".`);
			await refreshRooms();
		} catch (err) {
			setAdminMsg((err as Error).message ?? String(err));
		} finally {
			setAdminBusy(false);
		}
	};

	const removeAllRooms = async () => {
		setAdminBusy(true);
		setAdminMsg(undefined);
		try {
			const n = await deleteAllRooms();
			setAdminMsg(`Deleted ${n} room${n === 1 ? "" : "s"}.`);
			await refreshRooms();
		} catch (err) {
			setAdminMsg((err as Error).message ?? String(err));
		} finally {
			setAdminBusy(false);
		}
	};

	const enablePush = async () => {
		setPushError(undefined);
		setPushBusy(true);
		try {
			await enablePushNotifications();
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
				Only one device sims at a time — tap <b>Sim here</b> to move it.
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
					Sim here on connect
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
											🎮 <b>You're simming</b>
										</span>
									) : mpSyncHostName ? (
										<span className="text-body-secondary">
											🔒 <b>{mpSyncHostName}</b> is simming
										</span>
									) : (
										<span className="text-body-secondary">
											Nobody simming yet
										</span>
									)}
								</span>
								{!mpSyncIsHost ? (
									<button
										className="btn btn-primary btn-sm"
										disabled={takingWheel}
										onClick={takeWheel}
									>
										{takingWheel ? "Taking…" : "Sim here"}
									</button>
								) : null}
							</div>
						</>
					) : status === "connecting" ? (
						<p className="mb-0">Connecting…</p>
					) : mpSyncReconnecting ? (
						<p className="text-body-secondary mb-0">
							Reconnecting to the league… simming is paused until you're back
							online.
						</p>
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
						<p className="text-success mb-0">Notifications are on for this device.</p>
					) : (
						<>
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

			{connected ? (
				<div className="card mt-3" style={{ maxWidth: 500 }}>
					<div className="card-body">
						<div className="d-flex align-items-center justify-content-between mb-2">
							<h3 className="card-title h5 mb-0">Sync activity</h3>
							<button
								className="btn btn-link btn-sm p-0"
								disabled={activityLoading}
								onClick={() => void refreshActivity()}
							>
								{activityLoading ? "Refreshing…" : "Refresh"}
							</button>
						</div>

						<button
							className="btn btn-warning btn-sm mb-3"
							disabled={resyncing}
							onClick={() => void forceResync()}
						>
							{resyncing ? "Resyncing…" : "Force full resync"}
						</button>

						{resyncResult ? (
							<div className="alert alert-info py-2 mb-3">{resyncResult}</div>
						) : null}

						{activity.length === 0 ? (
							<p className="text-body-secondary mb-0">
								{activityLoading ? "Loading…" : "No changes in the log yet."}
							</p>
						) : (
							<ul className="list-group list-group-flush">
								{activity.map((item) => (
									<li
										key={item.key}
										className="list-group-item px-0 d-flex align-items-center gap-2"
									>
										<span
											title={item.caughtUp ? "Applied here" : "Not caught up yet"}
											style={{ fontSize: "1.1em" }}
										>
											{item.caughtUp ? "✅" : "⏳"}
										</span>
										<span className="flex-grow-1">
											{prettyAction(item.action)}
											{item.mine ? (
												<span className="badge text-bg-secondary ms-2">you</span>
											) : null}
											<span className="text-body-secondary d-block small">
												{item.records} record{item.records === 1 ? "" : "s"} ·{" "}
												{relativeTime(item.ts)}
											</span>
										</span>
									</li>
								))}
							</ul>
						)}
					</div>
				</div>
			) : null}

			<div className="card mt-3" style={{ maxWidth: 500 }}>
				<div className="card-body">
					<h3 className="card-title h5">Manage rooms</h3>

					{!adminUnlocked ? (
						<form
							className="d-flex gap-2"
							onSubmit={(event) => {
								event.preventDefault();
								void unlockAdmin();
							}}
						>
							<input
								type="password"
								className="form-control"
								placeholder="Password"
								value={adminInput}
								onChange={(event) => setAdminInput(event.target.value)}
							/>
							<button className="btn btn-secondary" type="submit">
								Unlock
							</button>
						</form>
					) : (
						<>
							<div className="d-flex gap-2 mb-3">
								<input
									type="text"
									className="form-control"
									placeholder="Delete a code…"
									value={deleteCode}
									onChange={(event) => setDeleteCode(event.target.value)}
								/>
								<button
									className="btn btn-danger"
									disabled={adminBusy || deleteCode.trim() === ""}
									onClick={() => {
										const code = deleteCode.trim();
										setDeleteCode("");
										void removeRoom(code);
									}}
								>
									Delete
								</button>
							</div>

							<div className="d-flex align-items-center gap-2 mb-2">
								<button
									className="btn btn-sm btn-light-bordered"
									disabled={adminBusy}
									onClick={() => void refreshRooms()}
								>
									{adminBusy ? "Working…" : "Refresh"}
								</button>
								{rooms.length > 0 ? (
									<button
										className="btn btn-sm btn-danger"
										disabled={adminBusy}
										onClick={() => void removeAllRooms()}
									>
										Delete all ({rooms.length})
									</button>
								) : null}
							</div>

							{rooms.length === 0 ? (
								<p className="text-body-secondary mb-0">No rooms listed.</p>
							) : (
								<ul className="list-group list-group-flush">
									{rooms.map((room) => (
										<li
											key={room.code}
											className="list-group-item px-0 d-flex align-items-center gap-2"
										>
											<span className="flex-grow-1">
												<b>{room.code}</b>
												{room.updatedAt ? (
													<span className="text-body-secondary d-block small">
														{relativeTime(room.updatedAt)}
													</span>
												) : null}
											</span>
											<button
												className="btn btn-sm btn-outline-danger"
												disabled={adminBusy}
												onClick={() => void removeRoom(room.code)}
											>
												Delete
											</button>
										</li>
									))}
								</ul>
							)}
						</>
					)}

					{adminMsg ? (
						<div className="alert alert-info py-2 mt-3 mb-0">{adminMsg}</div>
					) : null}
				</div>
			</div>
		</>
	);
};

export default MultiplayerSync;
