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
	setSyncDebugEnabled,
	syncDebugEnabled,
} from "../util/syncDebugStore.ts";
import {
	byoFirestoreEnabled,
	setByoFirestoreEnabled,
} from "../util/byoFirestore.ts";
import {
	decodeSyncInvite,
	encodeSyncInvite,
	isValidFirebaseConfig,
	looksLikeSyncInvite,
} from "../../common/syncInvite.ts";
import type { FirebaseConfig } from "../../common/firebaseConfig.ts";
import type { SyncRoom } from "../../worker/core/sync/adminRooms.ts";

// Cosmetic gate for the room-admin panel (real security is the Firestore rules).
const ADMIN_PASSWORD = "abc123";

type Status = "disconnected" | "connecting" | "connected";
type PushPermission = "default" | "denied" | "granted";

type SyncActivityItem = {
	key: string;
	action: string;
	ts: number;
	records: number;
	mine: boolean;
	caughtUp: boolean;
	attrs: string[];
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
	useTitleBar({ title: "Multiplayer Sync" });

	const {
		lid,
		mpSyncActive,
		mpSyncIsHost,
		mpSyncHostName,
		mpSyncReady,
		mpSyncReconnecting,
		mpSyncUpload,
	} = useLocal([
		"lid",
		"mpSyncActive",
		"mpSyncIsHost",
		"mpSyncHostName",
		"mpSyncReady",
		"mpSyncReconnecting",
		"mpSyncUpload",
	]);

	const [code, setCode] = useState("");
	const [isHost, setIsHost] = useState(false);
	const [status, setStatus] = useState<Status>("disconnected");
	const [error, setError] = useState<string | undefined>();
	const [claimingSimAuthority, setClaimingSimAuthority] = useState(false);

	// Bring-your-own-Firestore (opt-in). `byoConfigText` is where a host pastes
	// their Firebase config JSON; `invite` is the shareable token shown after a
	// custom-project connect.
	const [byoEnabled, setByoEnabled] = useState(byoFirestoreEnabled());
	const [byoConfigText, setByoConfigText] = useState("");
	const [invite, setInvite] = useState<string | undefined>();

	const [teams, setTeams] = useState<
		{ tid: number; region: string; name: string }[]
	>([]);
	const [userTid, setUserTid] = useState<number | undefined>();
	const [multiTeamMode, setMultiTeamMode] = useState(true);

	// Phone push notifications.
	const [pushSupport, setPushSupport] = useState(true);
	const [pushPermission, setPushPermission] =
		useState<PushPermission>(getPushPermission());
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

	const [syncDebug, setSyncDebug] = useState(syncDebugEnabled());
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
				// Restore the shareable invite if this is a bring-your-own-Firestore
				// room (the config is only kept in localStorage, not the worker).
				if (typeof lid === "number") {
					const stored = getStoredSync(lid);
					if (stored?.firebaseConfig && workerStatus.code) {
						setInvite(
							encodeSyncInvite(workerStatus.code, stored.firebaseConfig),
						);
					}
				}
				// Re-assert the shared sync state from the engine, in case this UI's
				// local state drifted (e.g. a reset that fired after connect) and is
				// showing a stale "nobody simming" / unlocked Play menu.
				void toWorker("main", "refreshSyncUIState", undefined);
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
		// Device-local team pick (userTid never syncs). Uses the dedicated,
		// non-sim-authority-locked call so a league-mate can switch even while
		// someone else is in charge of simming - updateGameAttributes would be
		// blocked for them.
		await toWorker("main", "setUserTidLocal", tid);
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
			const { total, applied, incomplete, failed } = await toWorker(
				"main",
				"resyncSharedLeague",
				undefined,
			);
			if (incomplete > 0 || failed) {
				setResyncResult(
					`Re-applied ${applied} of ${total} changes, but couldn't fully catch up${
						incomplete > 0
							? ` (${incomplete} change is missing part of its data in the cloud)`
							: ""
					}. The reliable fix is to re-share the league file: export it from the device that's simming and import it here.`,
				);
			} else {
				setResyncResult(
					`Re-applied ${applied} of ${total} change${total === 1 ? "" : "s"}. Your file is up to date.`,
				);
			}
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
			setRooms(await toWorker("main", "listSyncRooms", undefined));
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
			await toWorker("main", "deleteSyncRoom", code);
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
			const n = await toWorker("main", "deleteAllSyncRooms", undefined);
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
		} catch (error_) {
			setPushError((error_ as Error).message ?? String(error_));
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
			let innerCode = code.trim();
			let config: FirebaseConfig | undefined;

			if (byoEnabled) {
				if (looksLikeSyncInvite(code)) {
					// Joining via an invite: it carries the room code + project config.
					const decoded = decodeSyncInvite(code);
					innerCode = decoded.code;
					config = decoded.config;
				} else if (byoConfigText.trim() !== "") {
					// Hosting on your own project: parse the pasted config.
					let parsed: unknown;
					try {
						parsed = JSON.parse(byoConfigText);
					} catch {
						throw new Error("Firebase config must be valid JSON.");
					}
					if (!isValidFirebaseConfig(parsed)) {
						throw new Error(
							"Firebase config is missing required fields (apiKey, projectId, …).",
						);
					}
					config = parsed;
				}
			}

			await toWorker("main", "connectSharedLeague", {
				code: innerCode,
				isHost,
				// Typed by the user on this page - an explicit join, allowed to bind
				// this league file to the room.
				explicit: true,
				firebaseConfig: config,
			});
			setCode(innerCode);
			setStoredSync(lid, { code: innerCode, isHost, firebaseConfig: config });
			setInvite(config ? encodeSyncInvite(innerCode, config) : undefined);
			setStatus("connected");
		} catch (error_) {
			setError((error_ as Error).message ?? String(error_));
			setStatus("disconnected");
		}
	};

	const disconnect = async () => {
		await toWorker("main", "disconnectSharedLeague", undefined);
		if (typeof lid === "number") {
			clearStoredSync(lid);
		}
		setInvite(undefined);
		setStatus("disconnected");
	};

	const claimSimAuthority = async () => {
		setClaimingSimAuthority(true);
		try {
			await toWorker("main", "claimSyncAuthority", undefined);
		} finally {
			setClaimingSimAuthority(false);
		}
	};

	// Source of truth is the reactive worker state (mpSyncActive), NOT the local
	// one-shot `status` fetched on mount - otherwise an auto-reconnect that lands
	// AFTER the page mounted leaves it stuck showing "Not connected" while the
	// device is really connected (and the header dot is green). `status` is kept
	// only for the transient "connecting…" while a manual Connect is in flight.
	const connected = mpSyncActive || status === "connected";

	return (
		<>
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

			<div className="mb-3" style={{ maxWidth: 500 }}>
				<div className="form-check">
					<input
						id="sync-byo"
						type="checkbox"
						className="form-check-input"
						checked={byoEnabled}
						disabled={connected || status === "connecting"}
						onChange={(event) => {
							setByoFirestoreEnabled(event.target.checked);
							setByoEnabled(event.target.checked);
						}}
					/>
					<label
						className="form-check-label"
						htmlFor="sync-byo"
						title="Host the room on your own Firebase project instead of the built-in one"
					>
						Use your own Firestore
					</label>
				</div>

				{byoEnabled && !connected ? (
					<div className="mt-2">
						<label className="form-label" htmlFor="sync-byo-config">
							Firebase config (JSON)
						</label>
						<textarea
							id="sync-byo-config"
							className="form-control"
							rows={5}
							placeholder={
								'{"apiKey":"…","authDomain":"…","projectId":"…","storageBucket":"…","messagingSenderId":"…","appId":"…"}'
							}
							value={byoConfigText}
							onChange={(event) => setByoConfigText(event.target.value)}
						/>
					</div>
				) : null}

				{byoEnabled && connected && invite ? (
					<div className="mt-2">
						<label className="form-label" htmlFor="sync-invite">
							Invite code
						</label>
						<textarea
							id="sync-invite"
							className="form-control"
							rows={3}
							readOnly
							value={invite}
							onFocus={(event) => event.target.select()}
						/>
						<button
							type="button"
							className="btn btn-light btn-sm mt-2"
							onClick={() => {
								void navigator.clipboard?.writeText(invite);
							}}
						>
							Copy invite
						</button>
					</div>
				) : null}
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
							<p
								className={`${
									mpSyncReady ? "text-success" : "text-danger"
								} mb-2`}
							>
								<span
									aria-hidden
									style={{
										backgroundColor: mpSyncReady
											? "var(--bs-success)"
											: "var(--bs-danger)",
										borderRadius: "50%",
										display: "inline-block",
										height: 10,
										marginRight: 6,
										width: 10,
									}}
								/>
								{mpSyncReady ? "Ready" : "Not ready"} for cloud upload to{" "}
								<b>{code.trim()}</b>.
							</p>
							<div className="d-flex align-items-center gap-2 flex-wrap">
								<span>
									{mpSyncIsHost ? (
										<span
											className={
												mpSyncReady ? "text-success" : "text-body-secondary"
											}
										>
											<b>You're in charge of simming</b>
										</span>
									) : mpSyncHostName ? (
										<span className="text-body-secondary">
											<b>{mpSyncHostName}</b> is in charge of simming
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
										disabled={claimingSimAuthority}
										onClick={claimSimAuthority}
									>
										{claimingSimAuthority ? "Switching…" : "Sim here"}
									</button>
								) : null}
							</div>
							{mpSyncUpload && mpSyncUpload.total > 1 ? (
								<div className="mt-3">
									<div className="d-flex justify-content-between small mb-1">
										<span>☁ Uploading to the cloud — keep the app open</span>
										<span className="text-body-secondary">
											{mpSyncUpload.done}/{mpSyncUpload.total}
										</span>
									</div>
									<div className="progress" style={{ height: 6 }}>
										<div
											className="progress-bar bg-info"
											style={{
												width: `${Math.round((mpSyncUpload.done / mpSyncUpload.total) * 100)}%`,
											}}
										/>
									</div>
								</div>
							) : null}
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
						<p className="text-success mb-0">
							Notifications are on for this device.
						</p>
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

			<div className="card mt-3" style={{ maxWidth: 500 }}>
				<div className="card-body">
					<h3 className="card-title h5">Debug logs</h3>
					<div className="form-check">
						<input
							type="checkbox"
							className="form-check-input"
							id="sync-debug-toggle"
							checked={syncDebug}
							onChange={(e) => {
								setSyncDebugEnabled(e.target.checked);
								setSyncDebug(e.target.checked);
							}}
						/>
						<label className="form-check-label" htmlFor="sync-debug-toggle">
							Show sync debug logs on screen
						</label>
					</div>
					<p className="text-body-secondary small mb-0 mt-1">
						A panel appears at the bottom with live sync logs (catch-up,
						uploads, etc). Use its Copy button to share them.
					</p>
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
											title={
												item.caughtUp ? "Applied here" : "Not caught up yet"
											}
											style={{ fontSize: "1.1em" }}
										>
											{item.caughtUp ? "✅" : "⏳"}
										</span>
										<span className="flex-grow-1">
											{prettyAction(item.action)}
											{item.mine ? (
												<span className="badge text-bg-secondary ms-2">
													you
												</span>
											) : null}
											<span className="text-body-secondary d-block small">
												{item.records} record{item.records === 1 ? "" : "s"} ·{" "}
												{relativeTime(item.ts)}
											</span>
											{item.attrs.length > 0 ? (
												<span className="d-block small">
													{item.attrs.map((attr) => (
														<span
															key={attr}
															className={`badge me-1 ${attr === "phase" ? "text-bg-warning" : "text-bg-light"}`}
														>
															{attr}
														</span>
													))}
												</span>
											) : null}
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
