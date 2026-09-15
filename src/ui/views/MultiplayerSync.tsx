import { useEffect, useRef, useState } from "react";
import { generateRoomCode, roomCodeWarning } from "../../common/roomCode.ts";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { useLocal } from "../util/local.ts";
import { confirm } from "../util/confirm.tsx";
import { toWorker } from "../util/toWorker.ts";
import { buildSyncLogCapture } from "../util/syncDebugStore.ts";
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
	decodeSyncInvite,
	encodeSyncInvite,
	looksLikeSyncInvite,
} from "../../common/syncInvite.ts";
import {
	consoleUrls,
	parseFirebaseConfig,
} from "../../common/parseFirebaseConfig.ts";
import {
	PREFLIGHT_STEP_LABELS,
	type PreflightResult,
} from "../../common/preflight.ts";
import type { FirebaseConfig } from "../../common/firebaseConfig.ts";

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
		"main.fileTeamSeasonRecaps": "Filed team recaps",
		"main.filePlayerSeasonRecaps": "Filed player recaps",
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
	// Creating a room and joining one are different acts with different
	// inputs, and collapsing them into a single code box hid the one option
	// that can ONLY be set while creating: the protocol. Asking outright also
	// means each mode can say what it needs and nothing else.
	const [mode, setMode] = useState<"create" | "join">("join");
	const logCopied = useRef(false);
	const [, setLogCopiedTick] = useState(0);
	const [status, setStatus] = useState<Status>("disconnected");
	const [error, setError] = useState<string | undefined>();
	const [claimingSimAuthority, setClaimingSimAuthority] = useState(false);

	// Bring-your-own-Firestore. `byoConfigText` is whatever the host pasted out
	// of the Firebase console; `byoCheck` is the last preflight result, which is
	// what the setup checklist draws. `invite` is the one string a league-mate
	// needs - room code and project together - shown once connected.
	const [byoConfigText, setByoConfigText] = useState("");
	const [byoCheck, setByoCheck] = useState<PreflightResult | undefined>();
	const [byoChecking, setByoChecking] = useState(false);
	const [byoError, setByoError] = useState<string | undefined>();
	const [copiedRules, setCopiedRules] = useState(false);
	const [invite, setInvite] = useState<string | undefined>();

	// What the rest of the room sees next to this device's sims, notes and cards.
	// Blank falls back to the team this device manages.
	const [deviceName, setDeviceName] = useState("");
	const [deviceNamePlaceholder, setDeviceNamePlaceholder] = useState("");

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

	// A day simmed on this device that never reached the room. Reported first, so
	// the size of the repair is on screen before anything is published.
	const [unsynced, setUnsynced] = useState<any>();
	const [unsyncedBusy, setUnsyncedBusy] = useState(false);
	const [unsyncedResult, setUnsyncedResult] = useState<string | undefined>();

	// The same repair with the day named by hand, for a room that never recorded
	// a position of its own and so cannot be compared against.
	const [daySeason, setDaySeason] = useState("");
	const [dayNumber, setDayNumber] = useState("");
	const [dayReport, setDayReport] = useState<any>();
	const [dayBusy, setDayBusy] = useState(false);
	const [dayResult, setDayResult] = useState<string | undefined>();

	const [syncDebug, setSyncDebug] = useState(syncDebugEnabled());
	const [adminBusy, setAdminBusy] = useState(false);
	const [adminMsg, setAdminMsg] = useState<string | undefined>();

	// On mount, reflect whatever the worker's sync engine is currently doing
	// (it may already be connected from an auto-reconnect after refresh), and
	// load the multi-team-mode teams for the team picker.
	useEffect(() => {
		let cancelled = false;
		(async () => {
			const [workerStatus, syncTeams, name] = await Promise.all([
				toWorker("main", "getSyncStatus", undefined),
				toWorker("main", "getSyncTeams", undefined),
				toWorker("main", "getSyncDeviceName", undefined),
			]);
			if (cancelled) {
				return;
			}
			setDeviceName(name.stored);
			setDeviceNamePlaceholder(name.effective);
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
		// The unnamed-device fallback is the team, so the placeholder just changed.
		const team = teams.find((t) => t.tid === tid);
		if (team) {
			setDeviceNamePlaceholder(`${team.region} ${team.name}`);
		}
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

	const checkUnsynced = async () => {
		setUnsyncedBusy(true);
		setUnsyncedResult(undefined);
		try {
			setUnsynced(await toWorker("main", "reportUnsyncedDays", undefined));
		} catch (error) {
			setUnsyncedResult((error as Error).message ?? String(error));
		} finally {
			setUnsyncedBusy(false);
		}
	};

	const doPushUnsynced = async () => {
		setUnsyncedBusy(true);
		setUnsyncedResult(undefined);
		try {
			const out: any = await toWorker("main", "pushUnsyncedDays", undefined);
			if (out?.published) {
				setUnsyncedResult(
					out.outcome === "confirmed"
						? `Sent. ${out.report.games} game${out.report.games === 1 ? "" : "s"} from day ${out.report.days.join(", ")} are now in the room.`
						: `Queued. The room will have it as soon as the connection allows.`,
				);
				setUnsynced(undefined);
			} else {
				setUnsyncedResult(out?.report?.reason ?? "Nothing to send.");
			}
		} catch (error) {
			setUnsyncedResult((error as Error).message ?? String(error));
		} finally {
			setUnsyncedBusy(false);
		}
	};

	// Naming the day by hand, for a room that never stamped a position of its own
	// and so cannot be compared against.
	const checkDay = async () => {
		setDayBusy(true);
		setDayResult(undefined);
		setDayReport(undefined);
		try {
			setDayReport(
				await toWorker("main", "reportDayPush", {
					season: Number(daySeason),
					day: Number(dayNumber),
				}),
			);
		} catch (error) {
			setDayResult((error as Error).message ?? String(error));
		} finally {
			setDayBusy(false);
		}
	};

	const doPushDay = async () => {
		setDayBusy(true);
		setDayResult(undefined);
		try {
			const out: any = await toWorker("main", "pushDay", {
				season: Number(daySeason),
				day: Number(dayNumber),
			});
			if (out?.published) {
				setDayResult(
					out.outcome === "confirmed"
						? `Sent. Day ${out.report.day} of ${out.report.season} is now in the room.`
						: "Queued. The room will have it as soon as the connection allows.",
				);
				setDayReport(undefined);
			} else {
				setDayResult(out?.report?.reason ?? "Nothing to send.");
			}
		} catch (error) {
			setDayResult((error as Error).message ?? String(error));
		} finally {
			setDayBusy(false);
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

	// Check the pasted project before a league is trusted to it. The steps and
	// their fixes come back from the worker (which owns the Firebase side); this
	// only draws them.
	const checkByoProject = async () => {
		const parsed = parseFirebaseConfig(byoConfigText);
		if (!parsed.ok) {
			setByoCheck(undefined);
			setByoError(parsed.error);
			return;
		}
		setByoError(undefined);
		setByoChecking(true);
		try {
			setByoCheck(
				await toWorker("main", "preflightFirebaseConfig", parsed.config),
			);
		} catch (error) {
			setByoCheck(undefined);
			setByoError((error as Error).message ?? String(error));
		} finally {
			setByoChecking(false);
		}
	};

	// The rules ship with the app (public/firestore.rules), so the button hands
	// over the exact file `firebase deploy` would publish - there is no second
	// copy to fall behind it.
	const copyRules = async () => {
		try {
			const response = await fetch("/firestore.rules");
			await navigator.clipboard?.writeText(await response.text());
			setCopiedRules(true);
			setTimeout(() => setCopiedRules(false), 2000);
		} catch {
			setByoError(
				"Couldn't copy the rules. Open public/firestore.rules instead.",
			);
		}
	};

	// Clear this league's cloud data: every document the room accumulated, then
	// the room itself. Scoped to the room this device is actually in - there is
	// no way to reach anyone else's, and no listing of them to reach into (see
	// the note at the top of firestore.rules).
	const deleteCloudData = async (room: string) => {
		const proceed = await confirm(
			`Delete all cloud data for "${room}"? Everyone keeps their own local copy of the league, but the shared room is gone for good.`,
			{ okText: "Delete cloud data" },
		);
		if (!proceed) {
			return;
		}
		setAdminBusy(true);
		setAdminMsg(undefined);
		try {
			await toWorker("main", "disconnectSharedLeague", undefined);
			if (typeof lid === "number") {
				clearStoredSync(lid);
			}
			setInvite(undefined);
			setStatus("disconnected");
			await toWorker("main", "deleteSyncRoom", room);
			setAdminMsg(`Deleted the cloud data for "${room}".`);
		} catch (error) {
			setAdminMsg((error as Error).message ?? String(error));
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

			if (looksLikeSyncInvite(code)) {
				// Joining via an invite: one string carrying the room code AND the
				// project it lives in, so a league-mate needs no console of their own.
				const decoded = decodeSyncInvite(code);
				innerCode = decoded.code;
				config = decoded.config;
			} else if (byoConfigText.trim() !== "") {
				// Hosting on your own project.
				const parsed = parseFirebaseConfig(byoConfigText);
				if (!parsed.ok) {
					throw new Error(parsed.error);
				}
				config = parsed.config;
			}

			await toWorker("main", "connectSharedLeague", {
				code: innerCode,
				// Whoever creates the room is by definition its first simmer;
				// joining a room leaves that to the person already running it
				// unless the user asks for it.
				isHost: mode === "create" ? true : isHost,
				// Typed by the user on this page - an explicit join, allowed to bind
				// this league file to the room.
				explicit: true,
				firebaseConfig: config,
			});
			setCode(innerCode);
			setStoredSync(lid, {
				code: innerCode,
				isHost: mode === "create" ? true : isHost,
				firebaseConfig: config,
			});
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

	// The project the pasted config names, so a failing step can link straight
	// into THAT project's console. Read from the text rather than the checked
	// config, because the link matters most when the check just failed.
	const byoParsed = parseFirebaseConfig(byoConfigText);
	const byoProjectId = byoParsed.ok ? byoParsed.config.projectId : undefined;

	// Only worth saying while picking a NEW code; a code someone hands you is
	// not yours to second-guess.
	const codeWarning = roomCodeWarning(code);

	return (
		<>
			<div className="row" style={{ maxWidth: 500 }}>
				<div className="col-12 mb-3">
					<label className="form-label" htmlFor="sync-name">
						Your name
					</label>
					<input
						id="sync-name"
						type="text"
						className="form-control"
						maxLength={40}
						placeholder={deviceNamePlaceholder}
						value={deviceName}
						onChange={(event) => {
							setDeviceName(event.target.value);
						}}
						onBlur={() => {
							void toWorker("main", "setSyncDeviceName", deviceName);
						}}
					/>
				</div>
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

			{connected ? (
				<div className="mb-3" style={{ maxWidth: 500 }}>
					<div className="d-flex flex-wrap gap-3">
						<div>
							<div className="text-body-secondary small">Room</div>
							<div className="fw-bold">{code}</div>
						</div>
						<div>
							<div className="text-body-secondary small">Simming</div>
							<div className="fw-bold">
								{mpSyncIsHost ? "You" : (mpSyncHostName ?? "Nobody")}
							</div>
						</div>
					</div>
				</div>
			) : (
				<div style={{ maxWidth: 500 }}>
					<div className="btn-group mb-3" role="group">
						<button
							type="button"
							className={`btn ${mode === "join" ? "btn-primary" : "btn-light"}`}
							disabled={status === "connecting"}
							onClick={() => setMode("join")}
						>
							Join a room
						</button>
						<button
							type="button"
							className={`btn ${mode === "create" ? "btn-primary" : "btn-light"}`}
							disabled={status === "connecting"}
							onClick={() => {
								setMode("create");
								// Land on a usable code rather than a disabled button.
								if (code.trim() === "") {
									setCode(generateRoomCode());
								}
							}}
						>
							Create a room
						</button>
					</div>

					<div className="mb-3">
						<label className="form-label" htmlFor="sync-code">
							{mode === "create"
								? "New room code"
								: "Room code from your league-mate"}
						</label>
						<div className="input-group">
							<input
								id="sync-code"
								type="text"
								className="form-control"
								placeholder={
									mode === "create" ? "brisk-falcon-482" : "e.g. smith-dynasty"
								}
								value={code}
								disabled={status === "connecting"}
								onChange={(event) => setCode(event.target.value)}
							/>
							{mode === "create" ? (
								<button
									type="button"
									className="btn btn-secondary"
									disabled={status === "connecting"}
									onClick={() => setCode(generateRoomCode())}
								>
									Generate
								</button>
							) : null}
						</div>
						{mode === "create" && codeWarning ? (
							<div className="form-text text-warning">{codeWarning}</div>
						) : null}
					</div>

					{mode === "create" ? null : (
						<div className="form-check mb-3">
							<input
								id="sync-host"
								type="checkbox"
								className="form-check-input"
								checked={isHost}
								disabled={status === "connecting"}
								onChange={(event) => setIsHost(event.target.checked)}
							/>
							<label className="form-check-label" htmlFor="sync-host">
								Sim here on connect
							</label>
						</div>
					)}
				</div>
			)}

			{!connected && !looksLikeSyncInvite(code) ? (
				<div className="card mb-3" style={{ maxWidth: 500 }}>
					<div className="card-body">
						<h3 className="card-title h5">Your Firebase project</h3>

						<label className="form-label" htmlFor="sync-byo-config">
							Config from Project settings → General → Your apps
						</label>
						<textarea
							id="sync-byo-config"
							className="form-control"
							rows={5}
							spellCheck={false}
							placeholder={
								'const firebaseConfig = {\n  apiKey: "…",\n  authDomain: "…",\n  …\n};'
							}
							value={byoConfigText}
							onChange={(event) => {
								setByoConfigText(event.target.value);
								setByoCheck(undefined);
								setByoError(undefined);
							}}
						/>

						<div className="d-flex align-items-center gap-2 mt-2">
							<button
								type="button"
								className="btn btn-secondary"
								disabled={byoChecking || byoConfigText.trim() === ""}
								onClick={() => void checkByoProject()}
							>
								{byoChecking ? "Checking…" : "Check project"}
							</button>
							{byoCheck?.ok ? (
								<span className="text-success">Ready to host.</span>
							) : null}
						</div>

						{byoError ? (
							<div className="alert alert-danger py-2 mt-3 mb-0">
								{byoError}
							</div>
						) : null}

						{byoCheck ? (
							<ul className="list-unstyled mt-3 mb-0">
								{byoCheck.steps.map(({ step, ok }) => (
									<li
										key={step}
										className={ok ? "text-success" : "text-danger"}
									>
										{ok ? "✓" : "✗"} {PREFLIGHT_STEP_LABELS[step]}
									</li>
								))}
							</ul>
						) : null}

						{byoCheck?.problem ? (
							<div className="alert alert-warning py-2 mt-3 mb-0">
								<b>{byoCheck.problem.title}</b>
								<div>{byoCheck.problem.fix}</div>
								<div className="d-flex flex-wrap gap-2 mt-2">
									{byoCheck.link ? (
										<a
											className="btn btn-sm btn-light-bordered"
											href={consoleUrls(byoProjectId ?? "_")[byoCheck.link.url]}
											target="_blank"
											rel="noopener noreferrer"
										>
											{byoCheck.link.label}
										</a>
									) : null}
									{byoCheck.link?.rules ? (
										<button
											type="button"
											className="btn btn-sm btn-light-bordered"
											onClick={() => void copyRules()}
										>
											{copiedRules ? "Copied" : "Copy rules"}
										</button>
									) : null}
								</div>
								{byoCheck.detail ? (
									<div className="small text-body-secondary mt-2">
										{byoCheck.detail}
									</div>
								) : null}
							</div>
						) : null}
					</div>
				</div>
			) : null}

			{connected && invite ? (
				<div className="mb-3" style={{ maxWidth: 500 }}>
					<label className="form-label" htmlFor="sync-invite">
						Invite
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
						{status === "connecting"
							? "Connecting…"
							: mode === "create"
								? "Create room"
								: "Join room"}
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

						<button
							className="btn btn-light-bordered btn-sm mb-3 ms-2"
							disabled={unsyncedBusy}
							onClick={() => void checkUnsynced()}
						>
							{unsyncedBusy ? "Checking…" : "Check for unsent days"}
						</button>

						{unsynced?.kind === "found" ? (
							<div className="alert alert-warning py-2">
								<div>
									This device has played day {unsynced.days.join(", ")} of{" "}
									{unsynced.season} and the room is still on day{" "}
									{unsynced.roomDay}. Sending {unsynced.games} game
									{unsynced.games === 1 ? "" : "s"} ({unsynced.records}{" "}
									records).
								</div>
								<button
									className="btn btn-warning btn-sm mt-2"
									disabled={unsyncedBusy}
									onClick={() => void doPushUnsynced()}
								>
									{unsyncedBusy ? "Sending…" : "Send to the room"}
								</button>
							</div>
						) : null}
						{unsynced?.kind === "none" ? (
							<p className="text-body-secondary small">{unsynced.reason}</p>
						) : null}
						{unsyncedResult ? <p className="small">{unsyncedResult}</p> : null}

						<div className="d-flex align-items-center gap-2 mb-2 flex-wrap">
							<input
								type="number"
								className="form-control form-control-sm"
								style={{ width: 100 }}
								placeholder="Season"
								value={daySeason}
								onChange={(event) => setDaySeason(event.target.value)}
							/>
							<input
								type="number"
								className="form-control form-control-sm"
								style={{ width: 80 }}
								placeholder="Day"
								value={dayNumber}
								onChange={(event) => setDayNumber(event.target.value)}
							/>
							<button
								className="btn btn-light-bordered btn-sm"
								disabled={dayBusy || daySeason === "" || dayNumber === ""}
								onClick={() => void checkDay()}
							>
								{dayBusy ? "Checking…" : "Check this day"}
							</button>
						</div>

						{dayReport?.kind === "found" ? (
							<div className="alert alert-warning py-2">
								<div>
									Day {dayReport.day} of {dayReport.season}: {dayReport.games}{" "}
									game
									{dayReport.games === 1 ? "" : "s"} ({dayReport.records}{" "}
									records).
								</div>
								<ul className="mb-2 mt-1 ps-3 small">
									{dayReport.lines.map((line: string, i: number) => (
										<li key={i}>{line}</li>
									))}
								</ul>
								<button
									className="btn btn-warning btn-sm"
									disabled={dayBusy}
									onClick={() => void doPushDay()}
								>
									{dayBusy ? "Sending…" : "Send this day to the room"}
								</button>
							</div>
						) : null}
						{dayReport?.kind === "none" ? (
							<p className="text-body-secondary small">{dayReport.reason}</p>
						) : null}
						{dayResult ? <p className="small">{dayResult}</p> : null}

						<button
							className="btn btn-light-bordered btn-sm mb-3 ms-2"
							onClick={async () => {
								const text = await buildSyncLogCapture();
								try {
									await navigator.clipboard.writeText(text);
									logCopied.current = true;
									setLogCopiedTick((t) => t + 1);
									setTimeout(() => {
										logCopied.current = false;
										setLogCopiedTick((t) => t + 1);
									}, 2000);
								} catch {
									window.prompt("Copy the sync logs:", text);
								}
							}}
						>
							{logCopied.current ? "Copied!" : "Copy sync logs"}
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

			{connected && code.trim() !== "" ? (
				<div className="card mt-3" style={{ maxWidth: 500 }}>
					<div className="card-body">
						<h3 className="card-title h5">Cloud data</h3>
						<button
							className="btn btn-danger"
							disabled={adminBusy}
							onClick={() => void deleteCloudData(code.trim())}
						>
							{adminBusy ? "Deleting…" : "Delete this league's cloud data"}
						</button>
						{adminMsg ? (
							<div className="alert alert-info py-2 mt-3 mb-0">{adminMsg}</div>
						) : null}
					</div>
				</div>
			) : null}
		</>
	);
};

export default MultiplayerSync;
