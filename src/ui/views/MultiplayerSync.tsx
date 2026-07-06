import { useEffect, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { useLocal } from "../util/local.ts";
import { toWorker } from "../util/toWorker.ts";
import {
	clearStoredSync,
	getStoredSync,
	setStoredSync,
} from "../util/autoReconnectSync.ts";

type Status = "disconnected" | "connecting" | "connected";

const MultiplayerSync = () => {
	useTitleBar({ title: "Multiplayer Sync (Beta)" });

	const { lid } = useLocal(["lid"]);

	const [code, setCode] = useState("");
	const [isHost, setIsHost] = useState(false);
	const [status, setStatus] = useState<Status>("disconnected");
	const [error, setError] = useState<string | undefined>();

	const [teams, setTeams] = useState<
		{ tid: number; region: string; name: string }[]
	>([]);
	const [userTid, setUserTid] = useState<number | undefined>();
	const [multiTeamMode, setMultiTeamMode] = useState(true);

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

	const switchTeam = async (tid: number) => {
		await toWorker("main", "updateGameAttributes", { userTid: tid });
		setUserTid(tid);
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

	const connected = status === "connected";

	return (
		<>
			<p>
				Sync this league live with friends. Everyone must already be on the{" "}
				<b>same league file</b>, then connect with the <b>same league code</b>.
				From then on, your trades, roster moves, and signings appear on each
				other's devices automatically.
			</p>
			<p className="text-body-secondary">
				One person is the <b>host</b> and runs the simulations — their sim
				results are broadcast to everyone. Everyone else should leave the host
				box unchecked and not sim. Once connected, this league stays connected
				across refreshes automatically.
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
							<div className="form-text">
								Pick which of your league's teams you manage on this device.
								Only affects this device.
							</div>
						</>
					) : (
						<div className="alert alert-warning mb-0">
							Set up <b>Multi Team Mode</b> (Tools → Multi Team Mode) with each
							friend's team first, so everyone's team is human-controlled during
							sims. Then come back here to pick yours.
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
					<div className="form-text">
						Any shared word or phrase. Everyone in your league types the same
						one.
					</div>
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
					I'm the host (I run the sims). Only one person in the league should
					check this.
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
						<p className="text-success mb-0">
							Connected to <b>{code.trim()}</b>
							{isHost ? " as host" : ""} — live changes are syncing.
						</p>
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
		</>
	);
};

export default MultiplayerSync;
