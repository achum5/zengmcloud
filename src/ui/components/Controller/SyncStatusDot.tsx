import { useLocal } from "../../util/local.ts";

// A small green/red dot in the header showing whether this device is TRULY
// connected to the cloud - backed by confirmed live contact (mpSyncHealthy), not
// just "we have a session object". Only shown while in (or reconnecting to) a
// multiplayer session; hidden in single-player.
const SyncStatusDot = () => {
	const { mpSyncActive, mpSyncReconnecting, mpSyncHealthy } = useLocal([
		"mpSyncActive",
		"mpSyncReconnecting",
		"mpSyncHealthy",
	]);

	if (!mpSyncActive && !mpSyncReconnecting) {
		return null;
	}

	const live = mpSyncActive && mpSyncHealthy;
	const title = live
		? "Connected to the cloud"
		: mpSyncReconnecting
			? "Reconnecting to the cloud…"
			: "Not connected to the cloud";

	return (
		<span
			title={title}
			aria-label={title}
			style={{
				display: "inline-block",
				width: 9,
				height: 9,
				borderRadius: "50%",
				marginLeft: 8,
				verticalAlign: "middle",
				backgroundColor: live ? "var(--bs-success)" : "var(--bs-danger)",
			}}
		/>
	);
};

export default SyncStatusDot;
