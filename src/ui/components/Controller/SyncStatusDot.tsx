import { useLocal } from "../../util/local.ts";

// A small green/red dot in the header showing whether this device is ready to
// upload a cloud-tracked change right now. Only shown while in (or reconnecting
// to) a multiplayer session; hidden in single-player.
const SyncStatusDot = () => {
	const { mpSyncActive, mpSyncReady, mpSyncReconnecting } = useLocal([
		"mpSyncActive",
		"mpSyncReady",
		"mpSyncReconnecting",
	]);

	if (!mpSyncActive && !mpSyncReconnecting) {
		return null;
	}

	const ready = mpSyncActive && mpSyncReady;
	const title = ready
		? "Ready to sync changes to the cloud"
		: mpSyncReconnecting
			? "Reconnecting to the cloud…"
			: "Not ready to sync changes to the cloud";

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
				backgroundColor: ready ? "var(--bs-success)" : "var(--bs-danger)",
			}}
		/>
	);
};

export default SyncStatusDot;
