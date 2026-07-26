import { useState } from "react";
import { useLocal } from "../util/local.ts";
import { toWorker } from "../util/toWorker.ts";

// Take over simming on this device, without a trip to the Multiplayer Sync
// page. Renders nothing unless this device is synced AND somebody else is
// currently in charge - so on a solo league, or when you already hold it, it
// stays out of the way entirely.

export const SimHereButton = ({ className }: { className?: string }) => {
	const { mpSyncActive, mpSyncIsHost, mpSyncHostName } = useLocal([
		"mpSyncActive",
		"mpSyncIsHost",
		"mpSyncHostName",
	]);
	const [claiming, setClaiming] = useState(false);

	if (!mpSyncActive || mpSyncIsHost) {
		return null;
	}

	const claim = async () => {
		setClaiming(true);
		try {
			await toWorker("main", "claimSyncAuthority", undefined);
		} finally {
			setClaiming(false);
		}
	};

	return (
		<button
			className={className ?? "btn btn-primary btn-sm"}
			disabled={claiming}
			title={
				mpSyncHostName
					? `${mpSyncHostName} is in charge of simming`
					: "Take over simming on this device"
			}
			onClick={claim}
		>
			{claiming ? "Switching…" : "Sim here"}
		</button>
	);
};
