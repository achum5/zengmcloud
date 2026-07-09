import { useLocal } from "./local.ts";

// Shared check for "is this device currently barred from advancing the league".
// Used to disable sim / advance buttons while another device holds the wheel, or
// while we're reconnecting/offline. The worker enforces the same rule - this is
// purely for UX (disabled buttons + a tooltip). Draft PICKS are exempt and must
// NOT use this (any user drafts their own team on the clock).
export const useWheelLocked = (): {
	locked: boolean;
	reason: string | undefined;
} => {
	const {
		mpSyncActive,
		mpSyncIsHost,
		mpSyncHostName,
		mpSyncReady,
		mpSyncReconnecting,
	} = useLocal([
		"mpSyncActive",
		"mpSyncIsHost",
		"mpSyncHostName",
		"mpSyncReady",
		"mpSyncReconnecting",
	]);

	const locked =
		mpSyncReconnecting || (mpSyncActive && (!mpSyncIsHost || !mpSyncReady));
	const reason = !locked
		? undefined
		: mpSyncReconnecting
			? "Reconnecting to the league…"
			: !mpSyncIsHost
				? `${mpSyncHostName ?? "Another device"} has the wheel`
				: "Cloud sync is not ready";

	return { locked, reason };
};
