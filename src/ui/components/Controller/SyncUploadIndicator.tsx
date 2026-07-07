import { useLocal } from "../../util/local.ts";

// Shown in the header on the device that's uploading a change to the cloud, so
// the user knows an upload is in flight and NOT to close the app until it lands.
// Publishing is fire-and-forget/chunked, so a big change (e.g. a season rollover)
// takes a few seconds; closing mid-upload used to strand the room. The count is
// real (chunks done / total); the outbox finishes an interrupted upload on next
// launch, but keeping the app open avoids the round-trip.
const SyncUploadIndicator = () => {
	const { mpSyncUpload } = useLocal(["mpSyncUpload"]);

	if (!mpSyncUpload || mpSyncUpload.total <= 1) {
		return null;
	}

	return (
		<span
			className="text-info"
			title="Uploading to the cloud — keep the app open until this finishes"
		>
			{" · ☁ "}
			{mpSyncUpload.done}/{mpSyncUpload.total}
		</span>
	);
};

export default SyncUploadIndicator;
