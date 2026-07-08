import { useEffect, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";

// Header cloud-upload indicator, shown on ANY device (not just the simmer) for
// ANY change that syncs - a sim, a trade, a signing. While uploading it shows a
// cloud (with a done/total count for big, chunked changes so you know to keep the
// app open); the moment the change is confirmed uploaded it flashes a brief green
// "✓ synced" so you know it actually reached the cloud.
const CHECK_MS = 1500;

const SyncUploadIndicator = () => {
	const { mpSyncUpload, mpSyncUploadOk } = useLocal([
		"mpSyncUpload",
		"mpSyncUploadOk",
	]);

	const [showCheck, setShowCheck] = useState(false);
	const prevOk = useRef(mpSyncUploadOk);

	useEffect(() => {
		if (mpSyncUploadOk !== prevOk.current) {
			prevOk.current = mpSyncUploadOk;
			setShowCheck(true);
			const id = setTimeout(() => setShowCheck(false), CHECK_MS);
			return () => clearTimeout(id);
		}
	}, [mpSyncUploadOk]);

	if (mpSyncUpload) {
		return (
			<span
				className="text-info"
				title="Uploading to the cloud — keep the app open until it finishes"
			>
				{" · ☁"}
				{mpSyncUpload.total > 1 ? ` ${mpSyncUpload.done}/${mpSyncUpload.total}` : ""}
			</span>
		);
	}

	if (showCheck) {
		return (
			<span className="text-success" title="Synced to the cloud">
				{" · ✓"}
			</span>
		);
	}

	return null;
};

export default SyncUploadIndicator;
