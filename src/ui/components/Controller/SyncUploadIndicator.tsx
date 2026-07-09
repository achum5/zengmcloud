import { useEffect, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";

// Header cloud-upload indicator, shown on ANY device (not just the simmer) for
// ANY change that syncs - a sim, a trade, a signing. While uploading it shows a
// cloud (with a done/total count for big, chunked changes so you know to keep the
// app open); the moment the change is confirmed uploaded it flashes a brief green
// "✓ synced" so you know it actually reached the cloud.
const CHECK_MS = 1500;

// Every change passes through the queue for an instant on its way to the cloud;
// only show "queued" once it has actually lingered (i.e. the upload is stuck).
const QUEUED_DELAY_MS = 4000;

const SyncUploadIndicator = () => {
	const { mpSyncUpload, mpSyncUploadOk, mpPendingUploads } = useLocal([
		"mpSyncUpload",
		"mpSyncUploadOk",
		"mpPendingUploads",
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

	const hasQueued = mpPendingUploads > 0;
	const [showQueued, setShowQueued] = useState(false);

	useEffect(() => {
		if (hasQueued) {
			const id = setTimeout(() => setShowQueued(true), QUEUED_DELAY_MS);
			return () => clearTimeout(id);
		}
		setShowQueued(false);
	}, [hasQueued]);

	if (mpSyncUpload) {
		return (
			<span
				className="text-info"
				title="Uploading to the cloud — keep the app open until it finishes"
			>
				{" · ☁"}
				{mpSyncUpload.total > 1
					? ` ${mpSyncUpload.done}/${mpSyncUpload.total}`
					: ""}
			</span>
		);
	}

	if (showQueued && hasQueued) {
		return (
			<span
				className="text-warning"
				title="Saved locally, waiting to upload — retrying automatically"
			>
				{` · ☁ ${mpPendingUploads} queued`}
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
