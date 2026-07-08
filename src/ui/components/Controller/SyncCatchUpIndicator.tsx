import { useEffect, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";

// Header indicator shown while this device is draining a large backlog after
// being away (see SyncEngine.catchUp). Shows a percentage + a rough ETA derived
// from recent throughput, so a returning user can see it's working and about how
// much longer it'll take - instead of a frozen-looking app.

const formatEta = (seconds: number): string => {
	if (seconds < 60) {
		return `~${Math.max(1, Math.round(seconds))}s`;
	}
	if (seconds < 3600) {
		return `~${Math.round(seconds / 60)}m`;
	}
	return `~${Math.round(seconds / 360) / 10}h`;
};

const SyncCatchUpIndicator = () => {
	const { mpCatchUp } = useLocal(["mpCatchUp"]);

	// Estimate a rate (entries/sec) from the first sample of this drain to now, so
	// the ETA smooths out per-page jitter. Reset whenever a drain starts/ends.
	const anchor = useRef<{ done: number; t: number } | undefined>(undefined);
	const [eta, setEta] = useState<string | undefined>(undefined);

	useEffect(() => {
		if (!mpCatchUp) {
			anchor.current = undefined;
			setEta(undefined);
			return;
		}

		const now = Date.now();
		if (!anchor.current || mpCatchUp.done < anchor.current.done) {
			// New drain (or it restarted) - anchor here, no ETA yet.
			anchor.current = { done: mpCatchUp.done, t: now };
			setEta(undefined);
			return;
		}

		const doneSinceAnchor = mpCatchUp.done - anchor.current.done;
		const elapsed = (now - anchor.current.t) / 1000;
		if (doneSinceAnchor > 0 && elapsed > 1) {
			const rate = doneSinceAnchor / elapsed;
			const remaining = mpCatchUp.total - mpCatchUp.done;
			setEta(rate > 0 ? formatEta(remaining / rate) : undefined);
		}
	}, [mpCatchUp]);

	if (!mpCatchUp || mpCatchUp.total <= 0) {
		return null;
	}

	const pct = Math.min(
		99,
		Math.floor((mpCatchUp.done / mpCatchUp.total) * 100),
	);
	const title = `Catching up on changes made while you were away — ${mpCatchUp.done.toLocaleString()} of ${mpCatchUp.total.toLocaleString()} applied${
		eta ? ` (${eta} left)` : ""
	}. You can keep using the app; it'll finish in the background.`;

	return (
		<span
			className="badge rounded-pill text-bg-info ms-2 align-middle"
			title={title}
			style={{ fontWeight: 500 }}
		>
			Catching up {pct}%{eta ? ` · ${eta}` : ""}
		</span>
	);
};

export default SyncCatchUpIndicator;
