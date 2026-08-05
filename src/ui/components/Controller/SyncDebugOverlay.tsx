import { useEffect, useRef, useState } from "react";
import {
	clearSyncDebugEntries,
	getSyncDebugEntries,
	setSyncDebugEnabled,
	subscribeSyncDebug,
	syncDebugEnabled,
	type SyncDebugEntry,
	buildSyncLogCapture,
} from "../../util/syncDebugStore.ts";
import { showNotification } from "../../util/showNotification.ts";

// A fixed on-screen panel that shows the sync debug logs, for diagnosing sync
// issues on a device with no reachable console (a phone). Only present when sync
// debug logging is enabled; toggle it from Tools → Multiplayer.

const SyncDebugOverlay = () => {
	const [enabled, setEnabled] = useState(syncDebugEnabled());
	const [entries, setEntries] = useState<SyncDebugEntry[]>(
		getSyncDebugEntries(),
	);
	const [open, setOpen] = useState(true);
	const [filter, setFilter] = useState("");
	const bodyRef = useRef<HTMLDivElement | null>(null);
	const pinnedBottom = useRef(true);

	useEffect(() => {
		return subscribeSyncDebug((next) => {
			setEnabled(syncDebugEnabled());
			setEntries([...next]);
		});
	}, []);

	// Keep scrolled to the newest line unless the user has scrolled up.
	useEffect(() => {
		if (open && pinnedBottom.current && bodyRef.current) {
			bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
		}
	}, [entries, open]);

	if (!enabled) {
		return null;
	}

	const shown = filter
		? entries.filter((e) =>
				`${e.event} ${JSON.stringify(e.payload)}`
					.toLowerCase()
					.includes(filter.toLowerCase()),
			)
		: entries;

	const copyAll = async () => {
		// Snapshot + log lines via the shared builder (also behind the sync
		// page's Copy button), honoring this overlay's filter.
		const text = await buildSyncLogCapture(shown);
		try {
			await navigator.clipboard.writeText(text);
			showNotification({ type: "success", text: "Sync logs copied." });
		} catch {
			// Clipboard can be blocked; fall back to a selectable prompt.
			window.prompt("Copy the sync logs:", text);
		}
	};

	return (
		<div
			style={{
				position: "fixed",
				left: 0,
				right: 0,
				bottom: 0,
				zIndex: 2000,
				background: "rgba(10,10,12,0.96)",
				color: "#e6e6e6",
				borderTop: "2px solid #d63384",
				fontFamily: "monospace",
				fontSize: 11,
			}}
		>
			<div
				className="d-flex align-items-center gap-2 px-2 py-1"
				style={{ borderBottom: open ? "1px solid #333" : undefined }}
			>
				<b style={{ color: "#d63384" }}>sync-debug</b>
				<span className="text-body-secondary">{shown.length}</span>
				<input
					className="form-control form-control-sm"
					style={{ height: 24, maxWidth: 140, fontSize: 11 }}
					placeholder="filter"
					value={filter}
					onChange={(e) => setFilter(e.target.value)}
				/>
				<div className="ms-auto d-flex gap-1">
					<button
						type="button"
						className="btn btn-sm btn-primary py-0 px-2"
						style={{ fontSize: 11 }}
						onClick={copyAll}
					>
						Copy
					</button>
					<button
						type="button"
						className="btn btn-sm btn-secondary py-0 px-2"
						style={{ fontSize: 11 }}
						onClick={() => clearSyncDebugEntries()}
					>
						Clear
					</button>
					<button
						type="button"
						className="btn btn-sm btn-secondary py-0 px-2"
						style={{ fontSize: 11 }}
						onClick={() => setOpen((o) => !o)}
					>
						{open ? "Hide" : "Show"}
					</button>
					<button
						type="button"
						className="btn btn-sm btn-danger py-0 px-2"
						style={{ fontSize: 11 }}
						onClick={() => setSyncDebugEnabled(false)}
						title="Turn off sync debug logging"
					>
						Off
					</button>
				</div>
			</div>
			{open ? (
				<div
					ref={bodyRef}
					onScroll={() => {
						const el = bodyRef.current;
						if (el) {
							pinnedBottom.current =
								el.scrollHeight - el.scrollTop - el.clientHeight < 24;
						}
					}}
					style={{
						maxHeight: "38vh",
						overflowY: "auto",
						padding: "4px 8px",
						whiteSpace: "pre-wrap",
						wordBreak: "break-word",
						lineHeight: 1.35,
					}}
				>
					{shown.length === 0 ? (
						<div className="text-body-secondary">
							Waiting for sync activity…
						</div>
					) : (
						shown.map((e) => (
							<div key={e.seq} style={{ marginBottom: 2 }}>
								<span style={{ color: "#8ab4f8" }}>{e.at.slice(11, 19)}</span>{" "}
								<span style={{ color: "#f6c177" }}>{e.event}</span>{" "}
								{JSON.stringify(e.payload)}
							</div>
						))
					)}
				</div>
			) : null}
		</div>
	);
};

export default SyncDebugOverlay;
