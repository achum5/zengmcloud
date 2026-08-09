import { useState } from "react";

// Rebuilds the header and copies what happened to it, for a fault that only
// ever appears on a real iOS device (see stickyHeaderWatchdog.ts). The report
// comes from a running log rather than the moment of the tap, because reaching
// this button means scrolling to the top, where a broken header looks exactly
// like a working one.
export const HeaderRepairButton = () => {
	const [state, setState] = useState<"idle" | "copied" | "failed">("idle");

	return (
		<button
			type="button"
			className="btn btn-link text-body-secondary p-0 mx-2 border-0"
			title={
				state === "copied"
					? "Copied"
					: "Rebuild the header and copy a diagnostic report"
			}
			onClick={async () => {
				// Loaded on demand: the watchdog is deliberately kept out of the main
				// chunk, and a static import here would drag it back in.
				const [{ forceHeaderRepair }, { buildHeaderReport, copyText }] =
					await Promise.all([
						import("../../util/stickyHeaderWatchdog.ts"),
						import("../../util/stickyHeaderDiagnostics.ts"),
					]);
				await forceHeaderRepair();
				const copied = await copyText(buildHeaderReport());
				setState(copied ? "copied" : "failed");
				setTimeout(() => {
					setState("idle");
				}, 2000);
			}}
		>
			<span
				className={
					state === "copied"
						? "glyphicon glyphicon-ok text-success"
						: state === "failed"
							? "glyphicon glyphicon-remove text-danger"
							: "glyphicon glyphicon-exclamation-sign"
				}
			/>
		</button>
	);
};
