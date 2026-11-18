import type { ReactNode } from "react";
import type { View } from "../../../common/types.ts";
import { toWorker } from "../../util/toWorker.ts";

const handleAutoSort = async () => {
	await toWorker("main", "autoSortRoster", undefined);
};

const handleResetPT = async () => {
	await toWorker("main", "resetPlayingTime", undefined);
};

const InstructionsAndSortButtons = ({
	keepRosterSorted,
	editable,
	tid,
	oneRow,
	extra,
}: Pick<View<"roster">, "editable" | "tid"> & {
	keepRosterSorted: boolean;
	// The buttons, "Keep auto sorted" and anything else (Play Through
	// Injuries) on one row (the refreshed look).
	oneRow?: boolean;
	extra?: ReactNode;
}) => {
	const keepSorted = (
		<div className={oneRow ? "form-check mb-0" : "form-check mt-2"}>
			<input
				className="form-check-input"
				type="checkbox"
				checked={keepRosterSorted}
				id="ai-sort-user-roster"
				onChange={async () => {
					if (!keepRosterSorted) {
						await handleAutoSort();
					}
					await toWorker("main", "updateKeepRosterSorted", {
						tid,
						keepRosterSorted: !keepRosterSorted,
					});
				}}
			/>
			<label className="form-check-label" htmlFor="ai-sort-user-roster">
				Keep auto sorted
			</label>
		</div>
	);
	if (oneRow) {
		if (!editable && !extra) {
			return null;
		}
		return (
			<div className="roster-actions mb-3">
				{editable ? (
					<>
						<button className="btn btn-secondary" onClick={handleAutoSort}>
							Auto sort roster
						</button>
						<button className="btn btn-secondary" onClick={handleResetPT}>
							Reset playing time
						</button>
						{keepSorted}
					</>
				) : null}
				{extra}
			</div>
		);
	}
	return (
		<>
			{editable ? (
				<div className="mb-3">
					<div className="d-flex flex-wrap gap-2">
						{editable ? (
							<>
								<button className="btn btn-secondary" onClick={handleAutoSort}>
									Auto sort roster
								</button>
								<button className="btn btn-secondary" onClick={handleResetPT}>
									Reset playing time
								</button>
							</>
						) : null}
					</div>
					{editable ? keepSorted : null}
				</div>
			) : null}
		</>
	);
};

export default InstructionsAndSortButtons;
