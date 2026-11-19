import type { ReactNode } from "react";
import type { View } from "../../../common/types.ts";
import { toWorker } from "../../util/toWorker.ts";

const handleAutoSort = async () => {
	await toWorker("main", "autoSortRoster", undefined);
};

const handleResetPT = async () => {
	await toWorker("main", "resetPlayingTime", undefined);
};

const KeepSorted = ({
	keepRosterSorted,
	tid,
	className,
}: {
	keepRosterSorted: boolean;
	tid: number;
	className: string;
}) => (
	<div className={className}>
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

// THE REFRESHED LOOK'S: the roster's buttons, "Keep auto sorted" and Play
// Through Injuries, small, up by the team's logo (see .roster-top) - over the
// depth chart, in the room the team's info leaves.
export const RosterTools = ({
	keepRosterSorted,
	editable,
	tid,
	injuries,
}: Pick<View<"roster">, "editable" | "tid"> & {
	keepRosterSorted: boolean;
	injuries?: ReactNode;
}) => {
	if (!editable && !injuries) {
		return null;
	}
	return (
		<div className="roster-tools">
			{editable ? (
				<>
					<div className="btn-group roster-tools-sort">
						<button
							type="button"
							className="btn btn-light-bordered btn-sm"
							onClick={handleAutoSort}
							title="Auto sort roster"
						>
							Auto sort<span className="d-none d-md-inline"> roster</span>
						</button>
						<button
							type="button"
							className="btn btn-light-bordered btn-sm"
							onClick={handleResetPT}
							title="Reset playing time"
						>
							Reset <span className="d-md-none">PT</span>
							<span className="d-none d-md-inline">playing time</span>
						</button>
					</div>
					<KeepSorted
						keepRosterSorted={keepRosterSorted}
						tid={tid}
						className="form-check mb-0"
					/>
				</>
			) : null}
			{injuries}
		</div>
	);
};

const InstructionsAndSortButtons = ({
	keepRosterSorted,
	editable,
	tid,
}: Pick<View<"roster">, "editable" | "tid"> & {
	keepRosterSorted: boolean;
}) => {
	if (!editable) {
		return null;
	}
	return (
		<div className="mb-3">
			<div className="d-flex flex-wrap gap-2">
				<button className="btn btn-secondary" onClick={handleAutoSort}>
					Auto sort roster
				</button>
				<button className="btn btn-secondary" onClick={handleResetPT}>
					Reset playing time
				</button>
			</div>
			<KeepSorted
				keepRosterSorted={keepRosterSorted}
				tid={tid}
				className="form-check mt-2"
			/>
		</div>
	);
};

export default InstructionsAndSortButtons;
