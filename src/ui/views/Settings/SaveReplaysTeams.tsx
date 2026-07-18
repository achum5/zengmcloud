import { useLocal } from "../../util/local.ts";
import { orderBy } from "../../../common/utils.ts";

// The team picker for the "auto-save replays" setting. Value round-trips as a
// JSON string of team IDs (the jsonString setting type), so this just parses
// that string, shows a checkbox per team, and writes the new JSON string back.
const SaveReplaysTeams = ({
	disabled,
	value,
	onChange,
}: {
	disabled: boolean;
	value: string;
	onChange: (value: string) => void;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	let selected: number[];
	try {
		const parsed = JSON.parse(value);
		selected = Array.isArray(parsed)
			? parsed.filter((x) => typeof x === "number")
			: [];
	} catch {
		selected = [];
	}
	const selectedSet = new Set(selected);

	const teams = orderBy(
		teamInfoCache
			.map((t, tid) => ({ tid, ...t }))
			.filter((t) => t && !t.disabled),
		["region", "name", "tid"],
	);

	const setTids = (tids: number[]) => {
		onChange(JSON.stringify([...tids].sort((a, b) => a - b)));
	};

	const toggle = (tid: number) => {
		setTids(
			selectedSet.has(tid)
				? selected.filter((x) => x !== tid)
				: [...selected, tid],
		);
	};

	// -1 is the All-Star Game (its rosters are tids -1/-2). Kept separate from the
	// team list since it isn't a real franchise. -2 is the "all playoff games"
	// sentinel: save every playoff game regardless of team.
	const ALL_STAR = -1;
	const ALL_PLAYOFFS = -2;

	return (
		<div style={{ maxWidth: 420 }}>
			<div className="d-flex gap-2 mb-2">
				<button
					type="button"
					className="btn btn-secondary btn-sm"
					disabled={disabled}
					onClick={() =>
						setTids([
							...(selectedSet.has(ALL_STAR) ? [ALL_STAR] : []),
							...(selectedSet.has(ALL_PLAYOFFS) ? [ALL_PLAYOFFS] : []),
							...teams.map((t) => t.tid),
						])
					}
				>
					Select all
				</button>
				<button
					type="button"
					className="btn btn-secondary btn-sm"
					disabled={disabled || selected.length === 0}
					onClick={() => setTids([])}
				>
					Clear
				</button>
			</div>
			<div
				className="d-flex flex-column gap-1 border rounded p-2"
				style={{ maxHeight: 260, overflowY: "auto" }}
			>
				<div className="form-check mb-0">
					<input
						className="form-check-input"
						type="checkbox"
						id="saveReplays-allplayoffs"
						disabled={disabled}
						checked={selectedSet.has(ALL_PLAYOFFS)}
						onChange={() => toggle(ALL_PLAYOFFS)}
					/>
					<label className="form-check-label" htmlFor="saveReplays-allplayoffs">
						🏆 All playoff games
					</label>
				</div>
				<div className="form-check mb-0">
					<input
						className="form-check-input"
						type="checkbox"
						id="saveReplays-allstar"
						disabled={disabled}
						checked={selectedSet.has(ALL_STAR)}
						onChange={() => toggle(ALL_STAR)}
					/>
					<label className="form-check-label" htmlFor="saveReplays-allstar">
						⭐ All-Star Game
					</label>
				</div>
				<hr className="my-1" />
				{teams.map((t) => (
					<div key={t.tid} className="form-check mb-0">
						<input
							className="form-check-input"
							type="checkbox"
							id={`saveReplays-${t.tid}`}
							disabled={disabled}
							checked={selectedSet.has(t.tid)}
							onChange={() => toggle(t.tid)}
						/>
						<label
							className="form-check-label"
							htmlFor={`saveReplays-${t.tid}`}
						>
							{t.region} {t.name}
						</label>
					</div>
				))}
			</div>
		</div>
	);
};

export default SaveReplaysTeams;
