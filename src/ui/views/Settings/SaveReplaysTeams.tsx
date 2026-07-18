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

	return (
		<div style={{ maxWidth: 420 }}>
			<div className="d-flex gap-2 mb-2">
				<button
					type="button"
					className="btn btn-secondary btn-sm"
					disabled={disabled}
					onClick={() => setTids(teams.map((t) => t.tid))}
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
						<label className="form-check-label" htmlFor={`saveReplays-${t.tid}`}>
							{t.region} {t.name}
						</label>
					</div>
				))}
			</div>
		</div>
	);
};

export default SaveReplaysTeams;
