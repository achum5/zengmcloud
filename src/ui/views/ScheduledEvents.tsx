import { useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View, LocalStateUI } from "../../common/types.ts";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { getCols } from "../../common/getCols.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import { PHASE_TEXT } from "../../common/constants.ts";
import { settings } from "./Settings/settings.tsx";
import { Dropdown } from "react-bootstrap";
import { Modal } from "../components/Modal.tsx";
import { PlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import { useLocal } from "../util/local.ts";

const godModeOptions: Partial<
	Record<(typeof settings)[number]["key"], (typeof settings)[number]>
> = {};
for (const option of settings) {
	godModeOptions[option.key] = option;
}

type AugmentedScheduledEvent =
	View<"scheduledEvents">["scheduledEvents"][number];

const gameAttributeName = (key: string) => {
	if ((godModeOptions as any)[key]) {
		return (godModeOptions as any)[key].name;
	}

	if (key === "confs") {
		return "Conferences";
	}

	if (key === "divs") {
		return "Divisions";
	}

	if (key === "awards") {
		return "Awards";
	}

	return key;
};

const teamInfoKey = (key: string) => {
	if (key === "region") {
		return "Region";
	}

	if (key === "name") {
		return "Name";
	}

	if (key === "pop") {
		return "Population";
	}

	if (key === "cid") {
		return "Conference";
	}

	if (key === "did") {
		return "Division";
	}

	if (key === "abbrev") {
		return "Abbrev";
	}

	if (key === "imgURL") {
		return "Logo URL";
	}

	if (key === "colors") {
		return "Colors";
	}

	return key;
};

const formatSeason = (scheduledEvent: AugmentedScheduledEvent) => {
	const phaseText = PHASE_TEXT[scheduledEvent.phase]
		? helpers.upperCaseFirstLetter(PHASE_TEXT[scheduledEvent.phase])
		: "???";
	return (
		<>
			{scheduledEvent.season}
			<br />
			{phaseText}
		</>
	);
};

const formatType = (type: AugmentedScheduledEvent["type"]) => {
	if (type === "contraction") {
		return "Contraction";
	}

	if (type === "expansionDraft") {
		return "Expansion";
	}

	if (type === "gameAttributes") {
		return "League settings";
	}

	if (type === "teamInfo") {
		return "Team info";
	}

	if (type === "unretirePlayer") {
		return "Unretire player";
	}
};

const TeamNameBlock = ({
	all,
	current,
	teamInfoCache,
}: {
	all: AugmentedScheduledEvent[];
	current: AugmentedScheduledEvent;
	teamInfoCache: LocalStateUI["teamInfoCache"];
}) => {
	if (current.type !== "contraction" && current.type !== "teamInfo") {
		throw new Error("Invalid type");
	}

	const tid = current.info.tid;
	if (teamInfoCache[tid]) {
		const t = teamInfoCache[tid];
		return (
			<div>
				{t.region} {t.name}
				<br />
				Team ID: {tid}
			</div>
		);
	}

	// Must be a team that doesn't exist yet, look in all
	let t;
	for (const scheduledEvent of all) {
		if (scheduledEvent.type === "expansionDraft") {
			for (const t2 of scheduledEvent.info.teams) {
				if (t2.tid === tid) {
					t = t2;
					break;
				}
			}
		}
		if (t) {
			break;
		}
	}

	if (t) {
		return (
			<div>
				{t.region} {t.name} (future expansion team)
				<br />
				Team ID: {t.tid}
			</div>
		);
	}

	return (
		<div className="text-danger">
			Invalid team
			<br />
			Team ID: {tid}
		</div>
	);
};

const ViewEvent = ({
	all,
	current,
	teamInfoCache,
}: {
	all: AugmentedScheduledEvent[];
	current: AugmentedScheduledEvent;
	teamInfoCache: LocalStateUI["teamInfoCache"];
}) => {
	if (current.type === "contraction") {
		return (
			<TeamNameBlock
				all={all}
				current={current}
				teamInfoCache={teamInfoCache}
			/>
		);
	}

	if (current.type === "expansionDraft") {
		return (
			<ul className="list-unstyled mb-0">
				{current.info.teams.map((t, i) => (
					<li className={i > 0 ? "mt-3" : undefined} key={i}>
						{t.region} {t.name}
						{t.tid !== undefined ? (
							<>
								<br />
								Team ID: {t.tid}
							</>
						) : null}
					</li>
				))}
			</ul>
		);
	}

	if (current.type === "gameAttributes") {
		return (
			<table className="table table-nonfluid table-striped table-borderless table-sm">
				<tbody>
					{Object.entries(current.info).map(([key, value]) => {
						return (
							<tr key={key}>
								<td>{gameAttributeName(key)}</td>
								<td>
									{key === "confs" || key === "divs"
										? (value as any[]).map((x, i) => (
												<div key={i}>{x.name}</div>
											))
										: key === "awards"
											? (value as any[]).map((row) => row.shortName).join(", ")
											: Array.isArray(value)
												? JSON.stringify(value)
												: String(value)}
								</td>
							</tr>
						);
					})}
				</tbody>
			</table>
		);
	}

	if (current.type === "teamInfo") {
		return (
			<>
				<TeamNameBlock
					all={all}
					current={current}
					teamInfoCache={teamInfoCache}
				/>
				<table className="table table-nonfluid table-striped table-borderless table-sm mt-3">
					<tbody>
						{Object.entries(current.info)
							.filter(([key]) => key !== "tid" && key !== "srID")
							.map(([key, value]) => {
								return (
									<tr key={key}>
										<td>{teamInfoKey(key)}</td>
										<td>
											{Array.isArray(value) ? JSON.stringify(value) : value}
										</td>
									</tr>
								);
							})}
					</tbody>
				</table>
			</>
		);
	}

	if (current.type === "unretirePlayer") {
		return (
			<PlayerNameLabels
				pid={current.info.pid}
				skills={current.info.skills}
				legacyName={current.info.name}
			/>
		);
	}

	throw new Error("Invalid type");
};

const bulkDelete = (type: string) => async () => {
	await toWorker("main", "deleteScheduledEvents", type);
};

// Edit one scheduled event: when it fires (season + phase) and its raw payload.
// The payload is edited as JSON - the events page is already an advanced tool and
// the payload shape differs by type, so a single JSON field covers every type
// without a bespoke form each. The worker keeps the type fixed and sanitizes.
const EditScheduledEventModal = ({
	event,
	onCancel,
	onSaved,
}: {
	event: AugmentedScheduledEvent;
	onCancel: () => void;
	onSaved: () => void;
}) => {
	const [season, setSeason] = useState(String(event.season));
	const [phase, setPhase] = useState(String(event.phase));
	const [infoText, setInfoText] = useState(() => {
		// unretirePlayer's info is augmented with a name/skills for display; only
		// the stored { pid } is editable/saved.
		const info =
			event.type === "unretirePlayer"
				? { pid: (event.info as { pid: number }).pid }
				: event.info;
		return JSON.stringify(info, undefined, 2);
	});
	const [error, setError] = useState<string | undefined>();
	const [saving, setSaving] = useState(false);

	const save = async () => {
		setSaving(true);
		setError(undefined);
		try {
			const seasonNum = Number.parseInt(season);
			if (!Number.isFinite(seasonNum)) {
				throw new Error("Season must be a number");
			}
			const phaseNum = Number.parseInt(phase);
			if (!Number.isFinite(phaseNum)) {
				throw new Error("Invalid phase");
			}
			let info;
			try {
				info = JSON.parse(infoText);
			} catch {
				throw new Error("Details must be valid JSON");
			}
			await toWorker("main", "updateScheduledEvent", {
				id: event.id,
				type: event.type,
				season: seasonNum,
				phase: phaseNum,
				info,
			} as any);
			onSaved();
		} catch (error_) {
			setError((error_ as Error).message);
		} finally {
			setSaving(false);
		}
	};

	return (
		<Modal show onHide={onCancel}>
			<Modal.Header closeButton>
				<Modal.Title>Edit scheduled event</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				<div className="mb-3" style={{ maxWidth: 150 }}>
					<label className="form-label" htmlFor="se-season">
						Season
					</label>
					<input
						id="se-season"
						type="number"
						step={1}
						className="form-control"
						value={season}
						onChange={(event2) => setSeason(event2.target.value)}
					/>
				</div>
				<div className="mb-3" style={{ maxWidth: 250 }}>
					<label className="form-label" htmlFor="se-phase">
						Phase
					</label>
					<select
						id="se-phase"
						className="form-select"
						value={phase}
						onChange={(event2) => setPhase(event2.target.value)}
					>
						{Object.entries(PHASE_TEXT).map(([p, text]) => (
							<option key={p} value={p}>
								{helpers.upperCaseFirstLetter(text)}
							</option>
						))}
					</select>
				</div>
				<div className="mb-1">
					<label className="form-label" htmlFor="se-info">
						Details (JSON)
					</label>
					<textarea
						id="se-info"
						className="form-control font-monospace"
						rows={10}
						value={infoText}
						onChange={(event2) => setInfoText(event2.target.value)}
					/>
				</div>
				{error ? (
					<div className="alert alert-danger mt-2 mb-0">{error}</div>
				) : null}
			</Modal.Body>
			<Modal.Footer>
				<button
					className="btn btn-secondary"
					disabled={saving}
					onClick={onCancel}
				>
					Cancel
				</button>
				<button className="btn btn-primary" disabled={saving} onClick={save}>
					{saving ? "Saving…" : "Save"}
				</button>
			</Modal.Footer>
		</Modal>
	);
};

const ScheduledEvents = ({ scheduledEvents }: View<"scheduledEvents">) => {
	useTitleBar({
		title: "Scheduled Events",
	});

	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	const [editing, setEditing] = useState<AugmentedScheduledEvent | undefined>();

	const handleDelete = async (scheduledEvent: AugmentedScheduledEvent) => {
		if (window.confirm("Delete this scheduled event?")) {
			await toWorker("main", "deleteScheduledEvent", scheduledEvent.id);
		}
	};

	if (scheduledEvents.length === 0) {
		return (
			<>
				<p>No scheduled events found!</p>
				<p>
					Eventually you will be able to add scheduled events here, but
					currently they are only available in historical "real players" leagues
					where they are created by default when making a new league.
				</p>
			</>
		);
	}

	const cols = getCols(["Season", "Type", ""], {
		"": {
			width: "100%",
		},
	});

	const rows = scheduledEvents.map((scheduledEvent) => {
		return {
			key: scheduledEvent.id,
			data: [
				{
					value: formatSeason(scheduledEvent),
					sortValue: `${scheduledEvent.season} ${scheduledEvent.phase} ${scheduledEvent.id}`,
				},
				formatType(scheduledEvent.type),
				<div className="d-flex align-items-start">
					<div className="flex-grow-1">
						<ViewEvent
							all={scheduledEvents}
							current={scheduledEvent}
							teamInfoCache={teamInfoCache}
						/>
					</div>
					<div className="ms-2 d-flex flex-column gap-1">
						<button
							className="btn btn-light-bordered btn-sm"
							onClick={() => setEditing(scheduledEvent)}
						>
							Edit
						</button>
						<button
							className="btn btn-danger btn-sm"
							onClick={() => handleDelete(scheduledEvent)}
						>
							Delete
						</button>
					</div>
				</div>,
			],
		};
	});

	return (
		<>
			<p>
				Edit or delete any scheduled event below, or apply a bulk operation like
				removing all scheduled team contractions.
			</p>
			<Dropdown>
				<Dropdown.Toggle variant="danger" id="scheduled-events-bulk-delete">
					Bulk delete
				</Dropdown.Toggle>
				<Dropdown.Menu>
					<Dropdown.Item onClick={bulkDelete("all")}>
						All scheduled events
					</Dropdown.Item>
					<Dropdown.Item onClick={bulkDelete("expansionDraft")}>
						Expansion teams
					</Dropdown.Item>
					<Dropdown.Item onClick={bulkDelete("contraction")}>
						Team contractions
					</Dropdown.Item>
					<Dropdown.Item onClick={bulkDelete("teamInfo")}>
						Team info changes
					</Dropdown.Item>
					<Dropdown.Item onClick={bulkDelete("confs")}>
						Conference/division changes
					</Dropdown.Item>
					<Dropdown.Item onClick={bulkDelete("finance")}>
						League finance changes
					</Dropdown.Item>
					<Dropdown.Item onClick={bulkDelete("rules")}>
						League rule changes
					</Dropdown.Item>
					<Dropdown.Item onClick={bulkDelete("styleOfPlay")}>
						Style of play changes
					</Dropdown.Item>
					<Dropdown.Item onClick={bulkDelete("awards")}>
						Award settings changes
					</Dropdown.Item>
					<Dropdown.Item onClick={bulkDelete("unretirePlayer")}>
						Unretire players
					</Dropdown.Item>
				</Dropdown.Menu>
			</Dropdown>
			<DataTable
				cols={cols}
				defaultSort={[0, "asc"]}
				name="ScheduledEvents"
				rows={rows}
			/>
			{editing ? (
				<EditScheduledEventModal
					key={editing.id}
					event={editing}
					onCancel={() => setEditing(undefined)}
					onSaved={() => setEditing(undefined)}
				/>
			) : null}
		</>
	);
};

export default ScheduledEvents;
