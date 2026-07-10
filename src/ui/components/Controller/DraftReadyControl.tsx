import { useState } from "react";
import { Dropdown } from "react-bootstrap";
import { useLocal } from "../../util/local.ts";
import { toWorker } from "../../util/toWorker.ts";

// Header draft ready-up control, shown only while connected to a sync room
// during the draft. CPU picks advance only once EVERY user team has readied
// up; the button shows room readiness (e.g. "2/3") and the menu offers
// ready-through targets so you can pre-approve picks up to a point (e.g.
// through R1P16) and let the draft run to there on its own.
const DraftReadyControl = () => {
	const { mpDraftReady, mpSyncActive } = useLocal([
		"mpDraftReady",
		"mpSyncActive",
	]);
	const [busy, setBusy] = useState(false);

	if (!mpSyncActive || !mpDraftReady) {
		return null;
	}

	const s = mpDraftReady;

	const setReady = async (untilPick: number | null) => {
		setBusy(true);
		try {
			await toWorker("main", "draftSetReady", untilPick);
		} catch (error) {
			console.error(error);
		} finally {
			setBusy(false);
		}
	};

	const label = s.ready ? "✓" : "Ready";
	const thru =
		s.ready && s.myUntilPick !== undefined && s.myUntilPick > s.nextPick.number
			? ` thru ${s.upcoming.find((p) => p.number === s.myUntilPick)?.label ?? `#${s.myUntilPick}`}`
			: "";

	return (
		<Dropdown className="mx-2 flex-shrink-0" align="end">
			<Dropdown.Toggle
				variant={s.ready ? "success" : "warning"}
				size="sm"
				disabled={busy}
				title={
					s.onClockUser
						? "A league-mate is on the clock"
						: "CPU picks advance once every team is ready"
				}
			>
				{label} {s.readyTeams}/{s.totalTeams}
				<span className="d-none d-lg-inline">{thru}</span>
			</Dropdown.Toggle>
			<Dropdown.Menu>
				<Dropdown.Item
					onClick={() => setReady(s.nextPick.number)}
					disabled={busy}
				>
					Ready for {s.nextPick.label}
				</Dropdown.Item>
				{s.myPickNumber !== undefined && s.myPickNumber > s.nextPick.number ? (
					<Dropdown.Item
						onClick={() => setReady(s.myPickNumber!)}
						disabled={busy}
					>
						Ready until my pick
					</Dropdown.Item>
				) : null}
				{s.endOfRoundPick !== undefined &&
				s.endOfRoundPick > s.nextPick.number ? (
					<Dropdown.Item
						onClick={() => setReady(s.endOfRoundPick!)}
						disabled={busy}
					>
						Ready through this round
					</Dropdown.Item>
				) : null}
				{s.endOfDraftPick !== undefined &&
				s.endOfDraftPick > s.nextPick.number ? (
					<Dropdown.Item
						onClick={() => setReady(s.endOfDraftPick!)}
						disabled={busy}
					>
						Ready through end of draft
					</Dropdown.Item>
				) : null}
				{s.upcoming.length > 1 ? (
					<>
						<Dropdown.Divider />
						<Dropdown.Header>Ready through…</Dropdown.Header>
						<div style={{ maxHeight: 240, overflowY: "auto" }}>
							{s.upcoming.slice(1).map((p) => (
								<Dropdown.Item
									key={p.number}
									onClick={() => setReady(p.number)}
									disabled={busy}
								>
									{p.label}
									{p.mine ? " (my pick)" : ""}
								</Dropdown.Item>
							))}
						</div>
					</>
				) : null}
				{s.ready ? (
					<>
						<Dropdown.Divider />
						<Dropdown.Item onClick={() => setReady(null)} disabled={busy}>
							Not ready
						</Dropdown.Item>
					</>
				) : null}
			</Dropdown.Menu>
		</Dropdown>
	);
};

export default DraftReadyControl;
