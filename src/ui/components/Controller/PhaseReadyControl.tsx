import { useState } from "react";
import { Dropdown } from "react-bootstrap";
import { useLocal } from "../../util/local.ts";
import { toWorker } from "../../util/toWorker.ts";

// Header ready-up control, shown while connected to a sync room during a gated
// stage (draft lottery, draft, re-sign period, free agency). Gated steps only
// advance once EVERY user team has readied up; the button shows room readiness
// (e.g. "2/3") and the menu offers ready-through targets so a stretch of picks
// or free-agency days can run on its own.
const PhaseReadyControl = () => {
	const { mpPhaseReady, mpSyncActive } = useLocal([
		"mpPhaseReady",
		"mpSyncActive",
	]);
	const [busy, setBusy] = useState(false);

	if (!mpSyncActive || !mpPhaseReady) {
		return null;
	}

	const s = mpPhaseReady;

	const setReady = async (untilStep: number | null) => {
		setBusy(true);
		try {
			await toWorker("main", "draftSetReady", untilStep);
		} catch (error) {
			console.error(error);
		} finally {
			setBusy(false);
		}
	};

	const thruLabel =
		s.ready && s.myUntilStep !== undefined && s.myUntilStep > s.nextStep.number
			? (s.options.find((o) => o.step === s.myUntilStep)?.label ??
				(s.waypoints.find((w) => w.step === s.myUntilStep)?.label || ""))
			: "";

	return (
		<Dropdown className="mx-2 flex-shrink-0" align="end">
			<Dropdown.Toggle
				variant={s.ready ? "success" : "danger"}
				size="sm"
				disabled={busy}
				title={
					s.onClockUser
						? "A league-mate is on the clock"
						: "Advances once every team is ready"
				}
			>
				{s.ready ? "✓" : "Ready"} {s.readyTeams}/{s.totalTeams}
				{thruLabel ? (
					<span className="d-none d-lg-inline"> · {thruLabel}</span>
				) : null}
			</Dropdown.Toggle>
			<Dropdown.Menu>
				<Dropdown.Item
					onClick={() => setReady(s.nextStep.number)}
					disabled={busy}
				>
					{s.options.length > 0
						? `Ready for ${s.nextStep.label}`
						: `Ready: ${s.nextStep.label}`}
				</Dropdown.Item>
				{s.waypoints.map((w) => (
					<Dropdown.Item
						key={w.step}
						onClick={() => setReady(w.step)}
						disabled={busy}
					>
						{w.label}
					</Dropdown.Item>
				))}
				{s.options.length > 0 ? (
					<>
						<Dropdown.Divider />
						<Dropdown.Header>Ready through…</Dropdown.Header>
						<div style={{ maxHeight: 240, overflowY: "auto" }}>
							{s.options.map((o) => (
								<Dropdown.Item
									key={o.step}
									onClick={() => setReady(o.step)}
									disabled={busy}
								>
									{o.label}
									{o.mine ? " (my pick)" : ""}
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

export default PhaseReadyControl;
