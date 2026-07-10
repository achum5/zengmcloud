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
	const { mpPhaseReady, mpSyncActive, mpEditsPaused, mpCatchUp } = useLocal([
		"mpPhaseReady",
		"mpSyncActive",
		"mpEditsPaused",
		"mpCatchUp",
	]);
	const [busy, setBusy] = useState(false);

	if (!mpSyncActive || !mpPhaseReady) {
		return null;
	}

	const s = mpPhaseReady;

	// While this device is behind (catching up) or the room is mid-advance,
	// revoking/reducing readiness would act on a stale world - it could halt a
	// chain of steps the room already agreed to. Readying UP stays allowed.
	const paused = Boolean(mpEditsPaused) || mpCatchUp !== undefined;
	const isReduction = (step: number | null) =>
		step === null || (s.myUntilStep !== undefined && step < s.myUntilStep);
	const itemDisabled = (step: number | null) =>
		busy || (paused && isReduction(step));

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
					disabled={itemDisabled(s.nextStep.number)}
					active={s.myUntilStep === s.nextStep.number}
				>
					{s.options.length > 0
						? `Ready for ${s.nextStep.label}`
						: `Ready: ${s.nextStep.label}`}
				</Dropdown.Item>
				{s.waypoints.map((w) => (
					<Dropdown.Item
						key={w.step}
						onClick={() => setReady(w.step)}
						disabled={itemDisabled(w.step)}
						active={s.myUntilStep === w.step}
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
									disabled={itemDisabled(o.step)}
									active={s.myUntilStep === o.step}
								>
									{o.label}
								</Dropdown.Item>
							))}
						</div>
					</>
				) : null}
				{s.ready ? (
					<>
						<Dropdown.Divider />
						<Dropdown.Item
							onClick={() => setReady(null)}
							disabled={itemDisabled(null)}
							title={paused ? "Catching up on league changes…" : undefined}
						>
							Not ready
						</Dropdown.Item>
					</>
				) : null}
			</Dropdown.Menu>
		</Dropdown>
	);
};

export default PhaseReadyControl;
