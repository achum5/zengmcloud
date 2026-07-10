import { forwardRef, useState } from "react";
import { ButtonGroup, Dropdown } from "react-bootstrap";
import { useLocal } from "../../util/local.ts";
import { toWorker } from "../../util/toWorker.ts";

// Toggle for the people-icon extension: a plain button with no caret, so it
// reads as a single icon joined to the ready button.
const PeopleToggle = forwardRef<
	HTMLButtonElement,
	{ onClick?: (e: React.MouseEvent) => void; variant: string }
>(({ onClick, variant }, ref) => (
	<button
		ref={ref}
		type="button"
		className={`btn btn-${variant} btn-sm`}
		onClick={onClick}
		title="Ready-up status by team"
		aria-label="Ready-up status by team"
	>
		👥
	</button>
));

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

	const variant = s.ready ? "success" : "danger";
	const teams = s.teams ?? [];

	return (
		<ButtonGroup className="mx-2 flex-shrink-0">
			<Dropdown as={ButtonGroup} align="end">
				<Dropdown.Toggle as={PeopleToggle} variant={variant} />
				<Dropdown.Menu>
					<Dropdown.Header>Ready-up status</Dropdown.Header>
					{teams.map((t) => (
						<Dropdown.ItemText
							key={t.tid}
							className="d-flex justify-content-between gap-3"
						>
							<span className="text-nowrap">{t.name}</span>
							<span
								className={`text-nowrap ${
									t.ready
										? "text-success"
										: t.onClock
											? "text-warning"
											: "text-danger"
								}`}
							>
								{t.ready ? "✓ Ready" : t.onClock ? "On the clock" : "Not ready"}
							</span>
						</Dropdown.ItemText>
					))}
				</Dropdown.Menu>
			</Dropdown>
			<Dropdown as={ButtonGroup} align="end">
				<Dropdown.Toggle
					variant={variant}
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
		</ButtonGroup>
	);
};

export default PhaseReadyControl;
