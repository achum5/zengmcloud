import type { Option } from "../../common/types.ts";
import { confirm } from "./confirm.tsx";
import { local } from "./local.ts";
import { getPlayMenuAdvanceWarning } from "./playMenuAdvanceWarning.ts";
import { toWorker } from "./toWorker.ts";

// A list of team names, as prose: "Boston", "Boston and Sacramento",
// "Boston, Sacramento and Phoenix".
const nameList = (names: string[]) =>
	names.length <= 1
		? (names[0] ?? "")
		: `${names.slice(0, -1).join(", ")} and ${names.at(-1)}`;

// The confirmation in front of anything that runs the shared league: the Play
// menu, the command palette's copies of it, and the draft page's advance
// buttons. Which clicks get one, and why the rest are left alone, is
// getPlayMenuAdvanceWarning's business - this only asks.
//
// Both look alarming on purpose (red header, red button, Cancel focused rather
// than OK): the mistake being caught is a click made without looking, so a
// dialog that can be dismissed the same way would catch nothing.
export const confirmPlayMenuAdvance = async (
	option: Pick<Option, "id" | "label" | "url">,
): Promise<boolean> => {
	const { mpSyncActive, mpPhaseReady } = local.getState();
	const warning = getPlayMenuAdvanceWarning({
		option,
		mpSyncActive,
		mpPhaseReady,
	});
	if (!warning) {
		return true;
	}

	if (warning.kind === "phaseAdvance") {
		return confirm(
			<>
				<b>{warning.action}</b> moves every team in the league to the next
				phase.
			</>,
			{
				title: "Advance the whole league?",
				danger: true,
				okText: "Advance",
				cancelText: "Cancel",
			},
		);
	}

	const confirmed = await confirm(
		<>
			<p>
				{warning.readyTeams} of {warning.totalTeams} teams are ready.
				{warning.notReady.length > 0
					? ` Waiting on ${nameList(warning.notReady)}.`
					: ""}
				{warning.onClock.length > 0
					? ` ${nameList(warning.onClock)} on the clock.`
					: ""}
			</p>
			<p className="mb-0">
				<b>{warning.action}</b> runs it for the whole league anyway.
			</p>
		</>,
		{
			title: "Not everyone is readied up",
			danger: true,
			okText: "Advance anyway",
			cancelText: "Cancel",
		},
	);

	// Saying yes here is what gets a room past a sim stop. The ordinary sim path
	// refuses to cross one on its own - deliberately, so that simming harder is
	// never a way around the room - which left a league stranded whenever one
	// person could not get to their phone. This is the deliberate act that
	// releases it: the dialog above named exactly who is being stepped over.
	//
	// One shot, consumed by the next sim, and harmless when the confirmed action
	// is not a sim at all.
	if (confirmed) {
		try {
			await toWorker("main", "allowCrossingNextSimStop", undefined);
		} catch {
			// If the worker can't be told, the sim simply stops as it would have.
		}
	}

	return confirmed;
};
