import type { Option } from "../../common/types.ts";
import { confirm } from "./confirm.tsx";
import { local } from "./local.ts";
import { getPlayMenuAdvanceWarning } from "./playMenuAdvanceWarning.ts";

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

	return confirm(
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
};
