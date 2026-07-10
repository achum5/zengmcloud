import { useState } from "react";
import { toWorker } from "../../util/toWorker.ts";
import { confirm } from "../../util/confirm.tsx";
import { useLocal } from "../../util/local.ts";

export const DraftButtons = ({
	spectator,
	userRemaining,
	usersTurn,
	mpBlocked,
}: {
	spectator: boolean;
	userRemaining: boolean;
	usersTurn: boolean;
	// Synced, but this device isn't the simmer: it may draft its own player but
	// not advance the shared draft past other teams' picks.
	mpBlocked: boolean;
}) => {
	// This doesn't capture everything, since things can be triggered from outside of this component, but it's something
	const [running, setRunning] = useState(false);

	const { mpSyncActive } = useLocal(["mpSyncActive"]);

	// In a synced league, advancing the shared draft is irreversible for the
	// whole room - confirm so the simmer can't fat-finger past someone's pick.
	const confirmAdvance = async (message: string): Promise<boolean> => {
		if (!mpSyncActive) {
			return true;
		}
		return confirm(message, { okText: "Sim", cancelText: "Cancel" });
	};

	// The simmer drives the draft; a follower's advance buttons would just error
	// out against the sim authority guard, so hide them entirely.
	if (mpBlocked) {
		return null;
	}

	return (
		<div className="btn-group">
			<button
				className="btn btn-light-bordered"
				disabled={(usersTurn && !spectator) || running}
				onClick={async () => {
					try {
						setRunning(true);
						if (!(await confirmAdvance("Sim one pick?"))) {
							return;
						}
						await toWorker("playMenu", "onePick", undefined);
					} finally {
						setRunning(false);
					}
				}}
			>
				Sim one pick
			</button>
			<button
				className="btn btn-light-bordered"
				disabled={(usersTurn && !spectator) || !userRemaining || running}
				onClick={async () => {
					try {
						setRunning(true);
						if (!(await confirmAdvance("Sim to your next pick?"))) {
							return;
						}
						await toWorker("playMenu", "untilYourNextPick", undefined);
					} finally {
						setRunning(false);
					}
				}}
			>
				To your next pick
			</button>
			<button
				className="btn btn-light-bordered"
				disabled={running}
				onClick={async () => {
					try {
						setRunning(true);
						if (!(await confirmAdvance("Sim to the end of the draft?"))) {
							return;
						}
						if (userRemaining && !spectator) {
							const result = await confirm(
								"If you proceed, the AI will make your remaining picks for you. Are you sure?",
								{
									okText: "Let AI finish the draft",
									cancelText: "Cancel",
								},
							);

							if (!result) {
								return;
							}
						}
						await toWorker("playMenu", "untilEnd", undefined);
					} finally {
						setRunning(false);
					}
				}}
			>
				To end of draft
			</button>
		</div>
	);
};
