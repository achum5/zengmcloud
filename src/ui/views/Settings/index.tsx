import { useState } from "react";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import type { View } from "../../../common/types.ts";
import SettingsForm from "./SettingsForm.tsx";
import { showNotification } from "../../util/showNotification.ts";
import { toWorker } from "../../util/toWorker.ts";
import { useBlocker } from "../../hooks/useBlocker.ts";
import type { Settings as SettingsValues } from "../../../worker/views/settings.ts";
import { changedSettings } from "./changedSettings.ts";

const Settings = ({ initialSettings }: View<"settings">) => {
	useTitleBar({ title: "League Settings" });

	const { setDirty } = useBlocker();

	// What the form opened with, the baseline a save is measured against.
	const [opened, setOpened] = useState(initialSettings);

	return (
		<SettingsForm
			initialSettings={initialSettings}
			onUpdateExtra={() => {
				setDirty(true);
			}}
			onSave={async (settings) => {
				const saved = await toWorker(
					"main",
					"updateGameAttributesGodMode",
					changedSettings(opened, settings) as SettingsValues,
				);

				// In a shared league the multiplayer guard can refuse this before it
				// runs (not connected, still catching up, reconnecting) and say so
				// itself. Reporting success anyway is how a setting could be typed
				// in, confirmed saved, and be blank on the next visit.
				if (!saved) {
					showNotification({
						type: "error",
						text: "League settings were not saved.",
					});
					return;
				}

				setDirty(false);
				setOpened(settings);

				showNotification({
					type: "success",
					text: "League settings successfully updated.",
				});
			}}
		/>
	);
};

export default Settings;
