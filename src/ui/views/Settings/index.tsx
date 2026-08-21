import useTitleBar from "../../hooks/useTitleBar.tsx";
import type { View } from "../../../common/types.ts";
import SettingsForm from "./SettingsForm.tsx";
import { showNotification } from "../../util/showNotification.ts";
import { toWorker } from "../../util/toWorker.ts";
import { useBlocker } from "../../hooks/useBlocker.ts";

const Settings = ({ initialSettings }: View<"settings">) => {
	useTitleBar({ title: "League Settings" });

	const { setDirty } = useBlocker();

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
					settings,
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

				showNotification({
					type: "success",
					text: "League settings successfully updated.",
				});
			}}
		/>
	);
};

export default Settings;
