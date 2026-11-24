import { smartBrainOn } from "../core/realisticFrontOffice/isOn.ts";
import { local } from "../util/index.ts";
import { defineView } from "../util/defineView.ts";

export default defineView({
	id: "dangerZone",
	load: () => {
		return {
			autoSave: local.autoSave,
			smartCpu: smartBrainOn(),
		};
	},
});
