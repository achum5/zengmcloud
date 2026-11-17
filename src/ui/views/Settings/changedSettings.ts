import type { Settings as SettingsValues } from "../../../worker/views/settings.ts";

// The settings changed in the form since it opened (or was last saved). In a
// shared league a league mate's change can arrive while the form is open,
// and the form keeps the values it opened with: saving all of them wrote the
// old ones back over it.
export const changedSettings = (
	opened: SettingsValues,
	saving: SettingsValues,
) => {
	const changed: Partial<SettingsValues> = {};
	for (const key of Object.keys(saving) as (keyof SettingsValues)[]) {
		if (JSON.stringify(saving[key]) !== JSON.stringify(opened[key])) {
			(changed as any)[key] = saving[key];
		}
	}
	return changed;
};
