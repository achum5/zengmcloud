import { safeLocalStorage } from "../../util/safeLocalStorage.ts";

// (Told when a table's settings change, for another table of the same name
// on the page - see DataTable's syncSettings.)
export const SETTINGS_EVENT = "bbgm-datatable-settings";
const tell = (name: string) => {
	if (typeof window !== "undefined") {
		setTimeout(() => {
			window.dispatchEvent(new CustomEvent(SETTINGS_EVENT, { detail: name }));
		}, 0);
	}
};

class SettingsCache {
	disabled: boolean;

	name: string;

	constructor(name: string, disabled: boolean) {
		this.name = name;
		this.disabled = disabled;
	}

	set(key: string, value: unknown) {
		if (this.disabled) {
			return;
		}

		safeLocalStorage.setItem(`${key}:${this.name}`, JSON.stringify(value));
		tell(this.name);
	}

	get(key: string) {
		if (this.disabled) {
			return;
		}

		const raw = safeLocalStorage.getItem(`${key}:${this.name}`);
		if (raw === null) {
			return;
		}

		try {
			return JSON.parse(raw);
		} catch {
			return;
		}
	}

	clear(key: string) {
		if (this.disabled) {
			return;
		}

		safeLocalStorage.removeItem(`${key}:${this.name}`);
		tell(this.name);
	}
}

export default SettingsCache;
