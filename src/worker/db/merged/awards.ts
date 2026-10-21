import { relabelAwardsFromSettings } from "../../../common/awards.ts";
import type { Awards } from "../../../common/types.ts";
import { g } from "../../util/index.ts";
import { normalizeAwardsRow } from "../normalizeAwardsRow.ts";
import { MergedStore, type GetAllOptions } from "./engine.ts";

// A season keeps the label its awards were given at the time; the settings say
// what they are called now. See relabelAwardsFromSettings.
const relabel = (row: Awards): Awards => {
	const awards = relabelAwardsFromSettings(row?.awards, g.get("awards"));
	return awards === undefined || awards === row?.awards
		? row
		: { ...row, awards };
};

// A row from before the custom-awards upgrade can still be sitting in the
// store; anything reading award history walks `awards.awards` and would die on
// it. See normalizeAwardsRow. Neither step mutates the stored row.
const normalize = (row: Awards) => relabel(normalizeAwardsRow(row));

class AwardsStore extends MergedStore<"awards"> {
	override async get(key: number) {
		const row = await super.get(key);
		return row === undefined ? undefined : normalize(row);
	}

	override async getAll<Output = Awards>(
		selector?: undefined,
		options: GetAllOptions<Awards, Output> = {},
	) {
		const { filter, map } = options;

		const output: Output[] = [];
		await this.forEach(selector, (row) => {
			if (filter && !filter(row)) {
				return;
			}

			// With no map function, Output is Awards
			output.push(map ? map(row) : (row as unknown as Output));
		});

		return output;
	}

	override async forEach(selector: undefined, callback: (row: Awards) => void) {
		await super.forEach(selector, (row) => {
			callback(normalize(row));
		});
	}
}

export default new AwardsStore("awards", { small: true });
