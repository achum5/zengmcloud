import fs from "node:fs/promises";
import path from "node:path";
import { stripVTControlCharacters } from "node:util";
import type { RolldownPlugin } from "rolldown";
import { FOLDER, type BundleName } from "../rolldownConfig.ts";

export const startEnd = ({
	name,
	postMessage,
}: {
	name: BundleName;
	postMessage: (message: unknown) => void;
}): RolldownPlugin => {
	return {
		name: "start-end",

		buildStart() {
			postMessage({
				type: "start",
			});
		},

		async buildEnd(error) {
			if (error) {
				postMessage({
					type: "error",
					error,
				});

				const js = `throw new Error(${JSON.stringify(stripVTControlCharacters(error.message))})`;
				await fs.writeFile(path.join("build", FOLDER, `${name}.js`), js);
			}
		},

		writeBundle() {
			postMessage({
				type: "end",
			});
		},
	};
};
