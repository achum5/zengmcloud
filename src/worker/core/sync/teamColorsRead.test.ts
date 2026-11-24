// TEAM COLORS IN A SHARED LEAGUE. The top bar asks for every team's colors
// the moment it shows, which in a shared league is while the device is still
// reconnecting. A call that opens a capture window is turned away then, and
// the UI kept the empty answer: no team colors anywhere for the whole visit
// (home nights on the scores bar black, the week strip's capsule slate).
// A pure read never opens one.

import { assert, describe, test } from "vitest";

describe("team colors in a shared league", () => {
	test("the color reads skip capture, so the guard never turns them away", async () => {
		const source: string = await import(("node" + ":fs") as any).then(
			(fs: any) =>
				fs.readFileSync(
					new URL("../../index.ts", import.meta.url).pathname,
					"utf8",
				),
		);
		const skipBlock = source.slice(
			source.indexOf("SKIP_CHANGESET_CAPTURE"),
			source.indexOf("const isChangesetSuppressedCall"),
		);
		assert.include(skipBlock, '"beforeView"');
		assert.include(skipBlock, '"getAllTeamColorsUI"');
		assert.include(skipBlock, '"getTeamColorsUI"');
	});
});
