import { assert, test } from "vitest";
import { changedSettings } from "./changedSettings.ts";

test("a save sends only what changed since the form opened", () => {
	const opened = {
		difficulty: 3,
		difficultyTrade: 3.5,
		smartCpu: true,
		injuries: [{ name: "Sprained ankle", frequency: 1, games: 4 }],
	} as any;
	// A league mate turned Smart CPU off meanwhile; this form still shows it on
	// and changes only the trade difficulty.
	const saving = { ...opened, difficultyTrade: 4 };
	assert.deepEqual(changedSettings(opened, saving), { difficultyTrade: 4 });
	assert.deepEqual(changedSettings(opened, { ...opened }), {});
	assert.deepEqual(
		changedSettings(opened, {
			...opened,
			injuries: [{ name: "Sprained ankle", frequency: 2, games: 4 }],
		}),
		{ injuries: [{ name: "Sprained ankle", frequency: 2, games: 4 }] },
	);
});
