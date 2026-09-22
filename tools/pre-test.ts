import { existsSync } from "node:fs";
import { buildFacePrompt } from "./faceFromPhoto/buildPrompt.ts";
import { readFile } from "node:fs/promises";

const FILENAME = "build/files/league-schema.json";

const makeFile = async () => {
	const { createJsonSchemaFile } =
		await import("./build/createJsonSchemaFile.ts");
	await createJsonSchemaFile("test");
};

if (!existsSync(FILENAME)) {
	console.log("[pre-test] No league-schema.json found, creating...");
	await makeFile();
} else {
	const text = await readFile(FILENAME, "utf8");
	try {
		JSON.parse(text);
	} catch {
		// Invalid JSON in file somehow
		console.log("[pre-test] Invalid league-schema.json found, replacing...");
		await makeFile();
	}
}

// The face editor imports the photo-conversion prompt as a module. Regenerate
// it from PROMPT.md, the file people actually edit, so the two can't drift.
buildFacePrompt();
