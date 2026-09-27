import { readFileSync } from "node:fs";
import { buildFacePrompt } from "./faceFromPhoto/buildPrompt.ts";

const FILENAME = "build/files/league-schema.json";

const makeFile = async () => {
	const { createJsonSchemaFile } =
		await import("./build/createJsonSchemaFile.ts");
	await createJsonSchemaFile("test");
};

try {
	const text = readFileSync(FILENAME, "utf8");
	try {
		JSON.parse(text);

		// Valid JSON, nothing else to do
	} catch {
		// Invalid JSON in file somehow
		console.log("[pre-test] Invalid league-schema.json found, replacing...");
		await makeFile();
	}
} catch (error) {
	if (error.code === "ENOENT") {
		console.log("[pre-test] No league-schema.json found, creating...");
		await makeFile();
	} else {
		throw error;
	}
}

// The face editor imports the photo-conversion prompt as a module. Regenerate
// it from PROMPT.md, the file people actually edit, so the two can't drift.
buildFacePrompt();
