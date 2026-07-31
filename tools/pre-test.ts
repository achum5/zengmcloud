import { existsSync } from "node:fs";
import { buildFacePrompt } from "./faceFromPhoto/buildPrompt.ts";

if (!existsSync("build/files/league-schema.json")) {
	const { createJsonSchemaFile } =
		await import("./build/createJsonSchemaFile.ts");
	await createJsonSchemaFile("test");
}

// The face editor imports the photo-conversion prompt as a module. Regenerate
// it from PROMPT.md, the file people actually edit, so the two can't drift.
buildFacePrompt();
