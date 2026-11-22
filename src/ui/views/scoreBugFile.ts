import type { ScoreBugStyle } from "../../common/types.ts";
import { mapScoreBugURLs, parseScoreBug } from "../../common/scoreBug.ts";
import { PIC } from "../components/CourtImageField.tsx";
import { toWorker } from "../util/toWorker.ts";

// A SCORE BUG AS A FILE: the bug's JSON, checked, and the pictures and fonts
// written into it kept in the league as uploads are (one too big to keep
// stays in the bug).

export const parseScoreBugText = (text: string): ScoreBugStyle | string => {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch {
		return "Not valid JSON.";
	}
	return parseScoreBug(raw);
};

export const keepScoreBugPictures = async (
	bug: ScoreBugStyle,
): Promise<{
	bug: ScoreBugStyle;
	ids: string[];
	pictures: Record<string, string>;
}> => {
	const ids: string[] = [];
	const pictures: Record<string, string> = {};
	const out = await mapScoreBugURLs(bug, async (url) => {
		const font = url.startsWith("data:font/");
		if (!font && !url.startsWith("data:image/")) {
			return url;
		}
		try {
			const id = await toWorker(
				"main",
				font ? "storeScoreBugFont" : "storeCourtPicture",
				url,
			);
			ids.push(id);
			pictures[id] = url;
			return `${PIC}${id}`;
		} catch {
			return url;
		}
	});
	return { bug: out, ids, pictures };
};

const FONT_EXTENSIONS = ["woff2", "woff", "ttf", "otf"];

// A font file as a data URL the league keeps ("data:font/woff2;base64,..."),
// and the name it goes by - the file's.
export const fontFromFile = async (file: File) => {
	const ext = file.name.split(".").pop()?.toLowerCase() ?? "";
	if (!FONT_EXTENSIONS.includes(ext)) {
		throw new Error("Use a .woff2, .woff, .ttf or .otf file.");
	}
	const bytes = new Uint8Array(await file.arrayBuffer());
	let binary = "";
	for (let i = 0; i < bytes.length; i += 0x8000) {
		binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
	}
	return {
		name: file.name.replace(/\.[^.]+$/, ""),
		url: `data:font/${ext};base64,${btoa(binary)}`,
	};
};
