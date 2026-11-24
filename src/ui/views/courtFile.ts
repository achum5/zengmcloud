import type { CourtStyle } from "../../common/types.ts";
import { checkCourt, mapCourtURLs } from "../../common/courtFile.ts";
import { PIC } from "../components/CourtImageField.tsx";
import { toWorker } from "../util/toWorker.ts";

export { PICTURE_KEYS } from "../../common/courtFile.ts";

// A COURT AS A FILE: the court editor's JSON, checked field by field (see
// common/courtFile.ts), and the pictures in it kept in the league as
// uploads are.

// A court typed or pasted in as JSON, checked field by field - or why not.
export const parseCourt = (text: string): CourtStyle | string => {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch {
		return "Not valid JSON.";
	}
	return checkCourt(raw);
};

// A court's pictures that are written into it (data URLs) kept in the
// league, the court naming them by id - those too big to keep stay in it.
// The ids kept, and the pictures by id.
export const keepCourtPictures = async (
	court: CourtStyle,
): Promise<{
	court: CourtStyle;
	ids: string[];
	pictures: Record<string, string>;
}> => {
	const ids: string[] = [];
	const pictures: Record<string, string> = {};
	const out = await mapCourtURLs(court, async (url) => {
		if (!url.startsWith("data:image/")) {
			return url;
		}
		try {
			const id = await toWorker("main", "storeCourtPicture", url);
			ids.push(id);
			pictures[id] = url;
			return `${PIC}${id}`;
		} catch {
			return url;
		}
	});
	return { court: out, ids, pictures };
};
