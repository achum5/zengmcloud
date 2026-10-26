import type { FaceConfig } from "facesjs";
import { idb } from "../db/index.ts";

// The men in a five, to picture them: each one's face (or photo) and the name
// on his tag.
export type LineupPic = { face?: FaceConfig; imgURL?: string; name: string };

export const lineupPictures = async (
	pids: Iterable<number>,
): Promise<Record<number, LineupPic>> => {
	const want = [...new Set(pids)];
	const out: Record<number, LineupPic> = {};
	if (want.length === 0) {
		return out;
	}
	for (const p of await idb.merged.players.getAll({ pids: want })) {
		out[p.pid] = {
			face: p.face,
			imgURL: p.imgURL || undefined,
			name: p.lastName || p.firstName,
		};
	}
	return out;
};
