import { stageRefs } from "./crew.ts";
import { compileCourt } from "./director.ts";
import type { Look } from "./figure.ts";
import { rim } from "./rim.ts";
import { sculpt } from "./sculpt.ts";
import type { CourtReply, CourtRequest } from "./sculptPool.ts";

// A WORKER FOR THE 3D COURT, off the page's own thread (see sculptPool.ts):
// it stages a whole game from its play-by-play (see director.ts) - seconds
// of work that would otherwise freeze the page as a game opens - and it
// sculpts players' sprites: each player's look once, then poses, each sent
// back as his body's pixels with the rim round them (the page puts his face
// on, which it draws).

const looks = new Map<number, Look>();

const post = (reply: CourtReply, transfer: ArrayBuffer[] = []) => {
	(
		self as unknown as {
			postMessage: (m: unknown, t: ArrayBuffer[]) => void;
		}
	).postMessage(reply, transfer);
};

self.onmessage = (e: MessageEvent<CourtRequest>) => {
	const m = e.data;
	if (m.type === "look") {
		looks.set(m.id, m.look);
		return;
	}
	if (m.type === "compile") {
		let tl;
		try {
			tl = compileCourt(m.input);
		} catch {
			post({ id: m.id });
			post({ id: m.refsId });
			return;
		}
		post({ id: m.id, tl });
		// Then where the officials go, the whole game long: seconds more, which
		// the page works out bit by bit as it plays until these are in.
		try {
			stageRefs(tl);
			post({ id: m.refsId, refs: tl.refs });
		} catch {
			post({ id: m.refsId });
		}
		return;
	}
	const look = looks.get(m.lookId);
	if (!look) {
		post({ id: m.id });
		return;
	}
	try {
		const { w, h } = m.job;
		const made = sculpt(
			m.job.cam,
			m.job.st,
			m.job.body,
			look,
			m.job.px,
			m.job.ox,
			m.job.oy,
			w,
			h,
		);
		rim(made.img.data, w, h);
		if (made.over) {
			rim(made.over.data, w, h);
		}
		post(
			{ id: m.id, made },
			made.over
				? [
						made.img.data.buffer as ArrayBuffer,
						made.over.data.buffer as ArrayBuffer,
					]
				: [made.img.data.buffer as ArrayBuffer],
		);
	} catch {
		post({ id: m.id });
	}
};
