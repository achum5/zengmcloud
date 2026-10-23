// THE 3D COURT STAGED OFF THE PAGE. A whole game takes the director seconds
// to stage (see director.ts) - longer on a phone - and staged on the page's
// own thread nothing on the page moves while it is. So it is staged here, in
// a worker of its own (see stageCourt.ts). Other instances of it make the
// players' new pictures for the page as the game is shown (see farm.ts).
import "../common/polyfills.ts";
import {
	compileCourt,
	type CourtInput,
} from "../ui/views/LiveGame/court3d/director.ts";
import { farmHand, type ToHand } from "../ui/views/LiveGame/court3d/farm.ts";

// Up and running (see stageCourt.ts).
self.postMessage({ ready: true });

const hand = farmHand((msg, transfer) => {
	self.postMessage(msg, { transfer });
});

self.onmessage = (
	event: MessageEvent<{ id: number; input: CourtInput } | { farm: ToHand }>,
) => {
	if ("farm" in event.data) {
		hand(event.data.farm);
		return;
	}
	const { id, input } = event.data;
	try {
		self.postMessage({ id, timeline: compileCourt(input) });
	} catch (error) {
		self.postMessage({
			id,
			error:
				error instanceof Error ? (error.stack ?? error.message) : String(error),
		});
	}
};
