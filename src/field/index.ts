// THE 3D FOOTBALL FIELD OFF THE PAGE, as the court's worker is for
// basketball (see src/court). A whole game takes the director a good second
// to stage (see field3d/director.ts) - longer on a phone - and staged on the
// page's own thread nothing on the page moves while it is. So it is staged
// here (see field3d/stageField.ts). Other instances of it make the
// players' new pictures for the page as the game is shown (see
// field3d/farm.ts).
import "../common/polyfills.ts";
import {
	compileField,
	type FieldInput,
} from "../ui/views/LiveGame/field3d/director.ts";
import {
	fieldFarmHand,
	type ToFieldHand,
} from "../ui/views/LiveGame/field3d/farm.ts";

// Up and running (see stageField.ts).
self.postMessage({ ready: true });

const hand = fieldFarmHand((msg, transfer) => {
	self.postMessage(msg, { transfer });
});

self.onmessage = (
	event: MessageEvent<
		{ id: number; input: FieldInput } | { farm?: ToFieldHand }
	>,
) => {
	if ("farm" in event.data) {
		if (event.data.farm) {
			hand(event.data.farm);
		}
		return;
	}
	if (!("input" in event.data)) {
		return;
	}
	const { id, input } = event.data;
	try {
		self.postMessage({ id, timeline: compileField(input) });
	} catch (error) {
		self.postMessage({
			id,
			error:
				error instanceof Error ? (error.stack ?? error.message) : String(error),
		});
	}
};
