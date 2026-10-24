// THE FOOTBALL FIELD'S PLAYERS SCULPTED OFF THE PAGE: each instance of this
// worker makes the players' new pictures for the page as the game is shown
// (see field3d/farm.ts) - as the court's worker does for basketball (see
// src/court).
import "../common/polyfills.ts";
import {
	fieldFarmHand,
	type ToFieldHand,
} from "../ui/views/LiveGame/field3d/farm.ts";

const hand = fieldFarmHand((msg, transfer) => {
	self.postMessage(msg, { transfer });
});

self.onmessage = (event: MessageEvent<{ farm?: ToFieldHand }>) => {
	const msg = event.data?.farm;
	if (msg) {
		hand(msg);
	}
};
