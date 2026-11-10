// THE 3D COURT STAGED OFF THE PAGE. A whole game takes the director seconds
// to stage (see director.ts) - longer on a phone - and staged on the page's
// own thread nothing on the page moves while it is. So it is staged here, in
// a worker of its own (see stageCourt.ts). Other instances of it make the
// players' new pictures for the page as the game is shown (see farm.ts).
import "../common/polyfills.ts";
import { farmHand } from "../ui/views/LiveGame/court3d/farm.ts";
import { serveStaging } from "../ui/views/LiveGame/court3d/stager.ts";
import { stageGame } from "../ui/views/LiveGame/court3d/stepper.ts";

serveStaging(
	stageGame,
	farmHand((msg, transfer) => {
		self.postMessage(msg, { transfer });
	}),
);
