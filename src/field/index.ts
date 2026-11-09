// THE 3D FOOTBALL FIELD OFF THE PAGE, as the court's worker is for
// basketball (see src/court). A whole game takes the director a good second
// to stage (see field3d/director.ts) - longer on a phone - and staged on the
// page's own thread nothing on the page moves while it is. So it is staged
// here (see field3d/stageField.ts). Other instances of it make the
// players' new pictures for the page as the game is shown (see
// field3d/farm.ts).
import "../common/polyfills.ts";
import { serveStaging } from "../ui/views/LiveGame/court3d/stager.ts";
import { compileField } from "../ui/views/LiveGame/field3d/director.ts";
import { fieldFarmHand } from "../ui/views/LiveGame/field3d/farm.ts";

serveStaging(
	compileField,
	fieldFarmHand((msg, transfer) => {
		self.postMessage(msg, { transfer });
	}),
);
