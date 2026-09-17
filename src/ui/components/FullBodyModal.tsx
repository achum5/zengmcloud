import type { FaceConfig } from "facesjs";
import { Modal } from "./Modal.tsx";
import { FullBodyPlayer } from "./FullBodyPlayer.tsx";

// Clicking a face anywhere opens this: the player head to toe, in his team's
// uniform.
export const FullBodyModal = ({
	colors,
	face,
	jersey,
	jerseyNumber,
	hgt,
	name,
	onHide,
}: {
	colors?: [string, string, string];
	face: FaceConfig;
	jersey?: string;
	jerseyNumber?: string;
	hgt?: number;
	name?: string;
	onHide: () => void;
}) => (
	<Modal onHide={onHide} show>
		<Modal.Header closeButton>
			<Modal.Title>{name}</Modal.Title>
		</Modal.Header>
		<Modal.Body>
			<div style={{ maxWidth: 240 }} className="mx-auto">
				<FullBodyPlayer
					colors={colors}
					face={face}
					jersey={jersey}
					jerseyNumber={jerseyNumber}
					hgt={hgt}
				/>
			</div>
		</Modal.Body>
	</Modal>
);
