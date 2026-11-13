import { useState } from "react";
import { HeadlineScene } from "../views/LiveGame/court3d/HeadlineScene.tsx";
import { Modal } from "./Modal.tsx";
import { PlayButton } from "./PlayButton.tsx";

// A drafted player's walk-out (see HeadlineScene), from his row of a draft:
// the 3D court is fetched only once this is pressed.
export const DraftWalkButton = ({
	pid,
	name,
}: {
	pid: number;
	name: string;
}) => {
	const [show, setShow] = useState(false);
	return (
		<>
			<PlayButton
				className="ms-2"
				onClick={() => {
					setShow(true);
				}}
				title={`Watch ${name} walk out`}
			/>
			{show ? (
				<Modal
					centered
					show
					size="lg"
					onHide={() => {
						setShow(false);
					}}
				>
					<Modal.Header closeButton>
						<Modal.Title>{name}</Modal.Title>
					</Modal.Header>
					<Modal.Body className="p-2">
						<HeadlineScene scene={{ type: "draftPick", pid }} playing />
					</Modal.Body>
				</Modal>
			) : null}
		</>
	);
};
