import { useState } from "react";
import { useLocal } from "../../util/local.ts";
import { toWorker } from "../../util/toWorker.ts";
import { Modal } from "../Modal.tsx";

// A league-mate is simming a game live: watch it, or not. Its result stays
// hidden until this is answered, so there is no closing it any other way.
export const LiveOfferModal = () => {
	const { mpLiveOffer } = useLocal(["mpLiveOffer"]);
	// The broadcast already answered here, so the popup goes the moment it is
	// clicked rather than when the worker says so.
	const [answered, setAnswered] = useState<number | undefined>();

	const show = mpLiveOffer !== undefined && mpLiveOffer.startedAt !== answered;

	const answer = (watch: boolean) => {
		if (!mpLiveOffer) {
			return;
		}
		setAnswered(mpLiveOffer.startedAt);
		void toWorker("main", "answerLiveOffer", watch);
	};

	const label = mpLiveOffer?.label;

	return (
		<Modal show={show} backdrop="static" keyboard={false} centered>
			<Modal.Body
				className="fw-bold fs-5"
				title={mpLiveOffer ? `${mpLiveOffer.byName} is simming` : undefined}
			>
				{label ? `Watch ${label} live?` : "Watch the live game?"}
			</Modal.Body>
			<Modal.Footer>
				<button
					type="button"
					className="btn btn-secondary"
					onClick={() => {
						answer(false);
					}}
				>
					No
				</button>
				<button
					type="button"
					className="btn btn-primary"
					onClick={() => {
						answer(true);
					}}
				>
					Watch
				</button>
			</Modal.Footer>
		</Modal>
	);
};
