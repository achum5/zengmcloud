import { useEffect, useRef, useState } from "react";
import { useLocal } from "../../util/local.ts";
import { toWorker } from "../../util/toWorker.ts";
import { parseLiveGameView } from "../../views/LiveGame/court3d/setting.ts";
import { Modal } from "../Modal.tsx";

// How long to keep asking the worker for the game being offered while its
// play-by-play is still on its way down (ms between asks, and how many).
const PREVIEW_EVERY = 1500;
const PREVIEW_TRIES = 40;

// A league-mate is simming a game live: watch it, or not. Its result stays
// hidden until this is answered, so there is no closing it any other way.
export const LiveOfferModal = () => {
	const { gender, mpLiveOffer } = useLocal(["gender", "mpLiveOffer"]);
	// The broadcast already answered here, so the popup goes the moment it is
	// clicked rather than when the worker says so.
	const [answered, setAnswered] = useState<number | undefined>();
	// The one said yes to: its staging is the page's now.
	const watched = useRef<number | undefined>(undefined);

	const show = mpLiveOffer !== undefined && mpLiveOffer.startedAt !== answered;

	const answer = (watch: boolean) => {
		if (!mpLiveOffer) {
			return;
		}
		setAnswered(mpLiveOffer.startedAt);
		if (watch) {
			watched.current = mpLiveOffer.startedAt;
		}
		void toWorker("main", "answerLiveOffer", watch);
	};

	// Shown in 3D, the game is staged while the question is up, so a "Watch"
	// starts it rather than starting a minute of staging (see prestage.ts).
	// Not watched after all - turned down, or this device went live with a
	// game of its own - it is let go.
	const offeredGid = mpLiveOffer?.gid;
	const offered3d =
		__SPORT === "basketball" &&
		mpLiveOffer !== undefined &&
		parseLiveGameView(mpLiveOffer.view) === "3d"
			? mpLiveOffer.startedAt
			: undefined;
	useEffect(() => {
		if (offered3d === undefined || offeredGid === undefined) {
			return;
		}
		let alive = true;
		let started = false;
		void (async () => {
			for (let i = 0; i < PREVIEW_TRIES && alive; i++) {
				try {
					const preview = await toWorker(
						"main",
						"liveFollowPreview",
						undefined,
					);
					if (!alive) {
						return;
					}
					if (preview?.startedAt === offered3d) {
						const { prestageCourt } =
							await import("../../views/LiveGame/court3d/prestage.ts");
						if (alive) {
							started = true;
							prestageCourt(preview.events, preview.initialBoxScore, gender);
						}
						return;
					}
				} catch {
					// Asked again in a moment.
				}
				await new Promise((resolve) => setTimeout(resolve, PREVIEW_EVERY));
			}
		})();
		return () => {
			alive = false;
			if (started && watched.current !== offered3d) {
				void import("../../views/LiveGame/court3d/prestage.ts").then(
					({ dropPrestaged }) => {
						dropPrestaged(offeredGid);
					},
				);
			}
		};
	}, [offered3d, offeredGid, gender]);

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
