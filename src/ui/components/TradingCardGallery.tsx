import { useEffect, useState } from "react";
import type { TradingCard } from "../../common/types.ts";
import { toWorker } from "../util/toWorker.ts";
import { helpers } from "../util/helpers.ts";

// A grid of trading cards, plus the fullscreen viewer they open into. Used on
// the Create Cards page and at the bottom of every player page, so the two
// stay identical without either one owning the behavior.
//
// The interaction is three clicks deep and deliberately has no instructions on
// screen: click a card to open it, click the open card to flip it, click off it
// to close.

export type GalleryCard = TradingCard & { playerName?: string };

const CardFaces = ({
	card,
	flipped,
}: {
	card: GalleryCard;
	flipped: boolean;
}) => (
	<div className={`trading-card-flipper-inner${flipped ? " flipped" : ""}`}>
		<div className="trading-card-face">
			<img src={card.frontURL} alt={card.title} />
		</div>
		<div className="trading-card-face trading-card-face-back">
			{card.backURL ? (
				<img src={card.backURL} alt={`${card.title} (back)`} />
			) : (
				<span className="text-white-50">No back image</span>
			)}
		</div>
	</div>
);

const Viewer = ({
	card,
	onClose,
}: {
	card: GalleryCard;
	onClose: () => void;
}) => {
	const [flipped, setFlipped] = useState(false);

	useEffect(() => {
		const onKeyDown = (event: KeyboardEvent) => {
			if (event.key === "Escape") {
				onClose();
			}
		};
		document.addEventListener("keydown", onKeyDown);
		return () => {
			document.removeEventListener("keydown", onKeyDown);
		};
	}, [onClose]);

	return (
		// Closing lives on the backdrop, so the click that flips the card must not
		// reach it.
		<div className="trading-card-viewer" onClick={onClose}>
			<div className="d-flex flex-column align-items-center">
				<div
					className="trading-card-flipper"
					onClick={(event) => {
						event.stopPropagation();
						setFlipped((prev) => !prev);
					}}
				>
					<CardFaces card={card} flipped={flipped} />
				</div>
				<div className="text-white-50 mt-2 text-center small">
					{card.playerName ? `${card.playerName} · ` : ""}
					{card.title}
				</div>
			</div>
		</div>
	);
};

export const TradingCardGallery = ({
	cards,
	showPlayerName,
	onDeleted,
}: {
	cards: GalleryCard[];
	showPlayerName?: boolean;
	onDeleted?: () => void;
}) => {
	const [viewing, setViewing] = useState<string | undefined>();

	if (cards.length === 0) {
		return <p className="text-body-secondary mb-0">None</p>;
	}

	const viewingCard = cards.find((card) => card.id === viewing);

	return (
		<>
			<div className="d-flex flex-wrap gap-3">
				{cards.map((card) => (
					<div key={card.id} className="trading-card-tile position-relative">
						<button
							type="button"
							className="btn p-0 border-0 w-100"
							onClick={() => {
								setViewing(card.id);
							}}
						>
							<img
								className="trading-card"
								src={card.frontURL}
								alt={card.title}
							/>
						</button>
						{onDeleted ? (
							<button
								type="button"
								className="btn btn-sm btn-danger trading-card-delete py-0 px-1"
								title="Delete card"
								onClick={async () => {
									await toWorker("main", "deleteTradingCard", card.id);
									onDeleted();
								}}
							>
								<span className="glyphicon glyphicon-remove" />
							</button>
						) : null}
						<div className="small mt-1 lh-sm">
							{showPlayerName && card.playerName ? (
								<div className="fw-bold text-truncate">
									<a href={helpers.leagueUrl(["player", card.pid])}>
										{card.playerName}
									</a>
								</div>
							) : null}
							<div className="text-body-secondary">{card.title}</div>
						</div>
					</div>
				))}
			</div>
			{viewingCard ? (
				<Viewer
					card={viewingCard}
					onClose={() => {
						setViewing(undefined);
					}}
				/>
			) : null}
		</>
	);
};
