import { Modal } from "./Modal.tsx";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { appearanceForSeason } from "../../common/playerAppearance.ts";
import type { PlayerAppearance } from "../../common/playerAppearance.ts";
import type { FaceConfig } from "facesjs";

// Every season of a career, the way Basketball Reference stacks a player's
// headshots. Faces age now, so a fifteen-year career genuinely has something
// to show: the clean-shaven rookie, the year the beard arrived, the season the
// hairline started going.
//
// Seasons come from the caller (the player's ratings rows, which exist for
// every season he was in the league) rather than from the stored history -
// the history only holds the seasons that CHANGED, and a gallery built from
// those would skip most of the career.
export const PlayerAppearanceGallery = ({
	name,
	seasons,
	player,
	colors,
	jersey,
	onHide,
}: {
	name: string;
	seasons: number[];
	player: {
		face?: FaceConfig;
		imgURL?: string;
		appearances?: PlayerAppearance[];
	};
	colors?: [string, string, string];
	jersey?: string;
	onHide: () => void;
}) => {
	return (
		<Modal onHide={onHide} show size="lg" scrollable>
			<Modal.Header closeButton>
				<Modal.Title>{name}</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				<div className="d-flex flex-wrap gap-3">
					{seasons.map((season) => {
						const { face, imgURL } = appearanceForSeason(player, season);
						return (
							<div key={season} className="text-center">
								<div style={{ width: 90, height: 110 }}>
									<PlayerPicture
										face={face}
										imgURL={imgURL}
										colors={colors}
										jersey={jersey}
										lazy
									/>
								</div>
								<div className="text-body-secondary small">{season}</div>
							</div>
						);
					})}
				</div>
			</Modal.Body>
		</Modal>
	);
};
