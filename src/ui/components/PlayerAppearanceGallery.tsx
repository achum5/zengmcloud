import clsx from "clsx";
import { Modal } from "./Modal.tsx";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { appearanceForSeason } from "../../common/playerAppearance.ts";
import type { PlayerAppearance } from "../../common/playerAppearance.ts";
import { DEFAULT_JERSEY, DEFAULT_TEAM_COLORS } from "../../common/constants.ts";
import type { FaceConfig } from "facesjs";

// The team a player suited up for in one season, as that team looked THEN.
export type AppearanceTeam = {
	abbrev: string;
	colors: [string, string, string];
	jersey?: string;
	name: string;
	region: string;
	jerseyNumber?: string;
};

export type UniformStint = {
	team?: AppearanceTeam;
	seasons: number[];
};

// Consecutive seasons in the same uniform.
//
// Without this the gallery is one undifferentiated wall of heads, and the thing
// it is actually showing - a career in four jerseys - is the part you have to
// squint to find. Grouped, the stints ARE the structure.
//
// The key includes the jersey NUMBER as well as the team, which is how the
// jersey numbers already group on the player page: changing number is a
// different-looking season, and splitting there keeps the two displays telling
// the same story.
export const groupSeasonsByUniform = (
	seasons: number[],
	teams: Record<number, AppearanceTeam | undefined> | undefined,
): UniformStint[] => {
	const stints: UniformStint[] = [];
	let prevKey: string | undefined;
	for (const season of seasons) {
		const team = teams?.[season];
		const key = team
			? JSON.stringify([
					team.region,
					team.name,
					team.abbrev,
					team.colors,
					team.jersey,
					team.jerseyNumber,
				])
			: "";
		if (stints.length === 0 || key !== prevKey) {
			stints.push({ team, seasons: [season] });
		} else {
			stints.at(-1)!.seasons.push(season);
		}
		prevKey = key;
	}
	return stints;
};

export const formatSeasonRange = (seasons: number[]): string => {
	const start = seasons[0];
	const end = seasons.at(-1);
	if (start === undefined || end === undefined) {
		return "";
	}
	return start === end ? String(start) : `${start}-${end}`;
};

// Every season of a career, the way Basketball Reference stacks a player's
// headshots. Faces age now, so a fifteen-year career genuinely has something
// to show: the clean-shaven rookie, the year the beard arrived, the season the
// hairline started going - and the year the uniform changed.
//
// Seasons come from the caller (the player's ratings rows, which exist for
// every season he was in the league) rather than from the stored history -
// the history only holds the seasons that CHANGED, and a gallery built from
// those would skip most of the career.
export const PlayerAppearanceGallery = ({
	name,
	seasons,
	player,
	teams,
	highlightSeason,
	onHide,
}: {
	name: string;
	seasons: number[];
	player: {
		face?: FaceConfig;
		imgURL?: string;
		appearances?: PlayerAppearance[];
	};
	// Uniform per season. A season with no entry is one he spent on nobody's
	// roster - a draft prospect's years in the scouting pool - and it draws in
	// the neutral default rather than being papered over with whatever team he
	// happens to be on today.
	teams?: Record<number, AppearanceTeam | undefined>;
	highlightSeason?: number;
	onHide: () => void;
}) => {
	const stints = groupSeasonsByUniform(seasons, teams);

	return (
		<Modal onHide={onHide} show size="lg" scrollable>
			<Modal.Header closeButton>
				<Modal.Title className="d-flex align-items-baseline gap-2">
					{name}
					<span className="fs-6 fw-normal text-body-secondary">
						{formatSeasonRange(seasons)}
					</span>
				</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				{stints.map((stint, i) => {
					const team = stint.team;
					const stintColors = team?.colors ?? DEFAULT_TEAM_COLORS;
					const stintJersey = team?.jersey ?? DEFAULT_JERSEY;

					return (
						<div key={i} className={i > 0 ? "mt-4" : undefined}>
							<div className="player-appearance-stint">
								<span
									className="player-appearance-swatch"
									style={{
										backgroundColor: stintColors[0],
										borderColor: stintColors[2],
									}}
								/>
								<span className="fw-bold text-truncate">
									{team ? `${team.region} ${team.name}` : "No team"}
								</span>
								{team?.jerseyNumber ? (
									<span className="text-body-secondary">
										#{team.jerseyNumber}
									</span>
								) : null}
								<span className="player-appearance-rule" />
								<span className="text-body-secondary flex-shrink-0">
									{formatSeasonRange(stint.seasons)}
								</span>
							</div>
							<div className="player-appearance-grid">
								{stint.seasons.map((season) => {
									const { face, imgURL } = appearanceForSeason(player, season);
									return (
										<div
											key={season}
											className={clsx("player-appearance-card", {
												current: season === highlightSeason,
											})}
										>
											<div className="player-appearance-face">
												<PlayerPicture
													face={face}
													imgURL={imgURL}
													colors={stintColors}
													jersey={stintJersey}
													lazy
												/>
											</div>
											<div
												className="player-appearance-season"
												style={{
													backgroundColor: stintColors[0],
													borderTopColor: stintColors[2],
													color: stintColors[1],
												}}
											>
												{season}
											</div>
										</div>
									);
								})}
							</div>
						</div>
					);
				})}
			</Modal.Body>
		</Modal>
	);
};
