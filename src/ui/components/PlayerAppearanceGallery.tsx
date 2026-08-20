import clsx from "clsx";
import { Modal } from "./Modal.tsx";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { TeamLogoInline } from "./TeamLogoInline.tsx";
import { appearanceForSeason } from "../../common/playerAppearance.ts";
import type { PlayerAppearance } from "../../common/playerAppearance.ts";
import { DEFAULT_JERSEY, DEFAULT_TEAM_COLORS } from "../../common/constants.ts";
import { helpers } from "../util/helpers.ts";
import type { FaceConfig } from "facesjs";

// The team a player suited up for in one season, as that team looked THEN.
export type AppearanceTeam = {
	abbrev: string;
	colors: [string, string, string];
	// The team's mark that season. Optional because plenty of leagues have no
	// logos at all, in which case the stint heading falls back to a colour dot.
	imgURL?: string;
	imgURLSmall?: string;
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

// A teamless stretch at the very start is the years before he was drafted -
// the scouting pool, not a gap in a career. Later on, teamless means what it
// says: he was out of the league that year.
export const stintLabel = (stint: UniformStint, index: number): string => {
	if (stint.team) {
		return `${stint.team.region} ${stint.team.name}`;
	}
	return index === 0 ? "Draft prospect" : "No team";
};

// The stint heading's mark, logo or dot. One size for both so every team name
// starts at the same x down the modal, however many of them have a crest.
const MARK_SIZE = 22;

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
		pid: number;
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

					const logo = team?.imgURLSmall ?? team?.imgURL;

					return (
						<div key={i} className={i > 0 ? "mt-4" : undefined}>
							<div className="player-appearance-stint">
								{logo ? (
									<TeamLogoInline
										className="flex-shrink-0"
										imgURL={logo}
										size={MARK_SIZE}
									/>
								) : (
									<span
										className="player-appearance-mark"
										style={{ width: MARK_SIZE, height: MARK_SIZE }}
									>
										<span
											className="player-appearance-swatch"
											style={{
												backgroundColor: stintColors[0],
												borderColor: stintColors[2],
											}}
										/>
									</span>
								)}
								<span className="fw-bold text-truncate">
									{stintLabel(stint, i)}
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
									// Only a season he spent on a roster has a game log to open;
									// the years in the scouting pool have no games behind them.
									const Card = team ? "a" : "div";
									return (
										<Card
											key={season}
											href={
												team
													? helpers.leagueUrl([
															"player_game_log",
															player.pid,
															season,
														])
													: undefined
											}
											// Deliberately no onClick handler: the router picks the
											// click up off document, and closing the modal here could
											// detach the anchor before the event ever gets there. The
											// whole view unmounts on navigation anyway.
											title={team ? `${season} game log` : undefined}
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
										</Card>
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
