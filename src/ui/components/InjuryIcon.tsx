import { useId } from "react";
import { OverlayTrigger, Tooltip } from "react-bootstrap";
import { timeBetweenGames } from "../../common/timeBetweenGames.ts";
import { gamesOutText } from "../../common/gamesOut.ts";
import type { PlayerInjury } from "../../common/types.ts";
import { PHASE } from "../../common/constants.ts";
import { helpers } from "../util/helpers.ts";
import { useLocal } from "../util/local.ts";

export const InjuryIcon = ({
	asOfGame,
	className = "",
	injury,
}: {
	// The injury as it stood after a game (a game log), not now.
	asOfGame?: boolean;
	className?: string;
	injury?: PlayerInjury & {
		playingThrough?: boolean;
	};
}) => {
	const tooltipId = useId();
	const { phase } = useLocal(["phase"]);
	const offseason =
		!asOfGame && (phase <= PHASE.PRESEASON || phase > PHASE.PLAYOFFS);

	if (injury === undefined) {
		return null;
	}

	const colorClass = injury.playingThrough ? "warning" : "danger";

	let info;
	if (injury.gamesRemaining === -1) {
		// This is used in box scores, where it would be confusing to display "out X more days" in old box scores
		info = {
			title: injury.type,
			text: "+",
		};
	} else if (injury.gamesRemaining > 0 || injury.type !== "Healthy") {
		// On the NBA calendar, the calendar days it has left; in the offseason,
		// the days of next season it takes. And the games on his team's schedule
		// they take in.
		const days = injury.daysRemaining;
		const games = injury.gamesOut ? ` (${gamesOutText(injury.gamesOut)})` : "";
		let title =
			days === undefined
				? `${injury.type}, out ${
						injury.gamesRemaining
					} more ${timeBetweenGames(injury.gamesRemaining)}`
				: offseason
					? `${injury.type}, out the first ${days} ${helpers.plural("day", days)} of the regular season${games}`
					: `${injury.type}, out ${days} more ${helpers.plural("day", days)}${games}`;
		if (injury.playingThrough) {
			title += ", played through injury";
		}

		info = {
			title,
			text: days ?? injury.gamesRemaining,
		};
	}

	if (info) {
		return (
			<OverlayTrigger
				trigger={["hover", "focus", "click"]}
				overlay={<Tooltip id={tooltipId}>{info.title}</Tooltip>}
			>
				<span className={`badge bg-${colorClass} badge-injury ${className}`}>
					{info.text}
				</span>
			</OverlayTrigger>
		);
	}

	return null;
};
