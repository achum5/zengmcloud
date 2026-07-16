import { RecordAndPlayoffs } from "../../components/RecordAndPlayoffs.tsx";
import type { View } from "../../../common/types.ts";

const Seasons = ({ history }: Pick<View<"teamHistory">, "history">) => {
	const numTeamNames = new Set(
		history.map((h) => h.name).filter((name) => name !== undefined),
	).size;

	let prevName = numTeamNames === 1 ? history[0]!.name : undefined;
	const historySeasons = history.map((h, i) => {
		const recordAndPlayoffs = (
			<RecordAndPlayoffs
				abbrev={h.abbrev}
				className={
					h.playoffRoundsWon === h.numPlayoffRounds ? "fw-bold" : undefined
				}
				lost={h.lost}
				otl={h.otl}
				roundsWonText={h.roundsWonText}
				season={h.season}
				tid={h.tid}
				tied={h.tied}
				won={h.won}
			/>
		);

		let newName;
		if (h.name && prevName !== h.name) {
			newName = h.name;
			prevName = h.name;
		}

		// If a team was inactive for some number of seasons, add some vertical space in the gap
		const gap = i > 0 && h.season + 1 < history[i - 1]!.season;

		return (
			<div key={h.season} className={gap && !newName ? "mt-2" : undefined}>
				{newName ? (
					<h4 className={i > 0 ? "mt-2" : undefined}>{newName}</h4>
				) : null}
				{recordAndPlayoffs}
			</div>
		);
	});

	return <>{historySeasons}</>;
};

export default Seasons;
