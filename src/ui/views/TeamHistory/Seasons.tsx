import { RecordAndPlayoffs } from "../../components/RecordAndPlayoffs.tsx";
import type { View } from "../../../common/types.ts";

const Seasons = ({
	history,
	userSeasons,
}: Pick<View<"teamHistory">, "history" | "userSeasons">) => {
	const userSeasonsSet = new Set(userSeasons);

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
			// eslint-disable-next-line react-hooks/immutability -- https://github.com/react/react/issues/31569
			prevName = h.name;
		}

		// If a team was inactive for some number of seasons, add some vertical space in the gap
		const gap = i > 0 && h.season + 1 < history[i - 1]!.season;

		return (
			<div key={h.season} className={gap && !newName ? "mt-2" : undefined}>
				{newName ? (
					<h4 className={i > 0 ? "mt-2" : undefined}>{newName}</h4>
				) : null}
				<div>
					{userSeasonsSet.has(h.season) ? (
						<span
							className="table-info legend-square me-1"
							title="You were the GM"
						/>
					) : null}
					{recordAndPlayoffs}
				</div>
			</div>
		);
	});

	return (
		<>
			{historySeasons}
			{userSeasons.length > 0 ? (
				<p className="mt-2 text-body-secondary">
					Seasons when you were the GM are prefixed{" "}
					<span className="text-nowrap">
						with <span className="table-info legend-square" />
					</span>
				</p>
			) : null}
		</>
	);
};

export default Seasons;
