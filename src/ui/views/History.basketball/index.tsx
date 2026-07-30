import { MoreLinks } from "../../components/MoreLinks.tsx";
import { RetiredPlayers } from "../../components/RetiredPlayers.tsx";
import { SeasonRecap } from "../../components/SeasonRecap.tsx";
import { PlayerRecaps } from "../../components/PlayerRecaps.tsx";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import AwardsAndChamp from "./AwardsAndChamp.tsx";
import Team from "./Team.tsx";
import type { View } from "../../../common/types.ts";
import { useLocal } from "../../util/local.ts";
export type ActualProps = Exclude<
	View<"history">,
	{ invalidSeason: true; season: number }
> & { userTid: number };

const History = (props: View<"history">) => {
	const { invalidSeason, season } = props;

	useTitleBar({
		title: "Season Summary",
		jumpTo: true,
		jumpToSeason: season,
		dropdownView: "history",
		dropdownFields: {
			seasonsHistory: season,
		},
	});
	const { userTid } = useLocal(["userTid"]);

	if (invalidSeason) {
		return (
			<>
				<h2>Error</h2>
				<p>Invalid season.</p>
			</>
		);
	}

	const { awards, champ, confs, retiredPlayers, retiredStat } =
		props as ActualProps;

	return (
		<>
			<MoreLinks type="awards" page="history" season={season} />

			<div className="row">
				<div className="col-md-3 col-sm-4 col-12">
					<AwardsAndChamp
						awards={awards}
						champ={champ}
						confs={confs}
						season={season}
						userTid={userTid}
					/>
				</div>
				<div className="col-xl-2 col-md-3 col-sm-4 col-6">
					<Team
						name="All-League Teams"
						nested
						season={season}
						team={awards.allLeague}
						userTid={userTid}
					/>
				</div>
				<div className="col-xl-2 col-md-3 col-sm-4 col-6">
					<Team
						className="mb-3"
						name="All-Defensive Teams"
						nested
						season={season}
						team={awards.allDefensive}
						userTid={userTid}
					/>
					<Team
						className="mb-3"
						name="All-Rookie Team"
						season={season}
						team={awards.allRookie}
						userTid={userTid}
					/>
				</div>
				<div className="col-xl-5 col-md-3 col-sm-12">
					<RetiredPlayers
						retiredPlayers={retiredPlayers}
						retiredStat={retiredStat}
						season={season}
						userTid={userTid}
					/>
				</div>
			</div>

			{/* Every recap pass takes itself off the page once the season is fully
			    written, so a year with nothing here is a year that's done. They
			    share one flex row rather than a fixed two-column grid, since any
			    of them can be absent and a grid would leave holes where they
			    were. */}
			<div className="d-flex flex-wrap gap-4 mt-1">
				<SeasonRecap season={season} heading="Team Recaps (AI)" />

				<PlayerRecaps
					season={season}
					filter="players"
					heading="Player Season Recaps (AI)"
				/>

				{/* This season's own draft class, written after the draft. Its own
				    pass because none of these players has played a game, so a season
				    recap has nothing to recap. */}
				<PlayerRecaps
					season={season}
					filter="draftPicks"
					heading="Draft Class Writeups (AI)"
				/>

				{/* Next year's draft class, run separately: no stats, no season to
				    recap, and a scouting report is a different piece of writing from
				    a season recap. Absent when there's no class to scout. */}
				<PlayerRecaps
					season={season}
					filter="prospects"
					heading="Draft Prospect Reports (AI)"
				/>
			</div>
		</>
	);
};

export default History;
