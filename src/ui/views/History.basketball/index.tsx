import { MoreLinks } from "../../components/MoreLinks.tsx";
import { RetiredPlayers } from "../../components/RetiredPlayers.tsx";
import { RetiredRecap } from "../../components/RetiredRecap.tsx";
import { SeasonRecap } from "../../components/SeasonRecap.tsx";
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

			<div className="row">
				<div className="col-md-6 mt-1">
					<h2 className="h5">Team Recaps (AI)</h2>
					<SeasonRecap season={season} />
				</div>

				<div className="col-md-6 mt-1">
					<h2 className="h5">Retired Player Writeups (AI)</h2>
					<RetiredRecap season={season} />
				</div>
			</div>
		</>
	);
};

export default History;
