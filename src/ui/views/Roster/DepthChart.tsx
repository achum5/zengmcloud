import { Fragment } from "react";
import { depthChart } from "../../../common/depthChart.ts";
import { InjuryIcon } from "../../components/InjuryIcon.tsx";
import { PlayerNameLabels } from "../../components/PlayerNameLabels.tsx";
import { RatingsStatsPopover } from "../../components/RatingsStatsPopover/index.tsx";
import type { View } from "../../../common/types.ts";

const POSITIONS = [
	"Point guard",
	"Shooting guard",
	"Small forward",
	"Power forward",
	"Center",
];
const GROUPS = ["Starters", "2nd", "3rd"];

// The roster order as a depth chart: see common/depthChart.ts. Spots down the
// side, the starters and the strings behind them across - scrolling sideways
// on a phone, the spots staying put.
const DepthChart = ({
	numPlayersOnCourt,
	players,
	season,
	showRatings,
}: {
	numPlayersOnCourt: number;
	players: View<"roster">["players"];
	season: number;
	showRatings: boolean;
}) => {
	const groups = depthChart(players, numPlayersOnCourt, GROUPS.length);
	if (groups.length === 0) {
		return null;
	}

	return (
		<div className="depth-chart-scroll mb-3">
			<div
				className="depth-chart"
				style={{
					// (Two to the width on a phone - beside the spots and their
					// gaps, 2.25rem - so a swipe lands square on the next two.)
					gridTemplateColumns: `auto repeat(${groups.length}, minmax(min(20rem, calc((100cqw - 2.25rem) / 2)), 20rem))`,
				}}
			>
				<div className="depth-chart-spot" />
				{groups.map((group, i) => (
					<div
						key={i}
						className={`depth-chart-head${i === 0 ? " fw-bold" : ""}`}
					>
						{GROUPS[i]}
					</div>
				))}
				{Array.from({ length: numPlayersOnCourt }, (_, spot) => (
					<Fragment key={spot}>
						<div
							className="depth-chart-spot"
							title={numPlayersOnCourt === 5 ? POSITIONS[spot] : undefined}
						>
							{spot + 1}
						</div>
						{groups.map((group, i) => {
							const p = group[spot];
							return (
								<div key={i} className="depth-chart-player">
									{p ? (
										<>
											<span className="depth-chart-name">
												<PlayerNameLabels
													pid={p.pid}
													season={season}
													firstName={p.firstName}
													firstNameShort={p.firstNameShort}
													lastName={p.lastName}
													hideRatingsPopover
													neverShowCountry
												/>
											</span>
											<InjuryIcon injury={p.injury} />
											<RatingsStatsPopover
												pid={p.pid}
												season={season}
												defaultWatch={p.watch}
											/>
											{showRatings ? (
												<span className="depth-chart-ovr" title="Ovr/Pot">
													{p.ratings.ovr}/{p.ratings.pot}
												</span>
											) : null}
										</>
									) : (
										<span className="text-body-secondary">-</span>
									)}
								</div>
							);
						})}
					</Fragment>
				))}
			</div>
		</div>
	);
};

export default DepthChart;
