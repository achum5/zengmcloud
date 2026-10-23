import { Fragment } from "react";
import { depthChart } from "../../../common/depthChart.ts";
import { PlayerNameLabels } from "../../components/PlayerNameLabels.tsx";
import { helpers } from "../../util/helpers.ts";
import type { View } from "../../../common/types.ts";

const POSITIONS = [
	"Point guard",
	"Shooting guard",
	"Small forward",
	"Power forward",
	"Center",
];
const GROUPS = ["Starters", "2nd", "3rd"];
const LINE = [
	["pts", "P"],
	["trb", "R"],
	["ast", "A"],
	["per", "PER"],
] as const;

// His season under his name: points, rebounds and assists a game, and PER -
// nothing until he has played.
const StatLine = ({
	stats,
}: {
	stats: View<"roster">["players"][number]["stats"];
}) =>
	stats.gp > 0 ? (
		<div
			className="depth-chart-stats"
			title="Points, rebounds and assists per game, PER"
		>
			{LINE.map(([stat, label]) => (
				<span key={stat}>
					{helpers.roundStat(stats[stat], stat)}
					<span className="depth-chart-unit">{label}</span>
				</span>
			))}
		</div>
	) : null;

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
					gridTemplateColumns: `repeat(${groups.length + 1}, auto)`,
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
											<div className="depth-chart-name">
												<PlayerNameLabels
													pid={p.pid}
													season={season}
													defaultWatch={p.watch}
													firstName={p.firstName}
													firstNameShort={p.firstNameShort}
													lastName={p.lastName}
													injury={p.injury}
													skills={p.ratings.skills}
													neverShowCountry
												/>
												{showRatings ? (
													<span className="depth-chart-ovr" title="Ovr/Pot">
														{p.ratings.ovr}/{p.ratings.pot}
													</span>
												) : null}
											</div>
											<StatLine stats={p.stats} />
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
