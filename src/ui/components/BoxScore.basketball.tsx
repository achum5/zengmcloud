import { ResponsiveTableWrapper } from "./ResponsiveTableWrapper.tsx";
import { SafeHtml } from "../components/SafeHtml.tsx";
import { helpers } from "../util/helpers.ts";
import { getCols } from "../../common/getCols.ts";
import { sortByStats, StatsHeader } from "./BoxScore.football.tsx";
import { type MouseEvent, useState } from "react";
import { useLocal } from "../util/local.ts";
import {
	canHideBoxScoreTeam,
	getHideOtherBoxScore,
	orderBoxScoreTeams,
	setHideOtherBoxScore,
} from "../util/liveBoxScoreLayout.ts";
import type { SortBy } from "./DataTable/index.tsx";
import updateSortBys from "./DataTable/updateSortBys.ts";

const StatsTable = ({
	gid,
	Row,
	exhibition,
	forceRowUpdate,
	liveGameInProgress,
	numPlayersOnCourt,
	season,
	showHighlights,
	t,
}: {
	gid?: number;
	Row: any;
	exhibition?: boolean;
	forceRowUpdate: boolean;
	liveGameInProgress: boolean;
	numPlayersOnCourt: number;
	season: number;
	showHighlights?: boolean;
	t: any;
}) => {
	const [sortBys, setSortBys] = useState<SortBy[]>([]);

	const onClick = (event: MouseEvent, i: number) => {
		setSortBys((prevSortBys) => {
			const newSortBys =
				updateSortBys({
					cols,
					event,
					i,
					prevSortBys,
				}) ?? [];

			if (
				newSortBys.length === 1 &&
				prevSortBys.length === 1 &&
				newSortBys[0]![0] === prevSortBys[0]![0] &&
				newSortBys[0]![1] === "desc"
			) {
				// User just clicked twice on the same column. Reset sort.
				return [];
			}

			return newSortBys;
		});
	};

	const stats = [
		// Game score leads, ahead of minutes: it's the one-number summary of the
		// line that follows, so it reads better at the front than tacked on the end.
		"gmsc",
		"min",
		"pts",
		"trb",
		"ast",
		"fg",
		"tp",
		"ft",
		"orb",
		"tov",
		"stl",
		"blk",
		"ba",
		"pf",
		"pm",
	];
	const cols = getCols(
		stats.map((stat) => `stat:${stat}`),
		{
			"stat:fg": {
				desc: "Field Goals",
			},
			"stat:tp": {
				desc: "Three Pointers",
			},
			"stat:ft": {
				desc: "Free Throws",
			},
		},
	);

	// This is used for two purposes - keeping injured/DNP at the bottom while sorting, and also sorting in general for live sim (was too hard to account for this stuff in default sort from backend)
	const playersActiveOrPlayed = [];
	const playersInjuredOrDNP = [];
	for (const p of t.players) {
		let addToHealthy;
		if (liveGameInProgress) {
			addToHealthy =
				p.injury.gamesRemaining === 0 || p.min > 0 || p.injury.playingThrough;
		} else {
			addToHealthy = p.min > 0;
		}

		if (addToHealthy) {
			playersActiveOrPlayed.push(p);
		} else {
			playersInjuredOrDNP.push(p);
		}
	}

	if (sortBys.length > 0) {
		playersActiveOrPlayed.sort(
			sortByStats(stats, undefined, sortBys, (p, stat) => {
				if (stat === "trb") {
					return p.orb + p.drb;
				}

				if (stat === "gmsc") {
					return helpers.gameScore(p);
				}

				if (stat === "fg" || stat === "ft" || stat === "tp") {
					// Sort by FGM, FGM/FGA (+1 for divide by 0 and so 100% doesn't roll over), and # attempts (lower is better)
					return (
						p[stat] +
						p[stat] / (p[`${stat}a`] + 1) +
						(1000 - p[`${stat}a`]) / 1000
					);
				}

				return p[stat];
			}),
		);
	}

	const allStarGame = t.tid === -1 || t.tid === -2;
	const players = [...playersActiveOrPlayed, ...playersInjuredOrDNP];

	return (
		<ResponsiveTableWrapper>
			<table className="table table-striped table-borderless table-sm table-hover sticky-x">
				<thead>
					<tr>
						<th>Name</th>
						{typeof t.players[0].abbrev === "string" ? <th>Team</th> : null}
						<th>Pos</th>
						<StatsHeader
							cols={cols}
							onClick={onClick}
							sortBys={sortBys}
							sortable={t.players.length > 1}
						/>
						{showHighlights ? <th title="Player highlights" /> : null}
					</tr>
				</thead>
				<tbody>
					{players.map((p, i) => (
						<Row
							allStarGame={allStarGame}
							key={p.pid}
							exhibition={exhibition}
							gid={gid}
							lastStarter={sortBys.length === 0 && i + 1 === numPlayersOnCourt}
							liveGameInProgress={liveGameInProgress}
							p={p}
							forceUpdate={forceRowUpdate}
							season={season}
							showHighlights={showHighlights}
						/>
					))}
				</tbody>
				<tfoot>
					<tr>
						<th>Total</th>
						<th />
						{typeof t.players[0].abbrev === "string" ? <th /> : null}
						<th />
						<th>{Number.isInteger(t.min) ? t.min : t.min.toFixed(1)}</th>
						<th>{t.pts}</th>
						<th>{t.drb + t.orb}</th>
						<th>{t.ast}</th>
						<th>
							{t.fg}-{t.fga}
						</th>
						<th>
							{t.tp}-{t.tpa}
						</th>
						<th>
							{t.ft}-{t.fta}
						</th>
						<th>{t.orb}</th>
						<th>{t.tov}</th>
						<th>{t.stl}</th>
						<th>{t.blk}</th>
						<th>{t.ba}</th>
						<th>{t.pf}</th>
						<th />
						{showHighlights ? <th /> : null}
					</tr>
					<tr>
						{/* Pos, game score, minutes, points, rebounds, assists */}
						<th>Percentages</th>
						<th />
						{typeof t.players[0].abbrev === "string" ? <th /> : null}
						<th />
						<th />
						<th />
						<th />
						<th />
						<th>{helpers.roundStat((100 * t.fg) / t.fga, "fgp")}%</th>
						<th>{helpers.roundStat((100 * t.tp) / t.tpa, "tpp")}%</th>
						<th>{helpers.roundStat((100 * t.ft) / t.fta, "ftp")}%</th>
						{/* Offensive rebounds through plus/minus */}
						<th />
						<th />
						<th />
						<th />
						<th />
						<th />
						<th />
						{showHighlights ? <th /> : null}
					</tr>
				</tfoot>
			</table>
		</ResponsiveTableWrapper>
	);
};

const BoxScore = ({
	boxScore,
	Row,
	forceRowUpdate,
}: {
	boxScore: any;
	Row: any;
	forceRowUpdate: boolean;
}) => {
	// Historical games will have boxScore.won.name and boxScore.lost.name so use that for ordering, but live games
	// won't. This is hacky, because the existence of this property is just a historical coincidence, and maybe it'll
	// change in the future.
	const liveGameSim = boxScore.won?.name === undefined;
	const liveGameInProgress = liveGameSim && !boxScore.gameOver;

	// Whose device this is. Only meaningful for a live game - a historical box
	// score is a record, and reordering it by who happens to be reading would
	// make the same game look different on every device.
	const { userTid } = useLocal(["userTid"]);
	const [hideOther, setHideOther] = useState(getHideOtherBoxScore);

	const teams = liveGameSim
		? orderBoxScoreTeams(boxScore.teams, userTid)
		: boxScore.teams;

	return (
		<>
			{teams.map((t: any, i: number) => {
				const hideable = canHideBoxScoreTeam({
					tid: t.tid,
					userTid,
					liveGameInProgress,
				});
				const hidden = hideable && hideOther;

				return (
					<div
						key={t.abbrev}
						className="mb-3"
						id={i === 0 ? "scroll-team-1" : "scroll-team-2"}
						style={{
							scrollMarginTop: 136,
						}}
					>
						<h2 className="d-flex align-items-center gap-2">
							<span>
								{t.tid >= 0 ? (
									<a
										href={helpers.leagueUrl([
											"roster",
											`${t.abbrev}_${t.tid}`,
											boxScore.season,
										])}
									>
										{t.season !== undefined ? `${t.season} ` : null}
										{t.region} {t.name}
									</a>
								) : (
									<>
										{t.season !== undefined ? `${t.season} ` : null}
										{t.region} {t.name}
									</>
								)}
							</span>
							{hideable ? (
								<button
									type="button"
									className="btn btn-light-bordered btn-sm"
									onClick={() => {
										setHideOther(!hidden);
										setHideOtherBoxScore(!hidden);
									}}
									title={
										hidden
											? "Show this team's box score"
											: "Hide this team's box score"
									}
									aria-expanded={!hidden}
								>
									{hidden ? "Show" : "Hide"}
								</button>
							) : null}
						</h2>
						{hidden ? null : (
							<StatsTable
								gid={boxScore.gid}
								Row={Row}
								exhibition={boxScore.exhibition}
								forceRowUpdate={forceRowUpdate}
								liveGameInProgress={liveGameInProgress}
								numPlayersOnCourt={boxScore.numPlayersOnCourt ?? 5}
								season={boxScore.season}
								showHighlights={!!boxScore.hasReplay}
								t={t}
							/>
						)}
					</div>
				);
			})}
			{boxScore.gameOver !== false &&
			boxScore.clutchPlays &&
			boxScore.clutchPlays.length > 0
				? boxScore.clutchPlays.map((text: string, i: number) => (
						<p key={i}>
							<SafeHtml dirty={text} />
						</p>
					))
				: null}
		</>
	);
};

export default BoxScore;
