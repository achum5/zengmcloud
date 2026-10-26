import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { getCols } from "../../common/getCols.ts";
import { DataTable } from "../components/DataTable/index.tsx";
import { MoreLinks } from "../components/MoreLinks.tsx";
import type { View } from "../../common/types.ts";
import { PLAYER } from "../../common/constants.ts";
import { wrappedPlayerNameLabels } from "../components/PlayerNameLabels.tsx";
import type { DataTableRow } from "../components/DataTable/index.tsx";
import { RatingWithChange } from "../components/RatingWithChange.tsx";
import { StatWithChange } from "../components/StatWithChange.tsx";
import { useLocal } from "../util/local.ts";
import { getCol } from "../../common/getCol.ts";
import { Fragment, useState, type ReactNode } from "react";
import { Modal } from "../components/Modal.tsx";
import { formatPlayerAwardName } from "../../common/awards.ts";
import clsx from "clsx";
import type { getAwardCandidates } from "../../worker/core/awards/getAwardCandidates.ts";
import {
	americanToImpliedProb,
	formatAmerican,
} from "../../common/sportsbook.ts";

// The award score is small (often under 1), so it gets two decimals to tell
// close races apart.
const roundAwardStat = (value: unknown, stat: string) =>
	stat === "score" && typeof value === "number"
		? value.toFixed(2)
		: helpers.roundStat(value, stat);

const MARGIN = 14;

type Breakdown = InputAward["players"][number]["scoreBreakdown"];

const fmt = (x: number, digits = 3) =>
	Number.isInteger(x) ? String(x) : x.toFixed(digits);

const ScoreCell = ({
	awardName,
	breakdown,
	playerName,
	score,
}: {
	awardName: string;
	breakdown: NonNullable<Breakdown>;
	playerName: string;
	score: number;
}) => {
	const [show, setShow] = useState(false);
	const { compare, formula, terms, values } = breakdown;
	return (
		<>
			<button
				type="button"
				className="btn btn-link p-0 border-0 align-baseline"
				onClick={() => {
					setShow(true);
				}}
			>
				{score.toFixed(2)}
			</button>
			<Modal
				centered
				show={show}
				onHide={() => {
					setShow(false);
				}}
			>
				<Modal.Header closeButton>
					<Modal.Title>
						{playerName}, {awardName}
					</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					<code className="d-block mb-3 text-wrap">{formula}</code>
					<table className="table table-sm mb-3">
						<tbody>
							{values.map(([name, value]) => (
								<tr key={name}>
									<td>
										<code>{name}</code>
									</td>
									<td className="text-end">{fmt(value)}</td>
								</tr>
							))}
						</tbody>
					</table>
					{terms ? (
						<table className="table table-sm mb-3">
							<tbody>
								{terms.map((term, i) => (
									<tr key={i}>
										<td>
											<code>
												{i > 0 || term.sign < 0
													? `${term.sign < 0 ? "-" : "+"} `
													: null}
												{term.text}
											</code>
										</td>
										<td className="text-end">{fmt(term.value)}</td>
									</tr>
								))}
								<tr className="fw-bold">
									<td>{compare ? "This season" : "Score"}</td>
									<td className="text-end">
										{fmt(compare ? compare.current : score)}
									</td>
								</tr>
							</tbody>
						</table>
					) : null}
					{compare ? (
						<table className="table table-sm mb-0">
							<tbody>
								{!terms ? (
									<tr>
										<td>This season</td>
										<td className="text-end">{fmt(compare.current)}</td>
									</tr>
								) : null}
								<tr>
									<td>Last season</td>
									<td className="text-end">{fmt(compare.prev)}</td>
								</tr>
								<tr>
									<td>Best before</td>
									<td className="text-end">{fmt(compare.max)}</td>
								</tr>
								<tr className="fw-bold">
									<td>
										Score{" "}
										<code>
											{compare.kind === "mip"
												? "2 × this - last - best"
												: "min(this, best) - last"}
										</code>
									</td>
									<td className="text-end">{fmt(score)}</td>
								</tr>
							</tbody>
						</table>
					) : !terms ? (
						<div className="fw-bold d-flex justify-content-between">
							<span>Score</span>
							<span>{fmt(score)}</span>
						</div>
					) : null}
				</Modal.Body>
			</Modal>
		</>
	);
};

export type InputAward = Awaited<
	ReturnType<typeof getAwardCandidates>
>["awardCandidates"][number][number];

const Title = ({
	asterisk,
	award,
	confs,
	divs,
}: {
	asterisk: boolean;
	award: InputAward;
} & Pick<View<"awardRaces">, "confs" | "divs">) => {
	const group = award.group;
	return (
		<div>
			<h2>
				{formatPlayerAwardName({
					name: award.name,
					numTeams: award.numTeams,
					rank: award.rank ?? 1,
				})}
				{asterisk ? "*" : null}
			</h2>
			{group && group.type !== "playoffSeries" ? (
				<h3>
					{group.type === "conf"
						? confs[group.cid]?.name
						: divs[group.did]?.name}
				</h3>
			) : null}
		</div>
	);
};

const getRows = ({
	award,
	challengeNoRatings,
	currentSeason,
	season,
	teams,
	userTid,
}: {
	award: InputAward;
	challengeNoRatings: boolean;
	currentSeason: number;
	userTid: number;
} & Pick<View<"awardRaces">, "season" | "teams">) => {
	const { mip, rookie, players, stats } = award;
	const priced = players.some((p) => typeof p.odds === "number");

	const rows: DataTableRow<"player">[] = players.map((p, j) => {
		const ps = p.currentStats;
		const pr = p.ratings.findLast((row) => row.season === season);

		const scoreCell = (score: number | undefined) =>
			p.scoreBreakdown && typeof score === "number" && Number.isFinite(score)
				? {
						value: (
							<ScoreCell
								awardName={award.name}
								breakdown={p.scoreBreakdown}
								playerName={`${p.firstName} ${p.lastName}`}
								score={score}
							/>
						),
						sortValue: score,
					}
				: roundAwardStat(score, "score");

		const abbrev = ps?.abbrev;
		const tid = ps?.tid;
		const t = teams[tid];

		let recordOrPick = null;
		if (rookie) {
			if (p.draft.round > 0) {
				recordOrPick = `${p.draft.round}-${p.draft.pick}`;
				if (p.draft.year !== season - 1) {
					recordOrPick += ` (${p.draft.year})`;
				}
			}
		} else {
			if (t) {
				recordOrPick = helpers.formatRecord(t.seasonAttrs);
			}
		}

		const data: DataTableRow["data"] = [
			j + 1,
			wrappedPlayerNameLabels({
				injury: season === currentSeason ? p.injury : undefined,
				jerseyNumber: ps ? ps.jerseyNumber : undefined,
				pid: p.pid,
				season,
				skills: pr ? pr.skills : [],
				defaultWatch: p.watch,
				firstName: p.firstName,
				firstNameShort: p.firstNameShort,
				lastName: p.lastName,
			}),
			p.pos,
			p.age,
			<>
				<a href={helpers.leagueUrl(["roster", `${abbrev}_${tid}`, season])}>
					{abbrev}
				</a>
			</>,
			...(priced
				? [
						typeof p.odds === "number"
							? {
									value: (
										<span className="fw-bold text-primary">
											{formatAmerican(p.odds)}
										</span>
									),
									sortValue: americanToImpliedProb(p.odds),
								}
							: null,
					]
				: []),
			recordOrPick,
		];

		const showRatings = !challengeNoRatings || p.tid === PLAYER.RETIRED;

		if (mip) {
			data.push(
				pr && showRatings ? (
					<RatingWithChange change={pr.dovr}>{pr.ovr}</RatingWithChange>
				) : null,
			);

			const ps2 = p.stats.findLast((row) => {
				if (row.season !== season - 1) {
					return false;
				}

				if (award.statRange === undefined && row.playoffs !== false) {
					return false;
				}
				if (award.statRange === "playoffs" && row.playoffs !== true) {
					return false;
				}
				if (award.statRange === "combined" && row.playoffs !== "combined") {
					return false;
				}

				return true;
			});

			const comparePlayersRange =
				award.statRange === "playoffs"
					? "p"
					: award.statRange === "combined"
						? "c"
						: "r";

			data.push(
				...stats.map((stat) => {
					if (!ps && !ps2) {
						return null;
					}

					if (stat === "score") {
						return scoreCell(ps.score);
					}
					if (!ps2 || stat === "keyStats") {
						return roundAwardStat(ps[stat], stat);
					}

					return (
						<StatWithChange change={ps[stat] - ps2[stat]} stat={stat}>
							{ps[stat]}
						</StatWithChange>
					);
				}),
				<a
					href={helpers.leagueUrl([
						"compare_players",
						`${p.pid}-${season - 1}-${comparePlayersRange},${p.pid}-${season}-${comparePlayersRange}`,
					])}
				>
					Compare
				</a>,
			);
		} else {
			data.push(pr && showRatings ? pr.ovr : null);
			const statsRow = stats.map((stat) => {
				if (p.opoyOverride && stat === "score") {
					// Hide score from UI if opoyOverride because this player was put at #1 due to a different formula (opoyFormula)
					return {
						value: null,
						sortValue: Infinity,
					};
				}

				if (stat === "score" && ps && !p.statOverrides) {
					return scoreCell(ps.score);
				}

				return ps
					? roundAwardStat(
							p.statOverrides ? p.statOverrides[stat] : ps[stat],
							stat,
						)
					: null;
			});
			data.push(...statsRow);
		}

		return {
			key: p.pid,
			metadata: {
				type: "player",
				pid: p.pid,
				season,
				playoffs: "regularSeason",
			},
			data,
			classNames: {
				"table-danger": p.hof,
				"table-info": tid === userTid,
			},
		};
	});

	return rows;
};

type RowsInfo = Pick<
	Parameters<typeof getRows>[0],
	"award" | "season" | "teams"
>;

export const getAwardKey = (
	award: Pick<RowsInfo["award"], "group" | "numTeams" | "shortName" | "rank">,
) => {
	return JSON.stringify([
		award.shortName,
		award.group,
		award.numTeams ? award.rank : undefined,
	]);
};

export const AwardRaceTable = ({
	confs,
	divs,
	rowsInfo: { award, season, teams },
	titleOverride,
}: {
	rowsInfo: RowsInfo;
	titleOverride?: ReactNode;
} & Pick<View<"awardRaces">, "confs" | "divs">) => {
	const { mip, rookie, stats } = award;

	const asterisk =
		__SPORT === "football" &&
		award.numTeams === undefined &&
		award.opoyFormula !== undefined;

	const priced = award.players.some((p) => typeof p.odds === "number");
	const cols = getCols([
		"#",
		"Name",
		"Pos",
		"Age",
		"Team",
		rookie ? "Pick" : "Record",
		"Ovr",
		...stats.map((stat) => `stat:${stat}`),
	]);
	if (priced) {
		cols.splice(5, 0, {
			...getCol("Odds"),
			desc: "Live betting odds, from the current race (see Sportsbook)",
		});
	}
	if (mip) {
		cols.push(getCol("Compare"));
	}

	const title = titleOverride ?? (
		<Title asterisk={asterisk} award={award} confs={confs} divs={divs} />
	);

	const {
		challengeNoRatings,
		season: currentSeason,
		userTid,
	} = useLocal(["challengeNoRatings", "season", "userTid"]);

	const rows = getRows({
		award,
		challengeNoRatings,
		currentSeason,
		season,
		teams,
		userTid,
	});

	return (
		<>
			{rows.length > 0 ? (
				<DataTable
					classNameWrapper="mb-1"
					cols={cols}
					defaultSort={[0, "asc"]}
					defaultStickyCols={window.mobile ? 0 : 2}
					hideAllControls
					name={`AwardRaces${getAwardKey(award)}`}
					rows={rows}
					title={title}
				/>
			) : (
				<>
					{title}
					<p className="mt-2">No candidates yet...</p>
				</>
			)}
			{asterisk ? (
				<div className="text-body-secondary">
					* Exceptional QBs can win both MVP and {award.shortName}
				</div>
			) : null}
		</>
	);
};

const AwardRaces = ({
	awardCandidates,
	confs,
	divs,
	season,
	teams,
}: View<"awardRaces">) => {
	useTitleBar({
		title: "Award Races",
		jumpTo: true,
		jumpToSeason: season,
		dropdownView: "award_races",
		dropdownFields: {
			seasons: season,
		},
	});

	return (
		<>
			<MoreLinks type="awards" page="award_races" season={season} />

			<div className="row" style={{ marginTop: -MARGIN }}>
				{awardCandidates.map((award) => {
					const key = getAwardKey(award);

					return (
						<Fragment key={key}>
							<div
								className={clsx(
									award.mip ? "col-12 col-lg-9" : "col-12 col-lg-6",
								)}
								style={{ marginTop: MARGIN }}
							>
								<AwardRaceTable
									confs={confs}
									divs={divs}
									rowsInfo={{ award, season, teams }}
								/>
							</div>
						</Fragment>
					);
				})}
			</div>
		</>
	);
};

export default AwardRaces;
