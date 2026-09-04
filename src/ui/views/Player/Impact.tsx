import { DataTable } from "../../components/DataTable/index.tsx";
import { PlusMinus } from "../../components/PlusMinus.tsx";
import { getCols } from "../../../common/getCols.ts";
import { helpers } from "../../util/helpers.ts";
import HideableSection from "../../components/HideableSection.tsx";
import { wrappedTeamAbbrevLink } from "../../components/TeamAbbrevLink.tsx";
import type { View } from "../../../common/types.ts";
import { SeasonLink } from "./common.tsx";

// IMPACT, WHICH IS THE PART OF A CAREER A BOX SCORE CANNOT SHOW.
//
// Two tables, and they answer different questions on purpose. The first is
// RAPM season by season: what the man himself was worth, with the other nine
// on the floor accounted for, and where that stood in the league he did it in.
// The second is the raw evidence underneath - who he actually played beside,
// and what the scoreboard did while he did.
//
// The second table is not a rating and is not presented as one. A reserve who
// only ever plays next to the starting point guard will read as whatever the
// point guard is worth, which is why the possessions are in their own column
// and why a thin pairing never appears at all.

const Percentile = ({ value }: { value: number | undefined }) =>
	value === undefined ? null : (
		<span className="text-body-secondary ms-1">({value})</span>
	);

const Rating = ({
	value,
	percentile,
}: {
	value: number | undefined;
	percentile: number | undefined;
}) =>
	value === undefined ? null : (
		<>
			<PlusMinus>{value}</PlusMinus>
			<Percentile value={percentile} />
		</>
	);

// Sorting has to read the number, not the element the number is drawn in.
const rating = (value: number | undefined, percentile: number | undefined) => ({
	value: <Rating value={value} percentile={percentile} />,
	sortValue: value,
	searchValue: value === undefined ? "" : String(value),
});

type StatsRow = Record<string, any>;

const Impact = ({
	impact,
	pid,
	stats,
}: {
	impact: View<"player">["impact"];
	pid: number;
	stats: StatsRow[];
}) => {
	// Only the seasons that were actually rated. A year too short to separate
	// him from his teammates has no row rather than a zero.
	const seasons = stats.filter(
		(ps) => !ps.playoffs && ps.rapm !== undefined && ps.rapmPoss > 0,
	);

	if (seasons.length === 0 && !impact) {
		return null;
	}

	return (
		<HideableSection pageName="Player" title="Impact">
			<div className="row">
				{seasons.length > 0 ? (
					<div className="col-xl-5">
						<h3 className="fs-6">
							Regularized Adjusted Plus/Minus
							<span className="text-body-secondary ms-2 fw-normal">
								league percentile in parentheses
							</span>
						</h3>
						<DataTable
							className="datatable-negative-margin-top mb-3"
							cols={getCols([
								"Season",
								"Team",
								"Poss",
								"stat:orapm",
								"stat:drapm",
								"stat:rapm",
							])}
							defaultSort={[0, "desc"]}
							hideAllControls
							name="Player:ImpactRapm"
							rows={seasons.map((ps) => ({
								key: `${ps.season}-${ps.tid}`,
								data: [
									{
										value: <SeasonLink season={ps.season} pid={pid} />,
										sortValue: ps.season,
										searchValue: ps.season,
									},
									wrappedTeamAbbrevLink({
										abbrev: ps.abbrev,
										season: ps.season,
										tid: ps.tid,
									}),
									helpers.numberWithCommas(Math.round(ps.rapmPoss)),
									rating(ps.orapm, ps.orapmPct),
									rating(ps.drapm, ps.drapmPct),
									rating(ps.rapm, ps.rapmPct),
								],
							}))}
						/>
					</div>
				) : null}

				{impact && impact.partners.length > 0 ? (
					<div className="col-xl-7">
						<h3 className="fs-6">
							On The Floor With
							<span className="text-body-secondary ms-2 fw-normal">
								{impact.season}, net per 100 possessions
							</span>
						</h3>
						<DataTable
							className="datatable-negative-margin-top mb-3"
							cols={getCols(["Teammate", "Poss", "Together", "Apart"])}
							defaultSort={[1, "desc"]}
							hideAllControls
							name="Player:ImpactPartners"
							rows={impact.partners.map((partner) => ({
								key: partner.pid,
								data: [
									<a href={helpers.leagueUrl(["player", partner.pid])}>
										{partner.firstName} {partner.lastName}
									</a>,
									helpers.numberWithCommas(Math.round(partner.poss)),
									{
										value: <PlusMinus>{partner.together}</PlusMinus>,
										sortValue: partner.together,
									},
									{
										value: <PlusMinus>{partner.apart}</PlusMinus>,
										sortValue: partner.apart,
									},
								],
							}))}
						/>
					</div>
				) : null}
			</div>
		</HideableSection>
	);
};

export default Impact;
