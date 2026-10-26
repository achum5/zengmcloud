import { DataTable } from "../../components/DataTable/index.tsx";
import { PlusMinus } from "../../components/PlusMinus.tsx";
import { getCols } from "../../../common/getCols.ts";
import { helpers } from "../../util/helpers.ts";
import HideableSection from "../../components/HideableSection.tsx";
import type { View } from "../../../common/types.ts";

// WHO HE PLAYED BESIDE THIS SEASON, AND WHAT THE SCOREBOARD DID.
//
// Not a rating and not presented as one. A reserve who only ever plays next to
// the starting point guard will read as whatever the point guard is worth,
// which is why the possessions are in their own column and why a thin pairing
// never appears at all.

const Impact = ({ impact }: { impact: View<"player">["impact"] }) => {
	if (!impact || impact.partners.length === 0) {
		return null;
	}

	return (
		<HideableSection pageName="Player" title="On The Floor With">
			<div className="row">
				<div className="col-xl-7">
					<div className="text-body-secondary small">
						{impact.season}, net per 100 possessions
					</div>
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
			</div>
		</HideableSection>
	);
};

export default Impact;
