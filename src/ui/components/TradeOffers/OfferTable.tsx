import clsx from "clsx";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";
import { getCols } from "../../../common/getCols.ts";
import type { LocalStateUI } from "../../../common/types.ts";
import { DataTable, type Col } from "../DataTable/index.tsx";
import { HelpPopover } from "../HelpPopover.tsx";
import { SaveTrade } from "../SaveTrade.tsx";
import { wrappedCurrency } from "../wrappedCurrency.ts";
import { OvrChange } from "../../views/Trade/Summary.tsx";
import { teamOvrDeltaSymbols } from "../../../common/teamRatings.ts";
import type { OfferType } from "./Offer.tsx";

export const OfferTable = ({
	assetCols,
	challengeNoRatings,
	getAssetColContents,
	handleNegotiate,
	handleRemove,
	offers,
	salaryCap,
	salaryCapType,
}: {
	assetCols: Col[];
	challengeNoRatings: boolean;
	getAssetColContents: (offer: OfferType) => any[];
	handleNegotiate: (tradeInfo: {
		tid: number;
		pids: number[];
		pidsUser: number[];
		dpids: number[];
		dpidsUser: number[];
	}) => void;
	handleRemove?: (tid: number) => void;
	offers: OfferType[];
} & Pick<LocalStateUI, "salaryCap" | "salaryCapType">) => {
	const { hideTeamRatings, teamInfoCache, userTid } = useLocal([
		"hideTeamRatings",
		"teamInfoCache",
		"userTid",
	]);

	const offerCols = getCols(
		[
			"",
			"Team",
			"Record",
			"Ovr",
			"Ovr",
			salaryCapType === "none" ? "Payroll" : "Cap Space",
			"Cap",
			"Actions",
		],
		{
			"": {
				width: "1px",
			},
			Actions: {
				sortSequence: [],
				width: "1px",
			},
		},
	);
	offerCols[3]!.title = hideTeamRatings ? "Your Δ" : "Your Ovr";
	offerCols[3]!.desc = "Your team's change in ovr rating";
	offerCols[4]!.title = hideTeamRatings ? "Other Δ" : "Other Ovr";
	offerCols[4]!.desc = "Other team's change in ovr rating";
	offerCols.splice(5, 0, ...assetCols);

	// Sort by the CHANGE when ratings are hidden, so the column stays useful for
	// "which of these offers helps me most" - sorting by the resulting rating
	// would rank by how good the team already is, which is the hidden part.
	const ovrCell = ({
		ovrBefore,
		ovrAfter,
	}: {
		ovrBefore: number;
		ovrAfter: number;
	}) => {
		const diff = ovrAfter - ovrBefore;
		return {
			value: <OvrChange before={ovrBefore} after={ovrAfter} />,
			// SORTING stays on the true change even while the cell shows bands, so
			// "which of these offers helps me most" still orders correctly - and
			// ordering a list of offers is a far weaker signal than reading an exact
			// delta off a one-player swap, which is the leak the bands close.
			sortValue: hideTeamRatings ? diff : ovrAfter,
			// Searching has to match what is on screen, or typing "+++" finds
			// nothing while typing the hidden number finds a row.
			searchValue: hideTeamRatings
				? teamOvrDeltaSymbols(diff)
				: `${ovrBefore} ${ovrAfter}`,
		};
	};

	const offerRows = offers.map((offer, i) => {
		const salaryCapOrPayroll =
			salaryCapType === "none" ? offer.payroll : salaryCap - offer.payroll;

		const t = teamInfoCache[offer.tid];
		if (!t) {
			return {
				key: i,
				data: [],
			};
		}

		return {
			key: i,
			data: [
				<SaveTrade
					tradeTeams={[
						{
							pids: offer.pidsUser,
							dpids: offer.dpidsUser,
							tid: userTid,
						},
						{
							pids: offer.pids,
							dpids: offer.dpids,
							tid: offer.tid,
						},
					]}
				/>,
				<a href={helpers.leagueUrl(["roster", `${t.abbrev}_${offer.tid}`])}>
					{t.abbrev}
				</a>,
				helpers.formatRecord(offer),
				!challengeNoRatings ? ovrCell(offer.summary.teams[1]) : null,
				!challengeNoRatings ? ovrCell(offer.summary.teams[0]) : null,
				...getAssetColContents(offer),
				wrappedCurrency(salaryCapOrPayroll / 1000, "M"),
				{
					value: offer.summary.warning ? (
						<HelpPopover className="fs-4">{offer.summary.warning}</HelpPopover>
					) : null,
					sortValue: offer.summary.warningAmount ?? 0,
					classNames: clsx(
						"text-center",
						offer.summary.warning ? "table-danger" : undefined,
					),
				},
				<div className="d-flex align-items-center">
					<button
						type="submit"
						className="btn btn-light-bordered"
						onClick={() => {
							handleNegotiate({
								tid: offer.tid,
								pids: offer.pids,
								pidsUser: offer.pidsUser,
								dpids: offer.dpids,
								dpidsUser: offer.dpidsUser,
							});
						}}
					>
						Negotiate
					</button>
					{handleRemove ? (
						<button
							type="button"
							className="btn-close ms-2 p-0"
							title="Remove trade from list"
							onClick={() => {
								handleRemove(offer.tid);
							}}
						/>
					) : null}
				</div>,
			],
		};
	});

	if (offerRows.length === 0) {
		return null;
	}

	return (
		<DataTable
			className="align-top-all"
			clickable={false}
			cols={offerCols}
			defaultSort={[1, "asc"]}
			name={`TradingBlock:Offers`}
			rows={offerRows}
			small={false}
		/>
	);
};
