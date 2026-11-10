import { helpers } from "../../util/helpers.ts";
import { ABBREVS } from "../../../common/signingHow.ts";
import type { ContractCard as ContractCardData } from "../../../worker/core/realisticFrontOffice/capSheet.ts";
import { termsBadges } from "../../components/CapStatus.tsx";

// HIS CONTRACT UNDER THE CONTRACT RULES (realistic front offices' CBA-lite):
// the deal in one line, what he'll be when it's up, who holds his Bird
// rights, his max tier - above his salaries - and every contract he has signed
// and how, below them.

const money = (amount: number) => helpers.formatCurrency(amount / 1000, "M");

const Abbr = ({ k }: { k: keyof typeof ABBREVS }) => (
	<abbr title={ABBREVS[k]}>{k}</abbr>
);

export const ContractSummary = ({ card }: { card: ContractCardData }) => {
	const { deal } = card;
	return (
		<div className="mb-2">
			{deal ? (
				<div>
					<b>
						{deal.years} {helpers.plural("yr", deal.years)} /{" "}
						{money(deal.total)}
					</b>{" "}
					({money(deal.total / deal.years)}/yr)
					{deal.signed !== undefined ? (
						<>
							{" "}
							· {deal.verb} {deal.signed} with {deal.abbrev}
							{deal.method ? ` via ${deal.method}` : null}
						</>
					) : null}
				</div>
			) : null}
			<div>
				{deal ? (
					<>
						FA after {deal.exp} · <Abbr k={deal.restricted ? "RFA" : "UFA"} />{" "}
						·{" "}
					</>
				) : card.freeAgent ? (
					<>
						Free agent · <Abbr k={card.freeAgent.restricted ? "RFA" : "UFA"} />{" "}
						·{" "}
					</>
				) : null}
				<Abbr k="Bird" /> rights: {card.bird ?? "none"}
			</div>
			<div className="text-body-secondary">
				{card.service} {helpers.plural("season", card.service)} · max{" "}
				{card.maxPct}% of cap ({money(card.max)})
			</div>
		</div>
	);
};

// A season's markers in his salaries table.
export const SalaryMarks = ({
	marks,
}: {
	marks: ContractCardData["marks"][number] | undefined;
}) =>
	marks && marks.length > 0 ? (
		<>
			{marks.map((mark) => (
				<span
					key={mark}
					className={`badge ms-1 ${mark === "Prove-it" ? "text-bg-warning" : "text-bg-light border"}`}
					title={ABBREVS[mark]}
				>
					{mark}
				</span>
			))}
		</>
	) : null;

export const ContractHistory = ({ card }: { card: ContractCardData }) => {
	if (card.history.length === 0) {
		return null;
	}
	return (
		<table className="table table-sm table-striped mb-3">
			<thead>
				<tr>
					<th>Signed</th>
					<th>Team</th>
					<th className="text-end">Yrs</th>
					<th className="text-end">Total</th>
					<th>How</th>
				</tr>
			</thead>
			<tbody>
				{card.history.map((row, i) => (
					<tr key={i}>
						<td>{row.season}</td>
						<td>
							{row.tid >= 0 ? (
								<a
									href={helpers.leagueUrl([
										"roster",
										`${row.abbrev}_${row.tid}`,
										row.season,
									])}
								>
									{row.abbrev}
								</a>
							) : null}
						</td>
						<td className="text-end">{row.years}</td>
						<td className="text-end">{money(row.total)}</td>
						<td className="text-nowrap">
							{row.method}
							{row.how ? (
								<span className="ms-1">{termsBadges(row.how)}</span>
							) : null}
						</td>
					</tr>
				))}
			</tbody>
		</table>
	);
};
