import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

// Realistic Front Offices: deals held for bids, and CPU teams' trade
// proposals to this team.
const TradeBell = () => {
	const { pendingDeals, tradeProposals } = useLocal([
		"pendingDeals",
		"tradeProposals",
	]);
	const count = (pendingDeals ?? 0) + (tradeProposals ?? 0);

	if (count === 0) {
		return null;
	}

	return (
		<a
			href={helpers.leagueUrl(["trade_proposals"])}
			className="nav-link position-relative ms-2 px-1"
			title={[
				...(pendingDeals ? [plural(pendingDeals, "pending deal")] : []),
				...(tradeProposals ? [plural(tradeProposals, "trade proposal")] : []),
			].join(", ")}
		>
			<svg
				xmlns="http://www.w3.org/2000/svg"
				width="18"
				height="18"
				fill="currentColor"
				viewBox="0 0 16 16"
				aria-hidden="true"
			>
				<path d="M8 16a2 2 0 0 0 2-2H6a2 2 0 0 0 2 2M8 1.918l-.797.161A4 4 0 0 0 4 6c0 .628-.134 2.197-.459 3.742-.16.767-.376 1.566-.663 2.258h10.244c-.287-.692-.502-1.49-.663-2.258C12.134 8.197 12 6.628 12 6a4 4 0 0 0-3.203-3.92zM14.22 12c.223.447.481.801.78 1H1c.299-.199.557-.553.78-1C2.68 10.2 3 6.88 3 6c0-2.42 1.72-4.44 4.005-4.901a1 1 0 1 1 1.99 0A5 5 0 0 1 13 6c0 .88.32 4.2 1.22 6" />
			</svg>
			<span
				className="position-absolute badge rounded-pill bg-danger"
				style={{ top: 0, right: -6, fontSize: "0.65em" }}
			>
				{count}
			</span>
		</a>
	);
};

export default TradeBell;
