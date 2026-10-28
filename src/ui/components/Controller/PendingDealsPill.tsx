import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";

// Realistic Front Offices: CPU deals held for bids until the next sim day.
const PendingDealsPill = () => {
	const { pendingDeals } = useLocal(["pendingDeals"]);

	if (!pendingDeals) {
		return null;
	}

	return (
		<a
			href={helpers.leagueUrl(["trade_proposals"])}
			className="btn btn-sm btn-warning ms-2 text-nowrap"
			title="Bid before the next sim day"
		>
			{pendingDeals === 1 ? "Pending deal" : `${pendingDeals} pending deals`}
		</a>
	);
};

export default PendingDealsPill;
