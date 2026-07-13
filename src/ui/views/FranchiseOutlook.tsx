import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import type { View } from "../../common/types.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";

type TierKey = "allIn" | "buyer" | "fringe" | "seller" | "teardown";

// Each buy/sell tier gets a label + Bootstrap badge color.
const TIER_META: Record<TierKey, { label: string; className: string }> = {
	allIn: { label: "All-in", className: "bg-danger" },
	buyer: { label: "Buyer", className: "bg-success" },
	fringe: { label: "Fringe", className: "bg-secondary" },
	seller: { label: "Seller", className: "bg-warning text-dark" },
	teardown: { label: "Teardown", className: "bg-info text-dark" },
};

const TierBadge = ({ tier }: { tier: TierKey }) => {
	const meta = TIER_META[tier];
	return <span className={`badge ${meta.className}`}>{meta.label}</span>;
};

const TABLE_CLASS = "table table-striped table-borderless table-sm align-middle";

const FranchiseOutlook = ({
	teams,
	userTid,
	season,
}: View<"franchiseOutlook">) => {
	useTitleBar({ title: "Franchise Outlook" });

	return (
		<div style={{ overflowX: "auto" }}>
			<table className={TABLE_CLASS} style={{ minWidth: 720 }}>
				<thead>
					<tr>
						<th>Team</th>
						<th>Outlook</th>
						<th className="text-end">Record</th>
						<th>Seeking</th>
						<th>Needs</th>
						<th>Shopping</th>
						<th className="text-center">Core</th>
					</tr>
				</thead>
				<tbody>
					{teams.map((t) => {
						const isUser = t.tid === userTid;
						return (
							<tr
								key={t.tid}
								className={isUser ? "table-info" : undefined}
							>
								<td style={{ whiteSpace: "nowrap" }}>
									<TeamLogoInline
										imgURL={t.imgURL}
										imgURLSmall={t.imgURLSmall}
										includePlaceholderIfNoLogo
										size={20}
									/>{" "}
									<a
										href={helpers.leagueUrl([
											"roster",
											`${t.abbrev}_${t.tid}`,
											season,
										])}
										className="fw-bold"
									>
										{t.abbrev}
									</a>
									<span className="text-body-secondary ms-1 small">
										#{t.ovrRank}
									</span>
								</td>
								<td>
									<TierBadge tier={t.tier as TierKey} />
								</td>
								<td className="text-end" style={{ whiteSpace: "nowrap" }}>
									{t.won}-{t.lost}
								</td>
								<td className="small">{t.seeking}</td>
								<td>
									{t.needs.length === 0 ? (
										<span className="text-body-secondary">—</span>
									) : (
										t.needs.map((n) => (
											<span
												key={n.pos}
												className="badge bg-light-bordered text-body me-1"
											>
												{n.pos}
											</span>
										))
									)}
								</td>
								<td className="small">
									{t.shopping.length === 0 ? (
										<span className="text-body-secondary">—</span>
									) : (
										t.shopping.map((p, i) => (
											<span key={p.pid}>
												{i > 0 ? ", " : ""}
												<a href={helpers.leagueUrl(["player", p.pid])}>
													{p.name}
												</a>{" "}
												<span className="text-body-secondary">
													({p.ovr}, {p.age})
												</span>
											</span>
										))
									)}
								</td>
								<td
									className="text-center"
									title={t.buildingBlocks.join(", ")}
								>
									{t.buildingBlockCount}
								</td>
							</tr>
						);
					})}
				</tbody>
			</table>
		</div>
	);
};

export default FranchiseOutlook;
