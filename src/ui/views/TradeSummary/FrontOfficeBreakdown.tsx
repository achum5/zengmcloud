import type {
	FrontOfficeAsset,
	FrontOfficeBreakdown as Breakdown,
} from "../../../common/types.ts";
import { helpers } from "../../util/helpers.ts";
import { useLocal } from "../../util/local.ts";

// Realistic front offices: the whole calculation behind a CPU-to-CPU trade.

const SEASONS_SHOWN = 6;

const pct = (x: number) => `${(100 * x).toFixed(1)}%`;
const signed = (x: number, digits: number) =>
	`${x >= 0 ? "+" : ""}${x.toFixed(digits)}`;

const AssetRows = ({
	assets,
	seasons,
}: {
	assets: FrontOfficeAsset[];
	seasons: number[];
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	return (
		<>
			{assets.map((asset) => {
				const cells = seasons.map((season, y) => (
					<td key={season}>
						{asset.ovrBySeason[y] ? asset.ovrBySeason[y] : "-"}
						<span className="text-body-secondary">
							{" "}
							{pct(asset.hereBySeason[y] ?? 0)}
						</span>
					</td>
				));
				if (asset.kind === "player") {
					return (
						<tr key={`p${asset.pid}`}>
							<td>
								<a href={helpers.leagueUrl(["player", asset.pid])}>
									{asset.name}
								</a>
							</td>
							<td>
								{asset.age}y, {asset.ovr} ovr,{" "}
								{helpers.formatCurrency(asset.amount / 1000, "M")} thru{" "}
								{asset.exp}, re-sign {pct(asset.reSign)}
							</td>
							{cells}
						</tr>
					);
				}
				return (
					<tr key={`d${asset.dpid}`}>
						<td>
							{asset.season} round {asset.round} (
							{teamInfoCache[asset.originalTid]?.abbrev ?? asset.originalTid})
						</td>
						<td>
							place {asset.place + 1} ± {asset.spread}
						</td>
						{cells}
					</tr>
				);
			})}
		</>
	);
};

const FrontOfficeBreakdown = ({
	breakdown,
	abbrevs,
}: {
	breakdown: Breakdown;
	abbrevs: string[];
}) => {
	const seasons = breakdown.seasons.slice(0, SEASONS_SHOWN);
	return (
		<div className="mt-4">
			<h2>Front office</h2>
			{breakdown.sides.map((side, i) => (
				<div key={side.tid} className="mb-4">
					<h3>{abbrevs[i]}</h3>
					<p className="mb-2">{side.reason}</p>
					<p className="mb-2 text-body-secondary">
						Traded away: {side.tradedAway.join(" / ")}
					</p>
					<div className="table-responsive">
						<table className="table table-sm table-striped table-borderless w-auto">
							<thead>
								<tr>
									<th>Season</th>
									{breakdown.seasons.map((season) => (
										<th key={season}>{season}</th>
									))}
								</tr>
							</thead>
							<tbody>
								<tr>
									<td>Strength</td>
									{side.before.map((x, y) => (
										<td key={y}>
											{signed(x, 1)} → {signed(side.after[y]!, 1)}
										</td>
									))}
								</tr>
								<tr>
									<td>Title</td>
									{side.titleBefore.map((x, y) => (
										<td key={y}>
											{pct(x)} → {pct(side.titleAfter[y]!)}
										</td>
									))}
								</tr>
								<tr>
									<td>Cap room</td>
									{side.roomBefore.map((x, y) => (
										<td key={y}>
											{pct(x)} → {pct(side.roomAfter[y]!)}
										</td>
									))}
								</tr>
								<tr>
									<td>FA buys</td>
									{side.buysBefore.map((x, y) => (
										<td key={y}>
											{x.join("/") || "-"} →{" "}
											{side.buysAfter[y]!.join("/") || "-"}
										</td>
									))}
								</tr>
								<tr>
									<td>Value x100</td>
									{side.value.map((x, y) => (
										<td key={y}>{signed(100 * x, 2)}</td>
									))}
								</tr>
							</tbody>
						</table>
					</div>
					<div className="table-responsive">
						<table className="table table-sm table-striped table-borderless w-auto">
							<thead>
								<tr>
									<th />
									<th />
									{seasons.map((season) => (
										<th key={season}>{season}</th>
									))}
								</tr>
							</thead>
							<tbody>
								<tr>
									<th colSpan={2 + seasons.length}>Gets</th>
								</tr>
								<AssetRows assets={side.gets} seasons={seasons} />
								<tr>
									<th colSpan={2 + seasons.length}>Gives</th>
								</tr>
								<AssetRows assets={side.gives} seasons={seasons} />
							</tbody>
						</table>
					</div>
				</div>
			))}
		</div>
	);
};

export default FrontOfficeBreakdown;
