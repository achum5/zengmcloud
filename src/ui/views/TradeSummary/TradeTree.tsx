import { useEffect, useState } from "react";
import { toWorker } from "../../util/toWorker.ts";
import { helpers } from "../../util/helpers.ts";
import { formatGameDate } from "../../../common/calendarDates.ts";
import { useLocal } from "../../util/local.ts";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { PlayerPicture } from "../../components/PlayerPicture.tsx";
import type {
	TradeTree as Tree,
	TreeAsset,
	TreeEnd,
	TreeTrade,
	getTradeTrees,
} from "../../../worker/util/tradeTree.ts";

// THE TRADE TREE: what each team ended up with because of this trade - what
// it got, what it later turned those into, and so on, to where each branch
// ends. Worked out after the page is up, and shown only when something from
// the trade was traded on.

type Data = Awaited<ReturnType<typeof getTradeTrees>>;

const Logo = ({ tid }: { tid: number }) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const t = teamInfoCache[tid];
	const url = t?.imgURLSmall ?? t?.imgURL;
	return url ? <img className="tt-logo" src={url} alt="" /> : null;
};

const Face = ({ pid }: { pid: number }) => {
	const { lid } = useLocal(["lid"]);
	const face = usePlayerFace(pid, undefined, lid);
	return (
		<a
			className="tt-face"
			href={helpers.leagueUrl(["player", pid])}
			tabIndex={-1}
			aria-hidden="true"
		>
			{face && (face.face || face.imgURL) ? (
				<PlayerPicture
					face={face.face}
					imgURL={face.imgURL}
					colors={face.colors}
					jersey={face.jersey}
					lazy
				/>
			) : null}
		</a>
	);
};

const pickLine = (asset: TreeAsset) => {
	const pick = asset.pick;
	if (!pick) {
		return null;
	}
	const from = pick.abbrev ? `, from ${pick.abbrev}` : "";
	if (asset.pid !== undefined && pick.pick !== undefined) {
		return `${pick.season} pick ${pick.round}-${pick.pick}${from}`;
	}
	return pick.abbrev ? `From ${pick.abbrev}` : null;
};

const End = ({ end, abbrev }: { end: TreeEnd; abbrev: string }) => {
	switch (end.type) {
		case "still":
			return <span className="tt-end tt-end-still">On {abbrev} now</span>;
		case "retired":
			return <span className="tt-end">Retired {end.season}</span>;
		case "signed":
			return (
				<span className="tt-end">
					Signed with {end.abbrev}, {end.season}
				</span>
			);
		case "released":
			return <span className="tt-end">Released</span>;
		case "pick":
			return <span className="tt-end tt-end-still">Not used yet</span>;
		case "tradedWith":
			return (
				<span className="tt-end">
					Traded with {end.name}
					<span className="tt-up"> ↑</span>
				</span>
			);
		default:
			return null;
	}
};

const Deal = ({ trade }: { trade: TreeTrade }) => (
	<div className="tt-deal">
		<Logo tid={trade.tid} />
		<span>
			{trade.alsoSent.length > 0 ? (
				<>Traded with {trade.alsoSent.join(", ")} to </>
			) : (
				"Traded to "
			)}
			<a href={helpers.leagueUrl(["trade_summary", trade.eid])}>
				{trade.abbrev}{" "}
				{trade.date !== undefined
					? `on ${formatGameDate(trade.date, "withYear")}`
					: `in ${trade.season}`}
			</a>{" "}
			for
		</span>
	</div>
);

const Node = ({
	asset,
	abbrev,
	stat,
}: {
	asset: TreeAsset;
	abbrev: string;
	stat: string;
}) => {
	const sub = pickLine(asset);
	const traded = asset.end.type === "traded" ? asset.end.trade : undefined;
	return (
		<li className="tt-node">
			<div className="tt-asset">
				{asset.pid !== undefined ? (
					<Face pid={asset.pid} />
				) : (
					<span className="tt-pick" aria-hidden="true">
						{asset.pick ? helpers.ordinal(asset.pick.round) : "?"}
					</span>
				)}
				<div className="tt-main">
					<div className="tt-name">
						{asset.pid !== undefined ? (
							<a href={helpers.leagueUrl(["player", asset.pid])}>
								{asset.name}
							</a>
						) : (
							`${asset.name} round pick`
						)}
					</div>
					{sub ? <div className="tt-sub">{sub}</div> : null}
					<End end={asset.end} abbrev={abbrev} />
				</div>
				{asset.stat !== undefined ? (
					<div className="tt-stat">
						<b>{helpers.roundStat(asset.stat, "ws")}</b>
						<span>{stat}</span>
					</div>
				) : null}
			</div>
			{traded ? (
				<div className="tt-branch">
					<Deal trade={traded} />
					<Kids assets={traded.got} abbrev={abbrev} stat={stat} />
				</div>
			) : null}
		</li>
	);
};

const Kids = ({
	assets,
	abbrev,
	stat,
}: {
	assets: TreeAsset[];
	abbrev: string;
	stat: string;
}) =>
	assets.length === 0 ? (
		<ul className="tt-kids">
			<li className="tt-node">
				<div className="tt-asset tt-nothing">Nothing</div>
			</li>
		</ul>
	) : (
		<ul className="tt-kids">
			{assets.map((asset) => (
				<Node key={asset.key} asset={asset} abbrev={abbrev} stat={stat} />
			))}
		</ul>
	);

const TreeCard = ({ tree, stat }: { tree: Tree; stat: string }) => (
	<section className="lh-card tt-card">
		<div className="tt-root">
			<Logo tid={tree.tid} />
			<div className="tt-root-main">
				<h3 className="tt-root-title">{tree.abbrev}'s side</h3>
				<div className="tt-root-sub">
					{tree.players} {tree.players === 1 ? "player" : "players"}
					{tree.trades > 0
						? ` · ${tree.trades} more ${tree.trades === 1 ? "trade" : "trades"}`
						: null}
				</div>
			</div>
			<div className="tt-stat tt-root-stat">
				<b>{helpers.roundStat(tree.stat, "ws")}</b>
				<span>
					{stat} with {tree.abbrev}
				</span>
			</div>
		</div>
		<Kids assets={tree.got} abbrev={tree.abbrev} stat={stat} />
	</section>
);

const TradeTree = ({ eid, stat }: { eid: number; stat: string }) => {
	const { gameSimInProgress } = useLocal(["gameSimInProgress"]);
	const [data, setData] = useState<{ eid: number; value: Data } | undefined>();

	useEffect(() => {
		if (gameSimInProgress) {
			return;
		}
		let cancelled = false;
		(async () => {
			const value = await toWorker("main", "getTradeTrees", eid);
			if (!cancelled) {
				setData({ eid, value });
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [eid, gameSimInProgress]);

	const trees = data?.eid === eid ? data.value : undefined;
	if (!trees || trees.every((tree) => tree.trades === 0)) {
		return null;
	}

	return (
		<div className="tt">
			<h2>Trade tree</h2>
			<div className="tt-cols">
				{trees.map((tree) => (
					<TreeCard key={tree.tid} tree={tree} stat={stat} />
				))}
			</div>
		</div>
	);
};

export default TradeTree;
