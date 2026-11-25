import type {
	DiscriminateUnion,
	DraftPickSeason,
	EventBBGM,
} from "../../common/types.ts";
import { idb } from "../db/index.ts";
import { processAssets } from "../views/tradeSummary.ts";
import { getTeamInfoBySeason } from "./getTeamInfoBySeason.ts";
import { assetIsPlayer } from "./formatEventText.ts";
import { helpers } from "./index.ts";

// A TRADE TREE: everything a team ended up with because of one trade. What
// it got; and when it later traded one of those away, what it got for that,
// and so on down - until each branch ends with a player still on the team,
// retired, gone in free agency or released, or a pick not used yet.

type TradeEvent = DiscriminateUnion<EventBBGM, "type", "trade">;

export type TreePick = {
	season: DraftPickSeason;
	round: number;
	// The team it originally belonged to, when that isn't the team holding it.
	abbrev?: string;
	// Which pick it turned out to be, once used.
	pick?: number;
};

export type TreeEnd =
	| { type: "still" }
	| { type: "retired"; season: number }
	| { type: "signed"; season: number; abbrev: string }
	| { type: "released" }
	| { type: "pick" }
	| { type: "traded"; trade: TreeTrade }
	// Traded in the same deal as another branch, which shows what came back.
	| { type: "tradedWith"; name: string; eid: number }
	| { type: "unknown" };

export type TreeAsset = {
	key: string;
	// A player, or the player a pick was used on.
	pid?: number;
	name: string;
	pick?: TreePick;
	// Win shares (or the sport's equivalent) with the team.
	stat?: number;
	end: TreeEnd;
};

export type TreeTrade = {
	eid: number;
	season: number;
	tid: number;
	abbrev: string;
	// What else the team sent in that deal, from outside this tree.
	alsoSent: string[];
	got: TreeAsset[];
};

export type TradeTree = {
	tid: number;
	abbrev: string;
	got: TreeAsset[];
	stat: number;
	players: number;
	trades: number;
};

const MAX_NODES = 80;

const pickName = (pick: TreePick) =>
	typeof pick.season === "number"
		? `${pick.season} ${helpers.ordinal(pick.round)}`
		: `${helpers.upperCaseFirstLetter(pick.season)} ${helpers.ordinal(pick.round)}`;

// The next trade in which this team sent this pick on.
const nextPickTrade = async (dpid: number, afterEid: number, tid: number) => {
	const events = await idb.merged.events.getAll({ dpid });
	let next: TradeEvent | undefined;
	for (const event of events) {
		if (
			event.type !== "trade" ||
			event.eid <= afterEid ||
			!event.teams ||
			event.phase === undefined
		) {
			continue;
		}
		const mine = event.tids.indexOf(tid);
		if (mine < 0) {
			continue;
		}
		const sent = event.teams[mine === 0 ? 1 : 0].assets.some(
			(asset) => !assetIsPlayer(asset) && asset.dpid === dpid,
		);
		if (sent && (!next || event.eid < next.eid)) {
			next = event;
		}
	}
	return next;
};

export const getTradeTrees = async (
	eid: number,
): Promise<[TradeTree, TradeTree] | undefined> => {
	const event = await idb.merged.events.get(eid);
	if (
		!event ||
		event.type !== "trade" ||
		!event.teams ||
		event.phase === undefined
	) {
		return;
	}

	const build = async (i: 0 | 1): Promise<TradeTree> => {
		const tid = event.tids[i];
		const abbrev =
			(await getTeamInfoBySeason(tid, event.season))?.abbrev ?? "???";

		let nodes = 0;
		let stat = 0;
		const pids = new Set<number>();
		const shown = new Map<number, string>();
		const inTree = new Set<string>();
		const sentIn: {
			trade: TreeTrade;
			sent: { key: string; name: string }[];
		}[] = [];

		const follow = async (
			trade: TradeEvent,
			name: string,
		): Promise<TreeEnd> => {
			const already = shown.get(trade.eid);
			if (already !== undefined) {
				return { type: "tradedWith", name: already, eid: trade.eid };
			}
			shown.set(trade.eid, name);
			const mine = trade.tids.indexOf(tid) as 0 | 1;
			const other = trade.tids[mine === 0 ? 1 : 0];
			const node: TreeTrade = {
				eid: trade.eid,
				season: trade.season,
				tid: other,
				abbrev:
					(await getTeamInfoBySeason(other, trade.season))?.abbrev ?? "???",
				alsoSent: [],
				got: await received(trade, mine),
			};
			sentIn.push({
				trade: node,
				sent: trade.teams![mine === 0 ? 1 : 0].assets.map((asset) =>
					assetIsPlayer(asset)
						? { key: `p${asset.pid}`, name: asset.name }
						: {
								key: `d${asset.dpid}`,
								name: pickName({ season: asset.season, round: asset.round }),
							},
				),
			});
			return { type: "traded", trade: node };
		};

		const received = async (
			trade: TradeEvent,
			side: 0 | 1,
		): Promise<TreeAsset[]> => {
			const raw = trade.teams![side].assets;
			const infos = await processAssets(trade, side);
			const out: TreeAsset[] = [];
			for (const [j, info] of infos.entries()) {
				const asset = raw[j]!;
				nodes += 1;
				const key = assetIsPlayer(asset) ? `p${asset.pid}` : `d${asset.dpid}`;
				inTree.add(key);
				if (info.type === "player" || info.type === "realizedPick") {
					inTree.add(`p${info.pid}`);
				}

				let pick: TreePick | undefined;
				if (info.type === "realizedPick" || info.type === "unrealizedPick") {
					pick = {
						season: info.season,
						round: info.round,
						abbrev:
							info.tid === tid
								? undefined
								: (
										await getTeamInfoBySeason(
											info.tid,
											typeof info.season === "number"
												? info.season
												: trade.season,
										)
									)?.abbrev,
						pick: info.type === "realizedPick" ? info.pick : undefined,
					};
				}

				if (info.type === "deletedPlayer") {
					out.push({ key, name: info.name, end: { type: "unknown" } });
					continue;
				}

				const pickTrade = async () =>
					assetIsPlayer(asset)
						? undefined
						: await nextPickTrade(asset.dpid, trade.eid, tid);

				if (info.type === "unrealizedPick") {
					const next = nodes < MAX_NODES ? await pickTrade() : undefined;
					const name = pickName(pick!);
					out.push({
						key,
						name,
						pick,
						end: next ? await follow(next, name) : { type: "pick" },
					});
					continue;
				}

				// A player, or a pick used on one.
				const outcome = info.outcome;
				let end: TreeEnd = { type: "unknown" };
				let tradedPickOn = false;
				if (nodes >= MAX_NODES) {
					end = { type: "unknown" };
				} else if (outcome?.type === "tradeBeforeDraft") {
					const next = await pickTrade();
					if (next) {
						tradedPickOn = true;
						end = await follow(next, pickName(pick!));
					}
				} else if (outcome?.type === "trade" && outcome.eid !== undefined) {
					const next = await idb.merged.events.get(outcome.eid);
					if (
						next?.type === "trade" &&
						next.teams &&
						next.phase !== undefined
					) {
						end = await follow(next, info.name);
					}
				} else if (outcome?.type === "stillOnTeam") {
					end = { type: "still" };
				} else if (outcome?.type === "retired") {
					end = { type: "retired", season: outcome.season };
				} else if (
					outcome?.type === "freeAgent" ||
					outcome?.type === "godMode" ||
					outcome?.type === "sisyphus"
				) {
					end = {
						type: "signed",
						season: outcome.season,
						abbrev: outcome.abbrev,
					};
				} else if (outcome === undefined) {
					end = { type: "released" };
				}

				if (tradedPickOn) {
					// He was never the team's: the pick went on before the draft.
					out.push({ key, name: pickName(pick!), pick, end });
					continue;
				}

				pids.add(info.pid);
				stat += info.statTeam;
				out.push({
					key,
					pid: info.pid,
					name: info.name,
					pick,
					stat: info.statTeam,
					end,
				});
			}
			return out;
		};

		const got = await received(event, i);

		for (const { trade, sent } of sentIn) {
			trade.alsoSent = sent
				.filter((asset) => !inTree.has(asset.key))
				.map((asset) => asset.name);
		}

		return {
			tid,
			abbrev,
			got,
			stat,
			players: pids.size,
			trades: shown.size,
		};
	};

	return [await build(0), await build(1)];
};
