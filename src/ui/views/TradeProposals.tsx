import { PHASE } from "../../common/constants.ts";
import { tradesBlocked } from "../../common/tradesBlocked.ts";
import useTitleBar from "../hooks/useTitleBar.tsx";
import type { View } from "../../common/types.ts";
import { SummaryTeam } from "./Trade/Summary.tsx";
import { toWorker } from "../util/toWorker.ts";
import { useLocal } from "../util/local.ts";
import {
	Offer,
	OfferTable,
	type OfferType,
	pickScore,
	playerScore,
} from "./TradingBlock/index.tsx";
import { useState } from "react";
import { ActionButton } from "../components/ActionButton.tsx";
import useTradeOffersSwitch from "../hooks/useTradeOffersSwitch.tsx";

const TradeProposals = ({
	offers,
	pending,
	seed,
	standing,
}: View<"tradeProposals">) => {
	const {
		challengeNoRatings,
		challengeNoTrades,
		gameOver,
		phase,
		salaryCap,
		salaryCapType,
		spectator,
		teamInfoCache,
		tradesAfterDeadline,
	} = useLocal([
		"challengeNoRatings",
		"challengeNoTrades",
		"gameOver",
		"phase",
		"salaryCap",
		"salaryCapType",
		"spectator",
		"teamInfoCache",
		"tradesAfterDeadline",
	]);

	const [removedTids, setRemovedTids] = useState<number[]>([]);

	// Without this, we'd still see the old removedTids even after 10 games are played and there are new offers
	const [prevSeed, setPrevSeed] = useState(seed);
	if (seed !== prevSeed) {
		setRemovedTids([]);
		setPrevSeed(seed);
	}

	useTitleBar({ title: "Trade Proposals" });

	const [refreshing, setRefreshing] = useState(false);

	const tradeOffersSwitch = useTradeOffersSwitch();

	if (spectator) {
		return <p>You're not allowed to make trades in spectator mode.</p>;
	}

	if (challengeNoTrades) {
		return (
			<div>
				<p className="alert alert-danger d-inline-block">
					<b>Challenge Mode:</b> You're not allowed to make trades.
				</p>
			</div>
		);
	}

	if (
		tradesBlocked(phase, tradesAfterDeadline) ||
		phase === PHASE.FANTASY_DRAFT ||
		gameOver
	) {
		return (
			<div>
				<h2>Error</h2>
				<p>
					You're not allowed to make trades{" "}
					{phase === PHASE.AFTER_TRADE_DEADLINE
						? "after the trade deadline"
						: "now"}
					.
				</p>
			</div>
		);
	}

	const handleNegotiate = async (offer: {
		dpids: number[];
		dpidsUser: number[];
		pids: number[];
		pidsUser: number[];
		tid: number;
	}) => {
		await toWorker("actions", "tradeFor", {
			otherDpids: offer.dpids,
			otherPids: offer.pids,
			tid: offer.tid,
			userDpids: offer.dpidsUser,
			userPids: offer.pidsUser,
		});
	};

	const getSearchSortValues = (
		players: OfferType["players"],
		picks: OfferType["picks"],
	) => {
		return {
			searchValue: `${players
				.map((p) => `${p.firstName} ${p.lastName} ${p.ratings.pos}`)
				.join(" ")} ${picks.map((pick) => pick.desc).join(" ")}`,
			sortValue: playerScore(players) + pickScore(picks),
		};
	};

	const filteredOffers = offers.filter(
		(offer) => !removedTids.includes(offer.tid),
	);

	return (
		<>
			{pending.length > 0 ? (
				<>
					<h2>Pending deals</h2>
					{pending.map((deal) => (
						<div
							key={`${deal.tids[0]}-${deal.tids[1]}`}
							className="d-flex flex-wrap gap-5 mb-3"
						>
							<div className="w-100 mb-n4 text-body-secondary small">
								{deal.simsLeft === 0
									? "Closes when the next sim starts"
									: `Open for ${deal.simsLeft} more ${deal.simsLeft === 1 ? "sim" : "sims"}`}
							</div>
							{deal.summary.teams.map((t, k) => (
								<div key={k}>
									<SummaryTeam
										hideFinanceInfo
										showInlinePlayerInfo
										summary={deal.summary}
										t={t}
									/>
									{deal.notes ? (
										<div className="text-body-secondary small mb-1">
											{deal.notes[k]!.why}. Wants{" "}
											{deal.notes[k]!.wants.charAt(0).toLowerCase()}
											{deal.notes[k]!.wants.slice(1)}.
										</div>
									) : null}
									{!deal.human[k] &&
									deal.give[k]!.pids.length + deal.give[k]!.dpids.length > 0 ? (
										<button
											className="btn btn-sm btn-primary"
											onClick={() => {
												void handleNegotiate({
													dpids: deal.give[k]!.dpids,
													dpidsUser: [],
													pids: deal.give[k]!.pids,
													pidsUser: [],
													tid: deal.tids[k]!,
												});
											}}
										>
											{deal.bid[k] ? "Bid again" : "Bid"}
										</button>
									) : null}
									{deal.bidders[k]!.length > 0 ? (
										<span className="ms-2 text-body-secondary">
											Bids:{" "}
											{deal.bidders[k]!.map(
												(tid) => teamInfoCache[tid]?.abbrev,
											).join(", ")}
										</span>
									) : null}
								</div>
							))}
						</div>
					))}
					<h2>Trade proposals</h2>
				</>
			) : null}
			{standing ? (
				offers.length === 0 ? (
					<p>No trade proposals right now.</p>
				) : null
			) : (
				<>
					<p>
						These are trade proposals from up to 5 AI teams. New teams will
						appear here every 10 games.
					</p>
					<ActionButton
						onClick={async () => {
							setRefreshing(true);
							await toWorker("main", "incrementTradeProposalsSeed", undefined);
							setRefreshing(false);
						}}
						processing={refreshing}
						processingText="Loading"
						variant="secondary"
					>
						Refresh trade proposals
					</ActionButton>
				</>
			)}
			{tradeOffersSwitch.toggle}
			{tradeOffersSwitch.value === "table" ? (
				<OfferTable
					assetCols={[
						{
							title: "You receive",
							sortSequence: ["desc", "asc"],
							sortType: "number",
						},
						{
							title: "You trade away",
							sortSequence: ["desc", "asc"],
							sortType: "number",
						},
					]}
					getAssetColContents={(offer) => {
						return [
							{
								value: (
									<SummaryTeam
										hideFinanceInfo
										hideTeamOvr
										showInlinePlayerInfo
										summary={offer.summary}
										t={offer.summary.teams[0]}
									/>
								),
								...getSearchSortValues(offer.players, offer.picks),
							},
							{
								value: (
									<SummaryTeam
										hideFinanceInfo
										hideTeamOvr
										showInlinePlayerInfo
										summary={offer.summary}
										t={offer.summary.teams[1]}
									/>
								),
								...getSearchSortValues(offer.playersUser, offer.picksUser),
							},
						];
					}}
					challengeNoRatings={challengeNoRatings}
					handleNegotiate={handleNegotiate}
					handleRemove={(i) => {
						// The table lists the offers not yet removed.
						const tid = filteredOffers[i]!.tid;
						setRemovedTids((prevTids) => [...prevTids, tid]);
						if (standing) {
							void toWorker("main", "declineTradeProposal", tid);
						}
					}}
					offers={filteredOffers}
					salaryCap={salaryCap}
					salaryCapType={salaryCapType}
				/>
			) : (
				<>
					{filteredOffers.map((offer, i) => {
						return (
							<Offer
								key={offer.tid}
								challengeNoRatings={challengeNoRatings}
								onNegotiate={() => {
									handleNegotiate(offer);
								}}
								onRemove={() => {
									const tid = offer.tid;
									setRemovedTids((prevTids) => [...prevTids, tid]);
									if (standing) {
										void toWorker("main", "declineTradeProposal", tid);
									}
								}}
								salaryCap={salaryCap}
								salaryCapType={salaryCapType}
								teamInfo={teamInfoCache[offer.tid]!}
								hideTopTeamOvrs
								first={i === 0}
								{...offer}
							>
								<div className="d-flex gap-5">
									{offer.summary.teams.map((t, j) => {
										return (
											<div key={j}>
												<SummaryTeam
													hideFinanceInfo
													showInlinePlayerInfo
													summary={offer.summary}
													t={t}
												/>
											</div>
										);
									})}
								</div>
							</Offer>
						);
					})}
				</>
			)}
		</>
	);
};

export default TradeProposals;
