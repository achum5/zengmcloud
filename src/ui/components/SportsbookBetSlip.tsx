import { useState, type ReactNode } from "react";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { showNotification } from "../util/showNotification.ts";
import type { SportsbookMarket } from "../../common/types.ts";
import {
	americanToDecimal,
	combinedDecimalOdds,
	decimalToAmerican,
	formatAmerican,
	formatSportsbookMoney,
	parlayConflict,
} from "../../common/sportsbook.ts";

// Shared bet-slip pieces used by both the main Sportsbook page and a single
// game's prop board - kept as one module so "pick an odds button, stake it,
// place the slip atomically" behaves identically everywhere it appears.

export type Pick = {
	key: string;
	market: SportsbookMarket;
	odds: number;
	title: string;
	sub: string;
};

// An odds button: line above the price, fills in when it's in the slip. Fixed
// size so every column of them lines up.
export const OddsCell = ({
	line,
	odds,
	selected,
	onClick,
}: {
	line?: string;
	odds: number;
	selected: boolean;
	onClick: () => void;
}) => (
	<button
		type="button"
		onClick={onClick}
		className={`btn btn-sm w-100 d-flex flex-column align-items-center justify-content-center lh-1 rounded-2 ${selected ? "btn-primary" : "btn-light-bordered"}`}
		style={{ height: 44, padding: "2px 4px", minWidth: 72 }}
	>
		{line !== undefined ? (
			<span
				className={selected ? "" : "text-body-secondary"}
				style={{ fontSize: "0.72rem", opacity: selected ? 0.85 : undefined }}
			>
				{line}
			</span>
		) : null}
		<span className={`fw-bold ${selected ? "" : "text-primary"}`}>
			{formatAmerican(odds)}
		</span>
	</button>
);

// Local slip state (picks + stakes) plus the atomic place-the-whole-slip
// action. Each page that offers odds buttons gets its OWN slip instance -
// there's no cross-page shared slip, so navigating away clears it.
export const useBetSlip = (
	tid: number,
	opts?: { allStarRosterSize?: number },
) => {
	const [picks, setPicks] = useState<Pick[]>([]);
	const [stakes, setStakes] = useState<Record<string, string>>({});
	const [parlay, setParlay] = useState(false);
	const [parlayStake, setParlayStake] = useState("");
	const [placing, setPlacing] = useState(false);
	// What was just placed, shown in the slip until it's dismissed or a new
	// pick goes in - a ticket, the way a book confirms one.
	const [receipt, setReceipt] = useState<
		{ title: string; sub: string; stake: number; toWin: number }[] | undefined
	>();

	const selectedKeys = new Set(picks.map((p) => p.key));

	const togglePick = (pick: Pick) => {
		setReceipt(undefined);
		setPicks((prev) =>
			prev.some((p) => p.key === pick.key)
				? prev.filter((p) => p.key !== pick.key)
				: [...prev, pick],
		);
	};
	const removePick = (key: string) =>
		setPicks((prev) => prev.filter((p) => p.key !== key));
	// The board moved under the slip (a game simmed, a price changed): take
	// the board's current line and price for every pick still on it. Picks
	// whose market is gone stay, marked, until removed.
	const acceptChanges = (live: Map<string, Pick>) =>
		setPicks((prev) => prev.map((p) => live.get(p.key) ?? p));
	const clearSlip = () => {
		setPicks([]);
		setStakes({});
		setParlayStake("");
	};

	// Straight-bet totals (each pick staked separately).
	const totalStake = picks.reduce(
		(sum, p) => sum + (Number.parseFloat(stakes[p.key] ?? "") || 0),
		0,
	);
	const totalPayout = picks.reduce((sum, p) => {
		const stake = Number.parseFloat(stakes[p.key] ?? "") || 0;
		return sum + stake * americanToDecimal(p.odds);
	}, 0);

	// Parlay: the picks combine into one ticket, odds compound.
	const parlayDecimal = combinedDecimalOdds(picks.map((p) => p.odds));
	const parlayAmerican = decimalToAmerican(parlayDecimal);
	const parlayStakeNum = Number.parseFloat(parlayStake) || 0;
	const parlayPayout = parlayStakeNum * parlayDecimal;
	// Only offer parlays for 2+ legs; block genuinely contradictory tickets.
	const canParlay = picks.length >= 2;
	const conflict = canParlay
		? parlayConflict(
				picks.map((p) => p.market),
				{
					allStarRosterSize: opts?.allStarRosterSize,
				},
			)
		: undefined;
	const parlayActive = parlay && canParlay;

	const placeParlay = async () => {
		if (conflict) {
			showNotification({ type: "error", text: conflict });
			return;
		}
		if (parlayStakeNum <= 0) {
			showNotification({ type: "error", text: "Enter a stake first." });
			return;
		}
		setPlacing(true);
		try {
			await toWorker("main", "sportsbookPlaceBetSlip", {
				tid,
				parlay: true,
				stake: parlayStakeNum,
				picks: picks.map((p) => ({
					market: p.market,
					stake: parlayStakeNum,
					americanOdds: p.odds,
					label: `${p.title} — ${p.sub}`,
				})),
			});
			setReceipt([
				{
					title: `${picks.length}-leg parlay`,
					sub: picks.map((p) => p.title).join(", "),
					stake: parlayStakeNum,
					toWin: parlayPayout - parlayStakeNum,
				},
			]);
			clearSlip();
			await realtimeUpdate(["watchList"]);
		} catch (error) {
			showNotification({
				type: "error",
				text: error instanceof Error ? error.message : "Could not place bet.",
			});
		} finally {
			setPlacing(false);
		}
	};

	const placeStraights = async () => {
		const toPlace = picks.filter(
			(p) => (Number.parseFloat(stakes[p.key] ?? "") || 0) > 0,
		);
		if (toPlace.length === 0) {
			showNotification({ type: "error", text: "Enter a stake first." });
			return;
		}
		setPlacing(true);
		try {
			// One atomic call: every pick is validated together, and either all of
			// them are placed or (on any invalid pick) none are and no money moves.
			await toWorker("main", "sportsbookPlaceBetSlip", {
				tid,
				picks: toPlace.map((p) => ({
					market: p.market,
					stake: Number.parseFloat(stakes[p.key]!),
					americanOdds: p.odds,
					label: `${p.title} — ${p.sub}`,
				})),
			});
			setReceipt(
				toPlace.map((p) => {
					const stake = Number.parseFloat(stakes[p.key]!);
					return {
						title: p.title,
						sub: p.sub,
						stake,
						toWin: stake * (americanToDecimal(p.odds) - 1),
					};
				}),
			);
			clearSlip();
			await realtimeUpdate(["watchList"]);
		} catch (error) {
			showNotification({
				type: "error",
				text: error instanceof Error ? error.message : "Could not place bet.",
			});
		} finally {
			setPlacing(false);
		}
	};

	const placeBets = async () => {
		if (placing) {
			return;
		}
		if (parlayActive) {
			await placeParlay();
		} else {
			await placeStraights();
		}
	};

	return {
		picks,
		stakes,
		setStakes,
		parlay,
		setParlay,
		parlayActive,
		canParlay,
		parlayStake,
		setParlayStake,
		parlayDecimal,
		parlayAmerican,
		parlayPayout,
		conflict,
		placing,
		selectedKeys,
		togglePick,
		removePick,
		acceptChanges,
		receipt,
		dismissReceipt: () => setReceipt(undefined),
		clearSlip,
		totalStake,
		totalPayout,
		placeBets,
	};
};

// How a pick in the slip stands against the board as it is now: on it as
// picked, on it at another line or price, or off it altogether (its game has
// gone in, its market has closed).
const pickStatus = (p: Pick, live: Map<string, Pick> | undefined) => {
	if (!live) {
		return { kind: "ok" as const };
	}
	const now = live.get(p.key);
	if (!now) {
		return { kind: "closed" as const };
	}
	if (now.odds !== p.odds || now.title !== p.title) {
		return { kind: "changed" as const, now };
	}
	return { kind: "ok" as const };
};

export const BetSlipCard = ({
	slip,
	balance,
	live,
}: {
	slip: ReturnType<typeof useBetSlip>;
	balance: number;
	// Every pick the page currently offers, by key - so a pick made before the
	// board moved shows its new line or price, or that it's off the board.
	live?: Map<string, Pick>;
}) => {
	const statuses = slip.picks.map((p) => pickStatus(p, live));
	const anyClosed = statuses.some((st) => st.kind === "closed");
	const anyChanged = statuses.some((st) => st.kind === "changed");
	const risk = slip.parlayActive
		? Number.parseFloat(slip.parlayStake) || 0
		: slip.totalStake;
	const toWin = slip.parlayActive
		? slip.parlayPayout - risk
		: slip.totalPayout - slip.totalStake;
	const tooMuch = risk > balance;

	let action: ReactNode;
	if (anyClosed) {
		action = (
			<button
				className="btn btn-secondary w-100"
				onClick={() => {
					slip.picks.forEach((p, i) => {
						if (statuses[i]!.kind === "closed") {
							slip.removePick(p.key);
						}
					});
				}}
			>
				Remove closed picks
			</button>
		);
	} else if (anyChanged && live) {
		action = (
			<button
				className="btn btn-warning w-100"
				onClick={() => slip.acceptChanges(live)}
			>
				Accept changes
			</button>
		);
	} else {
		const disabled =
			slip.placing ||
			risk <= 0 ||
			tooMuch ||
			(slip.parlayActive && slip.conflict !== undefined);
		const staked = slip.picks.filter(
			(p) => (Number.parseFloat(slip.stakes[p.key] ?? "") || 0) > 0,
		).length;
		const what = slip.parlayActive
			? "parlay"
			: staked === 1
				? "bet"
				: `${staked} bets`;
		action = (
			<button
				className="btn btn-primary w-100"
				disabled={disabled}
				onClick={slip.placeBets}
			>
				{tooMuch
					? "Not enough $"
					: slip.placing
						? "Placing…"
						: risk > 0
							? `Place ${what} · ${formatSportsbookMoney(risk)}`
							: "Enter a stake"}
			</button>
		);
	}

	const stakeInput = (
		value: string,
		onChange: (value: string) => void,
		win: number,
	) => (
		<div className="input-group input-group-sm">
			<span className="input-group-text">$</span>
			<input
				type="number"
				min={0}
				className="form-control"
				placeholder="Risk"
				value={value}
				onChange={(e) => onChange(e.target.value)}
			/>
			<span className="input-group-text text-body-secondary">
				{win > 0 ? `To win ${formatSportsbookMoney(win)}` : "To win —"}
			</span>
		</div>
	);

	return (
		<div className="card">
			<div className="card-header d-flex justify-content-between align-items-center py-2">
				<span className="fw-bold">
					Bet Slip{" "}
					{slip.picks.length > 0 ? (
						<span className="badge text-bg-primary ms-1">
							{slip.picks.length}
						</span>
					) : null}
				</span>
				{slip.picks.length > 0 ? (
					<button
						className="btn btn-sm btn-link text-decoration-none p-0"
						onClick={slip.clearSlip}
					>
						Clear
					</button>
				) : null}
			</div>
			<div className="card-body py-2">
				{slip.receipt && slip.picks.length === 0 ? (
					<>
						<div className="fw-bold mb-2">
							{slip.receipt.length === 1
								? "Bet placed"
								: `${slip.receipt.length} bets placed`}
						</div>
						{slip.receipt.map((r, i) => (
							<div key={i} className="border-bottom pb-2 mb-2 small">
								<div className="fw-medium">{r.title}</div>
								<div className="text-body-secondary text-truncate">{r.sub}</div>
								<div>
									{formatSportsbookMoney(r.stake)} to win{" "}
									{formatSportsbookMoney(r.toWin)}
								</div>
							</div>
						))}
						<button
							className="btn btn-sm btn-secondary w-100"
							onClick={slip.dismissReceipt}
						>
							Done
						</button>
					</>
				) : slip.picks.length === 0 ? (
					<div className="text-body-secondary text-center py-4">
						Tap any odds to start a bet slip.
					</div>
				) : (
					<>
						{slip.canParlay ? (
							<div className="btn-group btn-group-sm w-100 mb-2">
								<button
									className={`btn ${slip.parlay ? "btn-outline-primary" : "btn-primary"}`}
									onClick={() => slip.setParlay(false)}
								>
									Straight
								</button>
								<button
									className={`btn ${slip.parlay ? "btn-primary" : "btn-outline-primary"}`}
									onClick={() => slip.setParlay(true)}
								>
									Parlay
								</button>
							</div>
						) : null}

						{slip.picks.map((p, i) => {
							const st = statuses[i]!;
							const stake = Number.parseFloat(slip.stakes[p.key] ?? "") || 0;
							return (
								<div key={p.key} className="border-bottom pb-2 mb-2">
									<div className="d-flex justify-content-between gap-2">
										<div
											className={`fw-medium ${st.kind === "closed" ? "text-decoration-line-through text-body-secondary" : ""}`}
										>
											{p.title}
										</div>
										<span className="text-nowrap">
											<span className="fw-bold me-2">
												{formatAmerican(p.odds)}
											</span>
											<button
												className="btn-close btn-close-sm"
												style={{ fontSize: "0.6rem" }}
												onClick={() => slip.removePick(p.key)}
												title="Remove"
											/>
										</span>
									</div>
									<div className="text-body-secondary small mb-1">{p.sub}</div>
									{st.kind === "closed" ? (
										<div className="small text-danger">No longer available</div>
									) : st.kind === "changed" ? (
										<div className="small text-warning-emphasis mb-1">
											Now {st.now.title} {formatAmerican(st.now.odds)}
										</div>
									) : null}
									{slip.parlayActive || st.kind === "closed"
										? null
										: stakeInput(
												slip.stakes[p.key] ?? "",
												(value) =>
													slip.setStakes((s) => ({ ...s, [p.key]: value })),
												stake * (americanToDecimal(p.odds) - 1),
											)}
								</div>
							);
						})}

						{slip.parlayActive ? (
							<div className="mb-2">
								{slip.conflict ? (
									<div className="text-danger small mb-2">{slip.conflict}</div>
								) : (
									<div className="d-flex justify-content-between small mb-1">
										<span className="text-body-secondary">
											{slip.picks.length}-leg parlay
										</span>
										<span className="fw-bold">
											{formatAmerican(slip.parlayAmerican)}
										</span>
									</div>
								)}
								{stakeInput(
									slip.parlayStake,
									slip.setParlayStake,
									slip.parlayPayout -
										(Number.parseFloat(slip.parlayStake) || 0),
								)}
							</div>
						) : null}

						<div className="d-flex justify-content-between small mb-1">
							<span className="text-body-secondary">Total risk</span>
							<span>{formatSportsbookMoney(risk)}</span>
						</div>
						<div className="d-flex justify-content-between fw-bold mb-2">
							<span>Total payout</span>
							<span>{formatSportsbookMoney(risk + Math.max(0, toWin))}</span>
						</div>
						{action}
					</>
				)}
			</div>
		</div>
	);
};
