import { useState } from "react";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { showNotification } from "../util/showNotification.ts";
import type { SportsbookMarket } from "../../common/types.ts";
import {
	americanToDecimal,
	formatAmerican,
	formatSportsbookMoney,
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
		className={`btn btn-sm w-100 d-flex flex-column align-items-center justify-content-center lh-1 ${selected ? "btn-primary" : "btn-light-bordered"}`}
		style={{ height: 42, padding: "2px 4px", minWidth: 72 }}
	>
		{line !== undefined ? (
			<span className="text-body-secondary" style={{ fontSize: "0.72rem" }}>
				{line}
			</span>
		) : null}
		<span className="fw-bold">{formatAmerican(odds)}</span>
	</button>
);

// Local slip state (picks + stakes) plus the atomic place-the-whole-slip
// action. Each page that offers odds buttons gets its OWN slip instance -
// there's no cross-page shared slip, so navigating away clears it.
export const useBetSlip = (tid: number) => {
	const [picks, setPicks] = useState<Pick[]>([]);
	const [stakes, setStakes] = useState<Record<string, string>>({});
	const [placing, setPlacing] = useState(false);

	const selectedKeys = new Set(picks.map((p) => p.key));

	const togglePick = (pick: Pick) => {
		setPicks((prev) =>
			prev.some((p) => p.key === pick.key)
				? prev.filter((p) => p.key !== pick.key)
				: [...prev, pick],
		);
	};
	const removePick = (key: string) =>
		setPicks((prev) => prev.filter((p) => p.key !== key));
	const clearSlip = () => {
		setPicks([]);
		setStakes({});
	};

	const totalStake = picks.reduce(
		(sum, p) => sum + (Number.parseFloat(stakes[p.key] ?? "") || 0),
		0,
	);
	const totalPayout = picks.reduce((sum, p) => {
		const stake = Number.parseFloat(stakes[p.key] ?? "") || 0;
		return sum + stake * americanToDecimal(p.odds);
	}, 0);

	const placeBets = async () => {
		if (placing) {
			return;
		}
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
			showNotification({
				type: "success",
				text: `Placed ${toPlace.length} bet${toPlace.length === 1 ? "" : "s"}.`,
			});
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

	return {
		picks,
		stakes,
		setStakes,
		placing,
		selectedKeys,
		togglePick,
		removePick,
		clearSlip,
		totalStake,
		totalPayout,
		placeBets,
	};
};

export const BetSlipCard = ({
	slip,
	balance,
}: {
	slip: ReturnType<typeof useBetSlip>;
	balance: number;
}) => (
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
			{slip.picks.length === 0 ? (
				<div className="text-body-secondary text-center py-4">
					Tap any odds to start a bet slip.
				</div>
			) : (
				<>
					{slip.picks.map((p) => {
						const stake = Number.parseFloat(slip.stakes[p.key] ?? "") || 0;
						const toWin = stake * (americanToDecimal(p.odds) - 1);
						return (
							<div key={p.key} className="border-bottom pb-2 mb-2">
								<div className="d-flex justify-content-between">
									<div className="fw-medium">{p.title}</div>
									<button
										className="btn-close btn-close-sm"
										style={{ fontSize: "0.6rem" }}
										onClick={() => slip.removePick(p.key)}
										title="Remove"
									/>
								</div>
								<div className="text-body-secondary small mb-1">
									{p.sub} · {formatAmerican(p.odds)}
								</div>
								<div className="input-group input-group-sm">
									<span className="input-group-text">$</span>
									<input
										type="number"
										min={0}
										className="form-control"
										placeholder="Stake"
										value={slip.stakes[p.key] ?? ""}
										onChange={(e) =>
											slip.setStakes((s) => ({
												...s,
												[p.key]: e.target.value,
											}))
										}
									/>
									<span className="input-group-text">
										{toWin > 0 ? `+${formatSportsbookMoney(toWin)}` : "—"}
									</span>
								</div>
							</div>
						);
					})}
					<div className="d-flex justify-content-between small mb-1">
						<span className="text-body-secondary">Total stake</span>
						<span>{formatSportsbookMoney(slip.totalStake)}</span>
					</div>
					<div className="d-flex justify-content-between fw-bold mb-2">
						<span>Potential payout</span>
						<span>{formatSportsbookMoney(slip.totalPayout)}</span>
					</div>
					<button
						className="btn btn-primary w-100"
						disabled={
							slip.placing ||
							slip.totalStake <= 0 ||
							slip.totalStake > balance
						}
						onClick={slip.placeBets}
					>
						{slip.totalStake > balance
							? "Not enough $"
							: slip.placing
								? "Placing…"
								: `Place ${slip.picks.length} bet${slip.picks.length === 1 ? "" : "s"}`}
					</button>
				</>
			)}
		</div>
	</div>
);
