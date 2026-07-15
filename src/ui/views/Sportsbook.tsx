import { Fragment, useState, type ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { showNotification } from "../util/showNotification.ts";
import type { View } from "../../common/types.ts";
import type { SportsbookMarket } from "../../common/types.ts";
import { useLocal } from "../util/local.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import {
	americanToDecimal,
	formatAmerican,
	formatSportsbookMoney,
	formatSportsbookMoneyFull,
} from "../../common/sportsbook.ts";

type Pick = {
	key: string;
	market: SportsbookMarket;
	odds: number;
	title: string;
	sub: string;
};

// An odds button: line above the price, fills in when it's in the slip. Fixed
// size so every column of them lines up.
const OddsCell = ({
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

// The standard table look used across the app.
const TABLE_CLASS =
	"table table-striped table-borderless table-sm align-middle mb-3";

const Sportsbook = ({ board, wallet, balances, season }: View<"sportsbook">) => {
	useTitleBar({ title: "Sportsbook" });

	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	const [tab, setTab] = useState<"games" | "futures" | "mybets">("games");
	const [picks, setPicks] = useState<Pick[]>([]);
	const [stakes, setStakes] = useState<Record<string, string>>({});
	const [slipOpenMobile, setSlipOpenMobile] = useState(false);
	const [placing, setPlacing] = useState(false);

	const teamName = (tid: number) => {
		const t = teamInfoCache[tid];
		return t ? `${t.region} ${t.name}` : "?";
	};
	const teamAbbrev = (tid: number) => teamInfoCache[tid]?.abbrev ?? "???";
	const Logo = ({ tid, size = 20 }: { tid: number; size?: number }) => {
		const t = teamInfoCache[tid];
		return (
			<TeamLogoInline
				imgURL={t?.imgURL}
				imgURLSmall={t?.imgURLSmall}
				size={size}
				includePlaceholderIfNoLogo
			/>
		);
	};
	const teamLink = (tid: number, text: ReactNode) => (
		<a
			href={helpers.leagueUrl(["roster", `${teamAbbrev(tid)}_${tid}`])}
			className="text-truncate"
		>
			{text}
		</a>
	);

	const selectedKeys = new Set(picks.map((p) => p.key));
	const togglePick = (pick: Pick) => {
		setPicks((prev) =>
			prev.some((p) => p.key === pick.key)
				? prev.filter((p) => p.key !== pick.key)
				: [...prev, pick],
		);
		setSlipOpenMobile(true);
	};
	const removePick = (key: string) =>
		setPicks((prev) => prev.filter((p) => p.key !== key));
	const clearSlip = () => {
		setPicks([]);
		setStakes({});
	};

	const signed = (n: number) => (n > 0 ? `+${n}` : `${n}`);

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
			for (const p of toPlace) {
				await toWorker("main", "sportsbookPlaceBet", {
					tid: wallet.tid,
					market: p.market,
					stake: Number.parseFloat(stakes[p.key]!),
					americanOdds: p.odds,
					label: `${p.title} — ${p.sub}`,
				});
			}
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

	const cancelBet = async (betID: number) => {
		try {
			await toWorker("main", "sportsbookCancelBet", { tid: wallet.tid, betID });
			await realtimeUpdate(["watchList"]);
		} catch (error) {
			showNotification({
				type: "error",
				text: error instanceof Error ? error.message : "Could not cancel.",
			});
		}
	};

	// ---- Bet slip --------------------------------------------------------
	const betslip = (
		<div className="card">
			<div className="card-header d-flex justify-content-between align-items-center py-2">
				<span className="fw-bold">
					Bet Slip{" "}
					{picks.length > 0 ? (
						<span className="badge text-bg-primary ms-1">{picks.length}</span>
					) : null}
				</span>
				{picks.length > 0 ? (
					<button
						className="btn btn-sm btn-link text-decoration-none p-0"
						onClick={clearSlip}
					>
						Clear
					</button>
				) : null}
			</div>
			<div className="card-body py-2">
				{picks.length === 0 ? (
					<div className="text-body-secondary text-center py-4">
						Tap any odds to start a bet slip.
					</div>
				) : (
					<>
						{picks.map((p) => {
							const stake = Number.parseFloat(stakes[p.key] ?? "") || 0;
							const toWin = stake * (americanToDecimal(p.odds) - 1);
							return (
								<div key={p.key} className="border-bottom pb-2 mb-2">
									<div className="d-flex justify-content-between">
										<div className="fw-medium">{p.title}</div>
										<button
											className="btn-close btn-close-sm"
											style={{ fontSize: "0.6rem" }}
											onClick={() => removePick(p.key)}
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
											value={stakes[p.key] ?? ""}
											onChange={(e) =>
												setStakes((s) => ({ ...s, [p.key]: e.target.value }))
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
							<span>{formatSportsbookMoney(totalStake)}</span>
						</div>
						<div className="d-flex justify-content-between fw-bold mb-2">
							<span>Potential payout</span>
							<span>{formatSportsbookMoney(totalPayout)}</span>
						</div>
						<button
							className="btn btn-primary w-100"
							disabled={
								placing || totalStake <= 0 || totalStake > wallet.balance
							}
							onClick={placeBets}
						>
							{totalStake > wallet.balance
								? "Not enough $"
								: placing
									? "Placing…"
									: `Place ${picks.length} bet${picks.length === 1 ? "" : "s"}`}
						</button>
					</>
				)}
			</div>
		</div>
	);

	// ---- Games -----------------------------------------------------------
	const gamesTab =
		board.games.length === 0 ? (
			<p className="text-body-secondary">
				No upcoming games to bet on right now.
			</p>
		) : (
			<div className="table-responsive">
				<table className={TABLE_CLASS} style={{ maxWidth: 640 }}>
					<thead>
						<tr>
							<th>Game</th>
							<th className="text-center" style={{ width: 84 }}>
								Spread
							</th>
							<th className="text-center" style={{ width: 84 }}>
								Money
							</th>
							<th className="text-center" style={{ width: 84 }}>
								Total
							</th>
						</tr>
					</thead>
					<tbody>
						{board.games.map((game) => {
							const sub = `${teamAbbrev(game.away.tid)} @ ${teamAbbrev(game.home.tid)}`;
							const rows = [
								{
									t: game.away,
									spreadLine: -game.spread.line,
									spreadOdds: game.spread.away,
									ml: game.moneyline.away,
									totKind: "over" as const,
									totOdds: game.total.over,
								},
								{
									t: game.home,
									spreadLine: game.spread.line,
									spreadOdds: game.spread.home,
									ml: game.moneyline.home,
									totKind: "under" as const,
									totOdds: game.total.under,
								},
							];
							return (
								<tr key={game.gid}>
									<td style={{ minWidth: 150 }}>
										{rows.map((r) => (
											<div
												key={r.t.tid}
												className="d-flex align-items-center gap-2 py-1"
											>
												<Logo tid={r.t.tid} />
												{teamLink(
													r.t.tid,
													`${r.t.region} ${r.t.name}`,
												)}
											</div>
										))}
									</td>
									{(["spread", "money", "total"] as const).map((col) => (
										<td key={col}>
											<div className="d-flex flex-column gap-1">
												{rows.map((r) => {
													if (col === "spread") {
														const key = `sp-${game.gid}-${r.t.tid}`;
														return (
															<OddsCell
																key={key}
																line={signed(r.spreadLine)}
																odds={r.spreadOdds}
																selected={selectedKeys.has(key)}
																onClick={() =>
																	togglePick({
																		key,
																		market: {
																			type: "gameSpread",
																			gid: game.gid,
																			pickTid: r.t.tid,
																			line: r.spreadLine,
																		},
																		odds: r.spreadOdds,
																		title: `${teamAbbrev(r.t.tid)} ${signed(r.spreadLine)}`,
																		sub,
																	})
																}
															/>
														);
													}
													if (col === "money") {
														const key = `ml-${game.gid}-${r.t.tid}`;
														return (
															<OddsCell
																key={key}
																odds={r.ml}
																selected={selectedKeys.has(key)}
																onClick={() =>
																	togglePick({
																		key,
																		market: {
																			type: "gameMoneyline",
																			gid: game.gid,
																			pickTid: r.t.tid,
																		},
																		odds: r.ml,
																		title: `${teamAbbrev(r.t.tid)} ML`,
																		sub,
																	})
																}
															/>
														);
													}
													const key = `tot-${game.gid}-${r.totKind}`;
													return (
														<OddsCell
															key={key}
															line={`${r.totKind === "over" ? "O" : "U"} ${game.total.line}`}
															odds={r.totOdds}
															selected={selectedKeys.has(key)}
															onClick={() =>
																togglePick({
																	key,
																	market: {
																		type: "gameTotal",
																		gid: game.gid,
																		side: r.totKind,
																		line: game.total.line,
																	},
																	odds: r.totOdds,
																	title: `${r.totKind === "over" ? "Over" : "Under"} ${game.total.line}`,
																	sub,
																})
															}
														/>
													);
												})}
											</div>
										</td>
									))}
								</tr>
							);
						})}
					</tbody>
				</table>
			</div>
		);

	// ---- Futures ---------------------------------------------------------
	const teamFuturesTable = (
		heading: string,
		subLabel: string,
		rows: {
			tid: number;
			abbrev: string;
			region: string;
			name: string;
			americanOdds: number;
		}[],
		mk: (tid: number) => SportsbookMarket,
		keyPrefix: string,
	) => (
		<Fragment key={keyPrefix}>
			<h3 className="h5">{heading}</h3>
			<div className="table-responsive">
				<table className={TABLE_CLASS} style={{ maxWidth: 380 }}>
					<tbody>
						{rows.map((r) => {
							const key = `${keyPrefix}-${r.tid}`;
							return (
								<tr key={key}>
									<td>
										<div className="d-flex align-items-center gap-2">
											<Logo tid={r.tid} />
											{teamLink(r.tid, `${r.region} ${r.name}`)}
										</div>
									</td>
									<td className="text-end" style={{ width: 90 }}>
										<OddsCell
											odds={r.americanOdds}
											selected={selectedKeys.has(key)}
											onClick={() =>
												togglePick({
													key,
													market: mk(r.tid),
													odds: r.americanOdds,
													title: `${teamAbbrev(r.tid)} — ${subLabel}`,
													sub: heading,
												})
											}
										/>
									</td>
								</tr>
							);
						})}
					</tbody>
				</table>
			</div>
		</Fragment>
	);

	const futuresTab = (
		<>
			<div className="row">
				<div className="col-md-6">
					{teamFuturesTable(
						"Championship",
						"Champion",
						board.championship,
						(tid) => ({ type: "champion", pickTid: tid, season }),
						"champ",
					)}
					{board.conferences.map((conf) =>
						teamFuturesTable(
							`${conf.name} Winner`,
							conf.name,
							conf.teams,
							(tid) => ({ type: "conf", pickTid: tid, cid: conf.cid, season }),
							`conf${conf.cid}`,
						),
					)}
				</div>
				<div className="col-md-6">
					{board.winTotals.length === 0 ? null : (
						<>
					<h3 className="h5">Season Win Totals</h3>
					<div className="table-responsive">
						<table className={TABLE_CLASS} style={{ maxWidth: 440 }}>
							<thead>
								<tr>
									<th>Team</th>
									<th className="text-end">Line</th>
									<th className="text-center" style={{ width: 90 }}>
										Over
									</th>
									<th className="text-center" style={{ width: 90 }}>
										Under
									</th>
								</tr>
							</thead>
							<tbody>
								{board.winTotals.map((t) => (
									<tr key={t.tid}>
										<td>
											<div className="d-flex align-items-center gap-2">
												<Logo tid={t.tid} />
												{teamLink(t.tid, t.abbrev)}
											</div>
										</td>
										<td className="text-end">{t.line}</td>
										{(["over", "under"] as const).map((side) => {
											const key = side === "over" ? `wto-${t.tid}` : `wtu-${t.tid}`;
											const odds = side === "over" ? t.over : t.under;
											return (
												<td key={side}>
													<OddsCell
														odds={odds}
														selected={selectedKeys.has(key)}
														onClick={() =>
															togglePick({
																key,
																market: {
																	type: "winTotal",
																	pickTid: t.tid,
																	side,
																	line: t.line,
																	season,
																},
																odds,
																title: `${teamAbbrev(t.tid)} ${side === "over" ? "Over" : "Under"} ${t.line}`,
																sub: "Season wins",
															})
														}
													/>
												</td>
											);
										})}
									</tr>
								))}
							</tbody>
						</table>
					</div>
						</>
					)}
					{board.divisions.map((div) =>
						teamFuturesTable(
							`${div.name} Winner`,
							div.name,
							div.teams,
							(tid) => ({ type: "div", pickTid: tid, did: div.did, season }),
							`div${div.did}`,
						),
					)}
				</div>
			</div>

			{board.awards.length === 0 ? null : <h2>Award Futures</h2>}
			<div className="row">
				{board.awards.map((race) => (
					<div key={race.award} className="col-lg-4 col-md-6">
						<h3 className="h5">{race.name}</h3>
						<table className={TABLE_CLASS} style={{ maxWidth: 380 }}>
							<tbody>
								{race.candidates.map((c) => {
									const key = `aw-${race.award}-${c.pid}`;
									return (
										<tr key={key}>
											<td>
												<a href={helpers.leagueUrl(["player", c.pid])}>
													{c.name}
												</a>{" "}
												<span className="text-body-secondary small">
													{teamAbbrev(c.tid)}
												</span>
											</td>
											<td className="text-end" style={{ width: 90 }}>
												<OddsCell
													odds={c.americanOdds}
													selected={selectedKeys.has(key)}
													onClick={() =>
														togglePick({
															key,
															market: {
																type: "award",
																award: race.award,
																pid: c.pid,
																season,
															},
															odds: c.americanOdds,
															title: `${c.name} — ${race.name}`,
															sub: race.name,
														})
													}
												/>
											</td>
										</tr>
									);
								})}
							</tbody>
						</table>
					</div>
				))}
			</div>
		</>
	);

	// ---- My Bets ---------------------------------------------------------
	const resultBadge = (r?: string) =>
		r === "won" ? (
			<span className="badge text-bg-success">Won</span>
		) : r === "lost" ? (
			<span className="badge text-bg-danger">Lost</span>
		) : r === "push" ? (
			<span className="badge text-bg-secondary">Push</span>
		) : (
			<span className="badge text-bg-warning">Open</span>
		);

	const betsTable = (bets: typeof wallet.bets, open: boolean) => (
		<div className="table-responsive">
			<table className={TABLE_CLASS} style={{ maxWidth: 680 }}>
				<thead>
					<tr>
						<th>Bet</th>
						<th className="text-end">Odds</th>
						<th className="text-end">Stake</th>
						<th className="text-end">{open ? "To win" : "Paid"}</th>
						<th className="text-center">Result</th>
						{open ? <th /> : null}
					</tr>
				</thead>
				<tbody>
					{bets.map((bet) => (
						<tr key={bet.betID}>
							<td>{bet.label}</td>
							<td className="text-end">{formatAmerican(bet.americanOdds)}</td>
							<td className="text-end">{formatSportsbookMoney(bet.stake)}</td>
							<td className="text-end">
								{open
									? formatSportsbookMoney(bet.stake * (bet.decimalOdds - 1))
									: bet.result === "won"
										? formatSportsbookMoney(bet.stake * bet.decimalOdds)
										: bet.result === "push"
											? formatSportsbookMoney(bet.stake)
											: "—"}
							</td>
							<td className="text-center">{resultBadge(bet.result)}</td>
							{open ? (
								<td className="text-end">
									<button
										className="btn btn-sm btn-link text-danger text-decoration-none p-0"
										onClick={() => cancelBet(bet.betID)}
									>
										Cancel
									</button>
								</td>
							) : null}
						</tr>
					))}
				</tbody>
			</table>
		</div>
	);

	const myBetsTab = (
		<>
			<h2>Open Bets</h2>
			{wallet.bets.length > 0 ? (
				betsTable(wallet.bets, true)
			) : (
				<p className="text-body-secondary">No open bets.</p>
			)}
			<h2>Settled</h2>
			{wallet.history.length > 0 ? (
				betsTable(wallet.history, false)
			) : (
				<p className="text-body-secondary">Nothing settled yet.</p>
			)}
		</>
	);

	return (
		<>
			<div className="mb-3">
				<div className="text-body-secondary small">
					{teamName(wallet.tid)} balance
				</div>
				<div className="h3 mb-2">{formatSportsbookMoneyFull(wallet.balance)}</div>
				{balances.length > 1 ? (
					<div className="d-flex flex-wrap gap-2 pb-1">
						{balances.map((b) => (
							<span
								key={b.tid}
								className={`badge d-inline-flex align-items-center gap-1 ${b.tid === wallet.tid ? "text-bg-primary" : "text-bg-secondary"}`}
								style={{ fontWeight: 400 }}
								title={teamName(b.tid)}
							>
								<Logo tid={b.tid} size={14} />
								{teamAbbrev(b.tid)} {formatSportsbookMoneyFull(b.balance)}
							</span>
						))}
					</div>
				) : null}
			</div>

			<ul className="nav nav-tabs mb-3">
				{(
					[
						["games", "Games"],
						["futures", "Futures"],
						[
							"mybets",
							`My Bets${wallet.bets.length > 0 ? ` (${wallet.bets.length})` : ""}`,
						],
					] as const
				).map(([key, label]) => (
					<li className="nav-item" key={key}>
						<button
							className={`nav-link ${tab === key ? "active" : ""}`}
							onClick={() => setTab(key as typeof tab)}
						>
							{label}
						</button>
					</li>
				))}
			</ul>

			<div className="row">
				<div className="col-lg-8 col-xl-9">
					{tab === "games"
						? gamesTab
						: tab === "futures"
							? futuresTab
							: myBetsTab}
				</div>
				<div className="col-lg-4 col-xl-3 d-none d-lg-block">
					<div className="position-sticky" style={{ top: "1rem" }}>
						{betslip}
					</div>
				</div>
			</div>

			{picks.length > 0 ? (
				<div
					className="d-lg-none position-fixed start-0 end-0 bottom-0 p-2"
					style={{ zIndex: 1030 }}
				>
					{slipOpenMobile ? (
						<div className="mx-auto" style={{ maxWidth: 520 }}>
							{betslip}
							<button
								className="btn btn-sm btn-secondary w-100 mt-1"
								onClick={() => setSlipOpenMobile(false)}
							>
								Hide slip
							</button>
						</div>
					) : (
						<button
							className="btn btn-primary w-100 shadow"
							onClick={() => setSlipOpenMobile(true)}
						>
							Bet Slip ({picks.length}) ·{" "}
							{totalStake > 0
								? `${formatSportsbookMoney(totalStake)} to win ${formatSportsbookMoney(totalPayout - totalStake)}`
								: "add stake"}
						</button>
					)}
				</div>
			) : null}

			<p className="text-body-secondary small mt-2">
				Play money — completely separate from the real game. Every preseason each
				team gets {formatSportsbookMoney(1_000_000)} more.
			</p>
		</>
	);
};

export default Sportsbook;
