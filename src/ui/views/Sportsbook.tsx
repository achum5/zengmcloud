import { Fragment, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { logEvent } from "../util/logEvent.ts";
import type { View } from "../../common/types.ts";
import type { SportsbookMarket } from "../../common/types.ts";
import { useLocal } from "../util/local.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import {
	americanToDecimal,
	formatAmerican,
	formatSportsbookMoney,
} from "../../common/sportsbook.ts";

type Pick = {
	key: string;
	market: SportsbookMarket;
	odds: number;
	title: string;
	sub: string;
};

// FanDuel-style odds cell: bordered box, line above the price, fills in when in
// the slip. Fixed size so the SPREAD/MONEY/TOTAL grid lines up perfectly.
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
		className={`btn btn-sm w-100 d-flex flex-column justify-content-center lh-1 ${selected ? "btn-primary" : "btn-outline-primary"}`}
		style={{ height: 44, padding: "2px 4px" }}
	>
		{line !== undefined ? (
			<span style={{ fontSize: "0.72rem" }}>{line}</span>
		) : null}
		<span className="fw-bold">{formatAmerican(odds)}</span>
	</button>
);

const Sportsbook = ({
	board,
	wallet,
	balances,
	season,
}: View<"sportsbook">) => {
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
	const Logo = ({ tid, size = 22 }: { tid: number; size?: number }) => {
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
			logEvent({ type: "error", text: "Enter a stake first.", saveToDb: false });
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
			logEvent({
				type: "success",
				text: `Placed ${toPlace.length} bet${toPlace.length === 1 ? "" : "s"}.`,
				saveToDb: false,
			});
			clearSlip();
			await realtimeUpdate(["watchList"]);
		} catch (error) {
			logEvent({
				type: "error",
				text: error instanceof Error ? error.message : "Could not place bet.",
				saveToDb: false,
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
			logEvent({
				type: "error",
				text: error instanceof Error ? error.message : "Could not cancel.",
				saveToDb: false,
			});
		}
	};

	// ---- Bet slip --------------------------------------------------------
	const betslip = (
		<div className="card shadow-sm">
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
						<div style={{ fontSize: "2rem" }}>🧾</div>
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
										<span className="input-group-text text-success">
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
							<span className="text-success">
								{formatSportsbookMoney(totalPayout)}
							</span>
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

	// ---- Games (aligned grid) --------------------------------------------
	const gridCols = "minmax(0, 1fr) 76px 76px 76px";
	const gamesTab =
		board.games.length === 0 ? (
			<p className="text-body-secondary">No upcoming games to bet on right now.</p>
		) : (
			<div className="card">
				<div className="card-body p-2">
					<div
						className="d-grid text-body-secondary text-uppercase fw-bold px-1 mb-1"
						style={{
							gridTemplateColumns: gridCols,
							gap: 6,
							fontSize: "0.68rem",
							letterSpacing: "0.03em",
						}}
					>
						<div />
						<div className="text-center">Spread</div>
						<div className="text-center">Money</div>
						<div className="text-center">Total</div>
					</div>

					{board.games.map((game, i) => {
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
							<div
								key={game.gid}
								className={`d-grid align-items-center py-2 ${i > 0 ? "border-top" : ""}`}
								style={{ gridTemplateColumns: gridCols, gap: 6, rowGap: 6 }}
							>
								{rows.map((r) => (
									<Fragment key={r.t.tid}>
										<div
											className="d-flex align-items-center gap-2 text-truncate"
										>
											<Logo tid={r.t.tid} />
											<a
												href={helpers.leagueUrl([
													"roster",
													`${r.t.abbrev}_${r.t.tid}`,
												])}
												className="text-truncate"
											>
												{r.t.region} {r.t.name}
											</a>
										</div>
										<OddsCell
											line={signed(r.spreadLine)}
											odds={r.spreadOdds}
											selected={selectedKeys.has(`sp-${game.gid}-${r.t.tid}`)}
											onClick={() =>
												togglePick({
													key: `sp-${game.gid}-${r.t.tid}`,
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
										<OddsCell
											odds={r.ml}
											selected={selectedKeys.has(`ml-${game.gid}-${r.t.tid}`)}
											onClick={() =>
												togglePick({
													key: `ml-${game.gid}-${r.t.tid}`,
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
										<OddsCell
											line={`${r.totKind === "over" ? "O" : "U"} ${game.total.line}`}
											odds={r.totOdds}
											selected={selectedKeys.has(`tot-${game.gid}-${r.totKind}`)}
											onClick={() =>
												togglePick({
													key: `tot-${game.gid}-${r.totKind}`,
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
									</Fragment>
								))}
							</div>
						);
					})}
				</div>
			</div>
		);

	// ---- Futures ---------------------------------------------------------
	const teamFuturesCard = (
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
		<div className="card mb-3">
			<div className="card-header py-2 fw-bold">{heading}</div>
			<div className="card-body p-0">
				{rows.map((r, i) => {
					const key = `${keyPrefix}-${r.tid}`;
					return (
						<div
							key={key}
							className={`d-flex justify-content-between align-items-center py-1 px-2 ${i > 0 ? "border-top" : ""}`}
						>
							<span className="d-flex align-items-center gap-2 text-truncate pe-2">
								<Logo tid={r.tid} size={20} />
								{r.region} {r.name}
							</span>
							<div style={{ width: 84, flexShrink: 0 }}>
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
							</div>
						</div>
					);
				})}
			</div>
		</div>
	);

	const futuresTab = (
		<div className="row">
			<div className="col-lg-6">
				{teamFuturesCard(
					"Championship",
					"Champion",
					board.championship,
					(tid) => ({ type: "champion", pickTid: tid, season }),
					"champ",
				)}
				{board.conferences.map((conf) =>
					teamFuturesCard(
						`${conf.name} Winner`,
						conf.name,
						conf.teams,
						(tid) => ({ type: "conf", pickTid: tid, cid: conf.cid, season }),
						`conf${conf.cid}`,
					),
				)}
			</div>
			<div className="col-lg-6">
				<div className="card mb-3">
					<div className="card-header py-2 fw-bold">Season Win Totals</div>
					<div className="card-body p-0">
						{board.winTotals.map((t, i) => (
							<div
								key={t.tid}
								className={`d-flex align-items-center py-1 px-2 ${i > 0 ? "border-top" : ""}`}
							>
								<span className="flex-grow-1 d-flex align-items-center gap-2 text-truncate">
									<Logo tid={t.tid} size={20} />
									{teamAbbrev(t.tid)}
									<span className="text-body-secondary small">{t.line}</span>
								</span>
								<div style={{ width: 76 }}>
									<OddsCell
										line="Over"
										odds={t.over}
										selected={selectedKeys.has(`wto-${t.tid}`)}
										onClick={() =>
											togglePick({
												key: `wto-${t.tid}`,
												market: {
													type: "winTotal",
													pickTid: t.tid,
													side: "over",
													line: t.line,
													season,
												},
												odds: t.over,
												title: `${teamAbbrev(t.tid)} Over ${t.line}`,
												sub: "Season wins",
											})
										}
									/>
								</div>
								<div style={{ width: 76 }} className="ms-2">
									<OddsCell
										line="Under"
										odds={t.under}
										selected={selectedKeys.has(`wtu-${t.tid}`)}
										onClick={() =>
											togglePick({
												key: `wtu-${t.tid}`,
												market: {
													type: "winTotal",
													pickTid: t.tid,
													side: "under",
													line: t.line,
													season,
												},
												odds: t.under,
												title: `${teamAbbrev(t.tid)} Under ${t.line}`,
												sub: "Season wins",
											})
										}
									/>
								</div>
							</div>
						))}
					</div>
				</div>

				{board.divisions.map((div) =>
					teamFuturesCard(
						`${div.name} Winner`,
						div.name,
						div.teams,
						(tid) => ({ type: "div", pickTid: tid, did: div.did, season }),
						`div${div.did}`,
					),
				)}
			</div>

			<div className="col-12">
				<h2 className="h5 mt-2">Award Futures</h2>
				<div className="row">
					{board.awards.map((race) => (
						<div key={race.award} className="col-lg-4 col-md-6 mb-3">
							<div className="card h-100">
								<div className="card-header py-2 fw-bold">{race.name}</div>
								<div className="card-body p-0">
									{race.candidates.map((c, i) => {
										const key = `aw-${race.award}-${c.pid}`;
										return (
											<div
												key={key}
												className={`d-flex justify-content-between align-items-center py-1 px-2 ${i > 0 ? "border-top" : ""}`}
											>
												<a
													href={helpers.leagueUrl(["player", c.pid])}
													className="text-truncate pe-2"
												>
													{c.name}{" "}
													<span className="text-body-secondary small">
														{teamAbbrev(c.tid)}
													</span>
												</a>
												<div style={{ width: 84, flexShrink: 0 }}>
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
												</div>
											</div>
										);
									})}
								</div>
							</div>
						</div>
					))}
				</div>
			</div>
		</div>
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

	const myBetsTab = (
		<div style={{ maxWidth: 640 }}>
			<h2 className="h6">Open Bets</h2>
			{wallet.bets.length > 0 ? (
				<ul className="list-group mb-4">
					{wallet.bets.map((bet) => (
						<li key={bet.betID} className="list-group-item">
							<div className="d-flex justify-content-between">
								<span className="fw-medium">{bet.label}</span>
								{resultBadge()}
							</div>
							<div className="small text-body-secondary d-flex justify-content-between align-items-center">
								<span>
									{formatSportsbookMoney(bet.stake)} @{" "}
									{formatAmerican(bet.americanOdds)} · to win{" "}
									{formatSportsbookMoney(bet.stake * (bet.decimalOdds - 1))}
								</span>
								<button
									className="btn btn-sm btn-link text-danger text-decoration-none p-0"
									onClick={() => cancelBet(bet.betID)}
								>
									Cancel
								</button>
							</div>
						</li>
					))}
				</ul>
			) : (
				<p className="text-body-secondary">No open bets.</p>
			)}

			<h2 className="h6">Settled</h2>
			{wallet.history.length > 0 ? (
				<ul className="list-group">
					{wallet.history.map((bet) => (
						<li key={bet.betID} className="list-group-item">
							<div className="d-flex justify-content-between">
								<span className="fw-medium">{bet.label}</span>
								{resultBadge(bet.result)}
							</div>
							<div className="small text-body-secondary">
								{formatSportsbookMoney(bet.stake)} @{" "}
								{formatAmerican(bet.americanOdds)}
								{bet.result === "won"
									? ` · won ${formatSportsbookMoney(bet.stake * bet.decimalOdds)}`
									: ""}
							</div>
						</li>
					))}
				</ul>
			) : (
				<p className="text-body-secondary">Nothing settled yet.</p>
			)}
		</div>
	);

	return (
		<>
			{/* Wallet header + fun balances leaderboard */}
			<div className="mb-3">
				<div className="text-body-secondary small">
					{teamName(wallet.tid)} balance
				</div>
				<div className="h3 mb-2 text-success">
					{formatSportsbookMoney(wallet.balance)}
				</div>
				<div
					className="d-flex gap-2 overflow-auto pb-1"
					style={{ scrollbarWidth: "thin" }}
				>
					{balances.map((b) => (
						<span
							key={b.tid}
							className={`badge d-inline-flex align-items-center gap-1 ${b.tid === wallet.tid ? "text-bg-success" : "text-bg-secondary"}`}
							style={{ fontWeight: 400 }}
							title={teamName(b.tid)}
						>
							<Logo tid={b.tid} size={14} />
							{teamAbbrev(b.tid)} {formatSportsbookMoney(b.balance)}
						</span>
					))}
				</div>
			</div>

			{/* Tabs */}
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

			{/* Mobile betslip: fixed bottom sheet */}
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

			<p className="text-body-secondary small mt-3">
				Play money — completely separate from the real game. Every preseason each
				team gets {formatSportsbookMoney(1_000_000)} more.
			</p>
		</>
	);
};

export default Sportsbook;
