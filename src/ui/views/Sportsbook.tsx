import { useEffect, useState, type ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { showNotification } from "../util/showNotification.ts";
import type { View } from "../../common/types.ts";
import type { SportsbookMarket } from "../../common/types.ts";
import { PHASE } from "../../common/constants.ts";
import { useLocal } from "../util/local.ts";
import { TeamLogoInline } from "../components/TeamLogoInline.tsx";
import {
	BetSlipCard,
	OddsCell,
	useBetSlip,
	type Pick,
} from "../components/SportsbookBetSlip.tsx";
import {
	formatAmerican,
	formatSportsbookMoney,
	formatSportsbookMoneyFull,
	marketGid,
} from "../../common/sportsbook.ts";

// The standard table look used across the app.
const TABLE_CLASS =
	"table table-striped table-borderless table-sm align-middle mb-3";

const Sportsbook = ({
	board,
	wallet,
	balances,
	leagueBets,
	gameLinks,
	season,
	phase,
}: View<"sportsbook">) => {
	useTitleBar({ title: "Sportsbook" });

	const { teamInfoCache } = useLocal(["teamInfoCache"]);

	const [tab, setTab] = useState<
		"games" | "futures" | "awards" | "mybets" | "leaguebets"
	>("games");
	const [slipOpenMobile, setSlipOpenMobile] = useState(false);
	const slip = useBetSlip(wallet.tid, {
		allStarRosterSize: board.allStarRosterSize,
	});

	// Catch up any bet whose outcome is already known but that a missed hook
	// didn't settle (e.g. this device was offline when a phase change decided
	// an award). A real captured worker call - see worker/api/index.ts
	// sportsbookSettle - so a payout it produces actually publishes to a
	// synced room instead of only applying to this device's local cache.
	useEffect(() => {
		void toWorker("main", "sportsbookSettle", undefined);
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, []);

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

	const selectedKeys = slip.selectedKeys;
	const togglePick = (pick: Pick) => {
		slip.togglePick(pick);
		setSlipOpenMobile(true);
	};

	const signed = (n: number) => (n > 0 ? `+${n}` : `${n}`);

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

	const betslip = <BetSlipCard slip={slip} balance={wallet.balance} />;

	// ---- Games -----------------------------------------------------------
	const gamesTab =
		board.games.length === 0 ? (
			<p className="text-body-secondary">
				No upcoming games to bet on right now.
			</p>
		) : (
			<div className="table-responsive">
				<table className={TABLE_CLASS} style={{ maxWidth: 720 }}>
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
							<th style={{ width: 90 }} />
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
									<td style={{ minWidth: 170 }}>
										{rows.map((r) => (
											<div
												key={r.t.tid}
												className="d-flex align-items-center gap-2 py-1"
											>
												<Logo tid={r.t.tid} />
												<div className="lh-sm">
													{teamLink(r.t.tid, `${r.t.region} ${r.t.name}`)}
													<div className="text-body-secondary small">
														{r.t.won}-{r.t.lost}
														{r.t.ats ? ` (${r.t.ats} ATS)` : ""}
													</div>
												</div>
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
									<td className="text-end align-middle">
										<a
											className="btn btn-sm btn-light-bordered"
											href={helpers.leagueUrl(["sportsbook", "game", game.gid])}
										>
											Props
										</a>
									</td>
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
		<div className="card mb-3" key={keyPrefix} style={{ maxWidth: 420 }}>
			<div className="card-header py-2 fw-bold">{heading}</div>
			<div className="card-body p-2">
				<div
					className="table-responsive"
					style={{ maxHeight: 360, overflowY: "auto" }}
				>
					<table className={TABLE_CLASS} style={{ marginBottom: 0 }}>
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
			</div>
		</div>
	);

	const futuresTab = (
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
												const key =
													side === "over" ? `wto-${t.tid}` : `wtu-${t.tid}`;
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
	);

	// ---- Awards ------------------------------------------------------------
	// A player-vs-a-role market ("will X win the trophy" / "will X make the
	// team"), as opposed to the team futures above. Shared table shape for all
	// of them: single-winner races (MVP, etc.), the binary All-Star/All-Rookie
	// "makes it" props, and each tier of All-League/All-Defensive.
	const playerCandidateTable = (
		heading: string,
		candidates: {
			pid: number;
			name: string;
			tid: number;
			abbrev: string;
			americanOdds: number;
		}[],
		mk: (pid: number) => SportsbookMarket,
		keyPrefix: string,
	) => (
		<div key={keyPrefix} className="col-lg-4 col-md-6 mb-3">
			<div className="card h-100">
				<div className="card-header py-2 fw-bold">{heading}</div>
				<div className="card-body p-2">
					{candidates.length === 0 ? (
						<p className="text-body-secondary small mb-0">No candidates.</p>
					) : (
						<div style={{ maxHeight: 360, overflowY: "auto" }}>
							<table className={TABLE_CLASS} style={{ marginBottom: 0 }}>
								<tbody>
									{candidates.map((c) => {
										const key = `${keyPrefix}-${c.pid}`;
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
																market: mk(c.pid),
																odds: c.americanOdds,
																title: `${c.name} — ${heading}`,
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
					)}
				</div>
			</div>
		</div>
	);

	const hasAnyAwardMarket =
		board.awards.length > 0 ||
		board.allStar.length > 0 ||
		board.allLeague.some((t) => t.candidates.length > 0) ||
		board.allDefensive.some((t) => t.candidates.length > 0) ||
		board.allRookie.length > 0;

	const awardsTab = !hasAnyAwardMarket ? (
		<p className="text-body-secondary">
			{phase >= PHASE.PLAYOFFS
				? "Award futures are closed for the season — they lock when the regular season ends."
				: "No award futures available right now — check back once the season's underway."}
		</p>
	) : (
		<>
			{board.awards.length > 0 ? (
				<>
					<h2 className="h4">Player Awards</h2>
					<div className="row">
						{board.awards.map((race) =>
							playerCandidateTable(
								race.name,
								race.candidates,
								(pid) => ({
									type: "award",
									award: race.award,
									pid,
									season,
								}),
								`aw-${race.award}`,
							),
						)}
					</div>
				</>
			) : null}

			{board.allStar.length > 0 ? (
				<>
					<h2 className="h4 mt-2">All-Star Team</h2>
					<div className="row">
						{playerCandidateTable(
							"Makes the All-Star Team",
							board.allStar,
							(pid) => ({ type: "allStarTeam", pid, season }),
							"as",
						)}
					</div>
				</>
			) : null}

			{board.allLeague.some((t) => t.candidates.length > 0) ? (
				<>
					<h2 className="h4 mt-2">All-League Team</h2>
					<div className="row">
						{board.allLeague.map((t) =>
							playerCandidateTable(
								t.title,
								t.candidates,
								(pid) => ({
									type: "allLeagueTeam",
									pid,
									tier: t.tier as 1 | 2 | 3,
									season,
								}),
								`al-${t.tier}`,
							),
						)}
					</div>
				</>
			) : null}

			{board.allDefensive.some((t) => t.candidates.length > 0) ? (
				<>
					<h2 className="h4 mt-2">All-Defensive Team</h2>
					<div className="row">
						{board.allDefensive.map((t) =>
							playerCandidateTable(
								t.title,
								t.candidates,
								(pid) => ({
									type: "allDefensiveTeam",
									pid,
									tier: t.tier as 1 | 2 | 3,
									season,
								}),
								`ad-${t.tier}`,
							),
						)}
					</div>
				</>
			) : null}

			{board.allRookie.length > 0 ? (
				<>
					<h2 className="h4 mt-2">All-Rookie Team</h2>
					<div className="row">
						{playerCandidateTable(
							"Makes the All-Rookie Team",
							board.allRookie,
							(pid) => ({ type: "allRookieTeam", pid, season }),
							"ar",
						)}
					</div>
				</>
			) : null}
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
		) : r === "void" ? (
			<span
				className="badge text-bg-secondary"
				title="Refunded — this market could no longer be resolved"
			>
				Void
			</span>
		) : (
			<span className="badge text-bg-warning">Open</span>
		);

	// A small per-leg outcome tag (nothing while the leg is still open).
	const legBadge = (r?: string) =>
		r === "won" ? (
			<span className="badge text-bg-success">✓</span>
		) : r === "lost" ? (
			<span className="badge text-bg-danger">✗</span>
		) : r === "push" || r === "void" ? (
			<span className="badge text-bg-secondary">
				{r === "push" ? "Push" : "Void"}
			</span>
		) : null;

	// The box score of the game a market is about, when it's been played (the
	// worker only sends links for played games, so open/future bets don't link).
	const boxScoreHref = (gid?: number) => {
		if (gid === undefined) {
			return undefined;
		}
		const link = gameLinks[gid];
		return link
			? helpers.leagueUrl(["game_log", link.abbrevTid, link.season, gid])
			: undefined;
	};

	// The "Bet" cell: a straight bet links to its game's box score; a parlay
	// lists each leg, every leg linking to its own game and showing how it landed.
	const BetCell = ({ bet }: { bet: (typeof wallet.bets)[number] }) => {
		if (bet.legs && bet.legs.length > 0) {
			return (
				<div>
					<div className="fw-medium">{bet.label}</div>
					<div className="ps-2 mt-1 d-flex flex-column gap-1">
						{bet.legs.map((leg, i) => {
							const href = boxScoreHref(marketGid(leg.market));
							return (
								<div
									key={i}
									className="small d-flex align-items-center gap-1 flex-wrap"
								>
									<span className="text-body-secondary">
										{formatAmerican(leg.americanOdds)}
									</span>
									{href ? (
										<a href={href}>{leg.label}</a>
									) : (
										<span>{leg.label}</span>
									)}
									{legBadge(leg.result)}
								</div>
							);
						})}
					</div>
				</div>
			);
		}
		const href = boxScoreHref(marketGid(bet.market));
		return href ? <a href={href}>{bet.label}</a> : <span>{bet.label}</span>;
	};

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
							<td>
								<BetCell bet={bet} />
							</td>
							<td className="text-end">{formatAmerican(bet.americanOdds)}</td>
							<td className="text-end">{formatSportsbookMoney(bet.stake)}</td>
							<td className="text-end">
								{open
									? formatSportsbookMoney(bet.stake * (bet.decimalOdds - 1))
									: bet.result === "won"
										? formatSportsbookMoney(bet.stake * bet.decimalOdds)
										: bet.result === "push" || bet.result === "void"
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

	// Every user team's slips - league-mates can see what each other has riding.
	const leagueBetsTab = (
		<>
			{leagueBets.map((team) => (
				<div key={team.tid} className="card mb-3" style={{ maxWidth: 720 }}>
					<div className="card-header py-2 d-flex align-items-center gap-2">
						<Logo tid={team.tid} size={20} />
						<span className="fw-bold">{teamName(team.tid)}</span>
						{team.tid === wallet.tid ? (
							<span className="badge text-bg-primary">You</span>
						) : null}
						<span className="ms-auto text-body-secondary">
							{formatSportsbookMoneyFull(team.balance)}
						</span>
					</div>
					<div className="card-body p-2">
						{team.open.length === 0 && team.settled.length === 0 ? (
							<p className="text-body-secondary small mb-0">No bets yet.</p>
						) : (
							<div className="table-responsive">
								<table className={TABLE_CLASS} style={{ marginBottom: 0 }}>
									<thead>
										<tr className="text-body-secondary small">
											<th>Bet</th>
											<th className="text-end">Odds</th>
											<th className="text-end">Stake</th>
											<th className="text-end">To win / Paid</th>
											<th className="text-center">Result</th>
										</tr>
									</thead>
									<tbody>
										{[...team.open, ...team.settled].map((bet) => (
											<tr key={`${bet.betID}-${bet.result ?? "open"}`}>
												<td>
													<BetCell bet={bet} />
												</td>
												<td className="text-end">
													{formatAmerican(bet.americanOdds)}
												</td>
												<td className="text-end">
													{formatSportsbookMoney(bet.stake)}
												</td>
												<td className="text-end">
													{bet.result === undefined
														? formatSportsbookMoney(
																bet.stake * (bet.decimalOdds - 1),
															)
														: bet.result === "won"
															? formatSportsbookMoney(
																	bet.stake * bet.decimalOdds,
																)
															: bet.result === "push" || bet.result === "void"
																? formatSportsbookMoney(bet.stake)
																: "—"}
												</td>
												<td className="text-center">
													{resultBadge(bet.result)}
												</td>
											</tr>
										))}
									</tbody>
								</table>
							</div>
						)}
					</div>
				</div>
			))}
		</>
	);

	return (
		<>
			<div className="mb-3">
				<div className="text-body-secondary small">
					{teamName(wallet.tid)} balance
				</div>
				<div className="h3 mb-2">
					{formatSportsbookMoneyFull(wallet.balance)}
				</div>
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
						["awards", "Awards"],
						[
							"mybets",
							`My Bets${wallet.bets.length > 0 ? ` (${wallet.bets.length})` : ""}`,
						],
						["leaguebets", "League Bets"],
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
							: tab === "awards"
								? awardsTab
								: tab === "leaguebets"
									? leagueBetsTab
									: myBetsTab}
				</div>
				<div className="col-lg-4 col-xl-3 d-none d-lg-block">
					<div className="position-sticky" style={{ top: "1rem" }}>
						{betslip}
					</div>
				</div>
			</div>

			{slip.picks.length > 0 ? (
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
							Bet Slip ({slip.picks.length}) ·{" "}
							{slip.totalStake > 0
								? `${formatSportsbookMoney(slip.totalStake)} to win ${formatSportsbookMoney(slip.totalPayout - slip.totalStake)}`
								: "add stake"}
						</button>
					)}
				</div>
			) : null}

			<p className="text-body-secondary small mt-2">
				Play money — completely separate from the real game. Every preseason
				each team gets {formatSportsbookMoney(1_000_000)} more.
			</p>
		</>
	);
};

export default Sportsbook;
